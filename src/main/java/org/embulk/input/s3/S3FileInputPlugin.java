/*
 * Copyright 2015 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.input.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.s3.explorer.S3NameOrderPrefixFileExplorer;
import org.embulk.input.s3.explorer.S3SingleFileExplorer;
import org.embulk.input.s3.explorer.S3TimeOrderPrefixFileExplorer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.aws.credentials.AwsCredentials;
import org.embulk.util.aws.credentials.AwsCredentialsTask;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.file.ResumableInputStream;
import org.embulk.util.retryhelper.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class S3FileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends AwsCredentialsTask, FileList.Task, RetrySupportPluginTask, Task
    {
        @Config("bucket")
        String getBucket();

        @Config("path_prefix")
        @ConfigDefault("null")
        Optional<String> getPathPrefix();

        @Config("path")
        @ConfigDefault("null")
        Optional<String> getPath();

        @Config("last_path")
        @ConfigDefault("null")
        Optional<String> getLastPath();

        @Config("access_key_id")
        @ConfigDefault("null")
        Optional<String> getAccessKeyId();

        @Config("http_proxy")
        @ConfigDefault("null")
        Optional<HttpProxy> getHttpProxy();
        void setHttpProxy(Optional<HttpProxy> httpProxy);

        @Config("incremental")
        @ConfigDefault("true")
        boolean getIncremental();

        @Config("skip_glacier_objects")
        @ConfigDefault("false")
        boolean getSkipGlacierObjects();

        @Config("use_modified_time")
        @ConfigDefault("false")
        boolean getUseModifiedTime();

        @Config("last_modified_time")
        @ConfigDefault("null")
        Optional<String> getLastModifiedTime();

        // TODO timeout, ssl, etc

        ////////////////////////////////////////
        // Internal configurations
        ////////////////////////////////////////

        FileList getFiles();
        void setFiles(FileList files);

        /**
         * end_modified_time is conditionally set if modified_time mode is enabled.
         *
         * It is internal state and must not be set in config.yml
         */
        @Config("__end_modified_time")
        @ConfigDefault("null")
        Optional<Date> getEndModifiedTime();
        void setEndModifiedTime(Optional<Date> endModifiedTime);

        @Config("endpoint")
        @ConfigDefault("null")
        Optional<String> getEndpoint();

        @Config("region")
        @ConfigDefault("null")
        Optional<String> getRegion();

        @Config("path_style_access")
        @ConfigDefault("false")
        boolean getPathStyleAccess();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);

        errorIfInternalParamsAreSet(task);
        validateInputTask(task);
        // list files recursively
        task.setFiles(listFiles(task));

        // number of processors is same with number of files
        return resume(task.toTaskSource(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        // validate task
        newS3Client(task);

        control.run(taskSource, taskCount);

        // build next config
        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

        // last_path
        if (task.getIncremental()) {
            if (task.getUseModifiedTime()) {
                Date endModifiedTime = task.getEndModifiedTime().orElse(new Date());
                configDiff.set("last_modified_time", new SimpleDateFormat(FULL_DATE_FORMAT).format(endModifiedTime));
            }
            else {
                Optional<String> lastPath = task.getFiles().getLastPath(task.getLastPath());
                logger.info("Incremental job, setting last_path to [{}]", lastPath.orElse(""));
                configDiff.set("last_path", lastPath);
            }
        }
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports)
    {
        // do nothing
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        return new S3FileInput(task, taskIndex);
    }

    private AWSCredentialsProvider getCredentialsProvider(PluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    private ClientConfiguration getClientConfiguration(PluginTask task)
    {
        ClientConfiguration clientConfig = new ClientConfiguration();

        //clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxConnections(50); // SDK default: 50
//        clientConfig.setMaxErrorRetry(3); // SDK default: 3
        clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000
        clientConfig.setRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY);
        // set http proxy
        if (task.getHttpProxy().isPresent()) {
            setHttpProxyInAwsClient(clientConfig, task.getHttpProxy().get());
        }

        return clientConfig;
    }

    private void setHttpProxyInAwsClient(ClientConfiguration clientConfig, HttpProxy httpProxy)
    {
        // host
        clientConfig.setProxyHost(httpProxy.getHost());

        // port
        if (httpProxy.getPort().isPresent()) {
            clientConfig.setProxyPort(httpProxy.getPort().get());
        }

        // https
        clientConfig.setProtocol(httpProxy.getHttps() ? Protocol.HTTPS : Protocol.HTTP);

        // user
        if (httpProxy.getUser().isPresent()) {
            clientConfig.setProxyUsername(httpProxy.getUser().get());
        }

        // password
        if (httpProxy.getPassword().isPresent()) {
            clientConfig.setProxyPassword(httpProxy.getPassword().get());
        }
    }

    /**
     * Build the common retry executor from some configuration params of plugin task.
     * @param task Plugin task.
     * @return RetryExecutor object
     */
    private static RetryExecutor retryExecutorFrom(RetrySupportPluginTask task)
    {
        return RetryExecutor.builder()
            .withRetryLimit(task.getMaximumRetries())
            .withInitialRetryWaitMillis(task.getInitialRetryIntervalMillis())
            .withMaxRetryWaitMillis(task.getMaximumRetryIntervalMillis())
            .build();
    }

    private FileList listFiles(final PluginTask task)
    {
        try {
            AmazonS3 client = newS3Client(task);
            String bucketName = task.getBucket();
            FileList.Builder builder = new FileList.Builder(task);
            RetryExecutor retryExec = retryExecutorFrom(task);

            if (task.getPath().isPresent()) {
                logger.info("Start getting object with path: [{}]", task.getPath().get());
                new S3SingleFileExplorer(bucketName, client, retryExec, task.getPath().get()).addToBuilder(builder);
                return builder.build();
            }

            // does not need to verify existent path prefix here since there is the validation requires either path or path_prefix
            logger.info("Start listing file with prefix [{}]", task.getPathPrefix().get());
            if (task.getPathPrefix().get().equals("/")) {
                logger.info("Listing files with prefix \"/\". This doesn't mean all files in a bucket. If you intend to read all files, use \"path_prefix: ''\" (empty string) instead.");
            }

            if (task.getUseModifiedTime()) {
                Date now = new Date();
                Optional<Date> from = task.getLastModifiedTime().isPresent()
                        ? Optional.of(parseDate(task.getLastModifiedTime().get()))
                        : Optional.empty();
                task.setEndModifiedTime(Optional.of(now));

                new S3TimeOrderPrefixFileExplorer(bucketName, client, retryExec, task.getPathPrefix().get(),
                        task.getSkipGlacierObjects(), from, now).addToBuilder(builder);
            }
            else {
                new S3NameOrderPrefixFileExplorer(bucketName, client, retryExec, task.getPathPrefix().get(),
                        task.getSkipGlacierObjects(), task.getLastPath().orElse(null)).addToBuilder(builder);
            }

            logger.info("Found total [{}] files", builder.size());
            return builder.build();
        }
        catch (AmazonServiceException ex) {
            if (ex.getErrorType().equals(AmazonServiceException.ErrorType.Client)) {
                // HTTP 40x errors. auth error, bucket doesn't exist, etc. See AWS document for the full list:
                // http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.getStatusCode() != 400   // 404 Bad Request is unexpected error
                        || "ExpiredToken".equalsIgnoreCase(ex.getErrorCode())) { // if statusCode == 400 && errorCode == ExpiredToken => throws ConfigException
                    throw new ConfigException(ex);
                }
            }
            throw ex;
        }
    }

    private void validateInputTask(final PluginTask task)
    {
        if (!task.getPathPrefix().isPresent() && !task.getPath().isPresent()) {
            throw new ConfigException("Either path or path_prefix is required");
        }
    }

    Date parseDate(final String value) {
        try {
            return new SimpleDateFormat(FULL_DATE_FORMAT).parse(value);
        }
        catch (final ParseException e) {
            throw new ConfigException("Unsupported DateTime value: '" + value + "', supported formats: [" + FULL_DATE_FORMAT + "]");
        }
    }

    static class S3InputStreamReopener
            implements ResumableInputStream.Reopener
    {
        private static final Logger log = LoggerFactory.getLogger(S3InputStreamReopener.class);

        private final AmazonS3 client;
        private final GetObjectRequest request;
        private final long contentLength;
        private final RetryExecutor retryExec;

        public S3InputStreamReopener(AmazonS3 client, GetObjectRequest request, long contentLength)
        {
            this(client, request, contentLength, null);
        }

        public S3InputStreamReopener(AmazonS3 client, GetObjectRequest request, long contentLength, RetryExecutor retryExec)
        {
            this.client = client;
            this.request = request;
            this.contentLength = contentLength;
            this.retryExec = retryExec;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException
        {
            log.warn(String.format("S3 read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
            request.setRange(offset, contentLength - 1);  // [first, last]

            return new DefaultRetryable<S3ObjectInputStream>(String.format("Getting object '%s'", request.getKey())) {
                @Override
                public S3ObjectInputStream call()
                {
                    return client.getObject(request).getObjectContent();
                }
            }.executeWithCheckedException(retryExec, IOException.class);
        }
    }

    public class S3FileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public S3FileInput(PluginTask task, int taskIndex)
        {
            super(Exec.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort()
        {
        }

        public TaskReport commit()
        {
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }

        @Override
        public void close()
        {
        }
    }

    static void errorIfInternalParamsAreSet(PluginTask task)
    {
        if (task.getEndModifiedTime().isPresent()) {
            throw new ConfigException("'__end_modified_time' must not be set.");
        }
    }

    // TODO create single-file InputStreamFileInput utility
    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private AmazonS3 client;
        private final String bucket;
        private final Iterator<String> iterator;
        private final RetryExecutor retryExec;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newS3Client(task);
            this.bucket = task.getBucket();
            this.iterator = task.getFiles().get(taskIndex).iterator();
            this.retryExec = retryExecutorFrom(task);
        }

        @Override
        public InputStreamFileInput.InputStreamWithHints openNextWithHints() throws IOException
        {
            if (!iterator.hasNext()) {
                return null;
            }
            final String key = iterator.next();
            final GetObjectRequest request = new GetObjectRequest(bucket, key);

            S3Object object = new DefaultRetryable<S3Object>(String.format("Getting object '%s'", request.getKey())) {
                @Override
                public S3Object call()
                {
                    return client.getObject(request);
                }
            }.executeWithCheckedException(retryExec, IOException.class);

            long objectSize = object.getObjectMetadata().getContentLength();
            // Some plugin users are parsing this output to get file list.
            // Keep it for now but might be removed in the future.
            logger.info("Open S3Object with bucket [{}], key [{}], with size [{}]", bucket, key, objectSize);
            InputStream inputStream = new ResumableInputStream(object.getObjectContent(), new S3InputStreamReopener(client, request, objectSize, retryExec));
            return new InputStreamFileInput.InputStreamWithHints(inputStream, String.format("s3://%s/%s", bucket, key));
        }

        @Override
        public void close()
        {
        }
    }

    private AmazonS3 newS3Client(final PluginTask task)
    {
        Optional<String> endpoint = task.getEndpoint();
        Optional<String> region = task.getRegion();
        Boolean pathStyleAccess = task.getPathStyleAccess();

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
                .standard()
                .withCredentials(getCredentialsProvider(task))
                .withClientConfiguration(getClientConfiguration(task))
                .withPathStyleAccessEnabled(pathStyleAccess);

        // Favor the `endpoint` configuration, then `region`, if both are absent then `s3.amazonaws.com` will be used.
        if (endpoint.isPresent()) {
            if (region.isPresent()) {
                logger.warn("Either configure endpoint or region, " +
                        "if both is specified only the endpoint will be in effect.");
            }
            builder.setEndpointConfiguration(new EndpointConfiguration(endpoint.get(), null));
        }
        else if (region.isPresent()) {
            builder.setRegion(region.get());
        }
        else {
            // This is to keep the AWS SDK upgrading to 1.11.x to be backward compatible with old configuration.
            //
            // On SDK 1.10.x, when neither endpoint nor region is set explicitly, the client's endpoint will be by
            // default `s3.amazonaws.com`. And for pre-Signature-V4, this will work fine as the bucket's region
            // will be resolved to the appropriate region on server (AWS) side.
            //
            // On SDK 1.11.x, a region will be computed on client side by AwsRegionProvider and the endpoint now will
            // be region-specific `<region>.s3.amazonaws.com` and might be the wrong one.
            //
            // So a default endpoint of `s3.amazonaws.com` when both endpoint and region configs are absent are
            // necessary to make old configurations won't suddenly break. The side effect is that this will render
            // AwsRegionProvider useless. And it's worth to note that Signature-V4 won't work with either versions with
            // no explicit region or endpoint as the region (inferrable from endpoint) are necessary for signing.
            builder.setEndpointConfiguration(new EndpointConfiguration("s3.amazonaws.com", null));
        }

        builder.withForceGlobalBucketAccessEnabled(true);
        return builder.build();
    }

    AmazonS3 newS3ClientForTesting(final PluginTask task)
    {
        return this.newS3Client(task);
    }

    private static final Logger logger = LoggerFactory.getLogger(S3FileInputPlugin.class);

    private static final String FULL_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
}
