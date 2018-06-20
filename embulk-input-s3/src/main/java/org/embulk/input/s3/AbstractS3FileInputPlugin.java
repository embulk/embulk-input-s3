package org.embulk.input.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.embulk.util.aws.credentials.AwsCredentials;
import org.embulk.util.aws.credentials.AwsCredentialsTask;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.List;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public abstract class AbstractS3FileInputPlugin
        implements FileInputPlugin
{
    private static final Logger LOGGER = Exec.getLogger(S3FileInputPlugin.class);
    private static final int INITIAL_RETRY_INTERVAL_MILLIS = 500;
    private static final int MAXIMUM_RETRY_INTERVAL_MILLIS = 30 * 1000;
    private static final int MAXIMUM_RETRIES = 3;

    public interface PluginTask
            extends AwsCredentialsTask, FileList.Task, Task
    {
        @Config("bucket")
        public String getBucket();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("last_path")
        @ConfigDefault("null")
        public Optional<String> getLastPath();

        @Config("access_key_id")
        @ConfigDefault("null")
        public Optional<String> getAccessKeyId();

        @Config("http_proxy")
        @ConfigDefault("null")
        public Optional<HttpProxy> getHttpProxy();

        public void setHttpProxy(Optional<HttpProxy> httpProxy);

        @Config("incremental")
        @ConfigDefault("true")
        public boolean getIncremental();

        @Config("skip_glacier_objects")
        @ConfigDefault("false")
        public boolean getSkipGlacierObjects();

        @Config("max_retry")
        @ConfigDefault("7")
        public int getMaxRetry();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("500")
        public int getInitRetryIntervalMillis();

        @Config("max_retry_interval_millis")
        @ConfigDefault("30000")
        public int getMaxRetryIntervalMillis();

        // TODO timeout, ssl, etc

        public FileList getFiles();

        public void setFiles(FileList files);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    protected abstract Class<? extends PluginTask> getTaskClass();

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(getTaskClass());

        // list files recursively
        task.setFiles(listFiles(task));

        // number of processors is same with number of files
        return resume(task.dump(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());

        // validate task
        newS3Client(task);

        control.run(taskSource, taskCount);

        // build next config
        ConfigDiff configDiff = Exec.newConfigDiff();

        // last_path
        if (task.getIncremental()) {
            Optional<String> lastPath = task.getFiles().getLastPath(task.getLastPath());
            LOGGER.info("Incremental job, setting last_path to [{}]", lastPath.orNull());
            configDiff.set("last_path", lastPath);
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

    /**
     * Provide an overridable default client.
     * Since this returns an immutable object, it is not for any further customizations by mutating,
     * e.g., {@link AmazonS3#setEndpoint} will throw a runtime {@link UnsupportedOperationException}
     * Subclass's customization should be done through {@link AbstractS3FileInputPlugin#defaultS3ClientBuilder}.
     * @param task Embulk plugin task
     * @return AmazonS3
     */
    protected AmazonS3 newS3Client(PluginTask task)
    {
        return defaultS3ClientBuilder(task).build();
    }

    /**
     * A base builder for the subclasses to then customize.builder
     * @param task Embulk plugin
     * @return AmazonS3 client b
     **/
    protected AmazonS3ClientBuilder defaultS3ClientBuilder(PluginTask task)
    {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(getCredentialsProvider(task))
                .withClientConfiguration(getClientConfiguration(task));
    }

    protected AWSCredentialsProvider getCredentialsProvider(PluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    protected ClientConfiguration getClientConfiguration(PluginTask task)
    {
        ClientConfiguration clientConfig = new ClientConfiguration();

        //clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxConnections(50); // SDK default: 50
        clientConfig.setMaxErrorRetry(3); // SDK default: 3
        clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000

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

    private FileList listFiles(PluginTask task)
    {
        LOGGER.info("Start listing file with prefix [{}]", task.getPathPrefix());
        try {
            AmazonS3 client = newS3Client(task);
            String bucketName = task.getBucket();

            if (task.getPathPrefix().equals("/")) {
                LOGGER.info("Listing files with prefix \"/\". This doesn't mean all files in a bucket. If you intend to read all files, use \"path_prefix: ''\" (empty string) instead.");
            }

            FileList.Builder builder = new FileList.Builder(task);
            listS3FilesByPrefix(builder, client, bucketName,
                    task.getPathPrefix(), task.getLastPath(), task.getSkipGlacierObjects(), task.getMaxRetry(), task.getInitRetryIntervalMillis(), task.getMaxRetryIntervalMillis());
            LOGGER.info("Found total [{}] files", builder.size());
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
        catch (InterruptedException | RetryGiveupException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Lists S3 filenames filtered by prefix.
     * <p>
     * The resulting list does not include the file that's size == 0.
     * @param builder custom Filelist builder
     * @param client Amazon S3
     * @param bucketName Amazon S3 bucket name
     * @param prefix Amazon S3 bucket name prefix
     * @param lastPath last path
     * @param skipGlacierObjects skip gracier objects
     * @param maxRetry maximum number of retry
     * @param initRetryIntervalMillis initial time of retry interval in millisecond
     * @param maxRetryIntervalMillis maximum time of retry interval in millisecond
     * @throws RetryGiveupException error when retrying
     * @throws InterruptedException error when retrying
     */
    public static void listS3FilesByPrefix(FileList.Builder builder,
                                           final AmazonS3 client, final String bucketName,
                                           final String prefix, Optional<String> lastPath, boolean skipGlacierObjects, int maxRetry, int initRetryIntervalMillis, int maxRetryIntervalMillis) throws RetryGiveupException, InterruptedException
    {
        String lastKey = lastPath.orNull();
        do {
            final String finalLastKey = lastKey;
            Optional<ObjectListing> optOl = S3FileInputUtils.executeWithRetry(maxRetry, initRetryIntervalMillis, maxRetryIntervalMillis, new S3FileInputUtils.AlwaysRetryRetryable<Optional<ObjectListing>>()
            {
                @Override
                public Optional<ObjectListing> call() throws AmazonServiceException
                {
                    ListObjectsRequest req = new ListObjectsRequest(bucketName, prefix, finalLastKey, null, 1024);
                    ObjectListing ol = client.listObjects(req);
                    return Optional.of(ol);
                }
                @Override
                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                        throws RetryGiveupException
                {
                    String message = String.format("S3 GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                            retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                    if (retryCount % retryLimit == 0) {
                        LOGGER.warn(message, exception);
                    }
                    else {
                        LOGGER.warn(message);
                    }
                }
            });
            if (!optOl.isPresent()) {
                break;
            }
            ObjectListing ol = optOl.get();
            for (S3ObjectSummary s : ol.getObjectSummaries()) {
                if (s.getStorageClass().equals(StorageClass.Glacier.toString())) {
                    if (skipGlacierObjects) {
                        Exec.getLogger("AbstractS3FileInputPlugin.class").warn("Skipped \"s3://{}/{}\" that stored at Glacier.", bucketName, s.getKey());
                        continue;
                    }
                    else {
                        throw new ConfigException("Detected an object stored at Glacier. Set \"skip_glacier_objects\" option to \"true\" to skip this.");
                    }
                }
                if (s.getSize() > 0) {
                    builder.add(s.getKey(), s.getSize());
                    if (!builder.needsMore()) {
                        LOGGER.warn("Too many files matched, stop listing file");
                        return;
                    }
                }
            }
            lastKey = ol.getNextMarker();
        } while (lastKey != null);
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());
        return new S3FileInput(task, taskIndex);
    }

    @VisibleForTesting
    static class S3InputStreamReopener
            implements ResumableInputStream.Reopener
    {
        private final Logger log = Exec.getLogger(S3InputStreamReopener.class);

        private final AmazonS3 client;
        private final GetObjectRequest request;
        private final long contentLength;

        public S3InputStreamReopener(AmazonS3 client, GetObjectRequest request, long contentLength)
        {
            this.client = client;
            this.request = request;
            this.contentLength = contentLength;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException
        {
            try {
                return S3FileInputUtils.executeWithRetry(MAXIMUM_RETRIES, INITIAL_RETRY_INTERVAL_MILLIS, MAXIMUM_RETRY_INTERVAL_MILLIS, new S3FileInputUtils.AlwaysRetryRetryable<InputStream>()
                {
                    @Override
                    public InputStream call() throws InterruptedIOException
                    {
                        log.warn(String.format("S3 read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
                        request.setRange(offset, contentLength - 1);  // [first, last]
                        return client.getObject(request).getObjectContent();
                    }
                    @Override
                    public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                            throws RetryGiveupException
                    {
                        String message = String.format("S3 GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                        if (retryCount % 3 == 0) {
                            log.warn(message, exception);
                        }
                        else {
                            log.warn(message);
                        }
                    }
                });
            }
            catch (RetryGiveupException ex) {
                Throwables.propagateIfInstanceOf(ex.getCause(), IOException.class);
                throw Throwables.propagate(ex.getCause());
            }
            catch (InterruptedException ex) {
                throw new InterruptedIOException();
            }
        }
    }

    public class S3FileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public S3FileInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort()
        {
        }

        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @Override
        public void close()
        {
        }
    }

    // TODO create single-file InputStreamFileInput utility
    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private AmazonS3 client;
        private final String bucket;
        private final Iterator<String> iterator;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newS3Client(task);
            this.bucket = task.getBucket();
            this.iterator = task.getFiles().get(taskIndex).iterator();
        }

        @Override
        public InputStream openNext() throws IOException
        {
            if (!iterator.hasNext()) {
                return null;
            }
            String key = iterator.next();
            GetObjectRequest request = new GetObjectRequest(bucket, key);
            S3Object obj = client.getObject(request);
            long objectSize = obj.getObjectMetadata().getContentLength();
            LOGGER.info("Open S3Object with bucket [{}], key [{}], with size [{}]", bucket, key, objectSize);
            return new ResumableInputStream(obj.getObjectContent(), new S3InputStreamReopener(client, request, objectSize));
        }

        @Override
        public void close()
        {
        }
    }
}
