package org.embulk.input.s3;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.InputStream;

import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.*;
import org.slf4j.Logger;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.ClientConfiguration;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public abstract class AbstractS3FileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
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

        @Config("secret_access_key")
        @ConfigDefault("null")
        public Optional<String> getSecretAccessKey();

        @Config("credential_provider")
        @ConfigDefault("null")
        public Optional<String> getCredentialProvider();

        // TODO timeout, ssl, etc

        // TODO support more options such as STS

        public List<String> getFiles();
        public void setFiles(List<String> files);

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
        return resume(task.dump(), task.getFiles().size(), control);
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
        if (task.getFiles().isEmpty()) {
            // keep the last value
            if (task.getLastPath().isPresent()) {
                configDiff.set("last_path", task.getLastPath().get());
            }
        } else {
            List<String> files = new ArrayList<String>(task.getFiles());
            Collections.sort(files);
            configDiff.set("last_path", files.get(files.size() - 1));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<CommitReport> successCommitReports)
    {
        // do nothing
    }

    protected AmazonS3Client newS3Client(PluginTask task)
    {
        return new AmazonS3Client(getCredentialsProvider(task), getClientConfiguration(task));
    }

    protected AWSCredentialsProvider getCredentialsProvider(PluginTask task)
    {
        if(task.getCredentialProvider().isPresent()){
            final Optional<AWSCredentialProviders> provider = AWSCredentialProviders.fromName(task.getCredentialProvider().get());
            return provider.isPresent() ? provider.get().createProvider(): fallbackToStaticProvider(task);
        }else{
            return fallbackToStaticProvider(task);
        }
    }

    private AWSCredentialsProvider fallbackToStaticProvider(PluginTask task){

        if(!task.getSecretAccessKey().isPresent()){
            throw new ConfigException("Attribute access_key_id is required but not set");
        }

        if(!task.getAccessKeyId().isPresent()){
            throw new ConfigException("Attribute secret_access_key is required but not set");
        }

        final AWSCredentials cred = new BasicAWSCredentials(task.getAccessKeyId().get(), task.getSecretAccessKey().get());
        return new StaticCredentialsProvider(cred);
    }

    protected ClientConfiguration getClientConfiguration(PluginTask task)
    {
        ClientConfiguration clientConfig = new ClientConfiguration();

        //clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxConnections(50); // SDK default: 50
        clientConfig.setMaxErrorRetry(3); // SDK default: 3
        clientConfig.setSocketTimeout(8*60*1000); // SDK default: 50*1000

        return clientConfig;
    }

    private List<String> listFiles(PluginTask task)
    {
        AmazonS3Client client = newS3Client(task);
        String bucketName = task.getBucket();

        return listS3FilesByPrefix(client, bucketName, task.getPathPrefix(), task.getLastPath());
    }

    /**
     * Lists S3 filenames filtered by prefix.
     *
     * The resulting list does not include the file that's size == 0.
     */
    public static List<String> listS3FilesByPrefix(AmazonS3Client client, String bucketName,
                                                   String prefix, Optional<String> lastPath)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        String lastKey = lastPath.orNull();
        do {
            ListObjectsRequest req = new ListObjectsRequest(bucketName, prefix, lastKey, null, 1024);
            ObjectListing ol = client.listObjects(req);
            for(S3ObjectSummary s : ol.getObjectSummaries()) {
                if (s.getSize() > 0) {
                    builder.add(s.getKey());
                }
            }
            lastKey = ol.getNextMarker();
        } while(lastKey != null);

        return builder.build();
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());
        return new S3FileInput(task, taskIndex);
    }

    private static class S3InputStreamReopener
            implements ResumableInputStream.Reopener
    {
        private final Logger log = Exec.getLogger(S3InputStreamReopener.class);

        private final AmazonS3Client client;
        private final GetObjectRequest request;
        private final long contentLength;

        public S3InputStreamReopener(AmazonS3Client client, GetObjectRequest request, long contentLength)
        {
            this.client = client;
            this.request = request;
            this.contentLength = contentLength;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException
        {
            try {
                return retryExecutor()
                        .withRetryLimit(3)
                        .withInitialRetryWait(500)
                        .withMaxRetryWait(30*1000)
                        .runInterruptible(new Retryable<InputStream>() {
                            @Override
                            public InputStream call() throws InterruptedIOException
                            {
                                log.warn(String.format("S3 read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
                                request.setRange(offset, contentLength - 1);  // [first, last]
                                return client.getObject(request).getObjectContent();
                            }

                            @Override
                            public boolean isRetryableException(Exception exception)
                            {
                                return true;  // TODO
                            }

                            @Override
                            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                    throws RetryGiveupException
                            {
                                String message = String.format("S3 GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                        retryCount, retryLimit, retryWait/1000, exception.getMessage());
                                if (retryCount % 3 == 0) {
                                    log.warn(message, exception);
                                } else {
                                    log.warn(message);
                                }
                            }

                            @Override
                            public void onGiveup(Exception firstException, Exception lastException)
                                    throws RetryGiveupException
                            {
                            }
                        });
            } catch (RetryGiveupException ex) {
                Throwables.propagateIfInstanceOf(ex.getCause(), IOException.class);
                throw Throwables.propagate(ex.getCause());
            } catch (InterruptedException ex) {
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

        public void abort() { }

        public CommitReport commit()
        {
            return Exec.newCommitReport();
        }

        @Override
        public void close() { }
    }

    // TODO create single-file InputStreamFileInput utility
    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private AmazonS3Client client;
        private final String bucket;
        private final String key;
        private boolean opened = false;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newS3Client(task);
            this.bucket = task.getBucket();
            this.key = task.getFiles().get(taskIndex);
        }

        @Override
        public InputStream openNext() throws IOException
        {
            if (opened) {
                return null;
            }
            opened = true;
            GetObjectRequest request = new GetObjectRequest(bucket, key);
            S3Object obj = client.getObject(request);
            return new ResumableInputStream(obj.getObjectContent(), new S3InputStreamReopener(client, request, obj.getObjectMetadata().getContentLength()));
        }

        @Override
        public void close() { }
    }
}
