package org.embulk.input.s3;

import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.InputStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.AmazonServiceException;
import org.embulk.config.Config;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.config.ConfigException;
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
    private final Logger log = Exec.getLogger(S3FileInputPlugin.class);

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

        @Config("client_config")
        @ConfigDefault("{}")
        public ClientConfigurationConfigurable.Task getClientConfigurationConfigurableTask();

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
        configDiff.set("last_path", task.getFiles().getLastPath(task.getLastPath()));

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<TaskReport> successTaskReports)
    {
        // do nothing
    }

    protected AmazonS3Client newS3Client(PluginTask task)
    {
        return new AmazonS3Client(getCredentialsProvider(task), getClientConfiguration(task));
    }

    protected AWSCredentialsProvider getCredentialsProvider(PluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    protected ClientConfiguration getClientConfiguration(PluginTask task)
    {
        ClientConfigurationConfigurable.Task configurableTask = task.getClientConfigurationConfigurableTask();
        return ClientConfigurationConfigurable.getClientConfiguration(configurableTask);
    }

    private FileList listFiles(PluginTask task)
    {
        try {
            AmazonS3Client client = newS3Client(task);
            String bucketName = task.getBucket();

            if (task.getPathPrefix().equals("/")) {
                log.info("Listing files with prefix \"/\". This doesn't mean all files in a bucket. If you intend to read all files, use \"path_prefix: ''\" (empty string) instead.");
            }

            FileList.Builder builder = new FileList.Builder(task);
            listS3FilesByPrefix(builder, client, bucketName,
                    task.getPathPrefix(), task.getLastPath());
            return builder.build();
        }
        catch (AmazonServiceException ex) {
            if (ex.getErrorType().equals(AmazonServiceException.ErrorType.Client)) {
                // HTTP 40x errors. auth error, bucket doesn't exist, etc. See AWS document for the full list:
                // http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.getStatusCode() != 400) {  // 404 Bad Request is unexpected error
                    throw new ConfigException(ex);
                }
            }
            throw ex;
        }
    }

    /**
     * Lists S3 filenames filtered by prefix.
     *
     * The resulting list does not include the file that's size == 0.
     */
    public static void listS3FilesByPrefix(FileList.Builder builder,
            AmazonS3Client client, String bucketName,
            String prefix, Optional<String> lastPath)
    {
        String lastKey = lastPath.orNull();
        do {
            ListObjectsRequest req = new ListObjectsRequest(bucketName, prefix, lastKey, null, 1024);
            ObjectListing ol = client.listObjects(req);
            for (S3ObjectSummary s : ol.getObjectSummaries()) {
                if (s.getSize() > 0) {
                    builder.add(s.getKey(), s.getSize());
                    if (!builder.needsMore()) {
                        return;
                    }
                }
            }
            lastKey = ol.getNextMarker();
        } while(lastKey != null);
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

        public TaskReport commit()
        {
            return Exec.newTaskReport();
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
            GetObjectRequest request = new GetObjectRequest(bucket, iterator.next());
            S3Object obj = client.getObject(request);
            return new ResumableInputStream(obj.getObjectContent(), new S3InputStreamReopener(client, request, obj.getObjectMetadata().getContentLength()));
        }

        @Override
        public void close() { }
    }
}
