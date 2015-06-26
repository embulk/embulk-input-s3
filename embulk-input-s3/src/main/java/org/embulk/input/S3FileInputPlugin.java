package org.embulk.input;

import com.google.common.base.Optional;
import com.amazonaws.services.s3.AmazonS3Client;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.s3.AbstractS3FileInputPlugin;

public class S3FileInputPlugin
        extends AbstractS3FileInputPlugin
{
    public interface S3PluginTask
            extends PluginTask
    {
        @Config("endpoint")
        @ConfigDefault("null")
        public Optional<String> getEndpoint();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return S3PluginTask.class;
    }

    @Override
    protected AmazonS3Client newS3Client(PluginTask task)
    {
        S3PluginTask t = (S3PluginTask) task;

        AmazonS3Client client = super.newS3Client(t);

        if (t.getEndpoint().isPresent()) {
            client.setEndpoint(t.getEndpoint().get());
        }

        return client;
    }
}
