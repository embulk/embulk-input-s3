package org.embulk.input;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import org.embulk.config.Config;
import org.embulk.config.ConfigInject;
import org.embulk.input.s3.AbstractS3FileInputPlugin;

public class RiakCsFileInputPlugin
        extends AbstractS3FileInputPlugin
{
    public interface RiakCsPluginTask
            extends PluginTask
    {
        @Config("endpoint")
        public String getEndpoint();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return RiakCsPluginTask.class;
    }

    @Override
    protected AmazonS3Client newS3Client(PluginTask task)
    {
        RiakCsPluginTask t = (RiakCsPluginTask) task;

        AmazonS3Client client = super.newS3Client(t);

        client.setEndpoint(t.getEndpoint());

        return client;
    }

    @Override
    protected ClientConfiguration getClientConfiguration(PluginTask task)
    {
        RiakCsPluginTask t = (RiakCsPluginTask) task;

        ClientConfiguration config = super.getClientConfiguration(t);
        config.setSignerOverride("S3SignerType");

        return config;
    }
}
