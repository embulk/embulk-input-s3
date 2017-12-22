package org.embulk.input.riak_cs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.embulk.config.Config;
import org.embulk.input.s3.AbstractS3FileInputPlugin;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import static com.amazonaws.services.s3.AmazonS3Client.*;
import static com.amazonaws.util.AwsHostNameUtils.parseRegion;
import static com.amazonaws.util.RuntimeHttpUtils.toUri;

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
    protected AmazonS3 newS3Client(PluginTask task)
    {
        RiakCsPluginTask t = (RiakCsPluginTask) task;

        return super
                .defaultS3ClientBuilder(task)
                .withEndpointConfiguration(new EndpointConfiguration(
                        t.getEndpoint(),
                        // Although client will treat endpoint's region as the signer region
                        // if we left this as null, but such that behaviour is undocumented,
                        // so it is explicitly calculated here for future-proofing.
                        parseRegion(
                                toUri(t.getEndpoint(), getClientConfiguration(task)).getHost(),
                                S3_SERVICE_NAME)))
                .build();
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
