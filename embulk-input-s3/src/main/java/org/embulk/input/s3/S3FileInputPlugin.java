package org.embulk.input.s3;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;

import static com.amazonaws.services.s3.AmazonS3Client.S3_SERVICE_NAME;
import static com.amazonaws.util.AwsHostNameUtils.parseRegion;
import static com.amazonaws.util.RuntimeHttpUtils.toUri;

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
    protected AmazonS3 newS3Client(PluginTask task)
    {
        S3PluginTask t = (S3PluginTask) task;

        AmazonS3ClientBuilder builder = super.defaultS3ClientBuilder(t);

        if (t.getEndpoint().isPresent()) {
            String endpoint = t.getEndpoint().get();
            builder.setEndpointConfiguration(new EndpointConfiguration(
                    endpoint,
                    // Although client will treat endpoint's region as the signer region
                    // if we left this as null, but such that behaviour is undocumented,
                    // so it is explicitly calculated here for future-proofing.
                    parseRegion(
                            toUri(endpoint, getClientConfiguration(task)).getHost(),
                            S3_SERVICE_NAME)));
        }

        return builder.build();
    }
}
