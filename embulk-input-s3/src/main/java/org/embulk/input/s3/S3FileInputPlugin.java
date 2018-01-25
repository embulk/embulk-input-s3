package org.embulk.input.s3;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class S3FileInputPlugin
        extends AbstractS3FileInputPlugin
{
    public interface S3PluginTask
            extends PluginTask
    {
        @Config("endpoint")
        @ConfigDefault("null")
        public Optional<String> getEndpoint();

        @Config("region")
        @ConfigDefault("null")
        public Optional<String> getRegion();
    }

    private static final Logger log = Exec.getLogger(S3FileInputPlugin.class);

    @Override
    protected Class<? extends PluginTask> getTaskClass() {
        return S3PluginTask.class;
    }

    @Override
    protected AmazonS3 newS3Client(PluginTask task)
    {
        S3PluginTask t = (S3PluginTask) task;
        Optional<String> endpoint = t.getEndpoint();
        Optional<String> region = t.getRegion();

        AmazonS3ClientBuilder builder = super.defaultS3ClientBuilder(t);

        if (endpoint.isPresent()) {
            if (region.isPresent()) {
                log.warn("Either configure endpoint or region, " +
                        "if both is specified only the endpoint will be in effect.");
            }
            builder.setEndpointConfiguration(new EndpointConfiguration(endpoint.get(), null));
        } else if (region.isPresent()) {
            builder.setRegion(region.get());
        } else {
            builder.setEndpointConfiguration(new EndpointConfiguration("s3.amazonaws.com", null));
        }

        return builder.build();
    }
}
