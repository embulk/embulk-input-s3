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

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class S3FileInputPlugin
        extends AbstractS3FileInputPlugin
{
    public interface S3PluginTask
            extends PluginTask
    {
        @Config("endpoint")
        @ConfigDefault("null")
        Optional<String> getEndpoint();

        @Config("region")
        @ConfigDefault("null")
        Optional<String> getRegion();
    }

    private static final Logger log = LoggerFactory.getLogger(S3FileInputPlugin.class);

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return S3PluginTask.class;
    }

    @Override
    protected AmazonS3 newS3Client(PluginTask task)
    {
        S3PluginTask t = (S3PluginTask) task;
        Optional<String> endpoint = t.getEndpoint();
        Optional<String> region = t.getRegion();

        AmazonS3ClientBuilder builder = super.defaultS3ClientBuilder(t);

        // Favor the `endpoint` configuration, then `region`, if both are absent then `s3.amazonaws.com` will be used.
        if (endpoint.isPresent()) {
            if (region.isPresent()) {
                log.warn("Either configure endpoint or region, " +
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

        return builder.build();
    }
}
