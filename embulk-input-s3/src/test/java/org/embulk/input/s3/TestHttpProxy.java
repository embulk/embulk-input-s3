/*
 * Copyright 2017 The Embulk project
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

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.s3.S3FileInputPlugin.S3PluginTask;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHttpProxy
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;

    @Before
    public void createResources()
    {
        config = runtime.getExec().newConfigSource();
        setupS3Config(config);
    }

    @Test
    public void checkDefaultHttpProxy()
    {
        ConfigSource conf = config.deepCopy();
        setupS3Config(conf);
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final S3PluginTask task = configMapper.map(config, S3PluginTask.class);
        assertTrue(!task.getHttpProxy().isPresent());
    }

    @Test
    public void checkHttpProxy()
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        { // specify host
            String host = "my_host";
            ConfigSource conf = config.deepCopy().set("host", host);
            final HttpProxy httpProxy = configMapper.map(conf, HttpProxy.class);
            assertHttpProxy(host, Optional.empty(), true, Optional.empty(), Optional.empty(),
                    httpProxy);
        }

        { // specify https=true explicitly
            String host = "my_host";
            ConfigSource conf = config.deepCopy()
                    .set("host", host)
                    .set("https", true);
            final HttpProxy httpProxy = configMapper.map(conf, HttpProxy.class);
            assertHttpProxy(host, Optional.empty(), true, Optional.empty(), Optional.empty(),
                    httpProxy);
        }

        { // specify https=false
            String host = "my_host";
            ConfigSource conf = config.deepCopy()
                    .set("host", host)
                    .set("https", false);
            final HttpProxy httpProxy = configMapper.map(conf, HttpProxy.class);
            assertHttpProxy(host, Optional.empty(), false, Optional.empty(), Optional.empty(),
                    httpProxy);
        }

        { // specify host, port
            String host = "my_host";
            int port = 8080;
            ConfigSource conf = config.deepCopy()
                    .set("host", host)
                    .set("port", port);
            final HttpProxy httpProxy = configMapper.map(conf, HttpProxy.class);
            assertHttpProxy(host, Optional.of(port), true, Optional.empty(), Optional.empty(),
                    httpProxy);
        }

        { // specify host, port, user, password
            String host = "my_host";
            int port = 8080;
            String user = "my_user";
            String password = "my_pass";
            ConfigSource conf = config.deepCopy()
                    .set("host", host)
                    .set("port", port)
                    .set("user", user)
                    .set("password", password);
            final HttpProxy httpProxy = configMapper.map(conf, HttpProxy.class);
            assertHttpProxy(host, Optional.of(port), true, Optional.of(user), Optional.of(password),
                    httpProxy);
        }
    }

    private static void setupS3Config(ConfigSource config)
    {
        config.set("bucket", "my_bucket").set("path_prefix", "my_path_prefix");
    }

    private static void assertHttpProxy(String host, Optional<Integer> port, boolean https, Optional<String> user, Optional<String> password,
            HttpProxy actual)
    {
        assertEquals(host, actual.getHost());
        assertEquals(port.isPresent(), actual.getPort().isPresent());
        if (port.isPresent()) {
            assertEquals(port.get(), actual.getPort().get());
        }

        assertEquals(https, actual.getHttps());

        assertEquals(user.isPresent(), actual.getUser().isPresent());
        if (user.isPresent()) {
            assertEquals(user.get(), actual.getUser().get());
        }
        assertEquals(password.isPresent(), actual.getPassword().isPresent());
        if (password.isPresent()) {
            assertEquals(password.get(), actual.getPassword().get());
        }
    }

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
}
