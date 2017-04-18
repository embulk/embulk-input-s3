package org.embulk.input.s3;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.s3.S3FileInputPlugin.S3PluginTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        S3PluginTask task = conf.loadConfig(S3PluginTask.class);
        assertTrue(!task.getHttpProxy().isPresent());
    }

    @Test
    public void checkHttpProxy()
    {
        { // specify host
            String host = "my_host";
            Map<String, Object> httpProxyMap = ImmutableMap.<String, Object>of("host", host);
            ConfigSource conf = config.deepCopy().set("http_proxy", httpProxyMap);
            S3PluginTask task = conf.loadConfig(S3PluginTask.class);

            assertHttpProxy(new HttpProxy(host, Optional.<Integer>absent(), false, Optional.<String>absent(), Optional.<String>absent()),
                    task.getHttpProxy().get());
        }

        { // specify host, port, use_ssl
            String host = "my_host";
            int port = 8080;
            boolean useSsl = true;
            Map<String, Object> httpProxyMap = ImmutableMap.<String, Object>of(
                    "host", host,
                    "port", 8080,
                    "https", true);
            ConfigSource conf = config.deepCopy().set("http_proxy", httpProxyMap);
            S3PluginTask task = conf.loadConfig(S3PluginTask.class);

            assertHttpProxy(new HttpProxy(host, Optional.of(port), true, Optional.<String>absent(), Optional.<String>absent()),
                    task.getHttpProxy().get());
        }

        { // specify host, port, use_ssl, user, password
            String host = "my_host";
            int port = 8080;
            boolean useSsl = true;
            String user = "my_user";
            String password = "my_pass";
            Map<String, Object> httpProxyMap = ImmutableMap.<String, Object>of(
                    "host", host,
                    "port", 8080,
                    "https", true,
                    "user", user,
                    "password", password);
            ConfigSource conf = config.deepCopy().set("http_proxy", httpProxyMap);
            S3PluginTask task = conf.loadConfig(S3PluginTask.class);

            assertHttpProxy(new HttpProxy(host, Optional.of(port), true, Optional.of(user), Optional.of(password)),
                    task.getHttpProxy().get());
        }
    }

    private static void setupS3Config(ConfigSource config)
    {
        config.set("bucket", "my_bucket").set("path_prefix", "my_path_prefix");
    }

    private static void assertHttpProxy(HttpProxy expected, HttpProxy actual)
    {
        assertEquals(expected.getHost(), actual.getHost());
        assertEquals(expected.getPort().isPresent(), actual.getPort().isPresent());
        if (expected.getPort().isPresent()) {
            assertEquals(expected.getPort().get(), actual.getPort().get());
        }
        assertEquals(expected.useHttps(), actual.useHttps());
        assertEquals(expected.getUser().isPresent(), actual.getUser().isPresent());
        if (expected.getUser().isPresent()) {
            assertEquals(expected.getUser().get(), actual.getUser().get());
        }
        assertEquals(expected.getPassword().isPresent(), actual.getPassword().isPresent());
        if (expected.getPassword().isPresent()) {
            assertEquals(expected.getPassword().get(), actual.getPassword().get());
        }
    }
}
