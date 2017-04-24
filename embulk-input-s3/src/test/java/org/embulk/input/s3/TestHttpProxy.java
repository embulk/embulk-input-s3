package org.embulk.input.s3;

import com.google.common.base.Optional;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.s3.S3FileInputPlugin.S3PluginTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
        S3PluginTask task = conf.loadConfig(S3PluginTask.class);
        assertTrue(!task.getHttpProxy().isPresent());
    }

    @Test
    public void checkHttpProxy()
    {
        { // specify host
            String host = "my_host";
            ConfigSource conf = config.deepCopy().set("host", host);
            HttpProxy httpProxy = conf.loadConfig(HttpProxy.class);
            assertHttpProxy(host, Optional.<Integer>absent(), true, Optional.<String>absent(), Optional.<String>absent(),
                    httpProxy);
        }

        { // specify https=true explicitly
            String host = "my_host";
            ConfigSource conf = config.deepCopy()
                    .set("host", host)
                    .set("https", true);
            HttpProxy httpProxy = conf.loadConfig(HttpProxy.class);
            assertHttpProxy(host, Optional.<Integer>absent(), true, Optional.<String>absent(), Optional.<String>absent(),
                    httpProxy);
        }

        { // specify https=false
            String host = "my_host";
            ConfigSource conf = config.deepCopy()
                    .set("host", host)
                    .set("https", false);
            HttpProxy httpProxy = conf.loadConfig(HttpProxy.class);
            assertHttpProxy(host, Optional.<Integer>absent(), false, Optional.<String>absent(), Optional.<String>absent(),
                    httpProxy);
        }

        { // specify host, port
            String host = "my_host";
            int port = 8080;
            ConfigSource conf = config.deepCopy()
                    .set("host", host)
                    .set("port", port);
            HttpProxy httpProxy = conf.loadConfig(HttpProxy.class);
            assertHttpProxy(host, Optional.of(port), true, Optional.<String>absent(), Optional.<String>absent(),
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
            HttpProxy httpProxy = conf.loadConfig(HttpProxy.class);
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
}
