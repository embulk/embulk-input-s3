package org.embulk.input.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static org.embulk.input.s3.TestS3FileInputPlugin.parserConfig;
import static org.embulk.input.s3.TestS3FileInputPlugin.schemaConfig;
import static org.junit.Assert.assertEquals;

public class TestAwsClientConfiguration
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private S3FileInputPlugin plugin;
    private ConfigSource config;

    @Before
    public void createResources()
    {
        plugin = new S3FileInputPlugin();
        config = runtime.getExec().newConfigSource()
                .set("type", "s3")
                .set("bucket", "dummy")
                .set("path_prefix", "dummy")
                .set("parser", parserConfig(schemaConfig()));
    }

    @Test
    public void setOneParam()
    {
        config.setNested("client_config", Exec.newConfigSource()
        .set("protocol", "HTTP")
        .set("user_agent", "test_embulk_input_s3"));

        ClientConfiguration clientConfiguration = getClientConfiguration();

        assertEquals(Protocol.HTTP, clientConfiguration.getProtocol());
        assertEquals("test_embulk_input_s3", clientConfiguration.getUserAgent());
    }

    @Test
    public void setTwoParam()
    {
        config.setNested("client_config", Exec.newConfigSource()
        .set("socket_send_buffer_size_hint", "1MB")
        .set("socket_receive_buffer_size_hint", "2MB"));

        ClientConfiguration clientConfiguration = getClientConfiguration();

        int[] socketBufferSizeHints = clientConfiguration.getSocketBufferSizeHints();
        assertEquals(1 << 20, socketBufferSizeHints[0]);
        assertEquals(2 << 20, socketBufferSizeHints[1]);
    }

    @Test
    public void defaultValue()
    {
        ClientConfiguration clientConfiguration = getClientConfiguration();

        assertEquals(3, clientConfiguration.getMaxErrorRetry());
        assertEquals(50, clientConfiguration.getMaxConnections());
        assertEquals(8*60*1000, clientConfiguration.getSocketTimeout());
    }

    @Test
    public void secureRandom()
            throws NoSuchAlgorithmException
    {
        config.setNested("client_config", Exec.newConfigSource()
                .setNested("secure_random", Exec.newConfigSource()
                        .set("algorithm", "SHA1PRNG")
                )
        );

        ClientConfiguration clientConfiguration = getClientConfiguration();

        assertEquals(SecureRandom.getInstance("SHA1PRNG").getAlgorithm(), clientConfiguration.getSecureRandom().getAlgorithm());
    }

    @Test(expected = ConfigException.class)
    public void secureRandomNoSuchAlgorithmException()
    {
        config.setNested("client_config", Exec.newConfigSource()
                .setNested("secure_random", Exec.newConfigSource()
                        .set("algorithm", "FOOOOOOO")
                )
        );

        ClientConfiguration clientConfiguration = getClientConfiguration();
    }

    private ClientConfiguration getClientConfiguration()
    {
        S3FileInputPlugin.S3PluginTask task = config.loadConfig(S3FileInputPlugin.S3PluginTask.class);
        return plugin.getClientConfiguration(task);
    }
}
