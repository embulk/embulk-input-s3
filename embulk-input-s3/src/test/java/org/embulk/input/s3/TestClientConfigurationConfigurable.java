package org.embulk.input.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;

import static org.embulk.input.s3.TestS3FileInputPlugin.parserConfig;
import static org.embulk.input.s3.TestS3FileInputPlugin.schemaConfig;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;

/**
 * Created by takahiro.nakayama on 3/30/16.
 */
public class TestClientConfigurationConfigurable
{
    private static String EMBULK_S3_TEST_BUCKET;
    private static String EMBULK_S3_TEST_ACCESS_KEY_ID;
    private static String EMBULK_S3_TEST_SECRET_ACCESS_KEY;
    private static final String EMBULK_S3_TEST_PATH_PREFIX = "embulk_input_s3_test";

    /*
     * This test case requires environment variables:
     *   EMBULK_S3_TEST_BUCKET
     *   EMBULK_S3_TEST_ACCESS_KEY_ID
     *   EMBULK_S3_TEST_SECRET_ACCESS_KEY
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        EMBULK_S3_TEST_BUCKET = System.getenv("EMBULK_S3_TEST_BUCKET");
        EMBULK_S3_TEST_ACCESS_KEY_ID = System.getenv("EMBULK_S3_TEST_ACCESS_KEY_ID");
        EMBULK_S3_TEST_SECRET_ACCESS_KEY = System.getenv("EMBULK_S3_TEST_SECRET_ACCESS_KEY");
        assumeNotNull(EMBULK_S3_TEST_BUCKET, EMBULK_S3_TEST_ACCESS_KEY_ID, EMBULK_S3_TEST_SECRET_ACCESS_KEY);
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private S3FileInputPlugin plugin;
    private ConfigSource config;
    private FileInputRunner runner;
    private TestPageBuilderReader.MockPageOutput output;

    @Before
    public void createResources()
    {
        plugin = new S3FileInputPlugin();
        config = runtime.getExec().newConfigSource()
                .set("type", "s3")
                .set("bucket", EMBULK_S3_TEST_BUCKET)
                .set("path_prefix", EMBULK_S3_TEST_PATH_PREFIX)
                .set("parser", parserConfig(schemaConfig()));
        runner = new FileInputRunner(runtime.getInstance(S3FileInputPlugin.class));
        output = new TestPageBuilderReader.MockPageOutput();
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
        .set("socket_send_buffer_size_hints", 4)
        .set("socket_receive_buffer_size_hints", 8));

        ClientConfiguration clientConfiguration = getClientConfiguration();

        int[] socketBufferSizeHints = clientConfiguration.getSocketBufferSizeHints();
        assertEquals(4, socketBufferSizeHints[0]);
        assertEquals(8, socketBufferSizeHints[1]);
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