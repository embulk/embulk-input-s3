package org.embulk.input.s3;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.s3.S3FileInputPlugin.S3PluginTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestRetrySupportPluginTask
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
    public void checkDefaultRetrySupportPluginTask()
    {
        ConfigSource conf = config.deepCopy();
        S3PluginTask task = conf.loadConfig(S3PluginTask.class);
        assertEquals(7, task.getMaximumRetries());
        assertEquals(30000, task.getInitialRetryIntervalMillis());
        assertEquals(480000, task.getMaximumRetryIntervalMillis());
    }

    @Test
    public void checkRetrySupportPluginTask()
    {
        { // specify maximum retries
            int maxRetries = 1;
            ConfigSource conf = config.deepCopy().set("maximum_retries", maxRetries);
            RetrySupportPluginTask retrySupportPluginTask = conf.loadConfig(RetrySupportPluginTask.class);
            assertEquals(1, retrySupportPluginTask.getMaximumRetries());
        }

        { // specify init retry interval
            int initRetryInterval = 100;
            ConfigSource conf = config.deepCopy().set("initial_retry_interval_millis", initRetryInterval);
            RetrySupportPluginTask retrySupportPluginTask = conf.loadConfig(RetrySupportPluginTask.class);
            assertEquals(initRetryInterval, retrySupportPluginTask.getInitialRetryIntervalMillis());
        }

        { // specify maximum retry interval
            int maxRetryInteval = 1000;
            ConfigSource conf = config.deepCopy().set("maximum_retry_interval_millis", maxRetryInteval);
            RetrySupportPluginTask retrySupportPluginTask = conf.loadConfig(RetrySupportPluginTask.class);
            assertEquals(maxRetryInteval, retrySupportPluginTask.getMaximumRetryIntervalMillis());
        }

        { // specify maximum retries, init retry interval, maximum retry interval
            int maxRetries = 1;
            int initRetryInterval = 100;
            int maxRetryInteval = 1000;
            ConfigSource conf = config.deepCopy()
                    .set("maximum_retries", maxRetries)
                    .set("initial_retry_interval_millis", initRetryInterval)
                    .set("maximum_retry_interval_millis", maxRetryInteval);
            RetrySupportPluginTask retrySupportPluginTask = conf.loadConfig(RetrySupportPluginTask.class);
            assertEquals(1, retrySupportPluginTask.getMaximumRetries());
            assertEquals(initRetryInterval, retrySupportPluginTask.getInitialRetryIntervalMillis());
            assertEquals(maxRetryInteval, retrySupportPluginTask.getMaximumRetryIntervalMillis());

        }
    }

    private static void setupS3Config(ConfigSource config)
    {
        config.set("bucket", "my_bucket").set("path_prefix", "my_path_prefix");
    }
}
