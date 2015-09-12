package org.embulk.input.riak_cs;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.s3.AbstractS3FileInputPlugin.PluginTask;
import org.embulk.input.s3.TestS3FileInputPlugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestRiakCsFileInputPlugin
{

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private RiakCsFileInputPlugin plugin;

    @Before
    public void createPlugin()
    {
        plugin = runtime.getInstance(RiakCsFileInputPlugin.class);
    }

    @Test
    public void createS3ClientSuccessfully()
    {
        ConfigSource config = TestS3FileInputPlugin.config().set("endpoint", "my.endpoint.com");
        PluginTask task = config.loadConfig(plugin.getTaskClass());
        plugin.newS3Client(task);
    }
}
