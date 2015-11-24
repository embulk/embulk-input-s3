package org.embulk.input.riak_cs;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.s3.AbstractS3FileInputPlugin.PluginTask;
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
        ConfigSource config = runtime.getExec().newConfigSource()
                .set("endpoint", "my.endpoint.com")
                .set("bucket", "my_bucket")
                .set("path_prefix", "my_path_prefix")
                .set("access_key_id", "my_access_key_id")
                .set("secret_access_key", "my_secret_access_key");
        PluginTask task = config.loadConfig(plugin.getTaskClass());
        plugin.newS3Client(task);
    }
}
