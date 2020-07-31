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
