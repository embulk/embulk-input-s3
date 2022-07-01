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

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFileList
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;

    @Before
    public void createConfigSource()
    {
        config = runtime.getExec().newConfigSource();
    }

    @Test
    public void checkMinTaskSize()
            throws Exception
    {
        { // not specify min_task_size
            FileList fileList = newFileList(config.deepCopy(),
                    "sample_00", 100L,
                    "sample_01", 150L,
                    "sample_02", 350L);

            assertEquals(3, fileList.getTaskCount());
            assertEquals("sample_00", fileList.get(0).get(0));
            assertEquals("sample_01", fileList.get(1).get(0));
            assertEquals("sample_02", fileList.get(2).get(0));
        }

        {
            FileList fileList = newFileList(config.deepCopy().set("min_task_size", 100),
                    "sample_00", 100L,
                    "sample_01", 150L,
                    "sample_02", 350L);

            assertEquals(3, fileList.getTaskCount());
            assertEquals("sample_00", fileList.get(0).get(0));
            assertEquals("sample_01", fileList.get(1).get(0));
            assertEquals("sample_02", fileList.get(2).get(0));
        }

        {
            FileList fileList = newFileList(config.deepCopy().set("min_task_size", 200),
                    "sample_00", 100L,
                    "sample_01", 150L,
                    "sample_02", 350L);

            assertEquals(2, fileList.getTaskCount());
            assertEquals("sample_00", fileList.get(0).get(0));
            assertEquals("sample_01", fileList.get(0).get(1));
            assertEquals("sample_02", fileList.get(1).get(0));
        }

        {
            FileList fileList = newFileList(config.deepCopy().set("min_task_size", 700),
                    "sample_00", 100L,
                    "sample_01", 150L,
                    "sample_02", 350L);

            assertEquals(1, fileList.getTaskCount());
            assertEquals("sample_00", fileList.get(0).get(0));
            assertEquals("sample_01", fileList.get(0).get(1));
            assertEquals("sample_02", fileList.get(0).get(2));
        }
    }

    private static FileList newFileList(ConfigSource config, Object... nameAndSize)
    {
        FileList.Builder builder = new FileList.Builder(config);

        for (int i = 0; i < nameAndSize.length; i += 2) {
            builder.add((String) nameAndSize[i], (long) nameAndSize[i + 1]);
        }

        return builder.build();
    }
}
