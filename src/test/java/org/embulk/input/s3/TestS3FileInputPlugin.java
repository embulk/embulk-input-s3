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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Region;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.embulk.input.s3.S3FileInputPlugin.S3PluginTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class TestS3FileInputPlugin
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

    private ConfigSource config;
    private Schema configSchema;
    private FileInputRunner runner;
    private MockPageOutput output;

    @Before
    public void createResources()
    {
        final List<Object> schemaConfig = schemaConfig();
        config = runtime.getExec().newConfigSource()
                .set("type", "s3")
                .set("bucket", EMBULK_S3_TEST_BUCKET)
                .set("access_key_id", EMBULK_S3_TEST_ACCESS_KEY_ID)
                .set("secret_access_key", EMBULK_S3_TEST_SECRET_ACCESS_KEY)
                .set("path_prefix", EMBULK_S3_TEST_PATH_PREFIX)
                .set("parser", parserConfig(schemaConfig));
        configSchema = buildSchema(schemaConfig);
        runner = new FileInputRunner(runtime.getInstance(S3FileInputPlugin.class));
        output = new MockPageOutput();
    }

    @Test
    public void simpleTest()
    {
        ConfigSource config = this.config.deepCopy();
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(configSchema, output);
    }

    @Test
    public void useLastPath()
    {
        ConfigSource config = this.config.deepCopy().set("last_path", EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv");
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertEquals(0, getRecords(configSchema, output).size());
    }

    @Test
    public void useIncremental()
    {
        ConfigSource config = this.config.deepCopy().set("incremental", false);
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertFalse(configDiff.has("last_path"));
    }

    @Test
    public void emptyFilesWithLastPath()
    {
        ConfigSource config = this.config.deepCopy()
                .set("path_prefix", "empty_files_prefix")
                .set("last_path", EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv");
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path")); // keep the last_path
        assertEquals(0, getRecords(configSchema, output).size());
    }

    @Test
    public void useTotalFileCountLimit()
    {
        ConfigSource config = this.config.deepCopy().set("total_file_count_limit", 0);
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertNull(configDiff.get(String.class, "last_path"));
        assertEquals(0, getRecords(configSchema, output).size());
    }

    @Test
    public void usePathMatchPattern()
    {
        { // match pattern
            ConfigSource config = this.config.deepCopy().set("path_match_pattern", "/sample_01");
            ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

            assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
            assertRecords(configSchema, output);
        }

        output = new MockPageOutput();
        { // not match pattern
            ConfigSource config = this.config.deepCopy().set("path_match_pattern", "/match/");
            ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

            assertNull(configDiff.get(String.class, "last_path"));
            assertEquals(0, getRecords(configSchema, output).size());
        }
    }

    @Test
    public void usePath()
    {
        ConfigSource config = this.config.deepCopy()
                .set("path", String.format("%s/sample_01.csv", EMBULK_S3_TEST_PATH_PREFIX))
                .set("path_prefix", null);
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));
        assertEquals(String.format("%s/sample_01.csv", EMBULK_S3_TEST_PATH_PREFIX), configDiff.get(String.class, "last_path"));
        assertRecords(configSchema, output);
    }

    @Test
    public void usePathAsHighPriorityThanPathPrefix()
    {
        ConfigSource config = this.config.deepCopy()
                .set("path", String.format("%s/sample_01.csv", EMBULK_S3_TEST_PATH_PREFIX))
                .set("path_prefix", "foo"); // path_prefix has the bad value, if path_prefix is chosen, expected result will be failed
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));
        assertEquals(String.format("%s/sample_01.csv", EMBULK_S3_TEST_PATH_PREFIX), configDiff.get(String.class, "last_path"));
        assertRecords(configSchema, output);
    }

    @Test
    public void configuredEndpoint()
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final ConfigSource configSource = config.deepCopy()
                .set("endpoint", "s3-ap-southeast-1.amazonaws.com")
                .set("region", "ap-southeast-2");
        final S3PluginTask task = configMapper.map(configSource, S3PluginTask.class);
        S3FileInputPlugin plugin = runtime.getInstance(S3FileInputPlugin.class);
        AmazonS3 s3Client = plugin.newS3Client(task);

        // Should not crash and favor the endpoint over the region configuration (there's a warning log though)
        assertEquals(s3Client.getRegion(), Region.AP_Singapore);
    }

    @Test
    public void configuredRegion()
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final ConfigSource configSource = config.deepCopy()
                .set("region", "ap-southeast-2")
                .remove("endpoint");
        final S3PluginTask task = configMapper.map(configSource, S3PluginTask.class);
        S3FileInputPlugin plugin = runtime.getInstance(S3FileInputPlugin.class);
        AmazonS3 s3Client = plugin.newS3Client(task);

        // Should reflect the region configuration as is
        assertEquals(s3Client.getRegion(), Region.AP_Sydney);
    }

    @Test
    public void unconfiguredEndpointAndRegion()
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final ConfigSource configSource = config.deepCopy()
                .remove("endpoint")
                .remove("region");
        final S3PluginTask task = configMapper.map(configSource, S3PluginTask.class);
        S3FileInputPlugin plugin = runtime.getInstance(S3FileInputPlugin.class);
        AmazonS3 s3Client = plugin.newS3Client(task);

        // US Standard region is a 'generic' one (s3.amazonaws.com), the expectation here that
        // the S3 client should not eagerly resolves for a specific region on client side.
        // Please refer to org.embulk.input.s3.S3FileInputPlugin#newS3Client for the details.
        assertEquals(s3Client.getRegion(), Region.US_Standard);
    }

    @Test
    public void testParseDate() throws ParseException {
        final S3FileInputPlugin plugin = runtime.getInstance(S3FileInputPlugin.class);
        final Date expectedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-12-03 10:15:30");
        assertEquals(expectedDate, plugin.parseDate("2011-12-03T10:15:30.000Z"));
    }

    @Test(expected = ConfigException.class)
    public void testNoMilliseconds()
    {
        final S3FileInputPlugin plugin = runtime.getInstance(S3FileInputPlugin.class);
        plugin.parseDate("2011-12-03T10:15:30Z");
    }

    @Test(expected = ConfigException.class)
    public void testOffsetNotSet()
    {
        final S3FileInputPlugin plugin = runtime.getInstance(S3FileInputPlugin.class);
        plugin.parseDate("2011-12-03T10:15:30.000");
    }

    static class Control
            implements InputPlugin.Control
    {
        private FileInputRunner runner;
        private PageOutput output;

        Control(FileInputRunner runner, PageOutput output)
        {
            this.runner = runner;
            this.output = output;
        }

        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(runner.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    static Map<String, Object> parserConfig(List<Object> schemaConfig)
    {
        final HashMap<String, Object> builder = new HashMap<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 0);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder;
    }

    static List<Object> schemaConfig()
    {
        final ArrayList<Object> builder = new ArrayList<>();
        builder.add(mapOf("name", "timestamp", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(mapOf("name", "host", "type", "string"));
        builder.add(mapOf("name", "path", "type", "string"));
        builder.add(mapOf("name", "method", "type", "string"));
        builder.add(mapOf("name", "referer", "type", "string"));
        builder.add(mapOf("name", "code", "type", "long"));
        builder.add(mapOf("name", "agent", "type", "string"));
        builder.add(mapOf("name", "user", "type", "string"));
        builder.add(mapOf("name", "size", "type", "long"));
        return builder;
    }

    static Map<String, String> mapOf(final String... args) {
        final HashMap<String, String> map = new HashMap<>();
        if (args.length % 2 != 0) {
            throw new RuntimeException("Unexpected mapOf.");
        }
        for (int i = 0; i < args.length; i += 2) {
            map.put(args[i], args[i + 1]);
        }
        return map;
    }

    static Schema buildSchema(final List<Object> schemaConfig)
    {
        final ArrayList<Column> columns = new ArrayList<>();
        int index = 0;
        for (final Object columnConfigObject : schemaConfig) {
            assumeTrue(columnConfigObject instanceof Map);
            final Map<Object, Object> columnConfigMap = (Map<Object, Object>) columnConfigObject;
            final String name = (String) columnConfigMap.get("name");
            final String type = (String) columnConfigMap.get("type");
            final String format;
            if (type.equals("timestamp")) {
                format = (String) columnConfigMap.get("format");
            } else {
                format = null;
            }
            final ColumnConfig columnConfig = new ColumnConfig(name, toType(type), format);
            columns.add(columnConfig.toColumn(index++));
        }
        return new Schema(columns);
    }

    static Type toType(final String type)
    {
        if (type.equals(Types.BOOLEAN.getName())) {
            return Types.BOOLEAN;
        }
        if (type.equals(Types.LONG.getName())) {
            return Types.LONG;
        }
        if (type.equals(Types.DOUBLE.getName())) {
            return Types.DOUBLE;
        }
        if (type.equals(Types.STRING.getName())) {
            return Types.STRING;
        }
        if (type.equals(Types.TIMESTAMP.getName())) {
            return Types.TIMESTAMP;
        }
        if (type.equals(Types.JSON.getName())) {
            return Types.JSON;
        }
        throw new RuntimeException();
    }

    static void assertRecords(Schema configSchema, MockPageOutput output)
    {
        List<Object[]> records = getRecords(configSchema, output);

        assertEquals(2, records.size());
        {
            Object[] record = records.get(0);
            assertEquals("2014-10-02 22:15:39 UTC", record[0].toString());
            assertEquals("84.186.29.187", record[1]);
            assertEquals("/category/electronics", record[2]);
            assertEquals("GET", record[3]);
            assertEquals("/category/music", record[4]);
            assertEquals(200L, record[5]);
            assertEquals("Mozilla/5.0", record[6]);
            assertEquals("-", record[7]);
            assertEquals(136L, record[8]);
        }

        {
            Object[] record = records.get(1);
            assertEquals("2014-10-02 22:15:01 UTC", record[0].toString());
            assertEquals("140.36.216.47", record[1]);
            assertEquals("/category/music?from=10", record[2]);
            assertEquals("GET", record[3]);
            assertEquals("-", record[4]);
            assertEquals(200L, record[5]);
            assertEquals("Mozilla/5.0", record[6]);
            assertEquals("-", record[7]);
            assertEquals(70L, record[8]);
        }
    }

    static List<Object[]> getRecords(Schema configSchema, MockPageOutput output)
    {
        return Pages.toObjects(configSchema, output.pages);
    }

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
}
