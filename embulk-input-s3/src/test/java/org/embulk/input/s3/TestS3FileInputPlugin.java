package org.embulk.input.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeNotNull;

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
    private FileInputRunner runner;
    private MockPageOutput output;

    @Before
    public void createResources()
    {
        config = runtime.getExec().newConfigSource()
                .set("type", "s3")
                .set("bucket", EMBULK_S3_TEST_BUCKET)
                .set("access_key_id", EMBULK_S3_TEST_ACCESS_KEY_ID)
                .set("secret_access_key", EMBULK_S3_TEST_SECRET_ACCESS_KEY)
                .set("path_prefix", EMBULK_S3_TEST_PATH_PREFIX)
                .set("parser", parserConfig(schemaConfig()));
        runner = new FileInputRunner(runtime.getInstance(S3FileInputPlugin.class));
        output = new MockPageOutput();
    }

    @Test
    public void simpleTest()
    {
        ConfigSource config = this.config.deepCopy();
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    @Test
    public void useLastPath()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy().set("last_path", EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv");
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertEquals(0, getRecords(config, output).size());
    }

    @Test
    public void emptyFilesWithLastPath()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy()
                .set("path_prefix", "empty_files_prefix")
                .set("last_path", EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv");
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path")); // keep the last_path
        assertEquals(0, getRecords(config, output).size());
    }

    @Test
    public void useTotalFileCountLimit()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy().set("total_file_count_limit", 0);
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertNull(configDiff.get(String.class, "last_path"));
        assertEquals(0, getRecords(config, output).size());
    }

    @Test
    public void usePathMatchPattern()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy().set("path_match_pattern", "/match/");
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertNull(configDiff.get(String.class, "last_path"));
        assertEquals(0, getRecords(config, output).size());
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

    static ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
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
        return builder.build();
    }

    static ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "timestamp", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "host", "type", "string"));
        builder.add(ImmutableMap.of("name", "path", "type", "string"));
        builder.add(ImmutableMap.of("name", "method", "type", "string"));
        builder.add(ImmutableMap.of("name", "referer", "type", "string"));
        builder.add(ImmutableMap.of("name", "code", "type", "long"));
        builder.add(ImmutableMap.of("name", "agent", "type", "string"));
        builder.add(ImmutableMap.of("name", "user", "type", "string"));
        builder.add(ImmutableMap.of("name", "size", "type", "long"));
        return builder.build();
    }

    static void assertRecords(ConfigSource config, MockPageOutput output)
    {
        List<Object[]> records = getRecords(config, output);

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

    static List<Object[]> getRecords(ConfigSource config, MockPageOutput output)
    {
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        return Pages.toObjects(schema, output.pages);
    }
}
