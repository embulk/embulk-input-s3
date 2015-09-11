package org.embulk.input.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.s3.AbstractS3FileInputPlugin.PluginTask;
import org.embulk.input.s3.AbstractS3FileInputPlugin.S3FileInput;
import org.embulk.input.s3.S3FileInputPlugin.S3PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.util.LineDecoder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class TestS3FileInputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private S3FileInputPlugin plugin;
    private AmazonS3Client client;

    @Before
    public void createResources()
    {
        config = config();
        plugin = spy(runtime.getInstance(S3FileInputPlugin.class));
        client = mock(AmazonS3Client.class);
    }

    @Test
    public void checkS3ClientCreatedSuccessfully()
    {
        PluginTask task = config().loadConfig(plugin.getTaskClass());
        plugin.newS3Client(task);
    }

    @Test
    public void listS3FilesByPrefix()
    {
        // AWSS3Client returns list1 for the first iteration and list2 next.
        List<S3ObjectSummary> list1 = s3objects("in/", 0L, "in/file/", 0L, "in/file/sample.csv.gz", 12345L);
        List<S3ObjectSummary> list2 = s3objects("sample2.csv.gz", 0L);
        ObjectListing ol = mock(ObjectListing.class);

        doReturn(list1).doReturn(list2).when(ol).getObjectSummaries();
        doReturn(ol).when(client).listObjects(any(ListObjectsRequest.class));
        doReturn("in/file/").doReturn(null).when(ol).getNextMarker();

        // It counts only size != 0 files.
        assertEquals(1, S3FileInputPlugin.listS3FilesByPrefix(client, "bucketName", "prefix", Optional.<String>absent()).size());
    }

    @Test
    public void checkLastPath()
    {
        doReturn(null).when(client).listObjects(any(ListObjectsRequest.class));
        doReturn(client).when(plugin).newS3Client(any(PluginTask.class));

        { // set a last file to last_path
            ObjectListing listing = listing("in/aa", 0L, "in/aa/a", 3L, "in/aa/b", 2L, "in/aa/c", 1L);
            doReturn(listing).when(client).listObjects(any(ListObjectsRequest.class));

            ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
                @Override
                public List<TaskReport> run(TaskSource taskSource, int taskCount)
                {
                    assertEquals(3, taskCount);
                    List<String> files = taskSource.loadTask(S3PluginTask.class).getFiles();
                    assertArrayEquals(new String[]{"in/aa/a", "in/aa/b", "in/aa/c"}, files.toArray(new String[files.size()]));
                    return emptyTaskReports(taskCount);
                }
            });

            assertEquals("in/aa/c", configDiff.get(String.class, "last_path"));
        }

        { // if files are empty and last_path is not specified, last_path is not set.
            ObjectListing listing = listing("in/aa", 0L);
            doReturn(listing).when(client).listObjects(any(ListObjectsRequest.class));

            ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
                @Override
                public List<TaskReport> run(TaskSource taskSource, int taskCount)
                {
                    assertEquals(0, taskCount);
                    assertTrue(taskSource.loadTask(S3PluginTask.class).getFiles().isEmpty());
                    return emptyTaskReports(taskCount);
                }
            });

            assertFalse(configDiff.has("last_path"));
        }

        { // if files are empty, keep the previous last_path.
            config.set("last_path", "in/bb");

            ObjectListing listing = listing("in/aa", 0L);
            doReturn(listing).when(client).listObjects(any(ListObjectsRequest.class));

            ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
                @Override
                public List<TaskReport> run(TaskSource taskSource, int taskCount) {
                    assertEquals(0, taskCount);
                    assertTrue(taskSource.loadTask(S3PluginTask.class).getFiles().isEmpty());
                    return emptyTaskReports(taskCount);
                }
            });

            assertEquals("in/bb", configDiff.get(String.class, "last_path"));
        }
    }

    @Test
    public void checkS3FileInputByOpen()
            throws Exception
    {
        doReturn(s3object("in/aa/a", "aa")).when(client).getObject(any(GetObjectRequest.class));
        doReturn(client).when(plugin).newS3Client(any(PluginTask.class));

        PluginTask task = config.loadConfig(plugin.getTaskClass());
        task.setFiles(Arrays.asList(new String[]{"in/aa/a"}));

        StringBuilder sbuf = new StringBuilder();
        try (S3FileInput input = (S3FileInput) plugin.open(task.dump(), 0)) {
            LineDecoder d = new LineDecoder(input, config.loadConfig(LineDecoder.DecoderTask.class));
            while (d.nextFile()) {
                sbuf.append(d.poll());
            }
        }
        assertEquals("aa", sbuf.toString());
    }

    public static ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("bucket", "my_bucket")
                .set("path_prefix", "my_path_prefix")
                .set("access_key_id", "my_access_key_id")
                .set("secret_access_key", "my_secret_access_key");
    }

    static ObjectListing listing(Object... keySizes)
    {
        ObjectListing listing = mock(ObjectListing.class);
        if (keySizes == null) {
            return listing;
        }

        List<S3ObjectSummary> s3objects = s3objects(keySizes);
        doReturn(s3objects).when(listing).getObjectSummaries();
        doReturn(null).when(listing).getNextMarker();
        return listing;
    }

    static List<S3ObjectSummary> s3objects(Object... keySizes)
    {
        ImmutableList.Builder<S3ObjectSummary> builder = new ImmutableList.Builder<>();
        if (keySizes == null) {
            return builder.build();
        }

        for (int i = 0; i < keySizes.length; i += 2) {
            String key = (String) keySizes[i];
            long size = (Long) keySizes[i + 1];
            builder.add(s3object(key, size));
        }
        return builder.build();
    }

    static S3ObjectSummary s3object(String key, long size)
    {
        S3ObjectSummary o = new S3ObjectSummary();
        o.setKey(key);
        o.setSize(size);
        return o;
    }

    static S3Object s3object(String key, String value)
    {
        S3Object o = new S3Object();
        o.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(value.getBytes()), null));
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(value.length());
        o.setObjectMetadata(om);
        return o;
    }

    static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(Exec.newTaskReport());
        }
        return reports.build();
    }
}
