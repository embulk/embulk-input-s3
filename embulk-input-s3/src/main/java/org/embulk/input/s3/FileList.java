package org.embulk.input.s3;

import java.util.List;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonCreator;

import javax.validation.constraints.Min;

// this class should be moved to embulk-core
public class FileList
{
    public interface Task
    {
        @Config("path_match_pattern")
        @ConfigDefault("\".*\"")
        String getPathMatchPattern();

        @Config("total_file_count_limit")
        @ConfigDefault("2147483647")
        int getTotalFileCountLimit();

        @Config("total_task_count_limit")
        @ConfigDefault("2147483647")
        @Min(1)
        int getTotalTaskCountLimit();
    }

    public static class Entry
    {
        private int index;
        private long size;

        @JsonCreator
        public Entry(
                @JsonProperty("index") int index,
                @JsonProperty("size") long size)
        {
            this.index = index;
            this.size = size;
        }

        @JsonProperty("index")
        public int getIndex() { return index; }

        @JsonProperty("size")
        public long getSize() { return size; }
    }

    public static class Builder
    {
        private final ByteArrayOutputStream binary;
        private final OutputStream stream;
        private final List<Entry> entries = new ArrayList<>();
        private String last = null;
        private long totalSize = 0;

        private int limitCount = Integer.MAX_VALUE;
        private Pattern pathMatchPattern;
        private int taskLimitCount;

        private final ByteBuffer castBuffer = ByteBuffer.allocate(4);

        public Builder(Task task)
        {
            this();
            this.limitCount = task.getTotalFileCountLimit();
            this.pathMatchPattern = Pattern.compile(task.getPathMatchPattern());
            this.taskLimitCount = task.getTotalTaskCountLimit();
        }

        public Builder(ConfigSource config)
        {
            this();
            this.pathMatchPattern = Pattern.compile(config.get(String.class, "path_match_pattern", ".*"));
            this.limitCount = config.get(int.class, "total_file_count_limit", Integer.MAX_VALUE);
            this.taskLimitCount = config.get(int.class, "total_task_count_limit", Integer.MAX_VALUE);
        }

        public Builder()
        {
            binary = new ByteArrayOutputStream();
            try {
                stream = new BufferedOutputStream(new GZIPOutputStream(binary));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        public Builder limitTotalFileCount(int limitCount)
        {
            this.limitCount = limitCount;
            return this;
        }

        public Builder pathMatchPattern(String pattern)
        {
            this.pathMatchPattern = Pattern.compile(pattern);
            return this;
        }

        public Builder limitTotalTaskCount(int taskLimitCount)
        {
            this.taskLimitCount = taskLimitCount;
            return this;
        }

        public int size()
        {
            return entries.size();
        }

        public boolean needsMore()
        {
            return size() < limitCount;
        }

        // returns true if this file is used
        public synchronized boolean add(String path, long size)
        {
            // TODO throw IllegalStateException if stream is already closed

            if (!needsMore()) {
                return false;
            }

            if (!pathMatchPattern.matcher(path).find()) {
                return false;
            }

            int index = entries.size();
            entries.add(new Entry(index, size));
            totalSize += size;

            byte[] data = path.getBytes(StandardCharsets.UTF_8);
            castBuffer.putInt(0, data.length);
            try {
                stream.write(castBuffer.array());
                stream.write(data);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }

            last = path;
            return true;
        }

        public FileList build()
        {
            try {
                stream.close();
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
            return new FileList(binary.toByteArray(), getSplits(entries), Optional.fromNullable(last));
        }

        private List<List<Entry>> getSplits(List<Entry> all)
        {
            long averageSize = totalSize / taskLimitCount;
            List<List<Entry>> tasks = new ArrayList<>();

            // need to suffle entries?

            long size = 0;
            List<Entry> task = new ArrayList<>();

            for (Entry e : all) {
                size += e.getSize();
                task.add(e);
                if (size >= averageSize) {
                    tasks.add(task);
                    size = 0;
                    task = new ArrayList<>();
                }
            }

            if (size != 0) {
                tasks.add(task);
            }

            return tasks;
        }
    }

    private final byte[] data;
    private final List<List<Entry>> tasks;
    private final Optional<String> last;

    @JsonCreator
    @Deprecated
    public FileList(
            @JsonProperty("data") byte[] data,
            @JsonProperty("tasks") List<List<Entry>> tasks,
            @JsonProperty("last") Optional<String> last)
    {
        this.data = data;
        this.tasks = tasks;
        this.last = last;
    }

    @JsonIgnore
    public Optional<String> getLastPath(Optional<String> lastLastPath)
    {
        if (last.isPresent()) {
            return last;
        }
        return lastLastPath;
    }

    @JsonIgnore
    public int getTaskCount()
    {
        return tasks.size();
    }

    @JsonIgnore
    public List<String> get(int i)
    {
        return new EntryList(data, tasks.get(i));
    }

    @JsonProperty("data")
    @Deprecated
    public byte[] getData()
    {
        return data;
    }

    @JsonProperty("tasks")
    @Deprecated
    public List<List<Entry>> getTasks()
    {
        return tasks;
    }

    @JsonProperty("last")
    @Deprecated
    public Optional<String> getLast()
    {
        return last;
    }

    private class EntryList
            extends AbstractList<String>
    {
        private final byte[] data;
        private final List<Entry> entries;
        private InputStream stream;
        private int current;

        private final ByteBuffer castBuffer = ByteBuffer.allocate(4);

        public EntryList(byte[] data, List<Entry> entries)
        {
            this.data = data;
            this.entries = entries;
            try {
                this.stream = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
            this.current = 0;
        }

        @Override
        public synchronized String get(int i)
        {
            Entry e = entries.get(i);
            if (e.getIndex() < current) {
                // rewind to the head
                try {
                    stream.close();
                    stream = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
                current = 0;
            }

            while (current < e.getIndex()) {
                readNext();
            }
            // now current == e.getIndex()
            return readNextString();
        }

        @Override
        public int size()
        {
            return entries.size();
        }

        private byte[] readNext()
        {
            try {
                stream.read(castBuffer.array());
                int n = castBuffer.getInt(0);
                byte[] b = new byte[n];  // here should be able to use a pooled buffer because read data is ignored if readNextString doesn't call this method
                stream.read(b);

                current++;

                return b;
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private String readNextString()
        {
            return new String(readNext(), StandardCharsets.UTF_8);
        }
    }
}
