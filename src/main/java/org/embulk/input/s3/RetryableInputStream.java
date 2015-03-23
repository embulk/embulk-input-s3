package org.embulk.input.s3;

import java.io.InputStream;
import java.io.IOException;

public class RetryableInputStream
        extends InputStream
{
    public interface Opener
    {
        public InputStream open(long offset, Exception exception) throws IOException;
    }

    private final Opener opener;
    protected InputStream in;
    private long offset;
    private long markedOffset;

    public RetryableInputStream(InputStream initialInputStream, Opener reopener)
    {
        this.opener = reopener;
        this.in = initialInputStream;
        this.offset = 0L;
        this.markedOffset = 0L;
    }

    public RetryableInputStream(Opener opener) throws IOException
    {
        this(opener.open(0, null), opener);
    }

    private void reopen(Exception exception) throws IOException
    {
        if (in != null) {
            in.close();
            in = null;
        }
        in = opener.open(offset, exception);
    }

    @Override
    public int read() throws IOException
    {
        while (true) {
            try {
                int v = in.read();
                offset += 1;
                return v;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        while (true) {
            try {
                int r = in.read(b);
                offset += r;
                return r;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        while (true) {
            try {
                int r = in.read(b, off, len);
                offset += r;
                return r;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public long skip(long n) throws IOException
    {
        while (true) {
            try {
                long r = in.skip(n);
                offset += r;
                return r;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public int available() throws IOException
    {
        return in.available();
    }

    @Override
    public void close() throws IOException
    {
        in.close();
    }

    @Override
    public void mark(int readlimit)
    {
        in.mark(readlimit);
        markedOffset = offset;
    }

    @Override
    public void reset() throws IOException
    {
        in.reset();
        offset = markedOffset;
    }

    @Override
    public boolean markSupported()
    {
        return in.markSupported();
    }
}
