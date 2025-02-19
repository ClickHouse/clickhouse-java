package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.Function;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;

@Deprecated
public final class IterableMultipleInputStream<T> extends AbstractByteArrayInputStream {
    private final Function<T, InputStream> func;
    private final Iterator<T> it;

    private ClickHouseInputStream in;

    private ClickHouseInputStream getInputStream() throws IOException {
        if (in == EmptyInputStream.INSTANCE || in.isClosed() || in.available() < 1) {
            while (it.hasNext()) {
                InputStream i = func.apply(it.next());
                if (i != null) {
                    in = ClickHouseInputStream.of(i, buffer.length);
                    break;
                }
            }
        }
        return in;
    }

    @Override
    protected int updateBuffer() throws IOException {
        position = 0;

        if (closed) {
            return limit = 0;
        }

        int len = buffer.length;
        int off = 0;
        while (len > 0) {
            int read = getInputStream().read(buffer, off, len);
            if (read == -1) {
                break;
            } else {
                off += read;
                len -= read;
            }
        }
        if (copyTo != null) {
            copyTo.write(buffer, 0, off);
        }
        return limit = off;
    }

    public IterableMultipleInputStream(Iterable<T> source, Function<T, InputStream> converter,
            int bufferSize, Runnable postCloseAction) {
        super(null, null, postCloseAction);

        func = ClickHouseChecker.nonNull(converter, "Converter");
        it = ClickHouseChecker.nonNull(source, "Source").iterator();
        in = EmptyInputStream.INSTANCE;

        // fixed buffer
        buffer = new byte[bufferSize];

        position = 0;
        limit = 0;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            LinkedList<String> errors = new LinkedList<>();
            try {
                in.close();
            } catch (Exception e) {
                errors.add(e.getMessage());
            }

            while (it.hasNext()) {
                try {
                    InputStream i = func.apply(it.next());
                    if (i != null) {
                        i.close();
                    }
                } catch (Exception e) {
                    errors.add(e.getMessage());
                }
            }

            if (!errors.isEmpty()) {
                throw new IOException("Failed to close input stream: " + String.join("\n", errors));
            }
        } finally {
            super.close();
        }
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        if (reader == null) {
            return byteBuffer.reset();
        }
        ensureOpen();

        LinkedList<byte[]> list = new LinkedList<>();
        int offset = position;
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = limit - position;
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int read = reader.update(buffer, position, limit);
                if (read == -1) {
                    byte[] bytes = new byte[limit];
                    System.arraycopy(buffer, position, bytes, position, remain);
                    length += remain;
                    position = limit;
                    list.add(bytes);
                    if (updateBuffer() < 1) {
                        closeQuietly();
                        more = false;
                    }
                } else {
                    length += read;
                    position += read;
                    list.add(buffer);
                    more = false;
                }
            }
        }
        return byteBuffer.update(list, offset, length);
    }

    @Override
    public long pipe(ClickHouseOutputStream output) throws IOException {
        long count = 0L;
        if (output == null || output.isClosed()) {
            return count;
        }
        ensureOpen();

        try {
            int remain = limit - position;
            if (remain > 0) {
                output.writeBuffer(getBuffer());
                count += remain;
                position = limit;
            }

            count += pipe(in, output, buffer);
            while (it.hasNext()) {
                InputStream i = func.apply(it.next());
                if (i != null) {
                    count += pipe(i, output, buffer);
                }
            }
        } finally {
            close();
        }
        return count;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n == Long.MAX_VALUE) {
            long count = in.skip(n);
            while (it.hasNext()) {
                InputStream i = null;
                try {
                    i = func.apply(it.next());
                    if (i != null) {
                        count += i.skip(n);
                        i.close();
                    }
                } finally {
                    if (i != null) {
                        try {
                            in.close();
                        } catch (Exception e) {
                            // ignore
                        }
                        in = ClickHouseInputStream.of(i, buffer.length);
                    }
                }
            }
            return count;
        }
        return super.skip(n);
    }
}
