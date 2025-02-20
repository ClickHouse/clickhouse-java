package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.InputStream;

import com.clickhouse.data.ClickHouseDeferredValue;

@Deprecated
public final class DeferredInputStream extends InputStream {
    private final ClickHouseDeferredValue<InputStream> ref;
    private InputStream in;

    protected InputStream getInput() {
        return in != null ? in : (in = ref.get());
    }

    public DeferredInputStream(ClickHouseDeferredValue<InputStream> in) {
        this.ref = in;
        this.in = null;
    }

    @Override
    public int available() throws IOException {
        return getInput().available();
    }

    @Override
    public void close() throws IOException {
        getInput().close();
    }

    @Override
    public int read() throws IOException {
        return getInput().read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return getInput().read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return getInput().read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return getInput().skip(n);
    }
}
