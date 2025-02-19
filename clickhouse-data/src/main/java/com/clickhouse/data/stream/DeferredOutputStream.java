package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.OutputStream;

import com.clickhouse.data.ClickHouseDeferredValue;

@Deprecated
public final class DeferredOutputStream extends OutputStream {
    private final ClickHouseDeferredValue<OutputStream> ref;
    private OutputStream out;

    protected OutputStream getOutput() {
        return out != null ? out : (out = ref.get());
    }

    public DeferredOutputStream(ClickHouseDeferredValue<OutputStream> out) {
        this.ref = out;
        this.out = null;
    }

    @Override
    public void close() throws IOException {
        getOutput().close();
    }

    @Override
    public void write(int b) throws IOException {
        getOutput().write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        getOutput().write(b, off, len);
    }
}
