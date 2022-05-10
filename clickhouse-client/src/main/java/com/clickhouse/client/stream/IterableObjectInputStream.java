package com.clickhouse.client.stream;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseChecker;

public class IterableObjectInputStream<T> extends AbstractByteArrayInputStream {
    private final Function<T, byte[]> func;
    private final Iterator<T> it;

    public IterableObjectInputStream(Iterable<T> source, Function<T, byte[]> converter, Runnable postCloseAction) {
        super(null, postCloseAction);

        func = ClickHouseChecker.nonNull(converter, "Converter");
        it = ClickHouseChecker.nonNull(source, "Source").iterator();
    }

    @Override
    protected int updateBuffer() throws IOException {
        position = 0;
        while (it.hasNext()) {
            T obj = it.next();
            byte[] bytes = obj != null ? func.apply(obj) : null;
            if (bytes != null && bytes.length > 0) {
                buffer = bytes;
                if (copyTo != null) {
                    copyTo.write(bytes);
                }
                return limit = bytes.length;
            }
        }
        buffer = ClickHouseByteBuffer.EMPTY_BYTES;
        return limit = 0;
    }
}
