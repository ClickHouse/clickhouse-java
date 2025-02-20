package com.clickhouse.data.stream;

import java.io.IOException;
import java.util.Iterator;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;

@Deprecated
public class IterableByteArrayInputStream extends AbstractByteArrayInputStream {
    private final Iterator<byte[]> it;

    public IterableByteArrayInputStream(Iterable<byte[]> source, Runnable postCloseAction) {
        super(null, null, postCloseAction);

        it = ClickHouseChecker.nonNull(source, "Source").iterator();
    }

    @Override
    protected int updateBuffer() throws IOException {
        position = 0;

        while (it.hasNext()) {
            byte[] bytes = it.next();
            int len = bytes != null ? bytes.length : 0;
            if (len > 0) {
                buffer = bytes;
                if (copyTo != null) {
                    copyTo.write(bytes);
                }
                return limit = len;
            }
        }

        buffer = ClickHouseByteBuffer.EMPTY_BYTES;
        return limit = 0;
    }
}
