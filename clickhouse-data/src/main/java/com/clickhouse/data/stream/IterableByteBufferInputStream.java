package com.clickhouse.data.stream;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;

@Deprecated
public class IterableByteBufferInputStream extends AbstractByteBufferInputStream {
    private final Iterator<ByteBuffer> it;

    public IterableByteBufferInputStream(Iterable<ByteBuffer> source, Runnable postCloseAction) {
        super(null, null, postCloseAction);

        it = ClickHouseChecker.nonNull(source, "Source").iterator();
    }

    @Override
    protected int updateBuffer() throws IOException {
        while (it.hasNext()) {
            ByteBuffer bytes = it.next();
            if (bytes != null && bytes.hasRemaining()) {
                buffer = bytes;
                if (copyTo != null) {
                    int position = bytes.position();
                    if (bytes.hasArray()) {
                        copyTo.write(bytes.array(), position, bytes.remaining());
                    } else {
                        byte[] b = new byte[bytes.remaining()];
                        bytes.get(b);
                        copyTo.write(b);
                        ((Buffer) bytes).position(position);
                    }
                }
                return bytes.remaining();
            }
        }
        buffer = ClickHouseByteBuffer.EMPTY_BUFFER;
        return 0;
    }
}
