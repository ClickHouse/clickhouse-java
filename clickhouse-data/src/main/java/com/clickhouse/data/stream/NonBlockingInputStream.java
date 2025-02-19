package com.clickhouse.data.stream;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;

@Deprecated
public class NonBlockingInputStream extends AbstractByteBufferInputStream {
    private final AdaptiveQueue<ByteBuffer> queue;
    private final long timeout;

    public NonBlockingInputStream(AdaptiveQueue<ByteBuffer> queue, long timeout, Runnable postCloseAction) {
        super(null, null, postCloseAction);

        this.queue = ClickHouseChecker.nonNull(queue, "Queue");
        this.timeout = timeout < 0L ? 0L : timeout;
    }

    @Override
    protected int updateBuffer() throws IOException {
        AdaptiveQueue<ByteBuffer> q = queue;
        long t = timeout;
        long startTime = t < 1L ? 0L : System.currentTimeMillis();
        ByteBuffer b;
        while ((b = q.poll()) == null) {
            if (closed) {
                throw new IOException("Cannot operate on a closed input stream");
            } else if (t > 0L && System.currentTimeMillis() - startTime >= t) {
                throw new IOException(ClickHouseUtils.format("Read timed out after %d ms", t));
            }
        }

        buffer = b;
        int remain = b.remaining();
        if (remain > 0 && copyTo != null) {
            int position = b.position();
            if (b.hasArray()) {
                copyTo.write(b.array(), position, remain);
            } else {
                byte[] bytes = new byte[remain];
                b.get(bytes);
                copyTo.write(bytes);
                ((Buffer) b).position(position);
            }
        }
        return remain;
    }
}
