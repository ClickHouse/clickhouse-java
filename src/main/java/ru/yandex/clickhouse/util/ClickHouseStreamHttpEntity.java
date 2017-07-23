package ru.yandex.clickhouse.util;

import com.google.common.base.Preconditions;
import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseStreamHttpEntity extends AbstractHttpEntity {

    private final ClickHouseStreamCallback callback;

    public ClickHouseStreamHttpEntity(ClickHouseStreamCallback callback) {
        Preconditions.checkNotNull(callback);
        this.callback = callback;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return null;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(out);
        callback.writeTo(stream);
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
