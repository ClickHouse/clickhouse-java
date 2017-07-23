package ru.yandex.clickhouse.util;

import com.google.common.base.Preconditions;
import org.apache.http.entity.AbstractHttpEntity;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.TimeZone;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseStreamHttpEntity extends AbstractHttpEntity {

    private final ClickHouseStreamCallback callback;
    private final TimeZone timeZone;
    private final ClickHouseProperties properties;

    public ClickHouseStreamHttpEntity(ClickHouseStreamCallback callback, TimeZone timeZone, ClickHouseProperties properties) {
        Preconditions.checkNotNull(callback);
        this.timeZone = timeZone;
        this.callback = callback;
        this.properties = properties;
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
        ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(out, timeZone, properties);
        callback.writeTo(stream);
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
