package ru.yandex.clickhouse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;

/**
 * Allow to inject sql query in the body, followed by row data
 */
public class BodyEntityWrapper extends AbstractHttpEntity {
    private final StringEntity sql;
    private final HttpEntity delegate;

	public BodyEntityWrapper(String sql, HttpEntity content) {
		this.sql = new StringEntity(sql+"\n", StandardCharsets.UTF_8);
		this.delegate = content;
	}

	@Override
	public boolean isRepeatable() {
        return delegate.isRepeatable();
	}

	@Override
	public long getContentLength() {
        return -1;
	}

	@Override
	public InputStream getContent() throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
	}

	@Override
	public void writeTo(OutputStream outstream) throws IOException {
		sql.writeTo(outstream);
        delegate.writeTo(outstream);
        outstream.flush();
	}

	@Override
	public boolean isStreaming() {
        return delegate.isStreaming();
	}

}
