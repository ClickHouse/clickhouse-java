package com.clickhouse.client.api.internal;

import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.hc.core5.function.Supplier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

class LZ4Entity implements HttpEntity {

    private HttpEntity httpEntity;

    private final boolean useHttpCompression;

    private final int bufferSize;

    private final boolean isResponse;

    private boolean serverCompression;

    private boolean clientCompression;

    private LZ4Factory lz4Factory = null;

    LZ4Entity(HttpEntity httpEntity, boolean useHttpCompression, boolean serverCompression, boolean clientCompression,
              int bufferSize, boolean isResponse, LZ4Factory lz4Factory) {
        this.httpEntity = httpEntity;
        this.useHttpCompression = useHttpCompression;
        this.bufferSize = bufferSize;
        this.serverCompression = serverCompression;
        this.clientCompression = clientCompression;
        this.isResponse = isResponse;
        this.lz4Factory = lz4Factory;
    }

    @Override
    public boolean isRepeatable() {
        return httpEntity.isRepeatable();
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        if (!isResponse && serverCompression) {
            throw new UnsupportedOperationException("Unsupported: getting compressed content of request");
        } else if (serverCompression) {
            if (useHttpCompression) {
                InputStream content = httpEntity.getContent();
                try {
                    return new FramedLZ4CompressorInputStream(content);
                } catch (IOException e) {
                    // This is the easiest way to handle empty content because
                    // - streams at this point wrapped with something else and we can't check content length
                    // - exception is thrown with no details
                    // So we just return original content and if there is a real data in it we will get error later
                    return content;
                }
            } else  {
                return new ClickHouseLZ4InputStream(httpEntity.getContent(), lz4Factory.fastDecompressor(),
                        bufferSize);
            }
        } else {
            return httpEntity.getContent();
        }
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        if (isResponse && serverCompression) {
            // called by us to get compressed response
            throw new UnsupportedOperationException("Unsupported: writing compressed response to elsewhere");
        } else if (clientCompression) {
            // called by client to send data
            if (useHttpCompression) {
                httpEntity.writeTo(new FramedLZ4CompressorOutputStream(outStream));
            } else {

                httpEntity.writeTo(new ClickHouseLZ4OutputStream(outStream, lz4Factory.fastCompressor(),
                        bufferSize));
            }
        } else {
            httpEntity.writeTo(outStream);
        }
    }

    @Override
    public boolean isStreaming() {
        return httpEntity.isStreaming();
    }

    @Override
    public Supplier<List<? extends Header>> getTrailers() {
        return httpEntity.getTrailers();
    }

    @Override
    public void close() throws IOException {
        httpEntity.close();
    }

    @Override
    public long getContentLength() {
        return httpEntity.getContentLength();
    }

    @Override
    public String getContentType() {
        return httpEntity.getContentType();
    }

    @Override
    public String getContentEncoding() {
        return httpEntity.getContentEncoding();
    }

    @Override
    public boolean isChunked() {
        return httpEntity.isChunked();
    }

    @Override
    public Set<String> getTrailerNames() {
        return httpEntity.getTrailerNames();
    }
}
