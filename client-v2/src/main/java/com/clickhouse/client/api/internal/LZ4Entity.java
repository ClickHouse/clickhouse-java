package com.clickhouse.client.api.internal;

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
    
    private boolean useHttpCompression;
    private boolean serverCompression; 
    private boolean clientCompression;
    
    
    LZ4Entity(HttpEntity httpEntity, boolean useHttpCompression, boolean serverCompression, boolean clientCompression) {
        this.httpEntity = httpEntity;
        this.useHttpCompression = useHttpCompression;
        this.serverCompression = serverCompression;
        this.clientCompression = clientCompression;
    }
    
    @Override
    public boolean isRepeatable() {
        return httpEntity.isRepeatable();
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        if (serverCompression && useHttpCompression) {
            return new FramedLZ4CompressorInputStream(httpEntity.getContent());
        } else {
            return httpEntity.getContent();
        }
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        if (clientCompression && useHttpCompression) {
            httpEntity.writeTo(new FramedLZ4CompressorOutputStream(outStream));
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
