package com.clickhouse.client.api.internal;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hc.core5.function.Supplier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

public class CompressedEntity implements HttpEntity {

    private HttpEntity httpEntity;
    private final boolean isResponse;
    private final CompressorStreamFactory compressorStreamFactory;
    private final String compressionAlgo;

    CompressedEntity(HttpEntity httpEntity, boolean isResponse, CompressorStreamFactory compressorStreamFactory) {
        this.httpEntity = httpEntity;
        this.isResponse = isResponse;
        this.compressorStreamFactory = compressorStreamFactory;
        this.compressionAlgo = getCompressionAlgoName(httpEntity.getContentEncoding());
    }

    @Override
    public boolean isRepeatable() {
        return httpEntity.isRepeatable();
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        if (!isResponse) {
            throw new UnsupportedOperationException("Unsupported: getting compressed content of request");
        }

        try {
            return compressorStreamFactory.createCompressorInputStream(compressionAlgo, httpEntity.getContent());
        } catch (CompressorException e) {
            throw new IOException("Failed to create decompressing input stream", e);
        }
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        if (isResponse) {
            // called by us to get compressed response
            throw new UnsupportedOperationException("Unsupported: writing compressed response to elsewhere");
        }

        try (OutputStream compressingStream = compressorStreamFactory.createCompressorOutputStream(compressionAlgo, outStream)){
            httpEntity.writeTo(compressingStream);
        } catch (CompressorException e) {
            throw new IOException("Failed to create compressing output stream", e);
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
        // compressed request length is unknown even if it is a byte[]
        return isResponse ? httpEntity.getContentLength() : -1;
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

    private String getCompressionAlgoName(String contentEncoding) {
        String algo = contentEncoding;
        if (algo.equalsIgnoreCase("gzip")) {
            algo = CompressorStreamFactory.GZIP;
        } else if (algo.equalsIgnoreCase("lz4")) {
            algo = CompressorStreamFactory.LZ4_FRAMED;
        }
        return algo;
    }
}
