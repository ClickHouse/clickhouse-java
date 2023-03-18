package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class FramedLZ4Utils {
    static InputStream wrap(InputStream input) throws IOException {
        return new org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream(input);
    }

    static OutputStream wrap(OutputStream output) throws IOException {
        return new org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream(output);
    }

    private FramedLZ4Utils() {
    }
}