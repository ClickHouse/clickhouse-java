package com.clickhouse.client.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;

public final class Brotli4jUtils {
    private static class Initializer {
        static {
            Brotli4jLoader.ensureAvailability();
        }

        static InputStream createInputStream(InputStream input, int bufferSize) throws IOException {
            return new BrotliInputStream(input, bufferSize);
        }

        static OutputStream createOutputStream(OutputStream output, int quality, int bufferSize)
                throws IOException {
            Encoder.Parameters params = new Encoder.Parameters().setQuality(quality);
            return new BrotliOutputStream(output, params, bufferSize);
        }
    }

    private Brotli4jUtils() {
    }

    public static InputStream createInputStream(InputStream input, int bufferSize) throws IOException {
        return Initializer.createInputStream(input, bufferSize);
    }

    public static OutputStream createOutputStream(OutputStream output, int quality, int bufferSize)
            throws IOException {
        return Initializer.createOutputStream(output, quality, bufferSize);
    }
}
