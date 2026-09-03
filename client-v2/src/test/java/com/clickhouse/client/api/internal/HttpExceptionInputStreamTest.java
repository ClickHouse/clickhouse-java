package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class HttpExceptionInputStreamTest {

    private static final String EXCEPTION_TAG = "0123456789abcdef";
    private static final String ERROR_MESSAGE =
            "Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)";

    @Test
    public void shouldDetectExceptionAcrossReadBoundaries() throws Exception {
        byte[] resultPrefix = "result-data".getBytes(StandardCharsets.UTF_8);
        byte[] body = responseBody(resultPrefix, exceptionFrame(EXCEPTION_TAG));
        InputStream fragmentedSource = new FilterInputStream(new ByteArrayInputStream(body)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return super.read(buffer, offset, Math.min(length, 1));
            }
        };

        try (InputStream input = new HttpExceptionInputStream(fragmentedSource, EXCEPTION_TAG, 200, "query-id")) {
            byte[] actualPrefix = new byte[resultPrefix.length];
            int offset = 0;
            while (offset < actualPrefix.length) {
                int read = input.read(actualPrefix, offset, actualPrefix.length - offset);
                Assert.assertTrue(read > 0);
                offset += read;
            }
            Assert.assertEquals(actualPrefix, resultPrefix);

            ServerException exception = Assert.expectThrows(ServerException.class, input::read);
            Assert.assertEquals(exception.getCode(), 159);
            Assert.assertEquals(exception.getQueryId(), "query-id");
        }
    }

    @Test
    public void shouldIgnoreFrameWithMismatchedTag() throws Exception {
        byte[] resultPrefix = "result-data".getBytes(StandardCharsets.UTF_8);
        byte[] body = responseBody(resultPrefix, exceptionFrame("fedcba9876543210"));

        try (InputStream input = new HttpExceptionInputStream(
                new ByteArrayInputStream(body), EXCEPTION_TAG, 200, "query-id")) {
            Assert.assertEquals(readAll(input), body);
        }
    }

    @Test
    public void shouldPreserveServerExceptionWhenFrameReadFails() throws Exception {
        byte[] resultPrefix = "result-data".getBytes(StandardCharsets.UTF_8);
        byte[] body = responseBody(resultPrefix, exceptionFrame(EXCEPTION_TAG));
        InputStream failingSource = new FilterInputStream(new ByteArrayInputStream(body)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                int read = super.read(buffer, offset, length);
                if (read < 0) {
                    throw new IOException("truncated response");
                }
                return read;
            }
        };

        try (InputStream input = new HttpExceptionInputStream(failingSource, EXCEPTION_TAG, 200, "query-id")) {
            byte[] actualPrefix = new byte[resultPrefix.length];
            int offset = 0;
            while (offset < actualPrefix.length) {
                int read = input.read(actualPrefix, offset, actualPrefix.length - offset);
                Assert.assertTrue(read > 0);
                offset += read;
            }
            Assert.assertEquals(actualPrefix, resultPrefix);

            ClientException exception = Assert.expectThrows(ClientException.class, input::read);
            Assert.assertTrue(exception.getCause() instanceof ServerException);
            Assert.assertEquals(((ServerException) exception.getCause()).getCode(), 159);
            Assert.assertEquals(exception.getSuppressed().length, 1);
            Assert.assertEquals(exception.getSuppressed()[0].getMessage(), "truncated response");
        }
    }

    private static byte[] responseBody(byte[] resultPrefix, byte[] exceptionFrame) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(resultPrefix);
        body.write(exceptionFrame);
        return body.toByteArray();
    }

    private static byte[] exceptionFrame(String tag) {
        String frame = "\r\n__exception__\r\n" + tag + "\r\n" + ERROR_MESSAGE
                + "\r\n" + ERROR_MESSAGE.getBytes(StandardCharsets.UTF_8).length + " " + tag
                + "\r\n__exception__\r\n";
        return frame.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] readAll(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[32];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }
}
