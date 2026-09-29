package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.TimeZone;

public class HttpExceptionInputStreamTest {

    private static final String EXCEPTION_TAG = "0123456789abcdef";
    private static final String ERROR_MESSAGE =
            "Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)\n";

    @DataProvider(name = "iterationModes")
    public Object[][] iterationModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "iterationModes")
    public void shouldDeliverLastRowBeforePrefetchFailure(boolean useHasNext) throws Exception {
        byte[] rows = {1, 1, 'v', 5, 'U', 'I', 'n', 't', '8', 1, 2};
        QuerySettings settings = new QuerySettings().setOption(
                ClientConfigProperties.USE_TIMEZONE.getKey(), TimeZone.getTimeZone("UTC"));
        try (InputStream input = new HttpExceptionInputStream(new ByteArrayInputStream(
                responseBody(rows, exceptionFrame(EXCEPTION_TAG))), EXCEPTION_TAG, 200, "query-id");
             RowBinaryWithNamesAndTypesFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(
                     input, settings, new BinaryStreamReader.DefaultByteBufferAllocator())) {
            for (int expected = 1; expected <= 2; expected++) {
                if (useHasNext) {
                    Assert.assertTrue(reader.hasNext());
                }
                Assert.assertEquals(((Number) reader.next().get("v")).intValue(), expected);
            }
            ServerException exception = useHasNext
                    ? Assert.expectThrows(ServerException.class, reader::hasNext)
                    : Assert.expectThrows(ServerException.class, reader::next);
            Assert.assertEquals(exception.getCode(), 159);
            Assert.assertSame(Assert.expectThrows(ServerException.class, reader::next), exception);
        }
    }

    @DataProvider(name = "smallReads")
    public Object[][] smallReads() {
        return new Object[][] {{0}, {1}, {8}};
    }

    @Test(dataProvider = "smallReads")
    public void shouldNotDrainResponseOnSmallRead(int readSize) throws Exception {
        byte[] body = "result-data-that-must-remain-unread".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream source = new ByteArrayInputStream(body);
        try (InputStream input = new HttpExceptionInputStream(source, EXCEPTION_TAG, 200, "query-id")) {
            if (readSize == 0) {
                Assert.assertEquals(input.read(), body[0]);
            } else {
                byte[] buffer = new byte[readSize];
                Assert.assertEquals(input.read(buffer), readSize);
                for (int i = 0; i < buffer.length; i++) {
                    Assert.assertEquals(buffer[i], body[i]);
                }
            }
            Assert.assertEquals(source.available(), body.length - Math.max(1, readSize));
        }
    }

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
        byte[] frame = exceptionFrame(EXCEPTION_TAG);
        byte[] body = responseBody(resultPrefix, Arrays.copyOf(frame, frame.length - 4));
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

    @DataProvider(name = "invalidFrames")
    public Object[][] invalidFrames() {
        String valid = new String(exceptionFrame(EXCEPTION_TAG), StandardCharsets.UTF_8);
        return new Object[][] {
                {valid.substring(0, valid.length() - 4)},
                {valid.replace(ERROR_MESSAGE.length() + " " + EXCEPTION_TAG, "1 " + EXCEPTION_TAG)},
                {valid.replace(ERROR_MESSAGE.length() + " " + EXCEPTION_TAG,
                        ERROR_MESSAGE.length() + " fedcba9876543210")}
        };
    }

    @Test(dataProvider = "invalidFrames")
    public void shouldRejectIncompleteOrInvalidFrameAtEof(String frame) throws Exception {
        try (InputStream input = new HttpExceptionInputStream(new ByteArrayInputStream(
                frame.getBytes(StandardCharsets.UTF_8)), EXCEPTION_TAG, 200, "query-id")) {
            ClientException exception = Assert.expectThrows(ClientException.class, input::read);
            Assert.assertEquals(exception.getMessage(), "Incomplete ClickHouse exception frame");
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
                + ERROR_MESSAGE.getBytes(StandardCharsets.UTF_8).length + " " + tag
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
