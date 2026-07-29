package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.client.api.internal.HttpAPIClientHelper;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.serde.DataSerializationException;
import com.clickhouse.client.api.transport.Endpoint;
import com.clickhouse.client.api.transport.internal.TransportRequest;
import com.clickhouse.client.api.transport.internal.TransportResponse;
import com.clickhouse.data.ClickHouseColumn;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.core5.io.IOCallback;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.SocketException;
import java.util.Collections;
import java.util.Map;

public class InsertExceptionClassificationTest {

    @Test
    public void insertPojoWriteFailureIsReportedAsDataTransferException() throws Exception {
        Client client = newClient(new CallbackHttpClientHelper(new ThresholdFailingOutputStream(64,
                new SocketException("Broken pipe (Write failed)"))));
        try {
            client.register(WritePojo.class, new TableSchema("test_table", null, "",
                    Collections.singletonList(ClickHouseColumn.of("value", "String"))));

            DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                    () -> client.insert("test_table", Collections.singletonList(new WritePojo(repeat('x', 4096)))));

            Assert.assertTrue(ex.getCause() instanceof SocketException, "Unexpected cause: " + ex.getCause());
            Assert.assertFalse(hasCause(ex, DataSerializationException.class),
                    "Network write failure must not be wrapped as DataSerializationException");
        } finally {
            client.close();
        }
    }

    @Test
    public void insertPojoGetterFailureRemainsDataSerializationException() throws Exception {
        Client client = newClient(new CallbackHttpClientHelper(new ByteArrayOutputStream()));
        try {
            client.register(BrokenGetterPojo.class, new TableSchema("test_table", null, "",
                    Collections.singletonList(ClickHouseColumn.of("value", "String"))));

            DataSerializationException ex = Assert.expectThrows(DataSerializationException.class,
                    () -> client.insert("test_table", Collections.singletonList(new BrokenGetterPojo())));

            Assert.assertNotNull(ex.getCause(), "Expected original reflection failure in cause chain");
            Assert.assertTrue(ex.getMessage().contains("Failed to serialize data"),
                    "Unexpected message: " + ex.getMessage());
        } finally {
            client.close();
        }
    }

    private static Client newClient(HttpAPIClientHelper helper) throws Exception {
        Client client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setUsername("default")
                .setPassword("")
                .build();

        Field httpClientHelperField = Client.class.getDeclaredField("httpClientHelper");
        httpClientHelperField.setAccessible(true);
        HttpAPIClientHelper original = (HttpAPIClientHelper) httpClientHelperField.get(client);
        if (original != null) {
            original.close();
        }
        httpClientHelperField.set(client, helper);
        return client;
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> expectedType) {
        Throwable current = throwable;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String repeat(char ch, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, ch);
        return new String(chars);
    }

    private static final class CallbackHttpClientHelper extends HttpAPIClientHelper {
        private final OutputStream outputStream;
        private IOCallback<OutputStream> writeCallback;
        private CallbackHttpClientHelper(OutputStream outputStream) {
            super(Collections.emptyMap(), null, false, LZ4Factory.fastestJavaInstance());
            this.outputStream = outputStream;
        }

        @Override
        public TransportRequest createRequest(Endpoint server, Map<String, Object> requestConfig, String body) {
            return null;
        }

        @Override
        public TransportRequest createRequest(Endpoint server, Map<String, Object> requestConfig, IOCallback<OutputStream> writeCallback) {
            this.writeCallback = writeCallback;
            return null;
        }

        @Override
        public TransportResponse executeRequest(TransportRequest request) throws Exception {
            writeCallback.execute(outputStream);
            return Mockito.mock(TransportResponse.class);
        }

        @Override
        public void close() {
            // No-op for tests.
        }
    }

    private static final class ThresholdFailingOutputStream extends OutputStream {
        private final int failAfterBytes;
        private final IOException failure;
        private int bytesWritten;

        private ThresholdFailingOutputStream(int failAfterBytes, IOException failure) {
            this.failAfterBytes = failAfterBytes;
            this.failure = failure;
        }

        @Override
        public void write(int b) throws IOException {
            if (bytesWritten >= failAfterBytes) {
                throw failure;
            }
            bytesWritten++;
        }
    }

    public static final class WritePojo {
        private final String value;

        public WritePojo(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static final class BrokenGetterPojo {
        public String getValue() {
            throw new IllegalStateException("boom");
        }
    }
}
