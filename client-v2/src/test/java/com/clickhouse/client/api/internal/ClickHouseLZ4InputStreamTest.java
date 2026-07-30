package com.clickhouse.client.api.internal;

import net.jpountz.lz4.LZ4Factory;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

public class ClickHouseLZ4InputStreamTest {

    @Test(groups = {"unit"})
    public void testCloseClosesUnderlyingStream() throws IOException {
        InputStream input = Mockito.mock(InputStream.class);
        ClickHouseLZ4InputStream stream = new ClickHouseLZ4InputStream(
                input,
                LZ4Factory.fastestJavaInstance().fastDecompressor(),
                1024);

        stream.close();

        Mockito.verify(input).close();
    }
}
