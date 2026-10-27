package com.clickhouse.client.api.internal;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ClientUtilsTest {

    @Test(groups = {"unit"})
    public void testQuietCloseSwallowsExceptionAndLogs() throws IOException {
        Logger log = Mockito.mock(Logger.class);
        IOException failure = new IOException("close failed");
        Closeable closeable = Mockito.mock(Closeable.class);
        Mockito.doThrow(failure).when(closeable).close();

        // Should not propagate the exception thrown by close()
        ClientUtils.quietClose(closeable, log);

        Mockito.verify(closeable).close();
        Mockito.verify(log).warn(Mockito.contains("Failed to close object"), Mockito.eq(failure));
    }

    @Test(groups = {"unit"})
    public void testQuietCloseClosesSuccessfully() throws IOException {
        Logger log = Mockito.mock(Logger.class);
        Closeable closeable = Mockito.mock(Closeable.class);

        ClientUtils.quietClose(closeable, log);

        Mockito.verify(closeable).close();
        Mockito.verifyNoInteractions(log);
    }

    @Test(groups = {"unit"})
    public void testQuietCloseWithNull() {
        Logger log = Mockito.mock(Logger.class);

        // Should be a no-op and not throw on a null closeable
        ClientUtils.quietClose(null, log);

        Mockito.verifyNoInteractions(log);
        Assert.assertTrue(true);
    }

    @Test(groups = {"unit"}, dataProvider = "putIfAbsentIgnoreCase")
    public void testPutIfAbsentIgnoreCase(String[] existingKeys, String key, String expectedMap) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String existingKey : existingKeys) {
            map.put(existingKey, "set");
        }

        ClientUtils.putIfAbsentIgnoreCase(map, key, "added");

        Assert.assertEquals(map.toString(), expectedMap);
    }

    @DataProvider(name = "putIfAbsentIgnoreCase")
    public static Object[][] putIfAbsentIgnoreCaseData() {
        return new Object[][]{
                {new String[]{}, "os", "{os=added}"},
                {new String[]{"os"}, "lv", "{os=set, lv=added}"},
                {new String[]{"os"}, "os", "{os=set}"},
                {new String[]{"os"}, "OS", "{os=set}"},
                {new String[]{"OS"}, "os", "{OS=set}"},
                {new String[]{"api", "OS"}, "os", "{api=set, OS=set}"},
        };
    }

    @Test(groups = {"unit"})
    public void testPutIfAbsentIgnoreCaseNullKey() {
        Map<String, String> map = new LinkedHashMap<>();

        Assert.expectThrows(NullPointerException.class, () -> ClientUtils.putIfAbsentIgnoreCase(map, null, "added"));
        Assert.assertTrue(map.isEmpty());
    }
}
