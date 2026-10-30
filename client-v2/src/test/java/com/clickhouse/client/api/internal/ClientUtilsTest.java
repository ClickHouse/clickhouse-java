package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ServerException;
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

    @Test(groups = {"unit"}, dataProvider = "findCause")
    public void testFindCause(int wrappers, Integer maxDepth, boolean found) {
        ServerException target = new ServerException(ServerException.NOTHING_TO_KILL, "No query to kill", 500, "q1");
        Throwable throwable = target;
        for (int i = 0; i < wrappers; i++) {
            throwable = new RuntimeException("wrapper " + i, throwable);
        }

        ServerException actual = maxDepth == null
                ? ClientUtils.findCause(throwable, ServerException.class)
                : ClientUtils.findCause(throwable, ServerException.class, maxDepth);

        Assert.assertSame(actual, found ? target : null);
    }

    @DataProvider(name = "findCause")
    public static Object[][] findCauseData() {
        return new Object[][]{
                {0, null, true},
                {3, null, true},
                {4, null, false},
                {0, 0, true},
                {1, 0, false},
                {5, 5, true},
        };
    }

    @Test(groups = {"unit"})
    public void testFindCauseReturnsNearestMatch() {
        IllegalArgumentException outer = new IllegalArgumentException("outer", new IllegalStateException("inner"));

        Assert.assertSame(ClientUtils.findCause(new Exception(outer), RuntimeException.class), outer);
    }

    @Test(groups = {"unit"}, timeOut = 10000)
    public void testFindCauseWithoutMatch() {
        Exception first = new Exception("first");
        Exception second = new Exception("second", first);
        first.initCause(second);

        Assert.assertNull(ClientUtils.findCause(null, ServerException.class));
        Assert.assertNull(ClientUtils.findCause(new RuntimeException(new IOException()), ServerException.class));
        Assert.assertNull(ClientUtils.findCause(first, ServerException.class, 10));
    }

    @Test(groups = {"unit"})
    public void testFindCauseInvalidArguments() {
        Throwable throwable = new RuntimeException();

        Assert.expectThrows(NullPointerException.class, () -> ClientUtils.findCause(throwable, null));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> ClientUtils.findCause(throwable, RuntimeException.class, -1));
    }
}
