package ru.yandex.clickhouse.util;

import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ClickHouseCookieStoreProviderTest {
    ClickHouseCookieStoreProvider cookieStoreProvider = new ClickHouseCookieStoreProvider();

    @Test
    public void testCookieStoreProviderWithNullHost() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseSharedCookieStore(true);
        props.setPort(8080);
        props.setDatabase("default");
        assertNull(cookieStoreProvider.getCookieStore(props));
    }

    @Test
    public void testCookieStoreProviderWithInvalidPort() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseSharedCookieStore(true);
        props.setHost("127.0.0.1");
        props.setPort(0);
        props.setDatabase("default");
        assertNull(cookieStoreProvider.getCookieStore(props));
    }

    @Test
    public void testCookieStoreProviderWithNullDBName() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseSharedCookieStore(true);
        props.setHost("127.0.0.1");
        props.setPort(0);
        assertNull(cookieStoreProvider.getCookieStore(props));
    }

    @Test
    public void testCookieStoreProviderWithSameDBAndSharedCookieStore() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseSharedCookieStore(true);
        props.setHost("127.0.0.1");
        props.setPort(0);
        props.setDatabase("default");
        assertEquals(cookieStoreProvider.getCookieStore(props), cookieStoreProvider.getCookieStore(props));
    }

    @Test
    public void testCookieStoreProviderWithSameDBAndPrivateCookieStore() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseSharedCookieStore(true);
        props.setHost("127.0.0.1");
        props.setPort(0);
        props.setDatabase("default");
        assertEquals(cookieStoreProvider.getCookieStore(props), cookieStoreProvider.getCookieStore(props));
    }

    @Test
    public void testCookieStoreProviderWithDiffDB() {
        ClickHouseProperties props1 = new ClickHouseProperties();
        props1.setUseSharedCookieStore(true);
        props1.setHost("127.0.0.1");
        props1.setPort(0);
        props1.setDatabase("default1");
        ClickHouseProperties props2 = new ClickHouseProperties(props1);
        props2.setDatabase("default2");
        assertEquals(cookieStoreProvider.getCookieStore(props1), cookieStoreProvider.getCookieStore(props2));
    }
}
