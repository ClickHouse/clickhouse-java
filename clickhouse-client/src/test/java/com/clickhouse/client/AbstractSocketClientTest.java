package com.clickhouse.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AbstractSocketClientTest {
    public static class CustomListFactory implements ClickHouseSocketFactory {
        @Override
        public <T> T create(ClickHouseConfig config, Class<T> clazz) throws IOException, UnsupportedOperationException {
            return null;
        }

        @Override
        public boolean supports(Class<?> clazz) {
            return List.class.isAssignableFrom(clazz);
        }
    }

    @Test(groups = { "unit" })
    public void testGetCustomSocketFactory() {
        CustomListFactory defaultFactory = new CustomListFactory();

        Assert.assertEquals(AbstractSocketClient.getCustomSocketFactory(null, null, null), null);
        Assert.assertEquals(AbstractSocketClient.getCustomSocketFactory(null, defaultFactory, null), defaultFactory);
        Assert.assertEquals(AbstractSocketClient.getCustomSocketFactory("", defaultFactory, null), defaultFactory);

        Assert.assertEquals(AbstractSocketClient.getCustomSocketFactory(CustomListFactory.class.getName(),
                defaultFactory, List.class),
                AbstractSocketClient.getCustomSocketFactory(CustomListFactory.class.getName(),
                        defaultFactory, List.class));

        ClickHouseSocketFactory factory = AbstractSocketClient.getCustomSocketFactory(CustomListFactory.class.getName(),
                defaultFactory, List.class);
        Assert.assertEquals(factory.getClass(), defaultFactory.getClass());
        Assert.assertNotEquals(factory, defaultFactory);

        factory = AbstractSocketClient.getCustomSocketFactory(CustomListFactory.class.getName(),
                defaultFactory, Map.class);
        Assert.assertEquals(factory, defaultFactory);
    }
}
