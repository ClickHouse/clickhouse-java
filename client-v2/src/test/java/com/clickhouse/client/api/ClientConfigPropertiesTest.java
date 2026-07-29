package com.clickhouse.client.api;


import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ClientConfigPropertiesTest {

    @Test(groups = {"unit"})
    public void testToKeyValuePairs() {

        Map<String, String> map = ClientConfigProperties.toKeyValuePairs("key1=value1,key2=value2");
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("key1"), "value1");
        Assert.assertEquals(map.get("key2"), "value2");

        map = ClientConfigProperties.toKeyValuePairs("key1=value1, key2 = value2");
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("key1"), "value1");
        Assert.assertEquals(map.get("key2"), "value2");

        map = ClientConfigProperties.toKeyValuePairs("key1");
        Assert.assertEquals(map.size(), 0);

        // TODO: improve implementation
//        map = ClientConfigProperties.toKeyValuePairs("key1=value1, ,key2=value2");
//        Assert.assertEquals(map.size(), 2);
//        Assert.assertEquals(map.get("key1"), "value1");
//        Assert.assertEquals(map.get("key2"), "value2");
    }

    @DataProvider(name = "sslCipherSuites")
    public static Object[][] sslCipherSuites() {
        return new Object[][]{
                // raw ssl_cipher_suites value -> expected parsed list
                {"TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256",
                        Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256")},
                {"TLS_AES_128_GCM_SHA256", Collections.singletonList("TLS_AES_128_GCM_SHA256")},
                // each surviving name is trimmed
                {" TLS_AES_256_GCM_SHA384 , TLS_AES_128_GCM_SHA256 ",
                        Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256")},
                // blank tokens from a leading, doubled or trailing comma (and whitespace-only) are dropped
                {",TLS_AES_256_GCM_SHA384,, TLS_AES_128_GCM_SHA256 ,",
                        Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256")},
                // no usable suite -> empty list (treated downstream as "no restriction")
                {"", Collections.emptyList()},
                {"   ", Collections.emptyList()},
                {",, ,", Collections.emptyList()},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "sslCipherSuites")
    public void testSslCipherSuitesParsedAndSanitized(String raw, List<String> expected) {
        Assert.assertEquals(ClientConfigProperties.SSL_CIPHER_SUITES.parseValue(raw), expected);
    }

    @Test(groups = {"unit"})
    public void testSslCipherSuitesUnsetParsesToNull() {
        Assert.assertNull(ClientConfigProperties.SSL_CIPHER_SUITES.parseValue(null));
    }

    @Test(groups = {"unit"})
    public void testParseConfigMapSanitizesSslCipherSuites() {
        Map<String, String> raw = new HashMap<>();
        raw.put(ClientConfigProperties.SSL_CIPHER_SUITES.getKey(),
                ",TLS_AES_256_GCM_SHA384,, TLS_AES_128_GCM_SHA256 ,");
        Map<String, Object> parsed = ClientConfigProperties.parseConfigMap(raw);
        Assert.assertEquals(parsed.get(ClientConfigProperties.SSL_CIPHER_SUITES.getKey()),
                Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"));
    }
}