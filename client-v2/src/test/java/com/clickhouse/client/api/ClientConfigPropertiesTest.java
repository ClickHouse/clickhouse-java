package com.clickhouse.client.api;


import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    @DataProvider(name = "validClientMetadata")
    public static Object[][] validClientMetadata() {
        return new Object[][]{
                {"fv=flink/2.0.0,api=table", new String[]{"fv", "flink/2.0.0", "api", "table"}},
                {" api = table , os_version = Mac OS X ", new String[]{"api", "table", "os_version", "Mac OS X"}},
                {"url=http://host:8123/path?x=1", new String[]{"url", "http://host:8123/path?x=1"}},
                {"api=,fv=flink/2.0.0,=x", new String[]{"fv", "flink/2.0.0"}},
                {"", new String[0]},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "validClientMetadata")
    public void testClientMetadataParsed(String raw, String[] expectedEntries) {
        Map<String, String> expected = new LinkedHashMap<>();
        for (int i = 0; i < expectedEntries.length; i += 2) {
            expected.put(expectedEntries[i], expectedEntries[i + 1]);
        }
        Map<?, ?> parsed = (Map<?, ?>) ClientConfigProperties.CLIENT_METADATA.parseValue(raw);
        Assert.assertEquals(new ArrayList<>(parsed.entrySet()), new ArrayList<>(expected.entrySet()));
    }

    @DataProvider(name = "invalidClientMetadata")
    public static Object[][] invalidClientMetadata() {
        return new Object[][]{
                {"a:b=c"}, {"a;b=c"}, {"a(b=c"}, {"a)b=c"}, {"a\\,b=c"}, {"a\\=b=c"}, {"my key=c"},
                {"k=v;x"}, {"k=v(x"}, {"k=v)x"},
                {"k=a\nb"}, {"k=a\tb"}, {"k=caf\u00e9"}, {"k\u00e9=v"},
                {"lv=java/8"}, {"os=Linux"}, {"OS=Linux"},
                {"fv=flink/2.0.0,api=table; lv:java/8"},
                {",api=table"}, {"fv=flink/2.0.0,,api=table"}, {"fv=flink/2.0.0,api,ver=1"},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "invalidClientMetadata")
    public void testInvalidClientMetadataRejected(String raw) {
        Assert.expectThrows(ClientMisconfigurationException.class,
                () -> ClientConfigProperties.CLIENT_METADATA.parseValue(raw));
    }

    @Test(groups = {"unit"})
    public void testClientMetadataUnsetParsesToNull() {
        Assert.assertNull(ClientConfigProperties.CLIENT_METADATA.parseValue(null));
    }

    @Test(groups = {"unit"})
    public void testFromKeyValuePairsRoundTrip() {
        Map<String, String> pairs = new LinkedHashMap<>();
        pairs.put("a=b", "c,d\\e=f");
        pairs.put("x,y", "z");
        Map<String, String> parsed = ClientConfigProperties.toKeyValuePairs(ClientConfigProperties.fromKeyValuePairs(pairs));
        Assert.assertEquals(new ArrayList<>(parsed.entrySet()), new ArrayList<>(pairs.entrySet()));
    }
}