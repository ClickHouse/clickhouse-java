package com.clickhouse.client.api.transport;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class HttpEndpointTest {

    @DataProvider(name = "basicEndpointConfigs")
    public Object[][] basicEndpointConfigs() {
        return new Object[][]{
                // host, port, secure, basePath, expectedHost, expectedPort, expectedSecure, expectedPath
                {"localhost", 8123, false, "/clickhouse", "localhost", 8123, false, "/clickhouse"},
                {"example.com", 8443, true, "/api", "example.com", 8443, true, "/api"},
                {"localhost", 80, false, null, "localhost", 80, false, "/"},
                {"example.com", 443, true, null, "example.com", 443, true, "/"},
                {"localhost", 8123, false, "", "localhost", 8123, false, "/"},
                {"localhost", 8123, false, "clickhouse", "localhost", 8123, false, "/clickhouse"},
                {"example.com", 8443, true, "/sales/db", "example.com", 8443, true, "/sales/db"},
        };
    }

    @DataProvider(name = "pathsWithSpaces")
    public Object[][] pathsWithSpaces() {
        return new Object[][]{
                // host, port, secure, basePath, expectedPath, shouldContainEncodedSpaces
                {"localhost", 8123, false, "/my path with spaces", "/my path with spaces", true},
                {"localhost", 8123, false, "my path with spaces", "/my path with spaces", true},
                {"example.com", 8443, true, "/api/v1/my resource name", "/api/v1/my resource name", true},
        };
    }

    @DataProvider(name = "ipv6Addresses")
    public Object[][] ipv6Addresses() {
        return new Object[][]{
                // host, port, secure, basePath, expectedHost, expectedPort, expectedSecure, expectedPath
                {"[::1]", 8123, false, null, "[::1]", 8123, false, "/"},
                {"[2001:db8::1]", 8443, true, "/clickhouse", "[2001:db8::1]", 8443, true, "/clickhouse"},
        };
    }

    @DataProvider(name = "baseUrlConfigs")
    public Object[][] baseUrlConfigs() {
        return new Object[][]{
                // host, port, secure, basePath, expectedBaseUrl
                {"localhost", 8123, false, null, "http://localhost:8123/"},
                {"localhost", 8123, false, "/", "http://localhost:8123/"},
                {"localhost", 8123, false, "/clickhouse", "http://localhost:8123/clickhouse"},
                {"example.com", 8443, true, "/api", "https://example.com:8443/api"},
                {"example.com", 443, true, "/sales/db", "https://example.com:443/sales/db"},
                {"localhost", 80, false, "path", "http://localhost:80/path"},
        };
    }

    @Test(dataProvider = "basicEndpointConfigs")
    public void testBasicEndpointCreation(String host, int port, boolean secure, String basePath,
                                          String expectedHost, int expectedPort, boolean expectedSecure, String expectedPath) {
        HttpEndpoint endpoint = new HttpEndpoint(host, port, secure, basePath);

        Assert.assertEquals(endpoint.getHost(), expectedHost, "Host mismatch");
        Assert.assertEquals(endpoint.getPort(), expectedPort, "Port mismatch");
        Assert.assertEquals(endpoint.isSecure(), expectedSecure, "Secure flag mismatch");
        Assert.assertEquals(endpoint.getPath(), expectedPath, "Path mismatch");
        Assert.assertNotNull(endpoint.getURI(), "URI should not be null");
        Assert.assertNotNull(endpoint.toString(), "toString should not be null");
    }

    @Test(dataProvider = "pathsWithSpaces")
    public void testPathsWithSpaces(String host, int port, boolean secure, String basePath,
                                    String expectedPath, boolean shouldContainEncodedSpaces) {
        HttpEndpoint endpoint = new HttpEndpoint(host, port, secure, basePath);

        Assert.assertEquals(endpoint.getPath(), expectedPath, "Path mismatch");

        // The URI/URL should properly encode spaces
        String uriString = endpoint.getURI().toString();
        if (shouldContainEncodedSpaces) {
            Assert.assertTrue(uriString.contains("%20"), "URI should contain encoded spaces: " + uriString);
        }
    }

    @Test(dataProvider = "ipv6Addresses")
    public void testIpv6Addresses(String host, int port, boolean secure, String basePath,
                                  String expectedHost, int expectedPort, boolean expectedSecure, String expectedPath) {
        HttpEndpoint endpoint = new HttpEndpoint(host, port, secure, basePath);

        Assert.assertEquals(endpoint.getHost(), expectedHost, "Host mismatch");
        Assert.assertEquals(endpoint.getPort(), expectedPort, "Port mismatch");
        Assert.assertEquals(endpoint.isSecure(), expectedSecure, "Secure flag mismatch");
        Assert.assertEquals(endpoint.getPath(), expectedPath, "Path mismatch");
        Assert.assertNotNull(endpoint.getURI(), "URI should not be null");
    }

    @Test(dataProvider = "baseUrlConfigs")
    public void testBaseUrl(String host, int port, boolean secure, String basePath, String expectedBaseUrl) {
        HttpEndpoint endpoint = new HttpEndpoint(host, port, secure, basePath);

        Assert.assertEquals(endpoint.toString(), expectedBaseUrl, "toString should match baseURL");
    }

    @Test
    public void testSecureVsInsecureScheme() {
        HttpEndpoint insecureEndpoint = new HttpEndpoint("localhost", 8123, false, null);
        HttpEndpoint secureEndpoint = new HttpEndpoint("localhost", 8443, true, null);

        Assert.assertEquals(insecureEndpoint.getURI().getScheme(), "http", "Insecure endpoint should use http://");
        Assert.assertFalse(insecureEndpoint.isSecure(), "Insecure endpoint should return false for isSecure()");

        Assert.assertEquals(secureEndpoint.getURI().getScheme(), "https", "Insecure endpoint should not use https://");
        Assert.assertTrue(secureEndpoint.isSecure(), "Secure endpoint should return true for isSecure()");
    }

    @Test
    public void testSpecialCharactersInPath() {
        // Test various special characters that need encoding
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/path/with/special?chars#and&more");

        Assert.assertNotNull(endpoint.getURI(), "URI should be created despite special characters");

        // The path should be stored as provided (normalized with leading slash)
        Assert.assertEquals(endpoint.getPath(), "/path/with/special?chars#and&more");
    }

    @Test
    public void testEmptyAndNullBasePath() {
        HttpEndpoint nullPath = new HttpEndpoint("localhost", 8123, false, null);
        HttpEndpoint emptyPath = new HttpEndpoint("localhost", 8123, false, "");

        Assert.assertEquals(nullPath.getPath(), "/", "Null basePath should result in /");
        Assert.assertEquals(emptyPath.getPath(), "/", "Empty basePath should result in /");

        Assert.assertEquals(nullPath.getURI().toString(), "http://localhost:8123/");
        Assert.assertEquals(emptyPath.getURI().toString(), "http://localhost:8123/");
    }

    @Test
    public void testPathNormalization() {
        // Path without leading slash should get one added
        HttpEndpoint withoutSlash = new HttpEndpoint("localhost", 8123, false, "api/v1");
        HttpEndpoint withSlash = new HttpEndpoint("localhost", 8123, false, "/api/v1");

        Assert.assertEquals(withoutSlash.getPath(), "/api/v1", "Path should be normalized with leading slash");
        Assert.assertEquals(withSlash.getPath(), "/api/v1", "Path with slash should remain unchanged");
    }

    @Test
    public void testMultiplePathSegments() {
        HttpEndpoint endpoint = new HttpEndpoint("example.com", 8443, true, "/api/v1/resources/items");

        Assert.assertEquals(endpoint.getPath(), "/api/v1/resources/items");
        Assert.assertEquals(endpoint.getURI().toString(), "https://example.com:8443/api/v1/resources/items");
    }

    @Test
    public void testUtf8CharactersInPath() {

        String cyrillicPath = "/база/данных";
        HttpEndpoint cyrillicEndpoint = new HttpEndpoint("localhost", 8123, false, cyrillicPath);
        Assert.assertEquals(cyrillicEndpoint.getPath(), cyrillicPath, "Cyrillic path should be preserved");
        Assert.assertTrue(cyrillicEndpoint.getURI().toASCIIString().contains("%"),
                "Cyrillic path should be percent-encoded in ASCII representation");
    }
}
