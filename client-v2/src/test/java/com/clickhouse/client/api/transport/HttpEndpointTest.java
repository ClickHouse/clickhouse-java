package com.clickhouse.client.api.transport;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;

public class HttpEndpointTest {

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_Http() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/clickhouse");
        
        Assert.assertEquals(endpoint.getHost(), "localhost");
        Assert.assertEquals(endpoint.getPort(), 8123);
        Assert.assertFalse(endpoint.isSecure());
        Assert.assertEquals(endpoint.getPath(), "/clickhouse");
        Assert.assertTrue(endpoint.getBaseURL().contains("http://localhost:8123"));
        Assert.assertTrue(endpoint.getBaseURL().contains("/clickhouse"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_Https() {
        HttpEndpoint endpoint = new HttpEndpoint("example.com", 8443, true, "/api");
        
        Assert.assertEquals(endpoint.getHost(), "example.com");
        Assert.assertEquals(endpoint.getPort(), 8443);
        Assert.assertTrue(endpoint.isSecure());
        Assert.assertEquals(endpoint.getPath(), "/api");
        Assert.assertTrue(endpoint.getBaseURL().contains("https://example.com:8443"));
        Assert.assertTrue(endpoint.getBaseURL().contains("/api"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_DefaultPortHttp() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 80, false, null);
        
        Assert.assertEquals(endpoint.getHost(), "localhost");
        Assert.assertEquals(endpoint.getPort(), 80);
        Assert.assertFalse(endpoint.isSecure());
        Assert.assertEquals(endpoint.getPath(), "/");
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_DefaultPortHttps() {
        HttpEndpoint endpoint = new HttpEndpoint("example.com", 443, true, null);
        
        Assert.assertEquals(endpoint.getHost(), "example.com");
        Assert.assertEquals(endpoint.getPort(), 443);
        Assert.assertTrue(endpoint.isSecure());
        Assert.assertEquals(endpoint.getPath(), "/");
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_NullPath() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, null);
        
        Assert.assertEquals(endpoint.getPath(), "/");
        Assert.assertTrue(endpoint.getBaseURL().contains("http://localhost:8123"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_EmptyPath() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "");
        
        Assert.assertEquals(endpoint.getPath(), "/");
        Assert.assertTrue(endpoint.getBaseURL().contains("http://localhost:8123"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_PathWithoutLeadingSlash() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "clickhouse");
        
        Assert.assertEquals(endpoint.getPath(), "/clickhouse");
        Assert.assertTrue(endpoint.getBaseURL().contains("/clickhouse"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_PathWithLeadingSlash() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/clickhouse");
        
        Assert.assertEquals(endpoint.getPath(), "/clickhouse");
        Assert.assertTrue(endpoint.getBaseURL().contains("/clickhouse"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_ComplexPath() {
        HttpEndpoint endpoint = new HttpEndpoint("example.com", 8443, true, "/sales/db");
        
        Assert.assertEquals(endpoint.getPath(), "/sales/db");
        Assert.assertTrue(endpoint.getBaseURL().contains("/sales/db"));
    }

    @Test(groups = {"unit"})
    public void testGetURL() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/clickhouse");
        
        URL url = endpoint.getURL();
        Assert.assertNotNull(url);
        Assert.assertEquals(url.getHost(), "localhost");
        Assert.assertEquals(url.getPort(), 8123);
        Assert.assertTrue(url.getPath().contains("clickhouse"));
    }

    @Test(groups = {"unit"})
    public void testGetURI() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/clickhouse");
        
        URI uri = endpoint.getURI();
        Assert.assertNotNull(uri);
        Assert.assertEquals(uri.getHost(), "localhost");
        Assert.assertEquals(uri.getPort(), 8123);
        Assert.assertTrue(uri.getPath().contains("clickhouse"));
    }

    @Test(groups = {"unit"})
    public void testToString() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/clickhouse");
        
        String str = endpoint.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("localhost"));
        Assert.assertTrue(str.contains("8123"));
        Assert.assertTrue(str.contains("clickhouse"));
    }

    @Test(groups = {"unit"})
    public void testImplementsEndpoint() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, null);
        
        Assert.assertTrue(endpoint instanceof Endpoint);
        Endpoint endpointInterface = endpoint;
        Assert.assertEquals(endpointInterface.getHost(), "localhost");
        Assert.assertEquals(endpointInterface.getPort(), 8123);
        Assert.assertTrue(endpointInterface.getBaseURL().contains("localhost"));
        Assert.assertTrue(endpointInterface.getBaseURL().contains("8123"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_PathWithSpaces() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/my path with spaces");
        
        Assert.assertEquals(endpoint.getHost(), "localhost");
        Assert.assertEquals(endpoint.getPort(), 8123);
        Assert.assertFalse(endpoint.isSecure());
        // Path should be stored as-is (decoded)
        Assert.assertEquals(endpoint.getPath(), "/my path with spaces");
        // baseURL should have encoded spaces (%20)
        Assert.assertTrue(endpoint.getBaseURL().contains("%20"));
        Assert.assertTrue(endpoint.getBaseURL().contains("my"));
        Assert.assertTrue(endpoint.getBaseURL().contains("path"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_PathWithSpacesWithoutLeadingSlash() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "my path with spaces");
        
        Assert.assertEquals(endpoint.getHost(), "localhost");
        Assert.assertEquals(endpoint.getPort(), 8123);
        // Path should have leading slash added
        Assert.assertEquals(endpoint.getPath(), "/my path with spaces");
        // baseURL should have encoded spaces
        Assert.assertTrue(endpoint.getBaseURL().contains("%20"));
    }

    @Test(groups = {"unit"})
    public void testConstructorWithHostPortSecurePath_PathWithMultipleSpaces() {
        HttpEndpoint endpoint = new HttpEndpoint("example.com", 8443, true, "/api/v1/my resource name");
        
        Assert.assertEquals(endpoint.getHost(), "example.com");
        Assert.assertEquals(endpoint.getPort(), 8443);
        Assert.assertTrue(endpoint.isSecure());
        // Path should be stored as-is (decoded)
        Assert.assertEquals(endpoint.getPath(), "/api/v1/my resource name");
        // baseURL should have encoded spaces
        Assert.assertTrue(endpoint.getBaseURL().contains("%20"));
        Assert.assertTrue(endpoint.getBaseURL().contains("api"));
        Assert.assertTrue(endpoint.getBaseURL().contains("v1"));
    }

    @Test(groups = {"unit"})
    public void testGetURI_PathWithSpaces() {
        HttpEndpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/my path");
        
        URI uri = endpoint.getURI();
        Assert.assertNotNull(uri);
        // URI.getPath() decodes spaces
        Assert.assertTrue(uri.getPath().contains("my"));
        Assert.assertTrue(uri.getPath().contains("path"));
        Assert.assertEquals(endpoint.getPath(), "/my path");
    }

    @Test(groups = {"unit"})
    public void testIPv6Address() {
        HttpEndpoint endpoint = new HttpEndpoint("[::1]", 8123, false, null);
        
        Assert.assertEquals(endpoint.getHost(), "[::1]");
        Assert.assertEquals(endpoint.getPort(), 8123);
        Assert.assertEquals(endpoint.getPath(), "/");
    }

    @Test(groups = {"unit"})
    public void testIPv6AddressWithPath() {
        HttpEndpoint endpoint = new HttpEndpoint("[2001:db8::1]", 8443, true, "/clickhouse");
        
        Assert.assertEquals(endpoint.getHost(), "[2001:db8::1]");
        Assert.assertEquals(endpoint.getPort(), 8443);
        Assert.assertTrue(endpoint.isSecure());
        Assert.assertEquals(endpoint.getPath(), "/clickhouse");
    }
}
