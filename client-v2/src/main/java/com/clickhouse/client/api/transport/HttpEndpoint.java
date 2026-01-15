package com.clickhouse.client.api.transport;

import com.clickhouse.client.api.ClientMisconfigurationException;

import java.net.URI;
import java.net.URL;

public class HttpEndpoint implements Endpoint {

    private final URI uri; // contains complete connection URL + parameters

    private final URL url; // only communication part

    private final String baseURL;

    private final String info;

    private final boolean secure;

    private final String host;

    private final int port;

    private final String path;

    public HttpEndpoint(String host, int port, boolean secure, String basePath){
        this.host = host;
        this.port = port;
        this.secure = secure;
        if (basePath != null && !basePath.isEmpty()) {
            // Ensure basePath starts with /
            this.path = basePath.startsWith("/") ? basePath : "/" + basePath;
        } else {
            this.path = "/";
        }
        
        // Use URI constructor to properly handle encoding of path segments
        try {
            this.uri = new URI(secure ? "https" : "http", null, host, port, path, null, null);
            this.url = this.uri.toURL();
        } catch (Exception e) {
            throw new ClientMisconfigurationException("Failed to create endpoint URL", e);
        }
        this.baseURL = uri.toString();
        this.info = baseURL;
    }

    @Override
    public String getBaseURL() {
        return baseURL;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }

    public URL getURL() {
        return url;
    }

    public URI getURI() {
        return uri;
    }

    public boolean isSecure() {
        return secure;
    }

    @Override
    public String toString() {
        return info;
    }
}
