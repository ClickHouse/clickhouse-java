package com.clickhouse.client.api.transport;

import java.net.MalformedURLException;
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

    public HttpEndpoint(String uri) throws MalformedURLException {
        this.uri = URI.create(uri);
        this.url = this.uri.toURL();
        this.baseURL = url.toString();
        this.info = baseURL;
        this.secure = this.uri.getScheme().equalsIgnoreCase("https");
        this.host = this.url.getHost();
        this.port = this.url.getPort() != -1 ? this.url.getPort() : (this.secure ? 443 : 80);
        this.path = this.uri.getPath() != null && !this.uri.getPath().isEmpty() ? this.uri.getPath() : "/";
    }

    public HttpEndpoint(String host, int port, boolean secure, String basePath) throws MalformedURLException {
        this.host = host;
        this.port = port;
        this.secure = secure;
        if (basePath != null && !basePath.isEmpty()) {
            // Ensure basePath starts with /
            this.path = basePath.startsWith("/") ? basePath : "/" + basePath;
        } else {
            this.path = "/";
        }
        
        StringBuilder uriBuilder = new StringBuilder();
        uriBuilder.append(secure ? "https" : "http");
        uriBuilder.append("://");
        uriBuilder.append(host);
        uriBuilder.append(":");
        uriBuilder.append(port);
        uriBuilder.append(this.path);
        
        String uriString = uriBuilder.toString();
        this.uri = URI.create(uriString);
        this.url = this.uri.toURL();
        this.baseURL = url.toString();
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
