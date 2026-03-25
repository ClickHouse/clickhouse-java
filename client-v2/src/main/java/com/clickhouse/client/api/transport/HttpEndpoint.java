package com.clickhouse.client.api.transport;

import com.clickhouse.client.api.ClientMisconfigurationException;

import java.net.URI;

public class HttpEndpoint implements Endpoint {

    private final URI uri; // only communication part

    private final String info;

    private final boolean secure;

    private final String host;

    private final int port;

    private final String path;

    public HttpEndpoint(String host, int port, boolean secure, String path){
        this.host = host;
        this.port = port;
        this.secure = secure;
        if (path != null && !path.isEmpty()) {
            // Ensure basePath starts with /
            this.path = path.startsWith("/") ? path : "/" + path;
        } else {
            this.path = "/";
        }
        
        // Use URI constructor to properly handle encoding of path segments
        // Encode path segments separately to preserve slashes
        try {
            String scheme = secure ? "https" : "http";
            String encodedPath = new URI(null, null, this.path, null).getRawPath();
            this.uri = new URI(scheme + "://" + host + ":" + port + encodedPath);
        } catch (Exception e) {
            throw new ClientMisconfigurationException("Failed to create endpoint URL", e);
        }
        this.info = uri.toString();
    }

    @Override
    public URI getURI() {
        return uri;
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

    public boolean isSecure() {
        return secure;
    }

    @Override
    public String toString() {
        return info;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof HttpEndpoint && uri.equals(((HttpEndpoint)obj).uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }
}
