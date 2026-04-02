package com.clickhouse.client.api.transport;

import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.internal.ValidationUtils;

import java.net.URI;
import java.net.MalformedURLException;
import java.net.URL;

public class HttpEndpoint implements Endpoint {

    private final URI uri; // only communication part

    private final String info;

    private final boolean secure;

    private final String host;

    private final int port;

    private final String path;

    public HttpEndpoint(String endpoint) {
        this(parseEndpointUrl(endpoint));
    }

    public HttpEndpoint(String host, int port, boolean secure, String path) {
        this(new EndpointDetails(validateHost(host), validatePort(port), secure, normalizePath(path)));
    }

    private HttpEndpoint(URL endpointUrl) {
        this(new EndpointDetails(
                validateHost(endpointUrl.getHost()),
                validatePort(endpointUrl.getPort()),
                isSecure(endpointUrl.getProtocol()),
                decodePath(endpointUrl.getPath())));
    }

    private HttpEndpoint(EndpointDetails endpointDetails) {
        this.host = endpointDetails.host;
        this.port = endpointDetails.port;
        this.secure = endpointDetails.secure;
        this.path = endpointDetails.path;
        this.uri = createUri(endpointDetails.host, endpointDetails.port, endpointDetails.secure, endpointDetails.path);
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

    private static URL parseEndpointUrl(String endpoint) {
        try {
            return new URL(endpoint);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Failed to parse endpoint URL", e);
        }
    }

    private static String validateHost(String host) {
        ValidationUtils.checkNonBlank(host, "host");
        return host;
    }

    private static int validatePort(int port) {
        if (port <= 0) {
            throw new ValidationUtils.SettingsValidationException("port", "Valid port must be specified");
        }
        ValidationUtils.checkRange(port, 1, ValidationUtils.TCP_PORT_NUMBER_MAX, "port");
        return port;
    }

    private static boolean isSecure(String protocol) {
        if ("https".equalsIgnoreCase(protocol)) {
            return true;
        }
        if ("http".equalsIgnoreCase(protocol)) {
            return false;
        }
        throw new IllegalArgumentException("Only HTTP and HTTPS protocols are supported");
    }

    private static String normalizePath(String path) {
        if (path != null && !path.isEmpty()) {
            return path.startsWith("/") ? path : "/" + path;
        }
        return "/";
    }

    private static String decodePath(String path) {
        String normalizedPath = normalizePath(path);
        return URI.create(normalizedPath.replace(" ", "%20")).getPath();
    }

    private static URI createUri(String host, int port, boolean secure, String path) {
        try {
            String scheme = secure ? "https" : "http";
            String authority = host + ":" + port;
            return new URI(scheme, authority, path, null, null);
        } catch (Exception e) {
            throw new ClientMisconfigurationException("Failed to create endpoint URL", e);
        }
    }

    private static final class EndpointDetails {
        private final String host;
        private final int port;
        private final boolean secure;
        private final String path;

        private EndpointDetails(String host, int port, boolean secure, String path) {
            this.host = host;
            this.port = port;
            this.secure = secure;
            this.path = path;
        }
    }
}
