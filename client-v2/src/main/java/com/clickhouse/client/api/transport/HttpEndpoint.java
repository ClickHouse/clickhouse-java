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

    public HttpEndpoint(String uri) throws MalformedURLException {
        this.uri = URI.create(uri);
        this.url = this.uri.toURL();
        this.baseURL = url.toString();
        this.info = baseURL;
        this.secure = this.uri.getScheme().equalsIgnoreCase("https");
    }

    @Override
    public String getBaseURL() {
        return baseURL;
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
