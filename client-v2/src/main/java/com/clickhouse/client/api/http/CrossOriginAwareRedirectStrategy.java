package com.clickhouse.client.api.http;

import org.apache.hc.client5.http.impl.LaxRedirectStrategy;
import org.apache.hc.client5.http.protocol.RedirectStrategy;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.protocol.HttpContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Redirect strategy with support for:
 * <ul>
 *     <li>restricting redirect status codes</li>
 *     <li>optionally allowing cross-origin redirects</li>
 *     <li>always blocking HTTP -&gt; HTTPS redirects</li>
 * </ul>
 */
public class CrossOriginAwareRedirectStrategy implements HttpRedirectPolicy, RedirectStrategy {
    private final RedirectStrategy delegate = LaxRedirectStrategy.INSTANCE;
    private final Set<Integer> allowedRedirectStatusCodes;
    private final boolean allowCrossOriginRedirects;

    public CrossOriginAwareRedirectStrategy(boolean allowCrossOriginRedirects) {
        this(Collections.<Integer>emptyList(), allowCrossOriginRedirects);
    }

    public CrossOriginAwareRedirectStrategy(Collection<Integer> allowedRedirectStatusCodes, boolean allowCrossOriginRedirects) {
        this.allowedRedirectStatusCodes = Collections.unmodifiableSet(new HashSet<Integer>(allowedRedirectStatusCodes));
        this.allowCrossOriginRedirects = allowCrossOriginRedirects;
    }

    public CrossOriginAwareRedirectStrategy withAllowedRedirectStatusCodes(Collection<Integer> allowedRedirectStatusCodes) {
        return new CrossOriginAwareRedirectStrategy(allowedRedirectStatusCodes, allowCrossOriginRedirects);
    }

    @Override
    public boolean isCrossOriginRedirectAllowed() {
        return allowCrossOriginRedirects;
    }

    @Override
    public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException {
        if (!allowedRedirectStatusCodes.contains(response.getCode()) || !delegate.isRedirected(request, response, context)) {
            return false;
        }

        URI requestUri = getRequestUri(request);
        URI redirectUri = delegate.getLocationURI(request, response, context);
        if (isHttpToHttpsRedirect(requestUri, redirectUri)) {
            return false;
        }
        if (!allowCrossOriginRedirects && !isSameOrigin(requestUri, redirectUri)) {
            return false;
        }
        return true;
    }

    @Override
    public URI getLocationURI(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException {
        return delegate.getLocationURI(request, response, context);
    }

    private static URI getRequestUri(HttpRequest request) throws HttpException {
        try {
            return request.getUri();
        } catch (URISyntaxException e) {
            throw new ProtocolException("Failed to read request URI", e);
        }
    }

    private static boolean isHttpToHttpsRedirect(URI source, URI target) {
        return "http".equalsIgnoreCase(source.getScheme()) && "https".equalsIgnoreCase(target.getScheme());
    }

    private static boolean isSameOrigin(URI source, URI target) {
        if (source.getScheme() == null || source.getHost() == null
                || target.getScheme() == null || target.getHost() == null) {
            return false;
        }
        return source.getScheme().equalsIgnoreCase(target.getScheme())
                && source.getHost().equalsIgnoreCase(target.getHost());
    }
}
