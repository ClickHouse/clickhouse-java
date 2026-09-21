package com.clickhouse.examples.web;

import com.clickhouse.examples.config.AuthProperties;
import com.clickhouse.examples.telemetry.SignalMetrics;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Rejects requests to protected endpoints that do not carry a valid API key.
 *
 * <p>Only paths under {@code /api/} are guarded; actuator/health style endpoints and the
 * root are left open. Rejected requests are counted as an OpenTelemetry metric.
 */
@Component
@Order(1)
public class ApiKeyAuthFilter extends OncePerRequestFilter {

    private final AuthProperties auth;
    private final SignalMetrics metrics;

    public ApiKeyAuthFilter(AuthProperties auth, SignalMetrics metrics) {
        this.auth = auth;
        this.metrics = metrics;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        String presented = request.getHeader(auth.headerName());
        if (!auth.isValid(presented)) {
            metrics.recordAuthFailure();
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "ApiKey header=\"" + auth.headerName() + "\"");
            response.getWriter().write("{\"error\":\"invalid or missing API key\"}");
            return;
        }
        chain.doFilter(request, response);
    }
}
