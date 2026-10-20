package com.clickhouse.client.api.internal;

/**
 * Versions of the libraries that the client is built with. The build generates this class from a template, so the
 * versions are correct also when the libraries are shaded into the client and cannot report their own version.
 */
public final class DependencyVersions {

    /**
     * Version of the Apache HttpClient library.
     */
    public static final String APACHE_HTTP_CLIENT = "${apache.httpclient.version}";

    private DependencyVersions() {
    }
}
