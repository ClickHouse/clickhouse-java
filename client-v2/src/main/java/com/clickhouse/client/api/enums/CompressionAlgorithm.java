package com.clickhouse.client.api.enums;

/**
 * Enumerates the compression algorithms the client can ask the server for and can apply itself.
 *
 * <p>The algorithm is requested with the HTTP content-coding of the operation - {@code Accept-Encoding}
 * for a response and {@code Content-Encoding} for a request - so a compressed body always uses the
 * algorithm of the request and never one the server picks on its own.</p>
 *
 * <ul>
 *     <li>{@link #LZ4} - default. Needs {@code org.lz4:lz4-java}, which the client depends on.</li>
 *     <li>{@link #ZSTD} - needs {@code com.github.luben:zstd-jni} on the classpath, which the client does not
 *     bring: an application that selects this algorithm declares the dependency itself.</li>
 *     <li>{@link #GZIP} - supported by the JDK, so it needs no additional dependency.</li>
 *     <li>{@link #NONE} - no compression, whatever {@code compress}/{@code decompress} are set to.</li>
 * </ul>
 */
public enum CompressionAlgorithm {

    /**
     * ClickHouse LZ4. Default algorithm.
     */
    LZ4("lz4"),

    /**
     * Zstandard. Requires {@code com.github.luben:zstd-jni} on the classpath.
     */
    ZSTD("zstd"),

    /**
     * gzip. Supported by the JDK.
     */
    GZIP("gzip"),

    /**
     * No compression.
     */
    NONE("none");

    private final String httpContentCoding;

    CompressionAlgorithm(String httpContentCoding) {
        this.httpContentCoding = httpContentCoding;
    }

    /**
     * Returns the HTTP content-coding token of the algorithm, as used in the {@code Accept-Encoding}
     * and {@code Content-Encoding} headers.
     *
     * @return content-coding token
     */
    public String getHttpContentCoding() {
        return httpContentCoding;
    }

    /**
     * Case-insensitive variant of {@link #valueOf(String)} that also accepts the content-coding token.
     *
     * @param value algorithm name or content-coding token in any case
     * @return matching algorithm
     * @throws IllegalArgumentException when the value does not match any algorithm
     */
    public static CompressionAlgorithm fromValue(String value) {
        for (CompressionAlgorithm algorithm : values()) {
            if (algorithm.name().equalsIgnoreCase(value) || algorithm.httpContentCoding.equalsIgnoreCase(value)) {
                return algorithm;
            }
        }
        throw new IllegalArgumentException("Unknown compression algorithm '" + value + "'");
    }
}
