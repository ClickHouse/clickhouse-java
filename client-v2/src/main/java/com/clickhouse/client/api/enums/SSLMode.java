package com.clickhouse.client.api.enums;

/**
 * Defines how strictly the client verifies a server identity when a secure protocol is used.
 *
 * <p>The mode affects only connections that are already using a secure transport (for example,
 * an {@code https://} endpoint). It does <b>not</b> enable encryption for plain protocols - an
 * {@code http://} endpoint stays unencrypted whatever the mode is.</p>
 *
 * <p>Modes from the least to the most strict:</p>
 * <ul>
 *     <li>{@link #Disabled} - SSL is not used. Plain protocols only.</li>
 *     <li>{@link #Trust} - encryption is used, but the server certificate chain is not validated
 *     and the hostname is not verified. Susceptible to MITM attacks - use only for testing or in
 *     fully trusted environments.</li>
 *     <li>{@link #VerifyCa} - the server certificate chain is validated against the trust material
 *     (default JVM trust store, configured trust store, or a CA certificate), but the hostname is
 *     not checked against the certificate.</li>
 *     <li>{@link #Strict} - full verification (default): certificate chain is validated and the
 *     hostname must match the certificate.</li>
 * </ul>
 */
public enum SSLMode {

    /**
     * SSL is not used. Connection is not encrypted.
     */
    Disabled,

    /**
     * Encryption without verification: any server certificate is accepted and
     * the hostname is not verified.
     */
    Trust,

    /**
     * Server certificate chain is validated, but the hostname is not verified.
     */
    VerifyCa,

    /**
     * Full verification: certificate chain is validated and the hostname must match
     * the certificate. Default mode.
     */
    Strict;

    /**
     * Case-insensitive variant of {@link #valueOf(String)}.
     *
     * @param value mode name in any case
     * @return matching mode
     * @throws IllegalArgumentException when the value does not match any mode
     */
    public static SSLMode fromValue(String value) {
        for (SSLMode mode : values()) {
            if (mode.name().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown SSL mode '" + value + "'");
    }
}
