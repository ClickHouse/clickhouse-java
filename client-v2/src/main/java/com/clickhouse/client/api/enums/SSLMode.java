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
 *     <li>{@link #DISABLED} - SSL is not used. Plain protocols only.</li>
 *     <li>{@link #TRUST} - the hostname is not verified and any server certificate is accepted, which
 *     is susceptible to MITM attacks - use that only for testing or in fully trusted environments. A
 *     configured trust store or CA certificate has no effect in this mode and is ignored (a warning is
 *     logged); a configured client certificate/key is still applied for mTLS.</li>
 *     <li>{@link #VERIFY_CA} - the server certificate chain is validated against the trust material
 *     (default JVM trust store, configured trust store, or a CA certificate), but the hostname is
 *     not checked against the certificate.</li>
 *     <li>{@link #STRICT} - full verification (default): certificate chain is validated and the
 *     hostname must match the certificate.</li>
 * </ul>
 */
public enum SSLMode {

    /**
     * SSL is not used. Connection is not encrypted. Doesn't work with HTTPS.
     * Reserved for TCP where protocol doesn't define encryption.
     */
    DISABLED,

    /**
     * The hostname is not verified and any server certificate is accepted. A configured trust store or
     * CA certificate has no effect in this mode and is ignored (a warning is logged). A configured
     * client certificate/key is still applied for mTLS.
     */
    TRUST,

    /**
     * Server certificate chain is validated, but the hostname is not verified.
     */
    VERIFY_CA,

    /**
     * Full verification: certificate chain is validated and the hostname must match
     * the certificate. Default mode for HTTPs.
     */
    STRICT;

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
