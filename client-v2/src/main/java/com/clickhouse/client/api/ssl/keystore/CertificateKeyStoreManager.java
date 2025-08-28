package com.clickhouse.client.api.ssl.keystore;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

/**
 * Manages KeyStore creation.
 *
 * @author sbushemelev
 */
public interface CertificateKeyStoreManager {

    /**
     * Creates KeyStore with client certificate and private key.
     */
    KeyStore getKeyStore(String clientCert, String clientKey) throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException, InvalidKeySpecException;

    /**
     * Creates KeyStore with root certificates only.
     */
    KeyStore getKeyStore(String sslRootCert) throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException;

}
