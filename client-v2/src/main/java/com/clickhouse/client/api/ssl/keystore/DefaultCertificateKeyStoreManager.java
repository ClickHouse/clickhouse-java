package com.clickhouse.client.api.ssl.keystore;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.data.ClickHouseUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;

public final class DefaultCertificateKeyStoreManager implements CertificateKeyStoreManager {

    private static final String PEM_HEADER_PREFIX = "---BEGIN ";
    private static final String PEM_HEADER_SUFFIX = " PRIVATE KEY---";
    private static final String PEM_FOOTER_PREFIX = "---END ";

    private final Map<String, Object> config;

    public DefaultCertificateKeyStoreManager(Map<String, Object> config) {
        this.config = config;
    }

    public KeyStore getKeyStore(String clientCert, String clientKey) throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException, InvalidKeySpecException {
        final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream in = ClickHouseUtils.getFileInputStream(clientCert)) {
            CertificateFactory factory = CertificateFactory.getInstance((String) this.config.get(ClientConfigProperties.SSL_CERTIFICATE_TYPE.getKey()));
            Certificate[] certChain = factory.generateCertificates(in).toArray(new Certificate[0]);
            ks.setKeyEntry("key", getPrivateKey(clientKey), null, certChain);
        }
        return ks;
    }

    public KeyStore getKeyStore(String sslRootCert) throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
        final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream in = ClickHouseUtils.getFileInputStream(sslRootCert)) {
            CertificateFactory factory = CertificateFactory.getInstance((String) this.config.get(ClientConfigProperties.SSL_CERTIFICATE_TYPE.getKey()));
            int index = 1;
            for (Certificate c : factory.generateCertificates(in)) {
                ks.setCertificateEntry("cert" + (index++), c);
            }
        }
        return ks;
    }

    private PrivateKey getPrivateKey(String keyFile) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        String algorithm = (String) this.config.get(ClientConfigProperties.SSL_KEY_ALGORITHM.getKey());
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(ClickHouseUtils.getFileInputStream(keyFile)))) {
            String line = reader.readLine();
            if (line != null) {
                algorithm = getAlgorithm(line, algorithm);

                while ((line = reader.readLine()) != null) {
                    if (line.contains(PEM_FOOTER_PREFIX)) {
                        break;
                    }
                    builder.append(line);
                }
            }
        }
        byte[] encoded = Base64.getDecoder().decode(builder.toString());
        KeyFactory kf = KeyFactory.getInstance(algorithm);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        return kf.generatePrivate(keySpec);
    }

    private String getAlgorithm(String header, String defaultAlg) {
        int startIndex = header.indexOf(PEM_HEADER_PREFIX);
        int endIndex = startIndex < 0 ? startIndex
                : header.indexOf(PEM_HEADER_SUFFIX, (startIndex += PEM_HEADER_PREFIX.length()));
        return startIndex < endIndex ? header.substring(startIndex, endIndex) : defaultAlg;
    }

}
