package com.clickhouse.client.config;

import com.clickhouse.data.ClickHouseUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;

public class ClickHouseDefaultSslContextProviderTest {
    static String readTestResource(String name) throws Exception {
        try (InputStream in = ClickHouseUtils.getFileInputStream(name)) {
            return new String(in.readAllBytes(), StandardCharsets.US_ASCII);
        }
    }

    @Test(groups = { "unit" })
    public void testGetAlgorithm() {
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("", null), null);
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("---BEGIN ", "x"), "x");
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("---BEGIN PRIVATE KEY---", "x"), "x");
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("---BEGIN  PRIVATE KEY---", "x"), "x");
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("-----BEGIN RSA PRIVATE KEY-----", ""),
                "RSA");
    }

    @Test(groups = { "unit" })
    public void testGetPrivateKey() throws Exception {
        // openssl genpkey -out pkey4test.pem -algorithm RSA -pkeyopt rsa_keygen_bits:2048
        Assert.assertNotNull(ClickHouseDefaultSslContextProvider.getPrivateKey("pkey4test.pem"));
    }

    @Test(groups = { "unit" })
    public void testGetCertificateInputStream() throws Exception {
        String pemContent = readTestResource("client.crt");
        try (InputStream in = ClickHouseDefaultSslContextProvider.getCertificateInputStream(pemContent)) {
            byte[] buffer = new byte[pemContent.length()];
            int read = in.read(buffer);
            Assert.assertEquals(new String(buffer, 0, read, StandardCharsets.US_ASCII), pemContent);
        }

        try (InputStream in = ClickHouseDefaultSslContextProvider.getCertificateInputStream("client.crt")) {
            Assert.assertTrue(in.read() != -1);
        }

        Assert.assertThrows(FileNotFoundException.class,
                () -> ClickHouseDefaultSslContextProvider.getCertificateInputStream("non-existent.crt"));
    }

    @Test(groups = { "unit" })
    public void testGetPrivateKeyFromPemContent() throws Exception {
        PrivateKey fromFile = ClickHouseDefaultSslContextProvider.getPrivateKey("pkey4test.pem");
        PrivateKey fromContent = ClickHouseDefaultSslContextProvider
                .getPrivateKey(readTestResource("pkey4test.pem"));
        Assert.assertEquals(fromContent, fromFile);
        Assert.assertEquals(fromContent.getAlgorithm(), fromFile.getAlgorithm());
        Assert.assertEquals(fromContent.getFormat(), fromFile.getFormat());
        Assert.assertEquals(fromContent.getEncoded(), fromFile.getEncoded());
    }

    @Test(groups = { "unit" })
    public void testGetKeyStoreFromPemContent() throws Exception {
        ClickHouseDefaultSslContextProvider provider = new ClickHouseDefaultSslContextProvider();

        KeyStore trustStore = provider.getKeyStore(readTestResource("client.crt"), null);
        Assert.assertNotNull(trustStore.getCertificate("cert1"));

        KeyStore keyStore = provider.getKeyStore(readTestResource("some_user.crt"),
                readTestResource("some_user.key"));
        Assert.assertNotNull(keyStore.getKey("key", null));
    }
}