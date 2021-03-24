package ru.yandex.clickhouse.util.ssl;

import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

/**
 * An insecure {@link javax.net.ssl.TrustManager}, that don't validate the certificate.
 */
public class NonValidatingTrustManager implements X509TrustManager {

  public X509Certificate[] getAcceptedIssuers() {
    return new X509Certificate[0];
  }

  public void checkClientTrusted(X509Certificate[] certs, String authType) {
  }

  public void checkServerTrusted(X509Certificate[] certs, String authType) {
  }
}
