package com.clickhouse.examples.jdbc;

/**
 * Sample of using clickhouse jdbc client with kerberos auth.
 * 
 * https://clickhouse.com/docs/en/operations/external-authenticators/kerberos
 */
public class GssAuthClient {

    private void execute(String url) {
        String url = "jdbc:ch:http://localhost:8123/default";       // only http protocol supports GSS auth
        Properties props = new Properties();
        props.setProperty("user", "userA");
        props.setProperty("gss_enabled", "true");
        props.setProperty("kerberos_server_name", "HTTP");      
        try (Connection conn = DriverManager.getConnection(url, props)) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT currentUser();");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }
    

    public static void main(String...args) {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/etc/jaas.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        new GssAuthClient().execute();
    }
}
