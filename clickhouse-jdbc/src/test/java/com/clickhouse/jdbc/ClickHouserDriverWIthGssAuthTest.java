package com.clickhouse.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.spi.LoginModule;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.KdcServerForTest;
import com.sun.security.auth.module.Krb5LoginModule;

public class ClickHouserDriverWIthGssAuthTest extends JdbcIntegrationTest {

    private static final KdcServerForTest kdcServer = KdcServerForTest.getInstance();

    @Test(groups = "integration")
    public void testConnect() throws Exception {
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        ClickHouseDriver driver = new ClickHouseDriver();
        Properties props = new Properties();
        props.setProperty("user", "bob"); // user with kerb auth
        props.setProperty("gss_enabled", "true");

        try (SystemPropertiesMock mock = SystemPropertiesMock.of(
                "java.security.krb5.conf", kdcServer.getKrb5Conf(),
                "java.security.auth.login.config", kdcServer.getBobJaasConf(),
                "javax.security.auth.useSubjectCredsOnly", "false",
                "sun.security.krb5.debug", "true",
                "clickhouse.test.kerb.sname", "HTTP@clickhouse-server.example.com");
                ClickHouseConnection conn = driver.connect("jdbc:clickhouse://" + address, props);
                PreparedStatement ps = conn.prepareStatement("SELECT currentUser()")) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            assertEquals("bob", rs.getString(1));
        }
    }

    @Test(groups = "integration")
    public void testConnectWithUseSubjectCredOnly() throws Exception {
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        ClickHouseDriver driver = new ClickHouseDriver();
        Properties props = new Properties();
        props.setProperty("gss_enabled", "true");

        Subject subject = new Subject();
        LoginModule krb5Module = new Krb5LoginModule();

        Map<String, String> options = new HashMap<>();
        options.put("principal", "bob@EXAMPLE.COM");
        options.put("useKeyTab", "true");
        options.put("keyTab", kdcServer.getBobKeyTabPath());
        options.put("doNotPrompt", "true");
        options.put("isInitiator", "true");
        options.put("refreshKrb5Config", "true");
        options.put("debug", "true");
        krb5Module.initialize(subject, null, null, options);
        
        try (SystemPropertiesMock mock = SystemPropertiesMock.of(
                "java.security.krb5.conf", kdcServer.getKrb5Conf(),
                "sun.security.krb5.debug", "true",
                "javax.security.auth.useSubjectCredsOnly", "true",
                "clickhouse.test.kerb.sname", "HTTP@clickhouse-server.example.com")) {
            krb5Module.login();
            krb5Module.commit();

            try (ClickHouseConnection conn = Subject.doAs(subject,
                    (PrivilegedExceptionAction<ClickHouseConnection>) () -> driver
                            .connect("jdbc:clickhouse://" + address + "?user=bob", props));
                    PreparedStatement ps = conn.prepareStatement("SELECT currentUser()")) {
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());

                assertEquals("bob", rs.getString(1));
            }
        }

    }

    @BeforeTest(groups = "integration")
    public static void setupKdc() {
        kdcServer.beforeSuite();
    }

    @AfterTest(groups = "integration")
    public static void shutdownKdc() {
        kdcServer.afterSuite();
    }

}
