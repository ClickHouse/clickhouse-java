package com.clickhouse.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.KdcServerForTest;

public class ClickHouserDriverWIthGssAuthTest extends JdbcIntegrationTest {

    private static final KdcServerForTest kdcServer = KdcServerForTest.getInstance();

    @Test(groups = "integration")
    public void testConnect() throws Exception {
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        Properties props = new Properties();
        props.setProperty("user", "bob");       // user with kerb auth
        props.setProperty("gss_enabled", "true");

        try (SystemPropertiesMock mock = SystemPropertiesMock.of(
            "java.security.krb5.conf", kdcServer.getKrb5Conf(),
            "java.security.auth.login.config", kdcServer.getBobJaasConf(),
            "javax.security.auth.useSubjectCredsOnly", "false",
            "sun.security.krb5.debug", "true",
            "clickhouse.test.kerb.sname", "HTTP@clickhouse-server.example.com")) {
            ClickHouseDriver driver = new ClickHouseDriver();
            try (ClickHouseConnection conn = driver.connect("jdbc:clickhouse://" + address, props);
                PreparedStatement ps = conn.prepareStatement("SELECT currentUser()")) {
                
                ResultSet rs = ps.executeQuery();

                assertTrue(rs.next());
                assertEquals("bob", rs.getString(1));
            } catch (Exception e) {
                throw e;
            }
        }
    }

    @BeforeTest
    public static void setupKdc() {
        kdcServer.beforeSuite();
    }

    @AfterTest
    public static void shutdownKdc() {
        kdcServer.afterSuite();
    }

}
