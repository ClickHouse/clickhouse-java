package com.clickhouse.jdbc;


import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.DataTypeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Properties;

@Test(groups = {"integration"})
public class JDBCDateTimeTests extends JdbcIntegrationTest {



    @Test(groups = {"integration"})
    void testDaysBeforeBirthdayParty() throws SQLException {

        LocalDate now = LocalDate.now();
        int daysBeforeParty = 10;
        LocalDate birthdate = now.plusDays(daysBeforeParty);


        Properties props = new Properties();
        props.put(ClientConfigProperties.USE_TIMEZONE.getKey(), "Asia/Tokyo");
        props.put(ClientConfigProperties.serverSetting("session_timezone"), "Asia/Tokyo");
        try (Connection conn = getJdbcConnection(props);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE test_days_before_birthday_party (id Int32, birthdate Date32) Engine MergeTree()");

            final String birthdateStr = birthdate.format(DataTypeUtils.DATE_FORMATTER);
            stmt.executeUpdate("INSERT INTO test_days_before_birthday_party VALUES (1, '" + birthdateStr + "')");

            try (ResultSet rs = stmt.executeQuery("SELECT id, birthdate, birthdate::String, timezone() FROM test_days_before_birthday_party")) {
                Assert.assertTrue(rs.next());

                LocalDate dateFromDb = rs.getObject(2, LocalDate.class);
                Assert.assertEquals(dateFromDb, birthdate);
                Assert.assertEquals(now.toEpochDay() - dateFromDb.toEpochDay(), -daysBeforeParty);
                Assert.assertEquals(rs.getString(4), "Asia/Tokyo");
            }

        }


    }

}
