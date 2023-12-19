package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseLoadBalancingPolicy;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class JdbcIssuesTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testDecompress() throws SQLException {
        String httpEndpoint = "http://" + getServerAddress(ClickHouseProtocol.HTTP) + "/";
        String TABLE_NAME = "decompress_issue";
        Properties prop = new Properties();
        prop.setProperty("decompress", "true");
        prop.setProperty("decompress_algorithm", "lz4");
        String url = String.format("jdbc:ch:%s", httpEndpoint);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, prop);
        String columnNames = "event_id";
        String columnValues = "('event_id String')";
        String sql = String.format("INSERT INTO %s (%s) SELECT %s FROM input %s", TABLE_NAME, columnNames, columnNames, columnValues);

        Connection conn = dataSource.getConnection("default", "");
        Statement st = conn.createStatement();
        st.execute(String.format("CREATE TABLE %s (`event_id` String) ENGINE = Log", TABLE_NAME));

        int count = 1;
        while (count <= 50000) {
            String content = StringUtils.repeat("*", count);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, content);
                ps.addBatch();
                ps.executeBatch();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println(count);
            }
            count *= 2;
        }
    }

}
