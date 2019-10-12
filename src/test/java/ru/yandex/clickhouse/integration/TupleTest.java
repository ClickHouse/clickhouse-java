package ru.yandex.clickhouse.integration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class TupleTest {
    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
    }

    @Test
    public void test() throws Exception {
      Statement statement = connection.createStatement();
      try {
        ResultSet rs = statement.executeQuery("select tuple(1,'a') as t");
        while (rs.next()) {
          Object result = rs.getObject("t");
          System.out.println(result);
          System.out.println(result.getClass());
        }
      } finally {
        statement.close();
      }
    }
}

