package ru.yandex.clickhouse;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHousePreparedStatementFactoryTest {

    @DataProvider(name = "queries-data-provider")
    public static Object[][] getParameters() {
        return new Object[][]{
                {
                        "select * from tables where table.t1 = \"value\"",
                        "select * from tables where table.t1 = \"value\""
                },
                {
                        "select * from tables where  table.t1 = \"value\"",
                        "select * from tables --- coolest query \n" +
                                "where /* hello world \\!**/ table.t1 = \"value\""
                },
                {
                        "select a from t where a = ? and b = 1",
                        "select a --what is it?\nfrom t where a = ? and b = 1"
                },
                {
                        "select a  from t where a = ? and b = 1",
                        "select a /*what is it?*/ from t where a = ? and b = 1"
                }};
    }

    @Test(dataProvider = "queries-data-provider")
    public void test(String expectedQuery, String queryWithComments) {
        Assert.assertEquals(expectedQuery, ClickHousePreparedStatementFactory.removeCommentsFrom(queryWithComments));
    }
}
