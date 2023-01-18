package com.clickhouse.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseRenameMethodTest {
    @Test(groups = { "unit" })
    public void testRenameNullOrEmptyString() {
        for (ClickHouseRenameMethod m : ClickHouseRenameMethod.values()) {
            Assert.assertEquals(m.rename(null), "");
            Assert.assertEquals(m.rename(""), "");
        }
    }

    @Test(groups = { "unit" })
    public void testNone() {
        Assert.assertEquals(ClickHouseRenameMethod.NONE.rename("\t \n \r"), "\t \n \r");
        Assert.assertEquals(ClickHouseRenameMethod.NONE.rename("test 1 2 3"), "test 1 2 3");
    }

    @Test(groups = { "unit" })
    public void testRemovePrefix() {
        Assert.assertEquals(ClickHouseRenameMethod.REMOVE_PREFIX.rename("\t \n \r"), "\t \n \r");
        Assert.assertEquals(ClickHouseRenameMethod.REMOVE_PREFIX.rename("test 1 2 3"), "test 1 2 3");
        Assert.assertEquals(ClickHouseRenameMethod.REMOVE_PREFIX.rename("test.1 2 3"), "1 2 3");
        Assert.assertEquals(ClickHouseRenameMethod.REMOVE_PREFIX.rename("test.1.2.3"), "3");
        Assert.assertEquals(ClickHouseRenameMethod.REMOVE_PREFIX.rename(".test"), "test");
        Assert.assertEquals(ClickHouseRenameMethod.REMOVE_PREFIX.rename("test."), "");
        Assert.assertEquals(ClickHouseRenameMethod.REMOVE_PREFIX.rename("."), "");
    }

    @Test(groups = { "unit" })
    public void testCamelCase() {
        Assert.assertEquals(ClickHouseRenameMethod.TO_CAMELCASE.rename("\t \n \r"), "");
        Assert.assertEquals(ClickHouseRenameMethod.TO_CAMELCASE.rename("test 1 2 3"), "test123");
        Assert.assertEquals(ClickHouseRenameMethod.TO_CAMELCASE.rename("test oNE Two_three"), "testONETwoThree");
        Assert.assertEquals(ClickHouseRenameMethod.TO_CAMELCASE.rename("test"), "test");
        Assert.assertEquals(ClickHouseRenameMethod.TO_CAMELCASE.rename(" test"), "Test");
        Assert.assertEquals(ClickHouseRenameMethod.TO_CAMELCASE.rename("test "), "test");

        Assert.assertEquals(ClickHouseRenameMethod.TO_CAMELCASE_WITHOUT_PREFIX.rename("a.test_col"), "testCol");
    }

    @Test(groups = { "unit" })
    public void testUnderscore() {
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("\t \n \r"), "");
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("TEST"), "TEST");
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("Test"), "Test");
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("TestONE"), "Test_oNE");
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("Test ONE"), "Test_oNE");
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("Test  oneTwo"), "Test_one_two");
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("testOnetWo"), "test_onet_wo");
        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE.rename("test12Three"), "test12_three");

        Assert.assertEquals(ClickHouseRenameMethod.TO_UNDERSCORE_WITHOUT_PREFIX.rename("a.t.est1\t 2Three"),
                "est1_2_three");
    }
}
