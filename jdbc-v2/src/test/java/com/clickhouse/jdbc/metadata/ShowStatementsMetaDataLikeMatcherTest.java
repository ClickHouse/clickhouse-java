package com.clickhouse.jdbc.metadata;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ShowStatementsMetaDataLikeMatcherTest {

    @DataProvider(name = "likePatterns")
    public Object[][] likePatterns() {
        return new Object[][] {
                {null, "any_name", true},
                {null, "", true},
                {"", "", true},
                {"", "a", false},
                {"%", "", true},
                {"%", "a\nb", true},
                {"abc", "abc", true},
                {"abc", "ABC", false},
                {"abc", "abcd", false},
                {"a_c", "abc", true},
                {"a_c", "ac", false},
                {"a%c", "ac", true},
                {"a%c", "a.b.c", true},
                {"a%c", "acd", false},
                {"a\\_c", "a_c", true},
                {"a\\_c", "abc", false},
                {"100\\%", "100%", true},
                {"100\\%", "1000", false},
                {"a\\\\b", "a\\b", true},
                {"a.c", "abc", false},
                {"(x)[y]*", "(x)[y]*", true},
                {"_", "😀", true},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "likePatterns")
    public void testLikeMatcher(String pattern, String value, boolean expected) {
        assertEquals(ShowStatementsMetaData.likeMatcher(pattern).test(value), expected,
                "'" + value + "' LIKE '" + pattern + "'");
    }
}
