package com.clickhouse.client.cli;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestHintTest {

    private static final List<Integer> NONE = Collections.emptyList();

    @DataProvider
    public Object[][] hints() {
        return new Object[][] {
                {"SELECT 1; -- { serverError UNKNOWN_TABLE }", Arrays.asList(60), NONE},
                {"SELECT 1; -- { serverError 60 }", Arrays.asList(60), NONE},
                {"SELECT 1; -- { serverError 153, ILLEGAL_DIVISION }", Arrays.asList(153, 153), NONE},
                {"SELECT; -- { clientError SYNTAX_ERROR }", NONE, Arrays.asList(62)},
                {"SELECT 1; -- { error 62 }", Arrays.asList(62), Arrays.asList(62)},
                {"SELECT 1 /* { serverError 60 } */;", Arrays.asList(60), NONE},
                {"SELECT 1; -- { serverError 60 } because there is no table", Arrays.asList(60), NONE},
                // A hint in a leading comment is ignored.
                {"-- { serverError 60 }\nSELECT 1;", NONE, NONE},
                // Braces in a string are not a hint.
                {"SELECT '-- { serverError 60 }';", NONE, NONE},
                {"SELECT 1; -- a comment", NONE, NONE},
        };
    }

    @Test(dataProvider = "hints")
    public void testParse(String text, List<Integer> server, List<Integer> client) {
        TestHint h = TestHint.parse(text);
        assertNull(h.parseError);
        assertEquals(h.serverErrors, server);
        assertEquals(h.clientErrors, client);
    }

    @Test
    public void testParseErrors() {
        assertNotNull(TestHint.parse("SELECT 1; -- { serverError NOT_A_REAL_ERROR_NAME }").parseError);
        assertNotNull(TestHint.parse("SELECT 1; -- { retry 3 until serverError 60 }").parseError);
    }

    @Test
    public void testMatches() {
        TestHint h = TestHint.parse("SELECT; -- { clientError SYNTAX_ERROR }");
        // Through the Java client the server finds syntax errors.
        assertTrue(h.matches(62));
        assertFalse(h.matches(60));
        assertEquals(h.label(), "clientError SYNTAX_ERROR");
    }

    @Test
    public void testEcho() {
        TestHint h = TestHint.parse("-- { echoOn }\nSELECT 1;");
        assertEquals(h.echo, Boolean.TRUE);
        h = TestHint.parse("SELECT 1; -- { echoOff }");
        assertEquals(h.echo, Boolean.FALSE);
    }

    @Test
    public void testErrorCodes() {
        assertEquals(ErrorCodes.code("UNKNOWN_TABLE"), Integer.valueOf(60));
        assertEquals(ErrorCodes.name(395), "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO");
        assertNull(ErrorCodes.code("NOT_A_REAL_ERROR_NAME"));
    }
}
