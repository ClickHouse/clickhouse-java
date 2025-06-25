package com.clickhouse.client.api.sql;

import com.clickhouse.jdbc.internal.SqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(groups = {"unit"})
public class SQLUtilsTest {
    // Test data for enquoteLiteral
    @DataProvider(name = "enquoteLiteralTestData")
    public Object[][] enquoteLiteralTestData() {
        return new Object[][] {
            // input, expected output
            {"test 123", "'test 123'"},
            {"こんにちは世界", "'こんにちは世界'"},
            {"O'Reilly", "'O''Reilly'"},
            {"😊👍", "'😊👍'"},
            {"", "''"},
            {"single'quote'double''quote\"", "'single''quote''doubl''e''"}
        };
    }

    // Test data for enquoteIdentifier
    @DataProvider(name = "enquoteIdentifierTestData")
    public Object[][] enquoteIdentifierTestData() {
        return new Object[][] {
            // input, expected output
            {"column1", "\"column1\""},
            {"table.name", "\"table.name\""},
            {"column with spaces", "\"column with spaces\""},
            {"column\"with\"quotes", "\"column\"\"with\"\"quotes\""},
            {"UPPERCASE", "\"UPPERCASE\""},
            {"1column", "\"1column\""},
            {"column-with-hyphen", "\"column-with-hyphen\""},
            {"😊👍", "\"😊👍\""},
            {"", "\"\""}
        };
    }
    
    @Test(dataProvider = "enquoteLiteralTestData")
    public void testEnquoteLiteral(String input, String expected) {
        assertEquals(SQLUtils.enquoteLiteral(input), expected);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEnquoteLiteral_NullInput() {
        SQLUtils.enquoteLiteral(null);
    }
    
    @Test(dataProvider = "enquoteIdentifierTestData")
    public void testEnquoteIdentifier(String input, String expected) {
        // Test with quotesRequired = true (always quote)
        assertEquals(SQLUtils.enquoteIdentifier(input), expected);
        assertEquals(SQLUtils.enquoteIdentifier(input, true), expected);
        
        // Test with quotesRequired = false (quote only if needed)
        boolean needsQuoting = !input.matches("[a-zA-Z_][a-zA-Z0-9_]*");
        String expectedUnquoted = needsQuoting ? expected : input;
        assertEquals(SQLUtils.enquoteIdentifier(input, false), expectedUnquoted);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEnquoteIdentifier_NullInput() {
        SQLUtils.enquoteIdentifier(null);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEnquoteIdentifier_NullInput_WithQuotesRequired() {
        SQLUtils.enquoteIdentifier(null, true);
    }
    
    @Test
    public void testEnquoteIdentifier_NoQuotesWhenNotNeeded() {
        // These identifiers don't need quoting
        String[] simpleIdentifiers = {
            "column1", "table_name", "_id", "a1b2c3", "ColumnName"
        };
        
        for (String id : simpleIdentifiers) {
            // With quotesRequired=false, should return as-is
            assertEquals(SQLUtils.enquoteIdentifier(id, false), id);
            // With quotesRequired=true, should be quoted
            assertEquals(SQLUtils.enquoteIdentifier(id, true), "\"" + id + "\"");
        }
    }
    
    @DataProvider(name = "simpleIdentifierTestData")
    public Object[][] simpleIdentifierTestData() {
        return new Object[][] {
            // identifier, expected result
            {"Hello", true},
            {"hello_world", true},
            {"Hello123", true},
            {"H", true},  // minimum length
            {"a".repeat(128), true},  // maximum length
            
            // Test cases from requirements
            {"G'Day", false},
            {"\"\"Bruce Wayne\"\"", false},
            {"GoodDay$", false},
            {"Hello\"\"World", false},
            {"\"\"Hello\"\"World\"\"", false},
            
            // Additional test cases
            {"", false},  // empty string
            {"123test", false},  // starts with number
            {"_test", false},  // starts with underscore
            {"test-name", false},  // contains hyphen
            {"test name", false},  // contains space
            {"test\"name", false},  // contains quote
            {"test.name", false},  // contains dot
            {"a".repeat(129), false},  // exceeds max length
            {"testName", true},
            {"TEST_NAME", true},
            {"test123", true},
            {"t123", true},
            {"t", true}
        };
    }
    
    @Test(dataProvider = "simpleIdentifierTestData")
    public void testIsSimpleIdentifier(String identifier, boolean expected) {
        assertEquals(SQLUtils.isSimpleIdentifier(identifier), expected,
            String.format("Failed for identifier: %s", identifier));
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testIsSimpleIdentifier_NullInput() {
        SQLUtils.isSimpleIdentifier(null);
    }

    @Test
    public void testUnquoteIdentifier() {
        String[] names = new String[]{"test", "`test name1`", "\"test name 2\""};
        String[] expected = new String[]{"test", "test name1", "test name 2"};

        for (int i = 0; i < names.length; i++) {
            assertEquals(SQLUtils.unquoteIdentifier(names[i]), expected[i]);
        }
    }
}