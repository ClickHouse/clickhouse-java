package com.clickhouse.client.api.sql;

public class SQLUtils {
    /**
     * Escapes and quotes a string literal for use in SQL queries.
     *
     * @param str the string to be quoted, cannot be null
     * @return the quoted and escaped string
     * @throws IllegalArgumentException if the input string is null
     */
    public static String enquoteLiteral(String str) {
        if (str == null) {
            throw new IllegalArgumentException("Input string cannot be null");
        }
        return "'" + str.replace("'", "''") + "'";
    }

    /**
     * Escapes and quotes an SQL identifier (e.g., table or column name) by enclosing it in double quotes.
     * Any existing double quotes in the identifier are escaped by doubling them.
     *
     * @param identifier the identifier to be quoted, cannot be null
     * @param quotesRequired if false, the identifier will only be quoted if it contains special characters
     * @return the quoted and escaped identifier, or the original identifier if quoting is not required
     * @throws IllegalArgumentException if the input identifier is null
     */
    public static String enquoteIdentifier(String identifier, boolean quotesRequired) {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier cannot be null");
        }
        
        if (!quotesRequired && !needsQuoting(identifier)) {
            return identifier;
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
    
    /**
     * Escapes and quotes an SQL identifier, always adding quotes.
     *
     * @param identifier the identifier to be quoted, cannot be null
     * @return the quoted and escaped identifier
     * @throws IllegalArgumentException if the input identifier is null
     * @see #enquoteIdentifier(String, boolean)
     */
    public static String enquoteIdentifier(String identifier) {
        return enquoteIdentifier(identifier, true);
    }
    
    /**
     * Checks if an identifier needs to be quoted.
     * An identifier needs quoting if it:
     * - Is empty
     * - Contains any non-alphanumeric characters except underscore
     * - Starts with a digit
     * - Is a reserved keyword (not implemented in this basic version)
     * 
     * @param identifier the identifier to check
     * @return true if the identifier needs to be quoted, false otherwise
     */
    private static boolean needsQuoting(String identifier) {
        if (identifier.isEmpty()) {
            return true;
        }
        
        // Check if first character is a digit
        if (Character.isDigit(identifier.charAt(0))) {
            return true;
        }
        
        // Check all characters are alphanumeric or underscore
        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Checks if the given string is a valid simple SQL identifier that doesn't require quoting.
     * A simple identifier must:
     * <ul>
     *   <li>Not be null or empty</li>
     *   <li>Be between 1 and 128 characters in length (inclusive)</li>
     *   <li>Start with an alphabetic character (a-z, A-Z)</li>
     *   <li>Contain only alphanumeric characters or underscores</li>
     *   <li>Not be enclosed in double quotes</li>
     * </ul>
     *
     * @param identifier the identifier to check
     * @return true if the identifier is a valid simple SQL identifier, false otherwise
     * @throws IllegalArgumentException if the input identifier is null
     */
    // Compiled pattern for simple SQL identifiers
    private static final java.util.regex.Pattern SIMPLE_IDENTIFIER_PATTERN = 
        java.util.regex.Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]{0,127}$");
    
    /**
     * Checks if the given string is a valid simple SQL identifier using a compiled regex pattern.
     * A simple identifier must match the pattern: ^[a-zA-Z][a-zA-Z0-9_]{0,127}$
     * 
     * @param identifier the identifier to check
     * @return true if the identifier is a valid simple SQL identifier, false otherwise
     * @throws IllegalArgumentException if the input identifier is null
     */
    public static boolean isSimpleIdentifier(String identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier cannot be null");
        }
        return SIMPLE_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }
}
