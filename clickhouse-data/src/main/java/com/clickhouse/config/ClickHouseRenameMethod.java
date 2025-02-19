package com.clickhouse.config;

import java.util.function.UnaryOperator;

/**
 * Methods for renaming.
 */
@Deprecated
public enum ClickHouseRenameMethod {
    /**
     * No OP.
     */
    NONE(null),
    /**
     * Removes prefix including the dot. So "d.t.col1" becomes "col1" and "col2"
     * remains the same.
     */
    REMOVE_PREFIX(s -> {
        int index = s.lastIndexOf('.');
        return index >= 0 ? s.substring(index + 1) : s;
    }),

    /**
     * Replaces whitespace and underscore to camel case. So "a simple_column"
     * becomes "aSimpleColumn" and "col_1 2" becomes "col12".
     */
    TO_CAMELCASE(s -> {
        StringBuilder builder = new StringBuilder(s.length());
        boolean toUpperCase = false;
        for (char ch : s.toCharArray()) {
            if (Character.isWhitespace(ch) || ch == '_') {
                toUpperCase = true;
            } else if (toUpperCase) {
                builder.append(Character.toUpperCase(ch));
                toUpperCase = false;
            } else {
                builder.append(ch);
            }
        }
        return builder.toString();
    }),
    /**
     * Removes prefix and replace whitespace and underscore to camel case.
     */
    TO_CAMELCASE_WITHOUT_PREFIX(s -> TO_CAMELCASE.rename(REMOVE_PREFIX.rename(s))),
    /**
     * Replaces whitespace and camel case to underscore. So "aSimpleColumn" becomes
     * "a_simple_column" and "col12" becomes "col_12".
     */
    TO_UNDERSCORE(s -> {
        StringBuilder builder = new StringBuilder(s.length() + 5);
        int prev = -1; // 0 - normal, 1 - whitespace, 2 - upper case
        for (char ch : s.toCharArray()) {
            if (Character.isWhitespace(ch)) {
                if (prev == 0) {
                    builder.append('_');
                }
                prev = 1;
            } else if (Character.isUpperCase(ch)) {
                if (prev == 0) {
                    builder.append('_').append(Character.toLowerCase(ch));
                } else if (prev == 1) {
                    builder.append(Character.toLowerCase(ch));
                } else {
                    builder.append(ch);
                }
                prev = 2;
            } else {
                builder.append(ch);
                prev = 0;
            }
        }
        return builder.toString();
    }),
    /**
     * Removes prefix and replace whitespace and camel case to underscore.
     */
    TO_UNDERSCORE_WITHOUT_PREFIX(s -> TO_UNDERSCORE.rename(REMOVE_PREFIX.rename(s)));

    private final UnaryOperator<String> renameFunc;

    ClickHouseRenameMethod(UnaryOperator<String> renameFunc) {
        this.renameFunc = renameFunc;
    }

    /**
     * Rename the given name.
     *
     * @param name name to change
     * @return non-null new name
     */
    public String rename(String name) {
        if (name == null) {
            name = "";
        }
        return renameFunc != null ? renameFunc.apply(name) : name;
    }
}
