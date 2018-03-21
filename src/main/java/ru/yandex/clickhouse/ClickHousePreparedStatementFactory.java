package ru.yandex.clickhouse;


import org.apache.http.impl.client.CloseableHttpClient;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.TimeZone;

final class ClickHousePreparedStatementFactory {
    private static final String INSERT = "insert";

    static ClickHousePreparedStatement getQuery(String query,
                                                CloseableHttpClient httpClient,
                                                ClickHouseConnection connection,
                                                ClickHouseProperties properties,
                                                TimeZone timeZone) throws SQLException {

        String queryWithoutComments = removeCommentsFrom(query);
        if (queryWithoutComments.regionMatches(true, 0, INSERT, 0, INSERT.length()))
            return new ClickHousePreparedInsertStatementImpl(httpClient, connection, properties, queryWithoutComments, timeZone);
        else
            return new ClickHousePreparedStatementImpl(httpClient, connection, properties, queryWithoutComments, timeZone);

    }

    static String removeCommentsFrom(String str) {
        StringBuilder query = new StringBuilder(str.length());
        boolean isMultilineComment = false;
        boolean isSingleLineComment = false;
        for (int i = 0; i < str.length(); i++) {
            //multi-line comments
            if (!isSingleLineComment && str.charAt(i) == '/' && isNextSymbol(i, str, '*'))
                isMultilineComment = true;
            if (isMultilineComment && str.charAt(i) == '*' && isNextSymbol(i, str, '/')) {
                i++;
                isMultilineComment = false;
                continue;
            }

            //single line comments
            if (!isMultilineComment && str.charAt(i) == '-' && isNextSymbol(i, str, '-'))
                isSingleLineComment = true;
            if (isSingleLineComment && str.charAt(i) == '\n') {
                isSingleLineComment = false;
                continue;
            }

            if (!isSingleLineComment && !isMultilineComment)
                query.append(str.charAt(i));
        }

        return query.toString().trim();
    }

    private static boolean isNextSymbol(int index, String str, char symbol) {
        index++;
        return index < str.length() - 1 && str.charAt(index) == symbol;
    }
}
