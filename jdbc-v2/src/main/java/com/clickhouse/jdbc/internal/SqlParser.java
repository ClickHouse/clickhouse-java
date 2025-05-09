package com.clickhouse.jdbc.internal;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.IterativeParseTreeWalker;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlParser {

    public ParsedStatement parsedStatement(String sql) {

        CharStream charStream = CharStreams.fromString(sql);
        ClickHouseLexer lexer = new ClickHouseLexer(charStream);
        ClickHouseParser parser = new ClickHouseParser(new CommonTokenStream(lexer));
        ClickHouseParser.QueryStmtContext parseTree = parser.queryStmt();
        ParsedStatement parserListener = new ParsedStatement();
        IterativeParseTreeWalker.DEFAULT.walk(parserListener, parseTree);


        return parserListener;
    }

    public ParsedPreparedStatement parsePreparedStatement(String sql) {
        CharStream charStream = CharStreams.fromString(sql);
        ClickHouseLexer lexer = new ClickHouseLexer(charStream);
        ClickHouseParser parser = new ClickHouseParser(new CommonTokenStream(lexer));
        ClickHouseParser.QueryStmtContext parseTree = parser.queryStmt();
        ParsedPreparedStatement parserListener = new ParsedPreparedStatement();
        IterativeParseTreeWalker.DEFAULT.walk(parserListener, parseTree);
        return parserListener;
    }

    private final static Pattern UNQUOTE_INDENTIFIER = Pattern.compile(
            "^[\\\"`]?(.+?)[\\\"`]?$"
    );

    public static String unquoteIdentifier(String str) {
        Matcher matcher = UNQUOTE_INDENTIFIER.matcher(str.trim());
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return str;
        }
    }

    public static String escapeQuotes(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str
                .replace("'", "\\'")
                .replace("\"", "\\\"");
    }
}
