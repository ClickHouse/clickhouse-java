package com.clickhouse.jdbc.internal;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.IterativeParseTreeWalker;

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
}
