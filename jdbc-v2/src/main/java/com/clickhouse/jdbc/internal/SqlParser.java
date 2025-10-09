package com.clickhouse.jdbc.internal;

import com.clickhouse.jdbc.internal.parser.ClickHouseLexer;
import com.clickhouse.jdbc.internal.parser.ClickHouseParser;
import com.clickhouse.jdbc.internal.parser.ClickHouseParserBaseListener;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.IterativeParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlParser {

    private static final Logger LOG = LoggerFactory.getLogger(SqlParser.class);

    public ParsedStatement parsedStatement(String sql) {
        ParsedStatement parserListener = new ParsedStatement();
        walkSql(sql,  parserListener);
        return parserListener;
    }

    public ParsedPreparedStatement parsePreparedStatement(String sql) {
        ParsedPreparedStatement parserListener = new ParsedPreparedStatement();
        walkSql(sql,  parserListener);
        return parserListener;
    }

    private ClickHouseParser walkSql(String sql, ClickHouseParserBaseListener listener ) {
        CharStream charStream = CharStreams.fromString(sql);
        ClickHouseLexer lexer = new ClickHouseLexer(charStream);
        ClickHouseParser parser = new ClickHouseParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ParserErrorListener());

        ClickHouseParser.QueryStmtContext parseTree = parser.queryStmt();
        IterativeParseTreeWalker.DEFAULT.walk(listener, parseTree);

        return parser;
    }

    private static class ParserErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            LOG.warn("SQL syntax error at line: " + line + ", pos: " + charPositionInLine + ", " + msg);
        }
    }

    static boolean isStmtWithResultSet(ClickHouseParser.QueryStmtContext stmtContext) {
        ClickHouseParser.QueryContext qCtx = stmtContext.query();
        return  qCtx != null && (qCtx.selectStmt() != null ||  qCtx.selectUnionStmt() != null ||
                qCtx.showStmt() != null || qCtx.explainStmt() != null || qCtx.describeStmt() != null ||
                qCtx.existsStmt() != null || qCtx.checkStmt() != null );
    }
}
