package com.clickhouse.jdbc.internal;

public class Antlr4ParserTest extends BaseSqlParserFacadeTest {
    public Antlr4ParserTest() throws Exception {
        super(SqlParserFacade.SQLParser.ANTLR4.name());
    }
}
