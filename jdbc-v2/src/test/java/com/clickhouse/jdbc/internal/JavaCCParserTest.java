package com.clickhouse.jdbc.internal;


import org.testng.annotations.Ignore;

@Ignore
public class JavaCCParserTest extends BaseSqlParserFacadeTest {
    public JavaCCParserTest() throws Exception {
        super(SqlParserFacade.SQLParser.JAVACC.name());
    }
}
