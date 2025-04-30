package com.clickhouse.jdbc.internal;

public class StatementParserListener extends ClickHouseParserBaseListener {

    String useDatabase;

    boolean hasResultSet;

    @Override
    public void enterSelectStmt(ClickHouseParser.SelectStmtContext ctx) {
        hasResultSet = true;
    }

    @Override
    public void enterUseStmt(ClickHouseParser.UseStmtContext ctx) {
        if (ctx.databaseIdentifier() != null) {
            useDatabase = ctx.databaseIdentifier().getText();
        }
    }

    @Override
    public void enterSetRoleStmt(ClickHouseParser.SetRoleStmtContext ctx) {
//        ctx.setRolesList()
    }
}
