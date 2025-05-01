package com.clickhouse.jdbc.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ParsedStatement extends ClickHouseParserBaseListener {

    private String useDatabase;

    private boolean hasResultSet;

    private boolean insert;

    private String insertTableId;

    private List<String> roles;

    public void setUseDatabase(String useDatabase) {
        this.useDatabase = useDatabase;
    }

    public void setHasResultSet(boolean hasResultSet) {
        this.hasResultSet = hasResultSet;
    }

    public boolean isHasResultSet() {
        return hasResultSet;
    }

    public void setInsert(boolean insert) {
        this.insert = insert;
    }

    public boolean isInsert() {
        return insert;
    }

    public void setInsertTableId(String insertTableId) {
        this.insertTableId = insertTableId;
    }

    public String getInsertTableId() {
        return insertTableId;
    }

    public String getUseDatabase() {
        return useDatabase;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public List<String> getRoles() {
        return roles;
    }

    @Override
    public void enterQueryStmt(ClickHouseParser.QueryStmtContext ctx) {
        ClickHouseParser.QueryContext qCtx = ctx.query();
        if (qCtx != null) {
            if (qCtx.selectStmt() != null || qCtx.selectUnionStmt() != null || qCtx.showStmt() != null
                    || qCtx.describeStmt() != null) {
                setHasResultSet(true);
            }
        }
    }

    @Override
    public void enterUseStmt(ClickHouseParser.UseStmtContext ctx) {
        if (ctx.databaseIdentifier() != null) {
            setUseDatabase(JdbcUtils.unquoteIdentifier(ctx.databaseIdentifier().getText()));
        }
    }

    @Override
    public void enterSetRoleStmt(ClickHouseParser.SetRoleStmtContext ctx) {
        if (ctx.NONE() != null) {
            setRoles(Collections.emptyList());
        } else {
            List<String> roles = new ArrayList<>();
            for (ClickHouseParser.IdentifierContext id : ctx.setRolesList().identifier()) {
                roles.add(JdbcUtils.unquoteIdentifier(id.getText()));
            }
            setRoles(roles);
        }
    }
}
