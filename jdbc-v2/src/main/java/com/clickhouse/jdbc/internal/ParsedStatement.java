package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.sql.SQLUtils;
import com.clickhouse.jdbc.internal.parser.ClickHouseParser;
import com.clickhouse.jdbc.internal.parser.ClickHouseParserBaseListener;
import org.antlr.v4.runtime.tree.ErrorNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ParsedStatement extends ClickHouseParserBaseListener {

    private String useDatabase;

    private boolean hasResultSet;

    private boolean insert;

    private String insertTableId;

    private List<String> roles;

    private boolean hasErrors;

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

    public boolean isHasErrors() {
        return hasErrors;
    }

    public void setHasErrors(boolean hasErrors) {
        this.hasErrors = hasErrors;
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        setHasErrors(true);
    }

    @Override
    public void enterQueryStmt(ClickHouseParser.QueryStmtContext ctx) {
        if (SqlParser.isStmtWithResultSet(ctx)) {
            setHasResultSet(true);
        }
    }

    @Override
    public void enterUseStmt(ClickHouseParser.UseStmtContext ctx) {
        if (ctx.databaseIdentifier() != null) {
            setUseDatabase(SQLUtils.unquoteIdentifier(ctx.databaseIdentifier().getText()));
        }
    }

    @Override
    public void enterSetRoleStmt(ClickHouseParser.SetRoleStmtContext ctx) {
        if (ctx.NONE() != null) {
            setRoles(Collections.emptyList());
        } else {
            List<String> roles = new ArrayList<>();
            for (ClickHouseParser.IdentifierContext id : ctx.setRolesList().identifier()) {
                roles.add(SQLUtils.unquoteIdentifier(id.getText()));
            }
            setRoles(roles);
        }
    }


}
