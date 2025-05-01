package com.clickhouse.jdbc.internal;

import org.antlr.v4.runtime.tree.ErrorNode;

import java.util.List;

/**
 * Parser listener that collects information for prepared statement.
 *
 */
public class ParsedPreparedStatement extends ClickHouseParserBaseListener {

    private String table;

    private String[] insertColumns;

    private boolean hasFuncWrappedParameter;

    private boolean hasErrors;

    private boolean hasResultSet;

    private boolean insert;

    private int argCount;

    private boolean canStream;

    private String insertTableId;

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

    public void setArgCount(int argCount) {
        this.argCount = argCount;
    }

    public int getArgCount() {
        return argCount;
    }

    public void setCanStream(boolean canStream) {
        this.canStream = canStream;
    }

    public boolean isCanStream() {
        return canStream;
    }

    public void setInsertTableId(String insertTableId) {
        this.insertTableId = insertTableId;
    }

    public String getInsertTableId() {
        return insertTableId;
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        hasErrors = true;
    }

    @Override
    public void enterInsertParameterFuncExpr(ClickHouseParser.InsertParameterFuncExprContext ctx) {
        hasFuncWrappedParameter = true;
    }

    @Override
    public void enterInsertParameter(ClickHouseParser.InsertParameterContext ctx) {
        super.enterInsertParameter(ctx);
        System.out.println("parameter: " + ctx.getText());
    }

    @Override
    public void enterInsertStmt(ClickHouseParser.InsertStmtContext ctx) {
        ClickHouseParser.TableIdentifierContext  tableId = ctx.tableIdentifier();
        if (tableId != null) {
            this.table = tableId.identifier().IDENTIFIER().getText();
        }

        ClickHouseParser.ColumnsClauseContext columns = ctx.columnsClause();
        if (columns != null) {
            List<ClickHouseParser.NestedIdentifierContext> names = columns.nestedIdentifier();
            this.insertColumns = new String[names.size()];
            for (int i = 0; i < names.size(); i++) {
                this.insertColumns[i] = names.get(i).getText();
            }
        }

        super.enterInsertStmt(ctx);
    }
}
