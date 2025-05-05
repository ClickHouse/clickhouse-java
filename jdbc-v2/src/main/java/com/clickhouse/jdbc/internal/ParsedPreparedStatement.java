package com.clickhouse.jdbc.internal;

import org.antlr.v4.runtime.tree.ErrorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Parser listener that collects information for prepared statement.
 *
 */
public class ParsedPreparedStatement extends ClickHouseParserBaseListener {
    private static final Logger LOG = LoggerFactory.getLogger(ParsedPreparedStatement.class);

    private String table;

    private String[] insertColumns;

    private boolean hasFuncWrappedParameter;

    private boolean hasErrors;

    private boolean hasResultSet;

    private boolean insert;

    private int argCount;

    private boolean canStream;

    private int[] paramPositions = new int[16];

    private int insertValuesClausePos = -1;

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

    public String[] getInsertColumns() {
        return insertColumns;
    }

    public String getTable() {
        return table;
    }

    public int getInsertValuesClausePos() {
        return insertValuesClausePos;
    }

    public int[] getParamPositions() {
        return paramPositions;
    }

    public boolean isCanStream() {
        // there are next forms of INSERT that can be streamed
        // INSERT INTO `table` [(col1, col2)] VALUES (?, ?, ?..)
        // INSERT INTO `table` [(col1, col2)] FORMAT TabSeparated - this need additional support
        // INSERT with select or functions around parameters cannot be streamed
        return canStream;
    }

    @Override
    public void enterColumnExprParam(ClickHouseParser.ColumnExprParamContext ctx) {
        appendParameter(ctx.start.getStartIndex());
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
        appendParameter(ctx.start.getStartIndex());
    }

    private void appendParameter(int startIndex) {
        argCount++;
        if (argCount > paramPositions.length) {
            paramPositions = Arrays.copyOf(paramPositions, paramPositions.length + 10);
        }
        paramPositions[argCount-1] = startIndex;
        if (LOG.isTraceEnabled()) {
            LOG.trace("parameter position {}", startIndex);
        }
    }

    @Override
    public void exitDataClauseValues(ClickHouseParser.DataClauseValuesContext ctx) {
        insertValuesClausePos = ctx.VALUES().getSymbol().getStopIndex();
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
