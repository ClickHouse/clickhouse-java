package com.clickhouse.jdbc.internal;

import org.antlr.v4.runtime.tree.ErrorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Parser listener that collects information for prepared statement.
 *
 */
public class ParsedPreparedStatement extends ClickHouseParserBaseListener {
    private static final Logger LOG = LoggerFactory.getLogger(ParsedPreparedStatement.class);

    private String table;

    private String useDatabase;

    private String[] insertColumns;

    private boolean useFunction;

    private boolean hasErrors;

    private boolean hasResultSet;

    private boolean insert;

    private boolean insertWithSelect;

    private List<String> roles;

    private int argCount;

    private int[] paramPositions = new int[16];

    private int assignValuesListStartPosition = -1;

    private int assignValuesListStopPosition = -1;

    private int assignValuesGroups = -1;

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

    public void setInsertWithSelect(boolean insertWithSelect) {
        this.insertWithSelect = insertWithSelect;
    }

    public boolean isInsertWithSelect() {
        return insertWithSelect;
    }

    public void setArgCount(int argCount) {
        this.argCount = argCount;
    }

    public int getArgCount() {
        return argCount;
    }

    public String[] getInsertColumns() {
        return insertColumns;
    }

    public String getTable() {
        return table;
    }

    public int[] getParamPositions() {
        return paramPositions;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public List<String> getRoles() {
        return roles;
    }

    public int getAssignValuesListStartPosition() {
        return assignValuesListStartPosition;
    }

    public int getAssignValuesListStopPosition() {
        return assignValuesListStopPosition;
    }

    public void setUseDatabase(String useDatabase) {
        this.useDatabase = useDatabase;
    }

    public String getUseDatabase() {
        return useDatabase;
    }

    public void setAssignValuesGroups(int assignValuesGroups) {
        this.assignValuesGroups = assignValuesGroups;
    }

    public int getAssignValuesGroups() {
        return assignValuesGroups;
    }

    public boolean isUseFunction() {
        return useFunction;
    }

    public void setUseFunction(boolean useFunction) {
        this.useFunction = useFunction;
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
        useFunction = true;
    }

    @Override
    public void enterAssignmentValuesList(ClickHouseParser.AssignmentValuesListContext ctx) {
        assignValuesListStartPosition = ctx.getStart().getStartIndex();
        assignValuesListStopPosition = ctx.getStop().getStopIndex();
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

        setInsert(true);
    }

    @Override
    public void enterDataClauseSelect(ClickHouseParser.DataClauseSelectContext ctx) {
        setInsertWithSelect(true);
    }

    @Override
    public void enterDataClauseValues(ClickHouseParser.DataClauseValuesContext ctx) {
        setAssignValuesGroups(ctx.assignmentValues().size());
    }

    @Override
    public void exitInsertParameterFuncExpr(ClickHouseParser.InsertParameterFuncExprContext ctx) {
        setUseFunction(true);
    }
}
