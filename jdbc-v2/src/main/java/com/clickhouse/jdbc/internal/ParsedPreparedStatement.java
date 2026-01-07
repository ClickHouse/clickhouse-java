package com.clickhouse.jdbc.internal;

import java.util.Arrays;
import java.util.List;

/**
 * Model of parsed statement with parameters
 */
public final class ParsedPreparedStatement {

    private String table;
    
    private String database;

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

    private int assignValuesGroups = 0;

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

    public int getArgCount() {
        return argCount;
    }

    public String[] getInsertColumns() {
        return insertColumns;
    }

    public void setInsertColumns(String[] insertColumns) {
        this.insertColumns = insertColumns;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
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

    public void setAssignValuesListStartPosition(int assignValuesListStartPosition) {
        this.assignValuesListStartPosition = assignValuesListStartPosition;
    }

    public int getAssignValuesListStopPosition() {
        return assignValuesListStopPosition;
    }

    public void setAssignValuesListStopPosition(int assignValuesListStopPosition) {
        this.assignValuesListStopPosition = assignValuesListStopPosition;
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

    public boolean isHasErrors() {
        return hasErrors;
    }

    public void setHasErrors(boolean hasErrors) {
        this.hasErrors = hasErrors;
    }

    void appendParameter(int startIndex) {
        argCount++;
        if (argCount > paramPositions.length) {
            paramPositions = Arrays.copyOf(paramPositions, paramPositions.length + 10);
        }
        paramPositions[argCount - 1] = startIndex;
    }
}
