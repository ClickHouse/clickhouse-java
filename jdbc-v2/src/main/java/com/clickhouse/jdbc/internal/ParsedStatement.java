package com.clickhouse.jdbc.internal;

import java.util.List;

/**
 * Model of parsed statement when no parameters are used.
 */
public final class ParsedStatement {

    private String useDatabase;

    private boolean hasResultSet;

    private boolean insert;

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
}
