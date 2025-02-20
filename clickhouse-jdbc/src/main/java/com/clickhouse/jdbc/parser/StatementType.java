package com.clickhouse.jdbc.parser;

@Deprecated
public enum StatementType {
    UNKNOWN(LanguageType.UNKNOWN, OperationType.UNKNOWN, false), // unknown statement
    ALTER(LanguageType.DDL, OperationType.UNKNOWN, false), // alter statement
    ALTER_DELETE(LanguageType.DML, OperationType.WRITE, false), // delete statement
    ALTER_UPDATE(LanguageType.DML, OperationType.WRITE, false), // update statement
    ATTACH(LanguageType.DDL, OperationType.UNKNOWN, false), // attach statement
    CHECK(LanguageType.DDL, OperationType.UNKNOWN, true), // check statement
    CREATE(LanguageType.DDL, OperationType.UNKNOWN, false), // create statement
    DELETE(LanguageType.DML, OperationType.WRITE, false), // the upcoming light-weight delete statement
    DESCRIBE(LanguageType.DDL, OperationType.READ, true), // describe/desc statement
    DETACH(LanguageType.DDL, OperationType.UNKNOWN, false), // detach statement
    DROP(LanguageType.DDL, OperationType.UNKNOWN, false), // drop statement
    EXISTS(LanguageType.DML, OperationType.READ, true), // exists statement
    EXPLAIN(LanguageType.DDL, OperationType.READ, true), // explain statement
    GRANT(LanguageType.DCL, OperationType.UNKNOWN, true), // grant statement
    INSERT(LanguageType.DML, OperationType.WRITE, false), // insert statement
    KILL(LanguageType.DCL, OperationType.UNKNOWN, false), // kill statement
    OPTIMIZE(LanguageType.DDL, OperationType.UNKNOWN, false), // optimize statement
    RENAME(LanguageType.DDL, OperationType.UNKNOWN, false), // rename statement
    REVOKE(LanguageType.DCL, OperationType.UNKNOWN, true), // revoke statement
    SELECT(LanguageType.DML, OperationType.READ, true), // select statement
    SET(LanguageType.DCL, OperationType.UNKNOWN, true), // set statement
    SHOW(LanguageType.DDL, OperationType.READ, true), // show statement
    SYSTEM(LanguageType.DDL, OperationType.UNKNOWN, false), // system statement
    TRUNCATE(LanguageType.DDL, OperationType.UNKNOWN, true), // truncate statement
    UPDATE(LanguageType.DML, OperationType.WRITE, false), // the upcoming light-weight update statement
    USE(LanguageType.DDL, OperationType.UNKNOWN, true), // use statement
    WATCH(LanguageType.DDL, OperationType.UNKNOWN, true), // watch statement
    TRANSACTION(LanguageType.TCL, OperationType.WRITE, true); // TCL statement

    private LanguageType langType;
    private OperationType opType;
    private boolean idempotent;

    StatementType(LanguageType langType, OperationType operationType, boolean idempotent) {
        this.langType = langType;
        this.opType = operationType;
        this.idempotent = idempotent;
    }

    LanguageType getLanguageType() {
        return this.langType;
    }

    OperationType getOperationType() {
        return this.opType;
    }

    boolean isIdempotent() {
        return this.idempotent;
    }
}
