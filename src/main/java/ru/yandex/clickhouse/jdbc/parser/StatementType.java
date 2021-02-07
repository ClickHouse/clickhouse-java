package ru.yandex.clickhouse.jdbc.parser;

public enum StatementType {
    UNKNOWN(LanguageType.UNKNOWN, OperationType.UNKNOWN), // unknown statement
    ALTER(LanguageType.DDL, OperationType.UNKNOWN), // alter statement
    ALTER_DELETE(LanguageType.DDL, OperationType.WRITE), // delete statement
    ALTER_UPDATE(LanguageType.DDL, OperationType.WRITE), // update statement
    ATTACH(LanguageType.DDL, OperationType.UNKNOWN), // attach statement
    CHECK(LanguageType.DDL, OperationType.UNKNOWN), // check statement
    CREATE(LanguageType.DDL, OperationType.UNKNOWN), // create statement
    DELETE(LanguageType.DML, OperationType.WRITE), // the upcoming light-weight delete statement
    DESCRIBE(LanguageType.DDL, OperationType.READ), // describe/desc statement
    DETACH(LanguageType.DDL, OperationType.UNKNOWN), // detach statement
    DROP(LanguageType.DDL, OperationType.UNKNOWN), // drop statement
    EXISTS(LanguageType.DML, OperationType.READ), // exists statement
    EXPLAIN(LanguageType.DDL, OperationType.READ), // explain statement
    GRANT(LanguageType.DCL, OperationType.UNKNOWN), // grant statement
    INSERT(LanguageType.DML, OperationType.WRITE), // insert statement
    KILL(LanguageType.DCL, OperationType.UNKNOWN), // kill statement
    OPTIMIZE(LanguageType.DDL, OperationType.UNKNOWN), // optimize statement
    RENAME(LanguageType.DDL, OperationType.UNKNOWN), // rename statement
    REVOKE(LanguageType.DCL, OperationType.UNKNOWN), // revoke statement
    SELECT(LanguageType.DML, OperationType.READ), // select statement
    SET(LanguageType.DCL, OperationType.UNKNOWN), // set statement
    SHOW(LanguageType.DDL, OperationType.READ), // show statement
    SYSTEM(LanguageType.DDL, OperationType.UNKNOWN), // system statement
    TRUNCATE(LanguageType.DDL, OperationType.UNKNOWN), // truncate statement
    UPDATE(LanguageType.DML, OperationType.WRITE), // the upcoming light-weight update statement
    USE(LanguageType.DDL, OperationType.UNKNOWN), // use statement
    WATCH(LanguageType.DDL, OperationType.UNKNOWN); // watch statement

    private LanguageType langType;
    private OperationType opType;

    StatementType(LanguageType langType, OperationType operationType) {
        this.langType = langType;
        this.opType = operationType;
    }

    LanguageType getLanguageType() {
        return this.langType;
    }

    OperationType getOperationType() {
        return this.opType;
    }
}
