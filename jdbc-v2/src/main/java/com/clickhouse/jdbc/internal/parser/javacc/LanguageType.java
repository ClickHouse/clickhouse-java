package com.clickhouse.jdbc.internal.parser.javacc;

public enum LanguageType {
    UNKNOWN, // unknown language
    DCL, // data control language
    DDL, // data definition language
    DML, // data manipulation language
    TCL // transaction control language
}
