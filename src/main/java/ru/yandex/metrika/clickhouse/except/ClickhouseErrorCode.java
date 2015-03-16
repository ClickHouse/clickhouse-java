package ru.yandex.metrika.clickhouse.except;

import java.util.*;

/**
* @author lopashev
* @since 18.02.15
*/
public enum ClickhouseErrorCode {
    OK                                      (0),
    NOT_FOUND_COLUMN_IN_BLOCK               (10),
    ATTEMPT_TO_READ_AFTER_EOF               (32),
    ILLEGAL_TYPE_OF_ARGUMENT                (43),
    ILLEGAL_COLUMN                          (44),
    UNKNOWN_IDENTIFIER                      (47),
    NOT_IMPLEMENTED                         (48),
    LOGICAL_ERROR                           (49),
    TYPE_MISMATCH                           (53),
    UNKNOWN_TABLE                           (60),
    SYNTAX_ERROR                            (62),
    TOO_MUCH_ROWS                           (158),
    TIMEOUT_EXCEEDED                        (159),
    TOO_SLOW                                (160),
    TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS    (166),
    TOO_BIG_AST                             (168),
    MULTIPLE_EXPRESSIONS_FOR_ALIAS          (179),
    SET_SIZE_LIMIT_EXCEEDED                 (191),
    SOCKET_TIMEOUT                          (209),
    NETWORK_ERROR                           (210),
    EMPTY_QUERY                             (211),
    MEMORY_LIMIT_EXCEEDED                   (241),
    POCO_EXCEPTION                          (1000);

    public final Integer code;

    private static final Map<Integer, ClickhouseErrorCode> byCodes;
    static {
        Map<Integer, ClickhouseErrorCode> map = new HashMap<Integer, ClickhouseErrorCode>();
        for (ClickhouseErrorCode errorCode : values())
            map.put(errorCode.code, errorCode);
        byCodes = Collections.unmodifiableMap(map);
    }

    ClickhouseErrorCode(Integer code) {
        this.code = code;
    }

    public static final Set<ClickhouseErrorCode> ALL = Collections.unmodifiableSet(EnumSet.allOf(ClickhouseErrorCode.class));

    public static final Set<ClickhouseErrorCode> API = Collections.unmodifiableSet(EnumSet.of(
            EMPTY_QUERY,
            NOT_FOUND_COLUMN_IN_BLOCK,
            ILLEGAL_TYPE_OF_ARGUMENT,
            ILLEGAL_COLUMN,
            UNKNOWN_IDENTIFIER,
            NOT_IMPLEMENTED,
            LOGICAL_ERROR,
            TYPE_MISMATCH,
            UNKNOWN_TABLE,
            SYNTAX_ERROR,
            TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS,
            TOO_BIG_AST,
            MULTIPLE_EXPRESSIONS_FOR_ALIAS,
            SET_SIZE_LIMIT_EXCEEDED,
            MEMORY_LIMIT_EXCEEDED
    ));

    public static final Set<ClickhouseErrorCode> DB = Collections.unmodifiableSet(EnumSet.of(
            ATTEMPT_TO_READ_AFTER_EOF,
            SOCKET_TIMEOUT,
            NETWORK_ERROR,
            POCO_EXCEPTION
    ));

    public static final Set<ClickhouseErrorCode> QUERY = Collections.unmodifiableSet(EnumSet.of(
            TOO_MUCH_ROWS,
            TIMEOUT_EXCEEDED,
            TOO_SLOW
    ));

    public static ClickhouseErrorCode fromCode(Integer code) {
        return byCodes.get(code);
    }

    @Override
    public String toString() {
        return name() + " (code " + code + ')';
    }
}
