package ru.yandex.metrika.clickhouse.except;

import java.util.*;

/**
* @author lopashev
* @since 18.02.15
*/
public enum CHErrorCode {
    OK                                      (0),
    NOT_FOUND_COLUMN_IN_BLOCK               (10),
    ATTEMPT_TO_READ_AFTER_EOF               (32),
    ILLEGAL_TYPE_OF_ARGUMENT                (43),
    ILLEGAL_COLUMN                          (44),
    UNKNOWN_IDENTIFIER                      (47),
    NOT_IMPLEMENTED                         (48),
    LOGICAL_ERROR                           (49),
    TYPE_MISMATCH                           (53),
    TABLE_ALREADY_EXISTS                    (57),
    UNKNOWN_TABLE                           (60),
    SYNTAX_ERROR                            (62),
    TOO_MUCH_ROWS                           (158),
    TIMEOUT_EXCEEDED                        (159),
    TOO_SLOW                                (160),
    TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS    (166),
    TOO_BIG_AST                             (168),
    CYCLIC_ALIASES                          (174),
    MULTIPLE_EXPRESSIONS_FOR_ALIAS          (179),
    SET_SIZE_LIMIT_EXCEEDED                 (191),
    SOCKET_TIMEOUT                          (209),
    NETWORK_ERROR                           (210),
    EMPTY_QUERY                             (211),
    MEMORY_LIMIT_EXCEEDED                   (241),
    TOO_MUCH_PARTS                          (252),
    DOUBLE_DISTRIBUTED                      (288),
    POCO_EXCEPTION                          (1000),
    UNKNOWN_EXCEPTION                       (1002);

    public final Integer code;

    private static final Map<Integer, CHErrorCode> byCodes;
    static {
        Map<Integer, CHErrorCode> map = new HashMap<Integer, CHErrorCode>();
        for (CHErrorCode errorCode : values())
            map.put(errorCode.code, errorCode);
        byCodes = Collections.unmodifiableMap(map);
    }

    CHErrorCode(Integer code) {
        this.code = code;
    }


    public static CHErrorCode fromCode(Integer code) {
        return byCodes.get(code);
    }

    @Override
    public String toString() {
        return name() + " (code " + code + ')';
    }
}
