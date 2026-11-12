package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.data.ClickHouseColumn;

import java.util.List;

/**
 * Position inside one scope: a row, array, tuple, map, or a single variant/dynamic value.
 */
final class SlotCursor {

    static final int UNKNOWN = -1;

    enum Kind {
        ROW,
        ARRAY,
        TUPLE,
        MAP,
        SINGLE,
        JSON
    }

    final Kind kind;

    private final List<ClickHouseColumn> slots;

    private final ClickHouseColumn repeated;

    private final ClickHouseColumn mapKey;

    private final ClickHouseColumn mapValue;

    /** Expected number of completed values, or {@link #UNKNOWN}. */
    final int declared;

    private int index;

    private boolean expectValue;

    private SlotCursor(Kind kind, List<ClickHouseColumn> slots, ClickHouseColumn repeated,
                       ClickHouseColumn mapKey, ClickHouseColumn mapValue, int declared) {
        this.kind = kind;
        this.slots = slots;
        this.repeated = repeated;
        this.mapKey = mapKey;
        this.mapValue = mapValue;
        this.declared = declared;
    }

    static SlotCursor row(List<ClickHouseColumn> columns) {
        return new SlotCursor(Kind.ROW, columns, null, null, null, columns.size());
    }

    static SlotCursor array(ClickHouseColumn element, int size) {
        return new SlotCursor(Kind.ARRAY, null, element, null, null, size);
    }

    static SlotCursor tuple(List<ClickHouseColumn> fields) {
        return new SlotCursor(Kind.TUPLE, fields, null, null, null, fields.size());
    }

    static SlotCursor map(ClickHouseColumn key, ClickHouseColumn value, int pairs) {
        return new SlotCursor(Kind.MAP, null, null, key, value, pairs);
    }

    static SlotCursor single(ClickHouseColumn value) {
        return new SlotCursor(Kind.SINGLE, null, value, null, null, 1);
    }

    /** A JSON object. {@code paths} is the number of leaf paths, or {@link #UNKNOWN}. */
    static SlotCursor json(int paths) {
        return new SlotCursor(Kind.JSON, null, null, null, null, paths);
    }

    ClickHouseColumn current() {
        switch (kind) {
            case ROW:
            case TUPLE:
                if (index >= slots.size()) {
                    throw new IllegalStateException("No more values in this " + kind.name().toLowerCase());
                }
                return slots.get(index);
            case ARRAY:
                if (declared != UNKNOWN && index >= declared) {
                    throw new IllegalStateException("No more array elements");
                }
                return repeated;
            case MAP:
                if (expectValue) {
                    return mapValue;
                }
                if (declared != UNKNOWN && index >= declared) {
                    throw new IllegalStateException("No more map entries");
                }
                return mapKey;
            case SINGLE:
                if (index >= 1) {
                    throw new IllegalStateException("The value has already been written");
                }
                return repeated;
            case JSON:
                throw new IllegalStateException("A JSON value is written by path");
            default:
                throw new IllegalStateException("Unknown scope");
        }
    }

    ClickHouseColumn repeated() {
        return repeated;
    }

    boolean expectingValue() {
        return expectValue;
    }

    int completed() {
        return index;
    }

    int size() {
        if (kind == Kind.ARRAY || kind == Kind.MAP) {
            return declared;
        }
        if (kind == Kind.SINGLE) {
            return 1;
        }
        if (kind == Kind.JSON) {
            return declared;
        }
        return slots.size();
    }

    boolean hasNext() {
        if (kind == Kind.MAP) {
            return !expectValue && (declared == UNKNOWN || index < declared);
        }
        if (kind == Kind.JSON) {
            return declared == UNKNOWN || index < declared;
        }
        int limit = kind == Kind.ARRAY ? declared : (kind == Kind.SINGLE ? 1 : slots.size());
        return index < limit;
    }

    int remaining() {
        if (kind == Kind.ARRAY) {
            return declared - index;
        }
        throw new IllegalStateException("remaining() is only valid inside an array");
    }

    void advance() {
        if (kind == Kind.MAP) {
            if (!expectValue) {
                expectValue = true;
            } else {
                expectValue = false;
                index++;
            }
            return;
        }
        index++;
    }

    /** Adds leaf paths contributed by a nested object that was flattened into this one. */
    void credit(int paths) {
        index += paths;
    }

    void forceComplete() {
        index = declared == UNKNOWN ? 1 : declared;
        expectValue = false;
    }

    void checkComplete() {
        if (kind == Kind.MAP && expectValue) {
            throw new IllegalStateException("Map entry is missing a value");
        }
        if (declared != UNKNOWN && index != declared) {
            throw new IllegalStateException("Expected " + declared + " values but completed " + index);
        }
    }
}
