package com.clickhouse.data;

import java.io.IOException;

/**
 * This class defines custom reading/writing logic, which can be used in
 * {@link ClickHouseInputStream#readCustom(ClickHouseDataUpdater)} and
 * {@link ClickHouseOutputStream#writeCustom(ClickHouseDataUpdater)}.
 */
@Deprecated
@FunctionalInterface
public interface ClickHouseDataUpdater {
    /**
     * Byte array(from {@code position} to {@code limit}) to update, usually read or
     * write.
     *
     * @param bytes    non-null byte array to update
     * @param position zero-based index indicating start position of the byte array
     * @param limit    zero-based index indicating end position of the byte array,
     *                 it should always greater than or equal to {@code position}
     * @return negative number, usually -1, indicates to more to update, or other
     *         number for bytes being updated
     * @throws IOException when it failed to update
     */
    int update(byte[] bytes, int position, int limit) throws IOException;
}
