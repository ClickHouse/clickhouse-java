package ru.yandex.clickhouse.util;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ClickHouseBlockChecksum {
    private final long first;
    private final long second;

    public ClickHouseBlockChecksum(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public static ClickHouseBlockChecksum fromBytes(byte[] checksum) {
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).put(checksum);
        ((Buffer) buffer).flip();
        return new ClickHouseBlockChecksum(buffer.getLong(), buffer.getLong());
    }

    public static ClickHouseBlockChecksum calculateForBlock(byte magic, int compressedSizeWithHeader, int uncompressedSize, byte[] data, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(compressedSizeWithHeader).order(ByteOrder.LITTLE_ENDIAN).put((byte)magic).putInt(compressedSizeWithHeader)
                .putInt(uncompressedSize).put(data, 0, length);
        ((Buffer) buffer).flip();
        return calculate(buffer.array());
    }

    public byte[] asBytes(){
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(first).putLong(second);
        ((Buffer) buffer).flip();
        return buffer.array();
    }

    private static ClickHouseBlockChecksum calculate(byte[] data) {
        long[] sum = ClickHouseCityHash.cityHash128(data, 0, data.length);
        return new ClickHouseBlockChecksum(sum[0], sum[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClickHouseBlockChecksum that = (ClickHouseBlockChecksum) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = (int) (first ^ (first >>> 32));
        result = 31 * result + (int) (second ^ (second >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" + first + ", " + second + '}';
    }
}
