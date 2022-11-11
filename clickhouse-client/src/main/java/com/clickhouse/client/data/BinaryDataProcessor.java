package com.clickhouse.client.data;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseDeserializer;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseSerializer;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

public interface BinaryDataProcessor {
    static class NullableDeserializer implements ClickHouseDeserializer {
        private final ClickHouseDeserializer deserializer;

        public NullableDeserializer(ClickHouseDeserializer deserializer) {
            this.deserializer = ClickHouseChecker.nonNull(deserializer, ClickHouseDeserializer.TYPE_NAME);
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return input.readBoolean() ? ref.resetToNullOrEmpty() : deserializer.deserialize(ref, input);
        }
    }

    static class NullableSerializer implements ClickHouseSerializer {
        private final ClickHouseSerializer serializer;

        public NullableSerializer(ClickHouseSerializer serializer) {
            this.serializer = ClickHouseChecker.nonNull(serializer, ClickHouseSerializer.TYPE_NAME);
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            if (value.isNullOrEmpty()) {
                output.writeBoolean(true);
            } else {
                serializer.serialize(value, output.writeBoolean(false));
            }
        }
    }

    static class DateDeSer implements ClickHouseDeserializer, ClickHouseSerializer {
        private static final Map<TimeZone, DateDeSer> cache = new HashMap<>();

        public static final DateDeSer of(ClickHouseConfig config) {
            TimeZone tz = ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).getTimeZoneForDate();
            return cache.computeIfAbsent(tz, DateDeSer::new);
        }

        protected final ZoneId zoneId;

        public DateDeSer(TimeZone tz) {
            this.zoneId = tz != null ? tz.toZoneId() : ClickHouseValues.SYS_ZONE;
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            LocalDate d = LocalDate.ofEpochDay(input.readBuffer(2).asUnsignedShort());
            if (!ClickHouseValues.SYS_ZONE.equals(zoneId)) {
                d = d.atStartOfDay(ClickHouseValues.SYS_ZONE).withZoneSameInstant(zoneId).toLocalDate();
            }
            return ref.update(d);
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            LocalDate d = value.asDate();
            if (!ClickHouseValues.SYS_ZONE.equals(zoneId)) {
                d = d.atStartOfDay(zoneId).withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate();
            }
            BinaryStreamUtils.writeUnsignedInt16(output,
                    ClickHouseChecker.between((int) d.toEpochDay(), ClickHouseValues.TYPE_DATE, 0,
                            BinaryStreamUtils.U_INT16_MAX));
        }
    }

    static class Date32DeSer implements ClickHouseDeserializer, ClickHouseSerializer {
        private static final Map<TimeZone, Date32DeSer> cache = new HashMap<>();

        public static final Date32DeSer of(ClickHouseConfig config) {
            TimeZone tz = ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).getTimeZoneForDate();
            return cache.computeIfAbsent(tz, Date32DeSer::new);
        }

        protected final ZoneId zoneId;

        public Date32DeSer(TimeZone tz) {
            this.zoneId = tz != null ? tz.toZoneId() : ClickHouseValues.SYS_ZONE;
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            LocalDate d = LocalDate.ofEpochDay(input.readBuffer(4).asInteger());
            if (!ClickHouseValues.SYS_ZONE.equals(zoneId)) {
                d = d.atStartOfDay(ClickHouseValues.SYS_ZONE).withZoneSameInstant(zoneId).toLocalDate();
            }
            return ref.update(d);
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            LocalDate d = value.asDate();
            if (!ClickHouseValues.SYS_ZONE.equals(zoneId)) {
                d = d.atStartOfDay(zoneId).withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate();
            }
            BinaryStreamUtils.writeInt32(output,
                    ClickHouseChecker.between((int) d.toEpochDay(), ClickHouseValues.TYPE_DATE,
                            BinaryStreamUtils.DATE32_MIN,
                            BinaryStreamUtils.DATE32_MAX));
        }
    }

    static class DateTime32DeSer implements ClickHouseDeserializer, ClickHouseSerializer {
        private static final Map<TimeZone, DateTime32DeSer> cache = new HashMap<>();

        public static final DateTime32DeSer of(ClickHouseConfig config, ClickHouseColumn column) {
            TimeZone tz = ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).hasTimeZone()
                    ? column.getTimeZone()
                    : ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).getUseTimeZone();
            return cache.computeIfAbsent(tz, DateTime32DeSer::new);
        }

        protected final ZoneId zoneId;

        public DateTime32DeSer(TimeZone tz) {
            this.zoneId = tz != null ? tz.toZoneId() : ClickHouseValues.UTC_ZONE;
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref.update(
                    LocalDateTime.ofInstant(Instant.ofEpochSecond(input.readBuffer(4).asUnsignedInteger()), zoneId));
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            long time = ClickHouseValues.UTC_ZONE.equals(zoneId) ? value.asDateTime().toEpochSecond(ZoneOffset.UTC)
                    : value.asDateTime().atZone(zoneId).toEpochSecond();

            BinaryStreamUtils.writeUnsignedInt32(output,
                    ClickHouseChecker.between(time, ClickHouseValues.TYPE_DATE_TIME, 0L,
                            BinaryStreamUtils.DATETIME_MAX));
        }
    }

    static class DateTime64DeSer implements ClickHouseDeserializer, ClickHouseSerializer {
        private static final int[] BASES = new int[] { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
                1000000000 };
        // use combined key for all timezones?
        private static final Map<Integer, DateTime64DeSer> cache = new HashMap<>();

        public static final DateTime64DeSer of(ClickHouseConfig config, ClickHouseColumn column) {
            TimeZone tz = ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).hasTimeZone()
                    ? column.getTimeZone()
                    : ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).getUseTimeZone();
            int scale = column.getScale();
            return ClickHouseValues.UTC_TIMEZONE.equals(tz)
                    ? cache.computeIfAbsent(scale, s -> new DateTime64DeSer(s, ClickHouseValues.UTC_TIMEZONE))
                    : new DateTime64DeSer(scale, tz);
        }

        private final ZoneId zoneId;
        private final int scale;

        public DateTime64DeSer(int scale, TimeZone tz) {
            this.scale = ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9);
            this.zoneId = tz != null ? tz.toZoneId() : ClickHouseValues.UTC_ZONE;
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            long value = input.readBuffer(8).asLong();
            int nanoSeconds = 0;
            if (scale > 0) {
                int factor = BASES[scale];
                nanoSeconds = (int) (value % factor);
                value /= factor;
                if (nanoSeconds < 0) {
                    nanoSeconds += factor;
                    value--;
                }
                if (nanoSeconds > 0L) {
                    nanoSeconds *= BASES[9 - scale];
                }
            }

            return ref.update(LocalDateTime.ofInstant(Instant.ofEpochSecond(value, nanoSeconds), zoneId));
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            LocalDateTime dt = value.asDateTime(scale);
            long v = ClickHouseChecker.between(
                    ClickHouseValues.UTC_ZONE.equals(zoneId) ? dt.toEpochSecond(ZoneOffset.UTC)
                            : dt.atZone(zoneId).toEpochSecond(),
                    ClickHouseValues.TYPE_DATE_TIME, BinaryStreamUtils.DATETIME64_MIN,
                    BinaryStreamUtils.DATETIME64_MAX);
            if (ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9) > 0) {
                v *= BASES[scale];
                int nanoSeconds = dt.getNano();
                if (nanoSeconds > 0L) {
                    v += nanoSeconds / BASES[9 - scale];
                }
            }

            BinaryStreamUtils.writeInt64(output, v);
        }
    }

    static class DecimalDeSer implements ClickHouseDeserializer, ClickHouseSerializer {
        protected final int scale;

        public static DecimalDeSer of(ClickHouseColumn column) {
            return of(ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getPrecision(), column.getScale());
        }

        public static DecimalDeSer of(int precision, int scale) {
            if (precision > ClickHouseDataType.Decimal128.getMaxScale()) {
                return new Decimal256DeSer(scale);
            } else if (precision > ClickHouseDataType.Decimal64.getMaxScale()) {
                return new Decimal128DeSer(scale);
            } else if (precision > ClickHouseDataType.Decimal32.getMaxScale()) {
                return new Decimal64DeSer(scale);
            } else {
                return new Decimal32DeSer(scale);
            }
        }

        public DecimalDeSer(int scale) {
            this.scale = scale;
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    static class Decimal32DeSer extends DecimalDeSer {
        private static final Map<Integer, DecimalDeSer> cache = new HashMap<>();

        public static final DecimalDeSer of(ClickHouseColumn column) {
            int scale = ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getScale();
            return cache.computeIfAbsent(scale, Decimal32DeSer::new);
        }

        public Decimal32DeSer(int scale) {
            super(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                    ClickHouseDataType.Decimal32.getMaxScale()));
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref.update(BigDecimal.valueOf(input.readBuffer(4).asInteger(), scale));
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            BigDecimal v = value.asBigDecimal();
            BinaryStreamUtils.writeInt32(output,
                    ClickHouseChecker.between(scale == 0 ? v : v.multiply(BigDecimal.TEN.pow(scale)),
                            ClickHouseValues.TYPE_BIG_DECIMAL, BinaryStreamUtils.DECIMAL32_MIN,
                            BinaryStreamUtils.DECIMAL32_MAX).intValue());
        }
    }

    static class Decimal64DeSer extends DecimalDeSer {
        private static final Map<Integer, DecimalDeSer> cache = new HashMap<>();

        public static final DecimalDeSer of(ClickHouseColumn column) {
            int scale = ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getScale();
            return cache.computeIfAbsent(scale, Decimal64DeSer::new);
        }

        public Decimal64DeSer(ClickHouseColumn column) {
            this(ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getScale());
        }

        public Decimal64DeSer(int scale) {
            super(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                    ClickHouseDataType.Decimal64.getMaxScale()));
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref.update(BigDecimal.valueOf(input.readBuffer(8).asLong(), scale));
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            BigDecimal v = value.asBigDecimal();
            BinaryStreamUtils.writeInt64(output,
                    ClickHouseChecker.between(scale == 0 ? v : v.multiply(BigDecimal.TEN.pow(scale)),
                            ClickHouseValues.TYPE_BIG_DECIMAL, BinaryStreamUtils.DECIMAL64_MIN,
                            BinaryStreamUtils.DECIMAL64_MAX).longValue());
        }
    }

    static class Decimal128DeSer extends DecimalDeSer {
        private static final Map<Integer, DecimalDeSer> cache = new HashMap<>();

        public static final DecimalDeSer of(ClickHouseColumn column) {
            int scale = ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getScale();
            return cache.computeIfAbsent(scale, Decimal128DeSer::new);
        }

        public Decimal128DeSer(ClickHouseColumn column) {
            this(ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getScale());
        }

        public Decimal128DeSer(int scale) {
            super(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                    ClickHouseDataType.Decimal128.getMaxScale()));
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref.update(BinaryStreamUtils.readDecimal128(input, scale));
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            BigDecimal v = value.asBigDecimal();
            BinaryStreamUtils.writeInt128(output,
                    ClickHouseChecker.between(scale == 0 ? v : v.multiply(BigDecimal.TEN.pow(scale)),
                            ClickHouseValues.TYPE_BIG_DECIMAL, BinaryStreamUtils.DECIMAL128_MIN,
                            BinaryStreamUtils.DECIMAL128_MAX).toBigInteger());
        }
    }

    static class Decimal256DeSer extends DecimalDeSer {
        private static final Map<Integer, DecimalDeSer> cache = new HashMap<>();

        public static final DecimalDeSer of(ClickHouseColumn column) {
            int scale = ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getScale();
            return cache.computeIfAbsent(scale, Decimal256DeSer::new);
        }

        public Decimal256DeSer(ClickHouseColumn column) {
            this(ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getScale());
        }

        public Decimal256DeSer(int scale) {
            super(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                    ClickHouseDataType.Decimal256.getMaxScale()));
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref.update(BinaryStreamUtils.readDecimal256(input, scale));
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            BigDecimal v = value.asBigDecimal();
            BinaryStreamUtils.writeInt256(output,
                    ClickHouseChecker.between(scale == 0 ? v : v.multiply(BigDecimal.TEN.pow(scale)),
                            ClickHouseValues.TYPE_BIG_DECIMAL, BinaryStreamUtils.DECIMAL256_MIN,
                            BinaryStreamUtils.DECIMAL256_MAX).toBigInteger());
        }
    }

    static class FixedStringDeSer implements ClickHouseDeserializer, ClickHouseSerializer {
        private final int length;

        public FixedStringDeSer(ClickHouseColumn column) {
            this(ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).getPrecision());
        }

        public FixedStringDeSer(int length) {
            if (length < 1) {
                throw new IllegalArgumentException("Length should be greater than zero");
            }
            this.length = length;
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref.update(input.readBuffer(length).compact().array());
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            byte[] bytes = value.asBinary();
            if (bytes.length != length) {
                byte[] b = new byte[length];
                System.arraycopy(bytes, 0, b, 0, Math.min(bytes.length, length));
                output.write(b);
            } else {
                output.write(bytes);
            }
        }
    }

    static ClickHouseValue readByteArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(input.readVarInt()).compact().array());
    }

    static ClickHouseValue readShortArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(input.readVarInt() * 2).asShortArray());
    }

    static ClickHouseValue readIntegerArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(input.readVarInt() * 4).asIntegerArray());
    }

    static ClickHouseValue readLongArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(input.readVarInt() * 8).asLongArray());
    }

    static ClickHouseValue readFloatArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(input.readVarInt() * 4).asFloatArray());
    }

    static ClickHouseValue readDoubleArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(input.readVarInt() * 8).asDoubleArray());
    }

    static ClickHouseValue readBool(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBoolean());
    }

    static void writeBool(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        output.writeBoolean(value.asBoolean());
    }

    static ClickHouseValue readEnum8(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        // ClickHouseEnumValue.of(r, c.getEnumConstants(), i.readByte())
        return ref.update(input.readByte());
    }

    static void writeEnum8(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        output.writeByte(value.asByte());
    }

    static ClickHouseValue readEnum16(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        // ClickHouseEnumValue.of(r, c.getEnumConstants(),
        // BinaryStreamUtils.readInt16(i))
        return ref.update(BinaryStreamUtils.readInt16(input));
    }

    static void writeEnum16(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInt16(output, value.asShort());
    }

    static ClickHouseValue readByte(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readByte());
    }

    static void writeByte(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        output.writeByte(value.asByte());
    }

    static ClickHouseValue readUInt8AsShort(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ClickHouseShortValue.of(ref, input.readUnsignedByte(), false);
    }

    static ClickHouseValue readShort(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readInt16(input));
    }

    static void writeShort(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInt16(output, value.asShort());
    }

    static ClickHouseValue readUInt16AsInt(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ClickHouseIntegerValue.of(ref, input.readBuffer(2).asUnsignedShort(), false);
    }

    static ClickHouseValue readInteger(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readInt32(input));
    }

    static void writeInteger(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInt32(output, value.asInteger());
    }

    static ClickHouseValue readUInt32AsLong(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ClickHouseLongValue.of(ref, input.readBuffer(4).asUnsignedInteger(), false);
    }

    static ClickHouseValue readLong(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(8).asLong());
    }

    static void writeLong(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInt64(output, value.asLong());
    }

    static ClickHouseValue readFloat(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(4).asFloat());
    }

    static void writeFloat(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeFloat32(output, value.asFloat());
    }

    static ClickHouseValue readDouble(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(8).asDouble());
    }

    static void writeDouble(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeFloat64(output, value.asDouble());
    }

    static ClickHouseValue readInt128(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(16).asBigInteger());
    }

    static void writeInt128(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInt128(output, value.asBigInteger());
    }

    static ClickHouseValue readUInt128(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(16).asUnsignedBigInteger());
    }

    static void writeUInt128(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeUnsignedInt128(output, value.asBigInteger());
    }

    static ClickHouseValue readInt256(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(32).asBigInteger());
    }

    static void writeInt256(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInt256(output, value.asBigInteger());
    }

    static ClickHouseValue readUInt256(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(32).asUnsignedBigInteger());
    }

    static void writeUInt256(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeUnsignedInt256(output, value.asBigInteger());
    }

    static ClickHouseValue readIpv4(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readInet4Address(input));
    }

    static void writeIpv4(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInet4Address(output, value.asInet4Address());
    }

    static ClickHouseValue readIpv6(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readInet6Address(input));
    }

    static void writeIpv6(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeInet6Address(output, value.asInet6Address());
    }

    static ClickHouseValue readBinaryString(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBytes(input.readVarInt()));
    }

    static void writeBinaryString(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        byte[] bytes = value.asBinary();
        output.writeVarInt(bytes.length).writeBytes(bytes);
    }

    static ClickHouseValue readTextString(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readUnicodeString());
    }

    static void writeTextString(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        output.writeUnicodeString(value.asString());
    }

    static ClickHouseValue readUuid(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readUuid(input));
    }

    static void writeUuid(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeUuid(output, value.asUuid());
    }

    static ClickHouseValue readGeoPoint(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readGeoPoint(input));
    }

    static void writeGeoPoint(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeGeoPoint(output, value.asObject(double[].class));
    }

    static ClickHouseValue readGeoRing(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readGeoRing(input));
    }

    static void writeGeoRing(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeGeoRing(output, value.asObject(double[][].class));
    }

    static ClickHouseValue readGeoPolygon(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readGeoPolygon(input));
    }

    static void writeGeoPolygon(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeGeoPolygon(output, value.asObject(double[][][].class));
    }

    static ClickHouseValue readGeoMultiPolygon(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(BinaryStreamUtils.readGeoMultiPolygon(input));
    }

    static void writeGeoMultiPolygon(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
        BinaryStreamUtils.writeGeoMultiPolygon(output, value.asObject(double[][][][].class));
    }
}
