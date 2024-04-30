package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static com.clickhouse.data.ClickHouseDataType.*;

public class SerializerUtils {
    public static void serializeData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the value to the stream based on the data type
        switch (column.getDataType()) {
            case Array:
                serializeArrayData(stream, value, column);
                break;
            case Tuple:
                serializeTupleData(stream, value, column);
                break;
            case Map:
                serializeMapData(stream, value, column);
                break;
            default:
                serializePrimitiveData(stream, value, column);
                break;

        }
    }

    private static void serializeArrayData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the array to the stream
        //The array is a list of values
        List<?> values = (List<?>) value;
        BinaryStreamUtils.writeVarInt(stream, values.size());
        for (Object val : values) {
            serializeData(stream, val, column.getArrayBaseColumn());
        }
    }

    private static void serializeTupleData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the tuple to the stream
        //The tuple is a list of values
        List<?> values = (List<?>) value;
        for (int i = 0; i < values.size(); i++) {
            serializeData(stream, values.get(i), column.getNestedColumns().get(i));
        }
    }

    private static void serializeMapData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the map to the stream
        //The map is a list of key-value pairs
        Map<?, ?> map = (Map<?, ?>) value;
        BinaryStreamUtils.writeVarInt(stream, map.size());
        map.forEach((key, val) -> {
            try {
                serializePrimitiveData(stream, key, Objects.requireNonNull(column.getKeyInfo()));
                serializeData(stream, val, Objects.requireNonNull(column.getValueInfo()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void serializePrimitiveData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the value to the stream based on the type
        switch (column.getDataType()) {
            case Int8:
                BinaryStreamUtils.writeInt8(stream, convertToInteger(value));
                break;
            case Int16:
                BinaryStreamUtils.writeInt16(stream, convertToInteger(value));
                break;
            case Int32:
                BinaryStreamUtils.writeInt32(stream, convertToInteger(value));
                break;
            case Int64:
                BinaryStreamUtils.writeInt64(stream, convertToLong(value));
                break;
            case Int128:
                BinaryStreamUtils.writeInt128(stream, convertToBigInteger(value));
                break;
            case Int256:
                BinaryStreamUtils.writeInt256(stream, convertToBigInteger(value));
                break;
            case UInt8:
                BinaryStreamUtils.writeUnsignedInt8(stream, convertToInteger(value));
                break;
            case UInt16:
                BinaryStreamUtils.writeUnsignedInt16(stream, convertToInteger(value));
                break;
            case UInt32:
                BinaryStreamUtils.writeUnsignedInt32(stream, convertToLong(value));
                break;
            case UInt64:
                BinaryStreamUtils.writeUnsignedInt64(stream, convertToLong(value));
                break;
            case UInt128:
                BinaryStreamUtils.writeUnsignedInt128(stream, convertToBigInteger(value));
                break;
            case UInt256:
                BinaryStreamUtils.writeUnsignedInt256(stream, convertToBigInteger(value));
                break;
            case Float32:
                BinaryStreamUtils.writeFloat32(stream, (Float) value);
                break;
            case Float64:
                BinaryStreamUtils.writeFloat64(stream, (Double) value);
                break;
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                BinaryStreamUtils.writeDecimal(stream, (BigDecimal) value, column.getPrecision(), column.getScale());
                break;
            case Bool:
                BinaryStreamUtils.writeBoolean(stream, (Boolean) value);
                break;
            case String:
                BinaryStreamUtils.writeString(stream, (String) value);
                break;
            case FixedString:
                BinaryStreamUtils.writeFixedString(stream, (String) value, column.getPrecision());
                break;
            case Date:
                BinaryStreamUtils.writeDate(stream, (LocalDate) value);
                break;
            case Date32:
                BinaryStreamUtils.writeDate32(stream, (LocalDate) value);
                break;
            case DateTime: //TODO: Discuss LocalDateTime vs ZonedDateTime and time zones (Who uses LocalDateTime?)
                BinaryStreamUtils.writeDateTime(stream, (LocalDateTime) value, column.getTimeZone());
                break;
            case DateTime64:
                BinaryStreamUtils.writeDateTime64(stream, (LocalDateTime) value, column.getScale(), column.getTimeZone());
                break;
            case UUID:
                BinaryStreamUtils.writeUuid(stream, (UUID) value);
                break;
            case Enum8:
                BinaryStreamUtils.writeEnum8(stream, (Byte) value);
                break;
            case Enum16:
                BinaryStreamUtils.writeEnum16(stream, convertToInteger(value));
                break;
            case IPv4:
                BinaryStreamUtils.writeInet4Address(stream, (Inet4Address) value);
                break;
            case IPv6:
                BinaryStreamUtils.writeInet6Address(stream, (Inet6Address) value);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + column.getDataType());
        }
    }


    public static Integer convertToInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to Integer");
        }
    }

    public static Long convertToLong(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1L : 0L;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to Long");
        }
    }

    public static BigInteger convertToBigInteger(Object value) {
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        } else if (value instanceof String) {
            return new BigInteger((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to BigInteger");
        }
    }


}
