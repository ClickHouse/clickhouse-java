package com.clickhouse.client.query;

import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

public class QuerySamplePOJO {
    private int int8;
    private int int8_default;
    private int int16;
    private int int16_default;
    private int int32;
    private int int32_default;
    private long int64;
    private long int64_default;
    private BigInteger int128;
    private BigInteger int128_default;
    private BigInteger int256;
    private BigInteger int256_default;

    private int uint8;
    private int uint16;
    private long uint32;
    private BigInteger uint64;
    private BigInteger uint128;
    private BigInteger uint256;

    private float float32;
    private double float64;

    private BigDecimal decimal32;
    private BigDecimal decimal64;
    private BigDecimal decimal128;
    private BigDecimal decimal256;

    private boolean bool;

    private String string;
    private String fixedString;

    private LocalDate date;
    private LocalDate date32;

    private LocalDateTime dateTime;
    private LocalDateTime dateTime64;

    private UUID uuid;

    private byte enum8;
    private int enum16;

    private Inet4Address ipv4;
    private Inet6Address ipv6;

    private List<String> array;
    private List<?> tuple;

    private Object[] tupleArray;

    private Map<String, Integer> map;
    private List<Integer> nestedInnerInt;
    private List<String> nestedInnerString;

    public QuerySamplePOJO() {
        final Random random = new Random();
        int8 = random.nextInt(128);
        int16 = random.nextInt(32768);
        int32 = random.nextInt();
        int64 = random.nextLong();
        BigInteger upper = BigInteger.valueOf(random.nextLong()).shiftLeft(64);
        BigInteger lower = BigInteger.valueOf(random.nextLong()).and(BigInteger.valueOf(Long.MAX_VALUE));

        int128 = upper.or(lower);

        BigInteger upper1 = BigInteger.valueOf(random.nextLong()).shiftLeft(192);
        BigInteger upper2 = BigInteger.valueOf(random.nextLong()).shiftLeft(128);
        BigInteger lower1 = BigInteger.valueOf(random.nextLong()).shiftLeft(64);
        BigInteger lower2 = BigInteger.valueOf(random.nextLong()).and(BigInteger.valueOf(Long.MAX_VALUE));

        int256 = upper1.or(upper2).or(lower1).or(lower2);


        uint8 = random.nextInt(255);
        uint16 = random.nextInt(32768);
        uint32 = (long) (random.nextDouble() * 4294967295L);

        long rndUInt64 = random.nextLong();
        uint64 = BigInteger.valueOf(rndUInt64);
        if (rndUInt64 < 0) {
            uint64 = uint64.add(BigInteger.ONE.shiftLeft(64));
        }

        uint128 = upper.or(lower).abs();
        uint256 = upper1.or(upper2).or(lower1).or(lower2).abs();

        float32 = random.nextFloat();
        float64 = random.nextDouble();

        decimal32 = BigDecimal.valueOf(random.nextDouble());
        decimal64 = BigDecimal.valueOf(random.nextDouble());
        decimal128 = BigDecimal.valueOf(random.nextDouble());
        decimal256 = BigDecimal.valueOf(random.nextDouble());

        bool = random.nextBoolean();

        string = RandomStringUtils.randomAlphabetic(1, 256);
        fixedString = RandomStringUtils.randomAlphabetic(3);

        date = LocalDate.now();
        date32 = LocalDate.now();

        dateTime = LocalDateTime.now();
        dateTime64 = LocalDateTime.now();

        uuid = UUID.randomUUID();

        enum8 = (byte) random.nextInt(27);
        enum16 = random.nextInt(27);

        try {
            byte[] addr4 = new byte[4];
            random.nextBytes(addr4);
            ipv4 = (Inet4Address) Inet4Address.getByAddress(addr4);

            byte[] addr6 = new byte[16];
            random.nextBytes(addr6);
            ipv6 = (Inet6Address) Inet6Address.getByAddress(addr6);
        } catch (UnknownHostException e) {
            ipv4 = null;
            ipv6 = null;
        }

        array = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
        tuple = Arrays.asList(random.nextInt(), random.nextDouble(), "a", "b");
        tupleArray = new Object[] {random.nextInt(), random.nextDouble(), "c", "d" };
        map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            map.put(String.valueOf((char) ('a' + i)), i + 1);
        }

        List<Integer> innerInt = new ArrayList<>();
        innerInt.add(random.nextInt(Integer.MAX_VALUE));
        nestedInnerInt = innerInt;

        List<String> innerString = new ArrayList<>();
        innerString.add(RandomStringUtils.randomAlphabetic(1, 256));
        nestedInnerString = innerString;
    }

    public int getInt8() {
        return int8;
    }

    public void setInt8(int int8) {
        this.int8 = int8;
    }

    public int getInt8Default() {
        return int8_default;
    }

    public int getInt16() {
        return int16;
    }

    public void setInt16(int int16) {
        this.int16 = int16;
    }

    public int getInt16Default() {
        return int16_default;
    }

    public int getInt32() {
        return int32;
    }

    public int getInt32Default() {
        return int32_default;
    }

    public void setInt32(int int32) {
        this.int32 = int32;
    }

    public long getInt64() {
        return int64;
    }

    public long getInt64Default() {
        return int64_default;
    }

    public void setInt64(long int64) {
        this.int64 = int64;
    }

    public BigInteger getInt128() {
        return int128;
    }

    public BigInteger getInt128Default() {
        return int128_default;
    }

    public void setInt128(BigInteger int128) {
        this.int128 = int128;
    }

    public BigInteger getInt256() {
        return int256;
    }

    public BigInteger getInt256Default() {
        return int256_default;
    }

    public void setInt256(BigInteger int256) {
        this.int256 = int256;
    }

    public int getUint8() {
        return uint8;
    }

    public void setUint8(int uint8) {
        this.uint8 = uint8;
    }

    public int getUint16() {
        return uint16;
    }

    public void setUint16(int uint16) {
        this.uint16 = uint16;
    }

    public long getUint32() {
        return uint32;
    }

    public void setUint32(long uint32) {
        this.uint32 = uint32;
    }

    public BigInteger getUint64() {
        return uint64;
    }

    public void setUint64(BigInteger uint64) {
        this.uint64 = uint64;
    }

    public BigInteger getUint128() {
        return uint128;
    }

    public void setUint128(BigInteger uint128) {
        this.uint128 = uint128;
    }

    public BigInteger getUint256() {
        return uint256;
    }

    public void setUint256(BigInteger uint256) {
        this.uint256 = uint256;
    }

    public float getFloat32() {
        return float32;
    }

    public void setFloat32(float float32) {
        this.float32 = float32;
    }

    public double getFloat64() {
        return float64;
    }

    public void setFloat64(double float64) {
        this.float64 = float64;
    }

    public BigDecimal getDecimal32() {
        return decimal32;
    }

    public void setDecimal32(BigDecimal decimal32) {
        this.decimal32 = decimal32;
    }

    public BigDecimal getDecimal64() {
        return decimal64;
    }

    public void setDecimal64(BigDecimal decimal64) {
        this.decimal64 = decimal64;
    }

    public BigDecimal getDecimal128() {
        return decimal128;
    }

    public void setDecimal128(BigDecimal decimal128) {
        this.decimal128 = decimal128;
    }

    public BigDecimal getDecimal256() {
        return decimal256;
    }

    public void setDecimal256(BigDecimal decimal256) {
        this.decimal256 = decimal256;
    }

    public boolean isBool() {
        return bool;
    }

    public void setBool(boolean bool) {
        this.bool = bool;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public String getFixedString() {
        return fixedString;
    }

    public void setFixedString(String fixedString) {
        this.fixedString = fixedString;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public LocalDate getDate32() {
        return date32;
    }

    public void setDate32(LocalDate date32) {
        this.date32 = date32;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public LocalDateTime getDateTime64() {
        return dateTime64;
    }

    public void setDateTime64(LocalDateTime dateTime64) {
        this.dateTime64 = dateTime64;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public byte getEnum8() {
        return enum8;
    }

    public void setEnum8(byte enum8) {
        this.enum8 = enum8;
    }

    public int getEnum16() {
        return enum16;
    }

    public void setEnum16(int enum16) {
        this.enum16 = enum16;
    }

    public Inet4Address getIpv4() {
        return ipv4;
    }

    public void setIpv4(Inet4Address ipv4) {
        this.ipv4 = ipv4;
    }

    public Inet6Address getIpv6() {
        return ipv6;
    }

    public void setIpv6(Inet6Address ipv6) {
        this.ipv6 = ipv6;
    }

    public List<String> getArray() {
        return array;
    }

    public void setArray(List<String> array) {
        this.array = array;
    }

    public List<?> getTuple() {
        return tuple;
    }

    public void setTuple(List<?> tuple) {
        this.tuple = tuple;
    }

    public Object[] getTupleArray() {
        return tupleArray;
    }

    public void setTupleArray(Object[] tupleArray) {
        this.tupleArray = tupleArray;
    }

    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }

    public List<Integer> getNestedInnerInt() {
        return nestedInnerInt;
    }

    public void setNestedInnerInt(List<Integer> nestedInnerInt) {
        this.nestedInnerInt = nestedInnerInt;
    }

    public List<String> getNestedInnerString() {
        return nestedInnerString;
    }

    public void setNestedInnerString(List<String> nestedInnerString) {
        this.nestedInnerString = nestedInnerString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuerySamplePOJO that = (QuerySamplePOJO) o;
        return int8 == that.int8 && int8_default == that.int8_default && int16 == that.int16 && int16_default == that.int16_default && int32 == that.int32 && int32_default == that.int32_default && int64 == that.int64 && int64_default == that.int64_default && uint8 == that.uint8 && uint16 == that.uint16 && uint32 == that.uint32 && Float.compare(float32, that.float32) == 0 && Double.compare(float64, that.float64) == 0 && bool == that.bool && enum8 == that.enum8 && enum16 == that.enum16 && Objects.equals(int128, that.int128) && Objects.equals(int128_default, that.int128_default) && Objects.equals(int256, that.int256) && Objects.equals(int256_default, that.int256_default) && Objects.equals(uint64, that.uint64) && Objects.equals(uint128, that.uint128) && Objects.equals(uint256, that.uint256) && Objects.equals(decimal32, that.decimal32) && Objects.equals(decimal64, that.decimal64) && Objects.equals(decimal128, that.decimal128) && Objects.equals(decimal256, that.decimal256) && Objects.equals(string, that.string) && Objects.equals(fixedString, that.fixedString) && Objects.equals(date, that.date) && Objects.equals(date32, that.date32) && Objects.equals(dateTime, that.dateTime) && Objects.equals(dateTime64, that.dateTime64) && Objects.equals(uuid, that.uuid) && Objects.equals(ipv4, that.ipv4) && Objects.equals(ipv6, that.ipv6) && Objects.equals(array, that.array) && Objects.equals(map, that.map) && Objects.equals(nestedInnerInt, that.nestedInnerInt) && Objects.equals(nestedInnerString, that.nestedInnerString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(int8, int8_default, int16, int16_default, int32, int32_default, int64, int64_default, int128, int128_default, int256, int256_default, uint8, uint16, uint32, uint64, uint128, uint256, float32, float64, decimal32, decimal64, decimal128, decimal256, bool, string, fixedString, date, date32, dateTime, dateTime64, uuid, enum8, enum16, ipv4, ipv6, array, map, nestedInnerInt, nestedInnerString);
    }

    @Override
    public String toString() {
        return "QuerySamplePOJO{" +
                "int8=" + int8 +
                ", int8_default=" + int8_default +
                ", int16=" + int16 +
                ", int16_default=" + int16_default +
                ", int32=" + int32 +
                ", int32_default=" + int32_default +
                ", int64=" + int64 +
                ", int64_default=" + int64_default +
                ", int128=" + int128 +
                ", int128_default=" + int128_default +
                ", int256=" + int256 +
                ", int256_default=" + int256_default +
                ", uint8=" + uint8 +
                ", uint16=" + uint16 +
                ", uint32=" + uint32 +
                ", uint64=" + uint64 +
                ", uint128=" + uint128 +
                ", uint256=" + uint256 +
                ", float32=" + float32 +
                ", float64=" + float64 +
                ", decimal32=" + decimal32 +
                ", decimal64=" + decimal64 +
                ", decimal128=" + decimal128 +
                ", decimal256=" + decimal256 +
                ", bool=" + bool +
                ", string='" + string + '\'' +
                ", fixedString='" + fixedString + '\'' +
                ", date=" + date +
                ", date32=" + date32 +
                ", dateTime=" + dateTime +
                ", dateTime64=" + dateTime64 +
                ", uuid=" + uuid +
                ", enum8=" + enum8 +
                ", enum16=" + enum16 +
                ", ipv4=" + ipv4 +
                ", ipv6=" + ipv6 +
                ", array=" + array +
                ", map=" + map +
                ", nestedInnerInt=" + nestedInnerInt +
                ", nestedInnerString=" + nestedInnerString +
                '}';
    }

    public static String generateTableCreateSQL(String tableName) {
        return "CREATE TABLE " + tableName + " (" +
                "int8 Int8, " +
                "int8_default Int8 DEFAULT 0, " +
                "int16 Int16, " +
                "int16_default Int16 DEFAULT 0, " +
                "int32 Int32, " +
                "int32_default Int32 DEFAULT 0, " +
                "int64 Int64, " +
                "int64_default Int64 DEFAULT 0, " +
                "int128 Int128, " +
                "int128_default Int128 DEFAULT 0, " +
                "int256 Int256, " +
                "int256_default Int256 DEFAULT 0, " +
                "uint8 UInt8, " +
                "uint16 UInt16, " +
                "uint32 UInt32, " +
                "uint64 UInt64, " +
                "uint128 UInt128, " +
                "uint256 UInt256, " +
                "float32 Float32, " +
                "float64 Float64, " +
                "decimal32 Decimal32(2), " +
                "decimal64 Decimal64(3), " +
                "decimal128 Decimal128(4), " +
                "decimal256 Decimal256(5), " +
                "bool UInt8, " +
                "string String, " +
                "fixedString FixedString(3), " +
                "date Date, " +
                "date32 Date, " +
                "dateTime DateTime, " +
                "dateTime64 DateTime64(3), " +
                "uuid UUID, " +
                "enum8 Enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4, 'e' = 5, 'f' = 6, 'g' = 7, 'h' = 8, 'i' = 9, 'j' = 10, 'k' = 11, 'l' = 12, 'm' = 13, 'n' = 14, 'o' = 15, 'p' = 16, 'q' = 17, 'r' = 18, 's' = 19, 't' = 20, 'u' = 21, 'v' = 22, 'w' = 23, 'x' = 24, 'y' = 25, 'z' = 26), " +
                "enum16 Enum16('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4, 'e' = 5, 'f' = 6, 'g' = 7, 'h' = 8, 'i' = 9, 'j' = 10, 'k' = 11, 'l' = 12, 'm' = 13, 'n' = 14, 'o' = 15, 'p' = 16, 'q' = 17, 'r' = 18, 's' = 19, 't' = 20, 'u' = 21, 'v' = 22, 'w' = 23, 'x' = 24, 'y' = 25, 'z' = 26), " +
                "ipv4 IPv4, " +
                "ipv6 IPv6, " +
                "array Array(String), " +
                "tuple Tuple(Int32, Float64, String, String), " +
                "tupleArray Tuple(Int32, Float64, String, String), " +
                "map Map(String, Int32), " +
                "nested Nested (innerInt Int32, innerString String)" +
                ") ENGINE = MergeTree ORDER BY ()";
    }
}
