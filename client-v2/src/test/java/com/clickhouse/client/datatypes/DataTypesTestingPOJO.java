package com.clickhouse.client.datatypes;

import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;


@Data
@ToString
public class DataTypesTestingPOJO {
    private byte byteValue;
    private Byte boxedByte;
    private byte int8;
    private short int16;
    private Short boxedShort;
    private int int32;
    private Integer boxedInt;
    private long int64;
    private Long boxedLong;
    private long int64_default;
    private BigInteger int128;
    private BigInteger int256;

    private short uInt8;
    private int uInt16;
    private long uInt32;
    private BigInteger uInt64;
    private BigInteger uInt128;
    private BigInteger uInt256;

    private float float32;
    private Float boxedFloat;
    private double float64;
    private Double boxedDouble;

    private BigDecimal decimal32;
    private BigDecimal decimal64;
    private BigDecimal decimal128;
    private BigDecimal decimal256;

    private boolean bool;
    private Boolean boxedBool;

    private String string;
    private String fixedString;

    private LocalDate date;
    private LocalDate date32;

    private LocalDateTime dateTime;
    private LocalDateTime dateTime32;
    private LocalDateTime dateTime64;

    private UUID UUID;

    private byte enum8;
    private int enum16;

    private Inet4Address IPv4;
    private Inet6Address IPv6;

    private ClickHouseGeoPointValue point;
    private ClickHouseGeoPolygonValue polygon;
    private ClickHouseGeoRingValue ring;
    private ClickHouseGeoMultiPolygonValue multiPolygon;

    private List<String> array;
    private List<?> tuple;
    private Map<String, Integer> map;
    private List<Integer> nestedInnerInt;
    private List<String> nestedInnerString;
    private List<Integer> nestedInnerNullableInt;

    private ClickHouseBitmap groupBitmapUint32;
    private ClickHouseBitmap groupBitmapUint64;

    private Period intervalYear;

    private Period intervalQuarter;

    private Period intervalMonth;

    private Period intervalWeek;

    private Period intervalDay;

    private Duration intervalHour;

    private Duration intervalMinute;

    private Duration intervalSecond;

    private Duration intervalMillisecond;

    private Duration intervalMicrosecond;

    private Duration intervalNanosecond;

    private SmallEnum smallEnum;

    private LargeEnum largeEnum;

    public DataTypesTestingPOJO() {
        final Random random = new Random();
        byteValue = (byte) random.nextInt();
        boxedByte = (byte) random.nextInt();
        int8 = (byte) random.nextInt(128);
        int16 = (short)random.nextInt(32768);
        boxedShort = (short) random.nextInt();
        int32 = random.nextInt();
        boxedInt = random.nextInt();
        int64 = random.nextLong();
        boxedLong = random.nextLong();
        BigInteger upper = BigInteger.valueOf(random.nextLong()).shiftLeft(64);
        BigInteger lower = BigInteger.valueOf(random.nextLong()).and(BigInteger.valueOf(Long.MAX_VALUE));

        int128 = upper.or(lower);

        BigInteger upper1 = BigInteger.valueOf(random.nextLong()).shiftLeft(192);
        BigInteger upper2 = BigInteger.valueOf(random.nextLong()).shiftLeft(128);
        BigInteger lower1 = BigInteger.valueOf(random.nextLong()).shiftLeft(64);
        BigInteger lower2 = BigInteger.valueOf(random.nextLong()).and(BigInteger.valueOf(Long.MAX_VALUE));

        int256 = upper1.or(upper2).or(lower1).or(lower2);

        uInt8 = (short) random.nextInt(255);
        uInt16 = random.nextInt(32768);
        uInt32 = (long) (random.nextDouble() * 4294967295L);
        uInt64 = BigInteger.valueOf((long) (random.nextDouble() * 18446744073709615L));


        uInt128 = upper.or(lower).abs();
        uInt256 = upper1.or(upper2).or(lower1).or(lower2).abs();

        float32 = random.nextFloat();
        float64 = random.nextDouble();
        boxedFloat = random.nextFloat();
        boxedDouble = random.nextDouble();

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
        dateTime32 = LocalDateTime.now();
        dateTime64 = LocalDateTime.now();

        UUID = UUID.randomUUID();

        enum8 = (byte) random.nextInt(27);
        enum16 = random.nextInt(27);

        try {
            byte[] addr4 = new byte[4];
            random.nextBytes(addr4);
            IPv4 = (Inet4Address) Inet4Address.getByAddress(addr4);

            byte[] addr6 = new byte[16];
            random.nextBytes(addr6);
            IPv6 = (Inet6Address) Inet6Address.getByAddress(addr6);
        } catch (UnknownHostException e) {
            IPv4 = null;
            IPv6 = null;
        }

        point = ClickHouseGeoPointValue.of(new double[]{random.nextFloat(), random.nextFloat()});
        polygon = ClickHouseGeoPolygonValue.of(new double[][][]{
                {
                    {random.nextFloat(), random.nextFloat()},
                    {random.nextFloat(), random.nextFloat()}
                },
        });
        ring = ClickHouseGeoRingValue.of(new double[][]{
                {random.nextFloat(), random.nextFloat()},
                {random.nextFloat(), random.nextFloat()},
                {random.nextFloat(), random.nextFloat()}});
        multiPolygon = ClickHouseGeoMultiPolygonValue.of(new double[][][][]{
                        {
                                {
                                        {random.nextFloat(), random.nextFloat()},
                                        {random.nextFloat(), random.nextFloat()}
                                },
                        },
                        {
                                {
                                        {random.nextFloat(), random.nextFloat()},
                                        {random.nextFloat(), random.nextFloat()}
                                },
                        }

                }
        );

        array = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
        tuple = Arrays.asList(uInt64, int32, string);
        map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            map.put(String.valueOf((char) ('a' + i)), i + 1);
        }

        List<Integer> innerInt = new ArrayList<>();
        innerInt.add(random.nextInt(Integer.MAX_VALUE));
        innerInt.add(random.nextInt(Integer.MAX_VALUE));
        nestedInnerInt = innerInt;

        List<String> innerString = new ArrayList<>();
        innerString.add(RandomStringUtils.randomAlphabetic(1, 256));
        innerString.add(RandomStringUtils.randomAlphabetic(1, 256));
        nestedInnerString = innerString;

        List<Integer> innerNullableInt = new ArrayList<>();
        innerNullableInt.add(null);
        innerNullableInt.add(random.nextInt(Integer.MAX_VALUE));
        nestedInnerNullableInt = innerNullableInt;

        groupBitmapUint32 = ClickHouseBitmap.wrap(random.ints(5, Integer.MAX_VALUE - 100, Integer.MAX_VALUE).toArray());
        groupBitmapUint64 = ClickHouseBitmap.wrap(random.longs(5, Long.MAX_VALUE - 100, Long.MAX_VALUE).toArray());

        intervalYear = Period.ofYears(random.nextInt(4000));
        intervalQuarter = Period.ofMonths(random.nextInt(10)  *3);
        intervalMonth = Period.ofMonths(random.nextInt(12));
        intervalWeek = Period.ofWeeks(random.nextInt(52));
        intervalDay = Period.ofDays(random.nextInt(30));
        intervalHour = Duration.ofHours(random.nextInt(24));
        intervalMinute = Duration.ofMinutes(random.nextInt(60));
        intervalSecond = Duration.ofSeconds(random.nextInt(60));
        intervalMillisecond =  Duration.ofMillis(random.nextInt());
        intervalMicrosecond =  Duration.ofNanos(random.nextInt() * 1000L);

        intervalNanosecond = Duration.ofNanos((random.nextInt()));

        smallEnum = SmallEnum.valueOf("CONSTANT_" + Math.max(1, random.nextInt(SmallEnum.values().length - 1)));
        largeEnum = LargeEnum.valueOf("CONSTANT_" + Math.max(1, random.nextInt(LargeEnum.values().length - 1)));
    }

    public boolean getBool() {
        return bool;
    }

    public static String generateTableCreateSQL(String tableName) {
        return "CREATE TABLE " + tableName + " (" +
                "byteValue Int8," +
                "int8 Int8, " +
                "boxedByte Int8, " +
                "int8_default Int8 DEFAULT 0, " +
                "int16 Int16, " +
                "boxedShort Int16, " +
                "int16_default Int16 DEFAULT 0, " +
                "int32 Int32, " +
                "boxedInt Int32, " +
                "int32_default Int32 DEFAULT 0, " +
                "int64 Int64, " +
                "boxedLong Int64, " +
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
                "boxedFloat Float32, " +
                "float64 Float64, " +
                "boxedDouble Float64, " +
                "decimal32 Decimal32(2), " +
                "decimal64 Decimal64(3), " +
                "decimal128 Decimal128(4), " +
                "decimal256 Decimal256(5), " +
                "bool UInt8, " +
//                "boxedBool UInt8, " +
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
                "tuple Tuple(UInt64, Int32, String), " +
                "map Map(String, Int32), " +
                "nested Nested (innerInt Int32, innerString String, " +
                "innerNullableInt Nullable(Int32)), " +
                "groupBitmapUint32 AggregateFunction(groupBitmap, UInt32), " +
                "groupBitmapUint64 AggregateFunction(groupBitmap, UInt64), " +
                "intervalYear IntervalDay, " +
                "intervalQuarter IntervalDay, " +
                "intervalMonth IntervalDay, " +
                "intervalWeek IntervalDay, " +
                "intervalDay IntervalDay, " +
                "intervalHour IntervalHour, " +
                "intervalMinute IntervalNanosecond, " +
                "intervalSecond IntervalNanosecond, " +
                "intervalMillisecond IntervalNanosecond, " +
                "intervalMicrosecond IntervalNanosecond, " +
                "intervalNanosecond IntervalNanosecond " +
                ") ENGINE = MergeTree ORDER BY ()";
    }

    public enum SmallEnum {
        CONSTANT_1, CONSTANT_2, CONSTANT_3, CONSTANT_4, CONSTANT_5, CONSTANT_6, CONSTANT_7, CONSTANT_8, CONSTANT_9, CONSTANT_10;
    }

    public enum LargeEnum {
        CONSTANT_1, CONSTANT_2, CONSTANT_3, CONSTANT_4, CONSTANT_5, CONSTANT_6, CONSTANT_7, CONSTANT_8, CONSTANT_9, CONSTANT_10,
        CONSTANT_11, CONSTANT_12, CONSTANT_13, CONSTANT_14, CONSTANT_15, CONSTANT_16, CONSTANT_17, CONSTANT_18, CONSTANT_19, CONSTANT_20,
        CONSTANT_21, CONSTANT_22, CONSTANT_23, CONSTANT_24, CONSTANT_25, CONSTANT_26, CONSTANT_27, CONSTANT_28, CONSTANT_29, CONSTANT_30,
        CONSTANT_31, CONSTANT_32, CONSTANT_33, CONSTANT_34, CONSTANT_35, CONSTANT_36, CONSTANT_37, CONSTANT_38, CONSTANT_39, CONSTANT_40,
        CONSTANT_41, CONSTANT_42, CONSTANT_43, CONSTANT_44, CONSTANT_45, CONSTANT_46, CONSTANT_47, CONSTANT_48, CONSTANT_49, CONSTANT_50,
        CONSTANT_51, CONSTANT_52, CONSTANT_53, CONSTANT_54, CONSTANT_55, CONSTANT_56, CONSTANT_57, CONSTANT_58, CONSTANT_59, CONSTANT_60,
        CONSTANT_61, CONSTANT_62, CONSTANT_63, CONSTANT_64, CONSTANT_65, CONSTANT_66, CONSTANT_67, CONSTANT_68, CONSTANT_69, CONSTANT_70,
        CONSTANT_71, CONSTANT_72, CONSTANT_73, CONSTANT_74, CONSTANT_75, CONSTANT_76, CONSTANT_77, CONSTANT_78, CONSTANT_79, CONSTANT_80,
        CONSTANT_81, CONSTANT_82, CONSTANT_83, CONSTANT_84, CONSTANT_85, CONSTANT_86, CONSTANT_87, CONSTANT_88, CONSTANT_89, CONSTANT_90,
        CONSTANT_91, CONSTANT_92, CONSTANT_93, CONSTANT_94, CONSTANT_95, CONSTANT_96, CONSTANT_97, CONSTANT_98, CONSTANT_99, CONSTANT_100,
        CONSTANT_101, CONSTANT_102, CONSTANT_103, CONSTANT_104, CONSTANT_105, CONSTANT_106, CONSTANT_107, CONSTANT_108, CONSTANT_109, CONSTANT_110,
        CONSTANT_111, CONSTANT_112, CONSTANT_113, CONSTANT_114, CONSTANT_115, CONSTANT_116, CONSTANT_117, CONSTANT_118, CONSTANT_119, CONSTANT_120,
        CONSTANT_121, CONSTANT_122, CONSTANT_123, CONSTANT_124, CONSTANT_125, CONSTANT_126, CONSTANT_127, CONSTANT_128, CONSTANT_129, CONSTANT_130,
        CONSTANT_131, CONSTANT_132, CONSTANT_133, CONSTANT_134, CONSTANT_135, CONSTANT_136, CONSTANT_137, CONSTANT_138, CONSTANT_139, CONSTANT_140,
        CONSTANT_141, CONSTANT_142, CONSTANT_143, CONSTANT_144, CONSTANT_145, CONSTANT_146, CONSTANT_147, CONSTANT_148, CONSTANT_149, CONSTANT_150,
        CONSTANT_151, CONSTANT_152, CONSTANT_153, CONSTANT_154, CONSTANT_155, CONSTANT_156, CONSTANT_157, CONSTANT_158, CONSTANT_159, CONSTANT_160,
        CONSTANT_161, CONSTANT_162, CONSTANT_163, CONSTANT_164, CONSTANT_165, CONSTANT_166, CONSTANT_167, CONSTANT_168, CONSTANT_169, CONSTANT_170,
        CONSTANT_171, CONSTANT_172, CONSTANT_173, CONSTANT_174, CONSTANT_175, CONSTANT_176, CONSTANT_177, CONSTANT_178, CONSTANT_179, CONSTANT_180,
        CONSTANT_181, CONSTANT_182, CONSTANT_183, CONSTANT_184, CONSTANT_185, CONSTANT_186, CONSTANT_187, CONSTANT_188, CONSTANT_189, CONSTANT_190,
        CONSTANT_191, CONSTANT_192, CONSTANT_193, CONSTANT_194, CONSTANT_195, CONSTANT_196, CONSTANT_197, CONSTANT_198, CONSTANT_199, CONSTANT_200,
        CONSTANT_201, CONSTANT_202, CONSTANT_203, CONSTANT_204, CONSTANT_205, CONSTANT_206, CONSTANT_207, CONSTANT_208, CONSTANT_209, CONSTANT_210,
        CONSTANT_211, CONSTANT_212, CONSTANT_213, CONSTANT_214, CONSTANT_215, CONSTANT_216, CONSTANT_217, CONSTANT_218, CONSTANT_219, CONSTANT_220,
        CONSTANT_221, CONSTANT_222, CONSTANT_223, CONSTANT_224, CONSTANT_225, CONSTANT_226, CONSTANT_227, CONSTANT_228, CONSTANT_229, CONSTANT_230,
        CONSTANT_231, CONSTANT_232, CONSTANT_233, CONSTANT_234, CONSTANT_235, CONSTANT_236, CONSTANT_237, CONSTANT_238, CONSTANT_239, CONSTANT_240,
        CONSTANT_241, CONSTANT_242, CONSTANT_243, CONSTANT_244, CONSTANT_245, CONSTANT_246, CONSTANT_247, CONSTANT_248, CONSTANT_249, CONSTANT_250,
        CONSTANT_251, CONSTANT_252, CONSTANT_253, CONSTANT_254, CONSTANT_255, CONSTANT_256, CONSTANT_257, CONSTANT_258, CONSTANT_259, CONSTANT_260
    }
}
