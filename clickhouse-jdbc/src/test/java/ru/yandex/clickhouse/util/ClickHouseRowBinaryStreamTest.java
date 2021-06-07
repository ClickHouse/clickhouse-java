package ru.yandex.clickhouse.util;

import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseRowBinaryStreamTest {

    @Test
    public void testUInt8() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeUInt8(true);
                    stream.writeUInt8(false);
                    stream.writeUInt8(1);
                    stream.writeUInt8(0);
                    stream.writeUInt8(255);
                    stream.writeUInt8(128);
                }
            },
            new byte[]{1, 0, 1, 0, -1, -128}
        );
    }

    @Test
    public void testUInt16() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeUInt16(0);
                    stream.writeUInt16(65535);
                    stream.writeUInt16(32768);
                }
            },
            new byte[]{0, 0, -1, -1, 0, -128}
        );
    }

    @Test
    public void testFloat64() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeFloat64(Double.NaN);
                }
            },
            new byte[]{0, 0, 0, 0, 0, 0, -8, 127}
        );
    }


    @Test
    public void testUInt64() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeUInt64(0);
                    stream.writeUInt64(new BigInteger("18446744073709551615"));
                }
            },
            new byte[]{0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1,}
        );
    }

    @Test
    public void testDecimal32() throws Exception {
        check(
                new StreamWriter() {
                    @Override
                    public void write(ClickHouseRowBinaryStream stream) throws Exception {
                        stream.writeDecimal32(new BigDecimal(10.23), 3);
                        stream.writeDecimal32(new BigDecimal(-99999.9998), 4);
                    }
                },
                new byte[]{-10, 39, 0, 0, 2, 54, 101, -60}
        );
    }

    @Test
    public void testDecimal64() throws Exception {
        check(
                new StreamWriter() {
                    @Override
                    public void write(ClickHouseRowBinaryStream stream) throws Exception {
                        stream.writeDecimal64(new BigDecimal(10.23), 3);
                        stream.writeDecimal64(new BigDecimal(-9999999999.99999998), 8);
                    }
                },
                new byte[]{-10, 39, 0, 0, 0, 0, 0, 0, 0, 0, -100, 88, 76, 73, 31, -14}
        );
    }

    @Test
    public void testDecimal128() throws Exception {
        check(
                new StreamWriter() {
                    @Override
                    public void write(ClickHouseRowBinaryStream stream) throws Exception {
                        stream.writeDecimal128(new BigDecimal(10.23), 3);
                        stream.writeDecimal128(new BigDecimal(-10.23), 3);
                    }
                },
                new byte[] {
                    -10, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    10, -40, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
                }
        );
    }

    @Test
    public void testDecimal256() throws Exception {
        check(
                new StreamWriter() {
                    @Override
                    public void write(ClickHouseRowBinaryStream stream) throws Exception {
                        stream.writeDecimal256(new BigDecimal(10.23), 3);
                        stream.writeDecimal256(new BigDecimal(-10.23), 3);
                    }
                },
                new byte[] {
                    -10, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    10, -40, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
                }
        );
    }

    @Test
    public void testOne() throws Exception {

        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeString("a.b.c");
                    stream.writeFloat64(42.21);
                    stream.writeUInt32(1492342562);
                    stream.writeDate(new Date(117, 3, 16));
                    stream.writeUInt32(1492350000);
                }
            },
            new byte[]{5, 97, 46, 98, 46, 99, 123, 20, -82, 71, -31, 26, 69, 64, 34, 87, -13, 88, 120, 67, 48, 116, -13, 88}
        );
        //Result of
        //clickhouse-client -q "select 'a.b.c', toFloat64(42.21), toUInt32(1492342562), toDate('2017-04-16'), toUInt32(1492350000) format RowBinary" | od -vAn -td1

    }

    @Test
    public void testUnsignedLeb128() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeUnsignedLeb128(0);
                    stream.writeUnsignedLeb128(624485);
                    stream.writeUnsignedLeb128(100000000);
                }
            },
            new byte[]{
                0,
                -27, -114, 38,
                -128, -62, -41, 47
            }
        );
    }


    @Test
    public void testNative() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeUnsignedLeb128(2);
                    stream.writeUnsignedLeb128(1);
                    stream.writeString("1");
                    stream.writeString("UInt8");
                    stream.writeUInt8(1);
                    stream.writeString("2");
                    stream.writeString("UInt8");
                    stream.writeUInt8(2);
                }
            },
            //clickhouse-client -q "select 1, 2 FORMAT Native"  | od -vAn -td1
            new byte[]{
                2, //Columns
                1, //Rows
                1, 49, //'1'
                5, 85, 73, 110, 116, 56, //'UInt8'
                1, //1
                1, 50, //'2'
                5, 85, 73, 110, 116, 56, //'Uint8'
                2 //2
            }
        );
    }


    @Test
    public void testStringArray() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeUnsignedLeb128(0);
                    stream.writeUnsignedLeb128(1);
                    stream.writeString("ax");
                }
            },
            new byte[]{0, 1, 2, 97, 120}
        );
    }

    @Test
    public void testUInt64Array() throws Exception {
        check(
            new StreamWriter() {
              @Override
              public void write(ClickHouseRowBinaryStream stream) throws Exception {
                stream.writeUInt64Array(new long[]{0, Long.MAX_VALUE});
                stream.writeUInt64Array(new BigInteger[]{BigInteger.ZERO, new BigInteger("18446744073709551615")});
              }
            },
            new byte[] { 2, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, 127, // First array
                2, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, } // Second array
        );
    }

    @Test
    public void testString() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.writeString(
                        "aaaa~����%20�&zzzzz"
                    );
                }
            },
            new byte[]{
                29, 97, 97, 97, 97, 126, -17, -65, -67, -17, -65, -67, -17, -65, -67, -17,
                -65, -67, 37, 50, 48, -17, -65, -67, 38, 122, 122, 122, 122, 122
            }
            //clickhouse-client -q "SELECT 'aaaa~����%20�&zzzzz' Format RowBinary"  | od -vAn -td1
        );
    }

    @Test
    public void testUUID() throws Exception {
        check(
                new StreamWriter() {
                    @Override
                    public void write(ClickHouseRowBinaryStream stream) throws Exception {
                        stream.writeUUID(UUID.fromString("123e4567-e89b-12d3-a456-426655440000"));
                    }
                },
                new byte[]{
                        -45, 18, -101, -24, 103, 69, 62, 18, 0, 0, 68, 85, 102, 66,  86, -92
                }
        );
    }

    @Test
    public void testUUIDArray() throws Exception {
        check(
                new StreamWriter() {
                    @Override
                    public void write(ClickHouseRowBinaryStream stream) throws Exception {
                        stream.writeUUIDArray(new UUID[] {
                            UUID.fromString("123e4567-e89b-12d3-a456-426655440000"),
                            UUID.fromString("123e4567-e89b-12d3-a456-426655440001")
                        });
                    }
                },
                new byte[] {
                    2, -45, 18, -101, -24, 103, 69, 62, 18, 0, 0, 68, 85, 102, 66,  86, -92,
                    -45, 18, -101, -24, 103, 69, 62, 18, 1, 0, 68, 85, 102, 66,  86, -92
                }
        );
    }

    @Test
    public void testWriteNullableInt32() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.markNextNullable(false);
                    stream.writeInt32(1);
                }
            },
            new byte[]{
                    0, 1, 0, 0, 0
            }
        );
        // clickhouse-client -q "SELECT CAST(1 AS Nullable(Int32)) Format RowBinary"  | od -vAn -td1
    }

    @Test
    public void testWriteNull() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRowBinaryStream stream) throws Exception {
                    stream.markNextNullable(true);
                }
            },
            new byte[]{
                    1
            }
        );
        // clickhouse-client -q "SELECT CAST(Null AS Nullable(Int32)) Format RowBinary"  | od -vAn -td1
    }

    private void check(StreamWriter streamWriter, byte[] expected) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(byteArrayOutputStream, TimeZone.getTimeZone("ETC"), new ClickHouseProperties());
        streamWriter.write(stream);
        Assert.assertEquals(byteArrayOutputStream.toByteArray(), expected);
    }

    private interface StreamWriter {
        void write(ClickHouseRowBinaryStream stream) throws Exception;
    }


}
