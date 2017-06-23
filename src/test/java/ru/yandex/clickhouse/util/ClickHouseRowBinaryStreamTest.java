package ru.yandex.clickhouse.util;

import com.google.common.primitives.UnsignedLong;
import org.joda.time.LocalDate;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.TimeZone;

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
                    stream.writeUInt64(UnsignedLong.valueOf("18446744073709551615"));
                }
            },
            new byte[]{0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1,}
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
                    stream.writeDate(new LocalDate(2017, 4, 16));
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

    private void check(StreamWriter streamWriter, byte[] expected) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(byteArrayOutputStream, TimeZone.getTimeZone("ETC"));
        streamWriter.write(stream);
        Assert.assertEquals(byteArrayOutputStream.toByteArray(), expected);
    }

    private interface StreamWriter {
        void write(ClickHouseRowBinaryStream stream) throws Exception;
    }


}