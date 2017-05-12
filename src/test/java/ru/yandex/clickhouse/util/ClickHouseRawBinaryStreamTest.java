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
public class ClickHouseRawBinaryStreamTest {

    @Test
    public void testUInt8() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRawBinaryStream stream) throws Exception {
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
                public void write(ClickHouseRawBinaryStream stream) throws Exception {
                    stream.writeUInt16(0);
                    stream.writeUInt16(65535);
                    stream.writeUInt16(32768);
                }
            },
            new byte[]{0, 0, -1, -1, 0, -128}
        );
    }

    @Test
    public void testUInt64() throws Exception {
        check(
            new StreamWriter() {
                @Override
                public void write(ClickHouseRawBinaryStream stream) throws Exception {
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
                public void write(ClickHouseRawBinaryStream stream) throws Exception {
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

    private void check(StreamWriter streamWriter, byte[] expected) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ClickHouseRawBinaryStream stream = new ClickHouseRawBinaryStream(byteArrayOutputStream, TimeZone.getTimeZone("ETC"));
        streamWriter.write(stream);
        Assert.assertEquals(byteArrayOutputStream.toByteArray(), expected);
    }

    private interface StreamWriter {
        void write(ClickHouseRawBinaryStream stream) throws Exception;
    }


}