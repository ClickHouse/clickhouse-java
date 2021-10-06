package ru.yandex.clickhouse.util;

import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;

public class ClickHouseRowBinaryInputStreamTest {

	@Test(groups = "unit")
	public void testUInt8() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{1, 0, 1, 0, -1, -128});

		assertEquals(input.readBoolean(), true);
		assertEquals(input.readBoolean(), false);
		assertEquals(input.readUInt8(), 1);
		assertEquals(input.readUInt8(), 0);
		assertEquals(input.readUInt8(), 255);
		assertEquals(input.readUInt8(), 128);
	}

	@Test(groups = "unit")
	public void testUInt8AsByte() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{1, 0, 1, 0, -1, -128});

		assertEquals(input.readUInt8AsByte(), (byte) 1);
		assertEquals(input.readUInt8AsByte(), (byte) 0);
		assertEquals(input.readUInt8AsByte(), (byte) 1);
		assertEquals(input.readUInt8AsByte(), (byte) 0);
		assertEquals(input.readUInt8AsByte(), (byte) 255);
		assertEquals(input.readUInt8AsByte(), (byte) 128);
	}

	@Test(groups = "unit")
	public void testUInt16() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{0, 0, -1, -1, 0, -128});

		assertEquals(input.readUInt16(), 0);
		assertEquals(input.readUInt16(), 65535);
		assertEquals(input.readUInt16(), 32768);
	}

	@Test(groups = "unit")
	public void testUInt16AsShort() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{0, 0, -1, -1, 0, -128});

		assertEquals(input.readUInt16AsShort(), (short) 0);
		assertEquals(input.readUInt16AsShort(), (short) 65535);
		assertEquals(input.readUInt16AsShort(), (short) 32768);
	}


	@Test(groups = "unit")
	public void testFloat64() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{0, 0, 0, 0, 0, 0, -8, 127});

		assertEquals(input.readFloat64(), Double.NaN);
	}

	@Test(groups = "unit")
	public void testUInt64() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1});

		assertEquals(input.readUInt64(), BigInteger.ZERO);
		assertEquals(input.readUInt64(), new BigInteger("18446744073709551615"));
	}

	@Test(groups = "unit")
	public void testUInt64AsLong() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1});

		assertEquals(input.readUInt64AsLong(), 0);
		assertEquals(input.readUInt64AsLong(), new BigInteger("18446744073709551615").longValue());
	}

	@Test(groups = "unit")
	public void testDecimal128() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{-10, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
		assertEquals(input.readDecimal128(3), new BigDecimal("10.230"));
		ClickHouseRowBinaryInputStream input2 = prepareStream(new byte[]{-2, 127, -58, -92, 126, -115, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0});
		assertEquals(input2.readDecimal128(2), new BigDecimal("9999999999999.98"));
	}

	@Test(groups = "unit")
	public void testDecimal64() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{-10, 39, 0, 0, 0, 0, 0, 0});
		assertEquals(input.readDecimal64(3), new BigDecimal("10.23"));
	}

	@Test(groups = "unit")
	public void testDecimal32() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{-10, 39, 0, 0});
		assertEquals(input.readDecimal32(3), new BigDecimal("10.23"));
	}

	@Test(groups = "unit")
	public void testFixedString() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{48, 49, 48, 49, 55, 49, 50, 50, 48, 48});

		assertEquals(input.readFixedString(10), "0101712200");

		ClickHouseRowBinaryInputStream inputZeroPaddedString = prepareStream(new byte[]{104, 101, 108, 108, 111, 0, 0, 0, 0, 0});

		assertEquals(inputZeroPaddedString.readFixedString(10), "hello\0\0\0\0\0");
	}

	@Test(groups = "unit")
	public void testOne() throws Exception {
		ClickHouseRowBinaryInputStream input = prepareStream(new byte[]{5, 97, 46, 98, 46, 99, 123, 20, -82, 71, -31, 26, 69, 64, 34, 87, -13, 88, 120, 67, 48, 116, -13, 88});

		assertEquals(input.readString(), "a.b.c");
		assertEquals(input.readFloat64(), 42.21);
		assertEquals(input.readUInt32(), 1492342562L);
		assertEquals(input.readDate(), new Date(117, 3, 16));
		assertEquals(input.readUInt32(), 1492350000L);
	}

	private ClickHouseRowBinaryInputStream prepareStream(byte[] input) throws Exception {
		return new ClickHouseRowBinaryInputStream(new ByteArrayInputStream(input), TimeZone.getTimeZone("ETC"), new ClickHouseProperties());
	}
}
