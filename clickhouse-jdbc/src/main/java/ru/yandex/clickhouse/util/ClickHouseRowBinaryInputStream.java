package ru.yandex.clickhouse.util;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static ru.yandex.clickhouse.util.ClickHouseRowBinaryStream.MILLIS_IN_DAY;

public class ClickHouseRowBinaryInputStream implements Closeable {
	private final DataInputStream in;
	private final TimeZone timeZone;

	public ClickHouseRowBinaryInputStream(InputStream is, TimeZone timeZone, ClickHouseProperties properties) {
		this.in = new DataInputStream(is);
		if (properties.isUseServerTimeZoneForDates()) {
			this.timeZone = timeZone;
		} else {
			this.timeZone = TimeZone.getDefault();
		}
	}

	public void readBytes(byte[] bytes) throws IOException {
		readBytes(bytes, 0, bytes.length);
	}

	public void readBytes(byte[] bytes, int offset, int length) throws IOException {
		while (length > 0) {
			int read = in.read(bytes, offset, length);
			if (read == -1)
				throw new EOFException();
			offset += read;
			length -= read;
		}
	}

	public int readByte() throws IOException {
		return in.readUnsignedByte();
	}

	public boolean readIsNull() throws IOException {
		int value = readByte();

		Utils.checkArgument(value, 0, 1);

		return value != 0;
	}

	public String readString() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		byte[] bytes = new byte[length];
		readBytes(bytes);

		return new String(bytes, StandardCharsets.UTF_8);
	}

	public String readFixedString(int length) throws IOException {
		byte[] bytes = new byte[length];
		readBytes(bytes);

		return new String(bytes, StandardCharsets.UTF_8);
	}

	public boolean readBoolean() throws IOException {
		int value = readUInt8();
		Utils.checkArgument(value, 0, 1);
		return value != 0;
	}

	public short readUInt8() throws IOException {
		return (short) in.readUnsignedByte();
	}

	/**
	 * Warning: the result is negative in Java if UInt8 &gt; 0x7f
     *
	 * @return next UInt8 value as a byte
	 * @throws IOException in case if an I/O error occurs
	 */
	public byte readUInt8AsByte() throws IOException {
		return in.readByte();
	}

	public byte readInt8() throws IOException {
		return in.readByte();
	}

	public int readUInt16() throws IOException {
		return Utils.readUnsignedShort(in);
	}

	/**
	 * Warning: the result is negative in Java if UInt16 &gt; 0x7fff
	 * @return next UInt16 value as a short
	 * @throws IOException in case if an I/O error occurs
	 */
	public short readUInt16AsShort() throws IOException {
		return (short) Utils.readUnsignedShort(in);
	}

	public short readInt16() throws IOException {
		return (short) Utils.readUnsignedShort(in);
	}

	public long readUInt32() throws IOException {
		return ((long) Utils.readInt(in)) & 0xffffffffL;
	}

	/**
	 * Warning: the result is negative in Java if UInt32 &gt; 0x7fffffff
	 * @return next UInt32 value as an int
	 * @throws IOException in case if an I/O error occurs
	 */
	public int readUInt32AsInt() throws IOException {
		return Utils.readInt(in);
	}

	public int readInt32() throws IOException {
		return Utils.readInt(in);
	}

	/**
	 * Warning: the result is negative in Java if UInt64 &gt; 0x7fffffffffffffff
	 * @return next UInt64 value as a long
	 * @throws IOException in case if an I/O error occurs
	 */
	public long readUInt64AsLong() throws IOException {
		return Utils.readLong(in);
	}

	public BigInteger readUInt64() throws IOException {
		return Utils.readLongAsBigInteger(in);
	}

	public long readInt64() throws IOException {
		return Utils.readLong(in);
	}

	public BigInteger readInt128() throws IOException {
        return Utils.readBigInteger(in, 16);
    }

    public BigInteger writeUInt128() throws IOException {
		return Utils.readBigInteger(in, 16);
    }

    public BigInteger writeInt256() throws IOException {
		return Utils.readBigInteger(in, 32);
    }

    public BigInteger writeUInt256() throws IOException {
        return Utils.readBigInteger(in, 32);
    }

	public Timestamp readDateTime() throws IOException {
		long value = readUInt32();
		return new Timestamp(TimeUnit.SECONDS.toMillis(value));
	}

	public Date readDate() throws IOException {
		int daysSinceEpoch = readUInt16();
		long utcMillis = daysSinceEpoch * MILLIS_IN_DAY;
		long localMillis = utcMillis - timeZone.getOffset(utcMillis);
		return new Date(localMillis);
	}

	public float readFloat32() throws IOException {
		return Float.intBitsToFloat(Utils.readInt(in));
	}

	public double readFloat64() throws IOException {
		return Double.longBitsToDouble(Utils.readLong(in));
	}

	public Date[] readDateArray() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		Date[] dates = new Date[length];
		for (int i = 0; i < length; i++) {
			dates[i] = readDate();
		}

		return dates;
	}

	public Timestamp[] readDateTimeArray() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		Timestamp[] dates = new Timestamp[length];
		for (int i = 0; i < length; i++) {
			dates[i] = readDateTime();
		}

		return dates;
	}

	public String[] readStringArray() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		String[] strings = new String[length];
		for (int i = 0; i < length; i++) {
			strings[i] = readString();
		}

		return strings;
	}

	public byte[] readInt8Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = readInt8();
		}

		return bytes;
	}

	public byte[] readUInt8ArrayAsByte() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = readUInt8AsByte();
		}

		return bytes;
	}

	public short[] readUInt8Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		short[] shorts = new short[length];
		for (int i = 0; i < length; i++) {
			shorts[i] = readUInt8();
		}

		return shorts;
	}

	public short[] readInt16Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		short[] shorts = new short[length];
		for (int i = 0; i < length; i++) {
			shorts[i] = readInt16();
		}

		return shorts;
	}

	public short[] readUInt16ArrayAsShort() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		short[] shorts = new short[length];
		for (int i = 0; i < length; i++) {
			shorts[i] = readUInt16AsShort();
		}

		return shorts;
	}

	public int[] readUInt16Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		int[] ints = new int[length];
		for (int i = 0; i < length; i++) {
			ints[i] = readUInt16();
		}

		return ints;
	}

	public int[] readInt32Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		int[] ints = new int[length];
		for (int i = 0; i < length; i++) {
			ints[i] = readInt32();
		}

		return ints;
	}

	public int[] readUInt32ArrayAsInt() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		int[] ints = new int[length];
		for (int i = 0; i < length; i++) {
			ints[i] = readUInt32AsInt();
		}

		return ints;
	}

	public long[] readUInt32Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		long[] longs = new long[length];
		for (int i = 0; i < length; i++) {
			longs[i] = readUInt32();
		}

		return longs;
	}

	public long[] readInt64Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		long[] longs = new long[length];
		for (int i = 0; i < length; i++) {
			longs[i] = readInt64();
		}

		return longs;
	}

	public long[] readUInt64ArrayAsLong() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		long[] longs = new long[length];
		for (int i = 0; i < length; i++) {
			longs[i] = readUInt64AsLong();
		}

		return longs;
	}

	public BigInteger[] readUInt64Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		BigInteger[] bigs = new BigInteger[length];
		for (int i = 0; i < length; i++) {
			bigs[i] = readUInt64();
		}

		return bigs;
	}

	public float[] readFloat32Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		float[] floats = new float[length];
		for (int i = 0; i < length; i++) {
			floats[i] = readFloat32();
		}

		return floats;
	}

	public double[] readFloat64Array() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		double[] doubles = new double[length];
		for (int i = 0; i < length; i++) {
			doubles[i] = readFloat64();
		}

		return doubles;
	}

	public UUID readUUID() throws IOException {
		byte[] array = new byte[16];
		readBytes(array);

		ByteBuffer bb = ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN);
		return new UUID(bb.getLong(), bb.getLong());
	}

	public UUID[] readUUIDArray() throws IOException {
		int length = Utils.readUnsignedLeb128(in);
		UUID[] uuids = new UUID[length];
		for (int i = 0; i < length; i++) {
			uuids[i] = readUUID();
		}

		return uuids;
	}

    public BigDecimal readDecimal32(int scale) throws IOException {
        int i = Utils.readInt(in);
        BigDecimal ten = BigDecimal.valueOf(10);
        BigDecimal s = ten.pow(scale);
        return new BigDecimal(i).divide(s);
    }

    public BigDecimal readDecimal64(int scale) throws IOException {
        long i = Utils.readLong(in);
        BigDecimal ten = BigDecimal.valueOf(10);
        BigDecimal s = ten.pow(scale);
        return new BigDecimal(i).divide(s);
    }

    public BigDecimal readDecimal128(int scale) throws IOException {
        byte[] r = new byte[16];
        for (int i = r.length; i > 0; i--) {
            r[i - 1] = (byte) in.readUnsignedByte();
        }
        BigDecimal res = new BigDecimal(new BigInteger(r), scale);
        return res;
    }

	public BigDecimal readDecimal256(int scale) throws IOException {
        byte[] r = new byte[32];
        for (int i = r.length; i > 0; i--) {
            r[i - 1] = (byte) in.readUnsignedByte();
        }
        BigDecimal res = new BigDecimal(new BigInteger(r), scale);
        return res;
    }

	public ClickHouseBitmap readBitmap(ClickHouseDataType innerType) throws IOException {
		return ClickHouseBitmap.deserialize(in, innerType);
	}

	@Override
	public void close() throws IOException {
		in.close();
	}
}
