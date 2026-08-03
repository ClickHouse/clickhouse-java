package com.clickhouse.client.query;

/**
 * POJO binding columns that the reader does not decode into the field's primitive type directly, so the compiled
 * setter has to convert (narrow) the value: a wide integer/float into a narrow primitive and a boxed number
 * ({@code Int128}, {@code Decimal64}) into a primitive.
 */
public class PrimitiveFieldsPOJO {

    private short int64AsShort;

    private byte float64AsByte;

    private long int128AsLong;

    private double decimal64AsDouble;

    private boolean int64AsBoolean;

    public short getInt64AsShort() {
        return int64AsShort;
    }

    public void setInt64AsShort(short int64AsShort) {
        this.int64AsShort = int64AsShort;
    }

    public byte getFloat64AsByte() {
        return float64AsByte;
    }

    public void setFloat64AsByte(byte float64AsByte) {
        this.float64AsByte = float64AsByte;
    }

    public long getInt128AsLong() {
        return int128AsLong;
    }

    public void setInt128AsLong(long int128AsLong) {
        this.int128AsLong = int128AsLong;
    }

    public double getDecimal64AsDouble() {
        return decimal64AsDouble;
    }

    public void setDecimal64AsDouble(double decimal64AsDouble) {
        this.decimal64AsDouble = decimal64AsDouble;
    }

    public boolean getInt64AsBoolean() {
        return int64AsBoolean;
    }

    public void setInt64AsBoolean(boolean int64AsBoolean) {
        this.int64AsBoolean = int64AsBoolean;
    }
}
