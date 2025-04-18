package com.clickhouse.client.api.data_formats;

import com.clickhouse.data.ClickHouseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Experimental API
 */
public interface ClickHouseBinaryFormatWriter {

    /**
     * Returns an output stream to which this writer is serializing values.
     * Caution: this method is not intended for application usage.
     * @return Output stream of the writer
     */
    OutputStream getOutputStream();

    int getRowCount();

    ClickHouseFormat getFormat();

    void clearRow();

    void setValue(String column, Object value);

    void setValue(int colIndex, Object value);

    void commitRow() throws IOException;

    void setByte(String column, byte value);

    void setByte(int colIndex, byte value);

    void setShort(String column, short value);

    void setShort(int colIndex, short value);

    void setInteger(String column, int value);

    void setInteger(int colIndex, int value);

    void setLong(String column, long value);

    void setLong(int colIndex, long value);

    void setBigInteger(int colIndex, BigInteger value);

    void setBigInteger(String column, BigInteger value);

    void setFloat(int colIndex, float value);

    void setFloat(String column, float value);

    void setDouble(int colIndex, double value);

    void setDouble(String column, double value);

    void setBigDecimal(int colIndex, BigDecimal value);

    void setBigDecimal(String column, BigDecimal value);

    void setBoolean(int colIndex, boolean value);

    void setBoolean(String column, boolean value);

    void setString(String column, String value);

    void setString(int colIndex, String value);

    void setDate(String column, LocalDate value);

    void setDate(int colIndex, LocalDate value);

    void setDateTime(String column, LocalDateTime value);

    void setDateTime(int colIndex, LocalDateTime value);

    void setDateTime(String column, ZonedDateTime value);

    void setDateTime(int colIndex, ZonedDateTime value);

    void setList(String column, List<?> value);

    void setList(int colIndex, List<?> value);

}
