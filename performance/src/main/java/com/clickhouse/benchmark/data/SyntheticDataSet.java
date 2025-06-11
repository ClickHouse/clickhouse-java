package com.clickhouse.benchmark.data;

import com.clickhouse.data.format.BinaryStreamUtils;

import java.io.ByteArrayOutputStream;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class SyntheticDataSet {

    private final int capacity;

    public SyntheticDataSet(int capacity) {
        this.capacity = capacity;
        generateData();
    }

    private void generateData() {
        generateDateTimeValues();
    }

    private void generateDateTimeValues() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        dateTimeValues = new LocalDateTime[capacity];
        TimeZone tz = TimeZone.getTimeZone("UTC");

        try {
            for (int i = 0; i < capacity; i++) {
                dateTimeValues[i] = LocalDateTime.now().plusSeconds(i);
                BinaryStreamUtils.writeDateTime64(out, dateTimeValues[i], 3, tz);

            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate date time values", e);
        }
        dateTimeValuesRowBinary = out.toByteArray();
    }

    private LocalDateTime[] dateTimeValues;

    private byte[] dateTimeValuesRowBinary;

    public LocalDateTime[] getDateTimeValues() {
        return dateTimeValues;
    }

    public byte[] getDateTimeValuesRowBinaryStream() {
        return dateTimeValuesRowBinary;
    }
}