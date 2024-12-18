package com.clickhouse.client.api.data_formats;

import java.io.OutputStream;

public class RowBinaryFormatWriter {

    private OutputStream out;

    public RowBinaryFormatWriter(OutputStream out) {
        this.out = out;
    }

    public void writeNull() {

    }

    public void writeDefault() {

    }

    public void writeInt(int value) {

    }

    public void writeFloat(float value) {
    }

    public void writeString(String value) {

    }


}
