package com.clickhouse.client.api.data_formats.internal;

public interface BinaryString extends Comparable<String> {


    int length();

    /**
     * Converts raw bytes to a string whenever size is.
     * @return String object
     */
    String asString();

    byte[] asBytes();
}
