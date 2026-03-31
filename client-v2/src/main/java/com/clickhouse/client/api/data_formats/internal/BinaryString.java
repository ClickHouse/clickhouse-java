package com.clickhouse.client.api.data_formats.internal;

import java.nio.ByteBuffer;

public interface BinaryString extends Comparable<String>, CharSequence {

    /**
     * Returns a backing byte buffer or creates one
     * @return ByteBuffer instance.
     */
    ByteBuffer rawBuffer();

    /**
     * Converts raw bytes to a string whenever size is.
     * @return String object
     */
    String asString();
}
