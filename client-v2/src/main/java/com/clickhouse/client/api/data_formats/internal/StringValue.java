package com.clickhouse.client.api.data_formats.internal;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Read-time holder for ClickHouse {@code String} or {@code FixedString} values that preserves raw bytes
 * to avoid lossy decoding and unnecessary allocations.
 * <p>
 * <b>This is an internal value holder, not a general-purpose type for user code.</b> It is produced only
 * by the read path when the binary-string feature is enabled (for example {@code GenericRecord.getObject}
 * or {@code BinaryStreamReader.readValue} without a type hint), so that callers that need exact bytes can
 * obtain them via {@link #toByteArray()} and callers that need text can decode via {@link #asString()}.
 * Instances cannot be created by user code: the constructors are package-private and only the binary
 * reader builds them. It is <b>not</b> a supported field type for POJO binding: declare POJO fields for
 * String/FixedString columns as {@link String} or {@code byte[]} instead. Normal application code should
 * generally consume {@link String} or {@code byte[]} rather than holding onto a {@code StringValue}.
 * <p>
 * <b>This is a mutable structure and must be used with care.</b> To avoid copying, it does not
 * duplicate the bytes it is given: the constructor wraps the supplied array instead of copying it, and
 * {@link #toByteArray()} returns a direct reference to the backing array when the value spans the whole
 * array. Consequently, mutating the source array, the array returned by {@link #toByteArray()}, or reading
 * the same value concurrently while it is being modified will change the observed value. Callers that need
 * an independent snapshot must copy the bytes themselves.
 * <p>
 * Backed by a {@link ByteBuffer} for a richer API and future off-heap memory support. The decoded
 * {@link String} produced by {@link #asString()} is cached.
 */
public class StringValue {

    /** Charset used by {@link #asString()} and {@link #toString()} when no charset is provided. */
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final ByteBuffer buffer;

    private final Charset defaultCharset;

    private volatile String cached;

    /**
     * Creates a value backed by the given bytes. The array is wrapped, not copied, so it must not be
     * modified after being passed in.
     *
     * @param bytes raw value bytes (not null)
     */
    StringValue(byte[] bytes) {
        this(bytes, DEFAULT_CHARSET);
    }

    /**
     * Creates a value backed by the given bytes using the provided default charset. The array is wrapped,
     * not copied, so it must not be modified after being passed in.
     *
     * @param bytes          raw value bytes (not null)
     * @param defaultCharset charset used by {@link #asString()} and {@link #toString()} (not null)
     */
    StringValue(byte[] bytes, Charset defaultCharset) {
        Objects.requireNonNull(bytes, "bytes cannot be null");
        Objects.requireNonNull(defaultCharset, "charset is required to convert bytes to String");

        this.buffer = ByteBuffer.wrap(bytes);
        this.defaultCharset = defaultCharset;
    }

    /**
     * Returns a read-only view over the raw bytes of this value. The returned buffer is independent
     * (its own position/limit) and shares no mutable state with this value.
     *
     * @return read-only buffer positioned at the first byte of the value
     */
    public ByteBuffer asByteBuffer() {
        return buffer.asReadOnlyBuffer();
    }

    /**
     * Returns the raw bytes of this value, honoring the backing buffer's offset and position.
     * <p>
     * As a zero-copy shortcut, when the value spans the entire backing array the live backing storage is
     * returned directly (mutating it mutates this value); otherwise an exact-size copy of the value's bytes
     * is returned. Callers that need a guaranteed independent snapshot should copy the result themselves.
     *
     * @return the value bytes (the live backing array when it spans the whole value, otherwise a copy)
     */
    public byte[] toByteArray() {
        byte[] array = buffer.array();
        int offset = buffer.arrayOffset() + buffer.position();
        int length = buffer.remaining();
        if (offset == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, offset, offset + length);
    }

    /**
     * @return number of bytes in this value
     */
    public int size() {
        return buffer.remaining();
    }

    /**
     * @return {@code true} if the value has no bytes
     */
    public boolean isEmpty() {
        return buffer.remaining() == 0;
    }

    /**
     * Decodes the value using the default charset (UTF-8 unless another was provided at construction).
     * The result is cached so repeated calls do not allocate a new string.
     *
     * @return decoded string
     */
    public String asString() {
        String s = cached;
        if (s == null) {
            s = decode(defaultCharset);
            cached = s;
        }
        return s;
    }

    /**
     * Decodes the value using the given charset. The result is cached only when the charset matches the
     * default charset of this value.
     *
     * @param charset charset to decode with (not null)
     * @return decoded string
     */
    public String asString(Charset charset) {
        Objects.requireNonNull(charset, "charset cannot be null");
        if (charset.equals(defaultCharset)) {
            return asString();
        }
        return decode(charset);
    }

    private String decode(Charset charset) {
        return new String(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), charset);
    }

    @Override
    public String toString() {
        return asString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StringValue)) {
            return false;
        }
        return buffer.equals(((StringValue) o).buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }
}
