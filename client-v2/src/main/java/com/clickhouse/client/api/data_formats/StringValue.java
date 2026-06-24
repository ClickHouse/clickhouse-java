package com.clickhouse.client.api.data_formats;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Holder for ClickHouse {@code String} or {@code FixedString} values that preserves raw bytes
 * to avoid lossy decoding and unnecessary allocations.
 * <p>
 * <b>This is a mutable structure and must be used with care.</b> To avoid copying, it does not
 * duplicate the bytes it is given: the constructor wraps the supplied array/buffer instead of
 * copying it, and {@link #toByteArray()} returns a direct reference to the backing array rather
 * than a defensive copy. Consequently, mutating the source array, the array returned by
 * {@link #toByteArray()}, or reading the same value concurrently while it is being modified will
 * change the observed value. Callers that need an independent snapshot must copy the bytes
 * themselves.
 * <p>
 * Backed by a {@link ByteBuffer} for a richer API and future off-heap memory support. Only heap
 * buffers (with an accessible backing array) are supported today; constructing a value from a
 * direct (off-heap) buffer is rejected. The decoded {@link String} produced by {@link #asString()}
 * is cached.
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
    public StringValue(byte[] bytes) {
        this(bytes, DEFAULT_CHARSET);
    }

    /**
     * Creates a value backed by the given bytes using the provided default charset. The array is wrapped,
     * not copied, so it must not be modified after being passed in.
     *
     * @param bytes          raw value bytes (not null)
     * @param defaultCharset charset used by {@link #asString()} and {@link #toString()} (not null)
     */
    public StringValue(byte[] bytes, Charset defaultCharset) {
        this(ByteBuffer.wrap(bytes), defaultCharset);
    }

    /**
     * Creates a value backed by the remaining content of the given buffer using the provided default charset.
     * The buffer is referenced, not copied, so its content must not be modified afterwards.
     *
     * @param buffer         backing heap buffer (not null); its remaining bytes define the value
     * @param defaultCharset charset used by {@link #asString()} and {@link #toString()} (not null)
     * @throws IllegalArgumentException if the buffer is a direct (off-heap) buffer with no accessible array
     */
    public StringValue(ByteBuffer buffer, Charset defaultCharset) {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        Objects.requireNonNull(defaultCharset, "charset is required to convert buffer to String");

        if (!buffer.hasArray()) {
            throw new IllegalArgumentException("Can work only with heap buffer.");
        }

        // Keep an independent view so external position/limit changes do not affect this value.
        this.buffer = buffer.slice();
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
     * Returns a direct reference to the backing byte array of this value (no copy is made).
     * <p>
     * The returned array is the live backing storage: mutating it mutates this value, and any change
     * to the underlying bytes is reflected here. Callers that need an independent, immutable snapshot
     * must copy the result themselves.
     *
     * @return the backing array holding the value bytes
     */
    public byte[] toByteArray() {
        return buffer.array();
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
        return new String(buffer.array(), charset);
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
