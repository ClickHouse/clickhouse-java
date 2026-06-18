package com.clickhouse.client.api.data_formats;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Holder for a ClickHouse {@code String} (or {@code FixedString}) value that keeps the original bytes
 * as they were received from the server instead of eagerly decoding them into a {@link String}.
 * <p>
 * ClickHouse {@code String} columns are arbitrary byte sequences and are not guaranteed to be valid
 * text in any particular encoding (for example a {@code String} may store a hash, a serialized blob or
 * text in a non UTF-8 charset). Decoding such values as UTF-8 is lossy. This class preserves the raw
 * bytes so that:
 * <ul>
 *     <li>binary content can be read back exactly via {@link #toByteArray()} or {@link #asByteBuffer()};</li>
 *     <li>text content can be decoded with the charset the caller knows about via {@link #asString(Charset)};</li>
 *     <li>large values are not duplicated into a {@link String} unless the caller actually needs one.</li>
 * </ul>
 * <p>
 * The value is backed by a {@link ByteBuffer} which exposes a richer API to callers and allows the
 * implementation to use direct (off-heap) memory in the future without changing this contract.
 * Instances are immutable: the backing buffer is never mutated and callers receive read-only views or
 * copies. The {@link String} produced by {@link #asString()} is cached so repeated access (for example
 * inside a row loop) does not allocate a new object every time.
 */
public final class StringValue {

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
        this(ByteBuffer.wrap(bytes), DEFAULT_CHARSET);
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
     * Creates a value backed by a region of the given array. The array is referenced, not copied.
     *
     * @param bytes  raw value bytes (not null)
     * @param offset start offset in the array
     * @param length number of bytes
     */
    public StringValue(byte[] bytes, int offset, int length) {
        this(ByteBuffer.wrap(bytes, offset, length), DEFAULT_CHARSET);
    }

    /**
     * Creates a value backed by the remaining content of the given buffer.
     *
     * @param buffer backing buffer (not null); its remaining bytes define the value
     */
    public StringValue(ByteBuffer buffer) {
        this(buffer, DEFAULT_CHARSET);
    }

    /**
     * Creates a value backed by the remaining content of the given buffer using the provided default charset.
     *
     * @param buffer         backing buffer (not null); its remaining bytes define the value
     * @param defaultCharset charset used by {@link #asString()} and {@link #toString()} (not null)
     */
    public StringValue(ByteBuffer buffer, Charset defaultCharset) {
        if (buffer == null) {
            throw new NullPointerException("buffer is null");
        }
        if (defaultCharset == null) {
            throw new NullPointerException("defaultCharset is null");
        }
        // Keep an independent view so external position/limit changes do not affect this value.
        this.buffer = buffer.slice();
        this.defaultCharset = defaultCharset;
    }

    /**
     * Creates a value from a Java string encoded with UTF-8.
     *
     * @param value source string (not null)
     * @return new value
     */
    public static StringValue of(String value) {
        return of(value, DEFAULT_CHARSET);
    }

    /**
     * Creates a value from a Java string encoded with the given charset.
     *
     * @param value   source string (not null)
     * @param charset charset used to encode the string (not null)
     * @return new value
     */
    public static StringValue of(String value, Charset charset) {
        StringValue sv = new StringValue(value.getBytes(charset), charset);
        if (charset.equals(DEFAULT_CHARSET)) {
            sv.cached = value;
        }
        return sv;
    }

    /**
     * Creates a value from the given bytes. The array is wrapped, not copied.
     *
     * @param bytes raw value bytes (not null)
     * @return new value
     */
    public static StringValue of(byte[] bytes) {
        return new StringValue(bytes);
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
     * Returns a fresh copy of the raw bytes of this value.
     *
     * @return new byte array with the value bytes
     */
    public byte[] toByteArray() {
        ByteBuffer view = buffer.duplicate();
        if (view.hasArray()) {
            int start = view.arrayOffset() + view.position();
            return Arrays.copyOfRange(view.array(), start, start + view.remaining());
        }
        byte[] out = new byte[view.remaining()];
        view.get(out);
        return out;
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
        if (charset == null) {
            throw new NullPointerException("charset is null");
        }
        if (charset.equals(defaultCharset)) {
            return asString();
        }
        return decode(charset);
    }

    /**
     * Returns a stream over the raw bytes of this value. Useful for JDBC binary/ascii stream access.
     *
     * @return input stream over the value bytes
     */
    public InputStream asInputStream() {
        ByteBuffer view = buffer.duplicate();
        if (view.hasArray()) {
            int start = view.arrayOffset() + view.position();
            return new ByteArrayInputStream(view.array(), start, view.remaining());
        }
        return new ByteArrayInputStream(toByteArray());
    }

    private String decode(Charset charset) {
        ByteBuffer view = buffer.duplicate();
        if (view.hasArray()) {
            return new String(view.array(), view.arrayOffset() + view.position(), view.remaining(), charset);
        }
        byte[] tmp = new byte[view.remaining()];
        view.get(tmp);
        return new String(tmp, charset);
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
