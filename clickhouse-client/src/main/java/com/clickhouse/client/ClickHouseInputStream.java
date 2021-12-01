package com.clickhouse.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Extended input stream.
 */
public abstract class ClickHouseInputStream extends InputStream {
    static final class SimpleInputStream extends ClickHouseInputStream {
        private final InputStream in;

        private boolean closed;

        protected SimpleInputStream(InputStream input) {
            this.in = ClickHouseChecker.nonNull(input, "InputStream");
            this.closed = false;
        }

        @Override
        public int available() throws IOException {
            return in.available();
        }

        @Override
        public byte readByte() throws IOException {
            int v = in.read();
            if (v == -1) {
                close();
                throw new EOFException();
            }

            return (byte) v;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            in.close();
            closed = true;
        }

        @Override
        public int read() throws IOException {
            return in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return in.skip(n);
        }
    }

    public static ClickHouseInputStream of(InputStream input) {
        return input instanceof ClickHouseInputStream ? (ClickHouseInputStream) input : new SimpleInputStream(input);
    }

    /**
     * Reads an unsigned byte from the input stream.
     *
     * @return unsigned byte
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public int readUnsignedByte() throws IOException {
        return 0xFF & readByte();
    }

    /**
     * Reads one single byte from the input stream. This is faster than
     * {@link #read()}.
     *
     * @return byte value if present
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public abstract byte readByte() throws IOException;

    /**
     * Checks if the input stream has been closed or not.
     *
     * @return true if the input stream has been closed; false otherwise
     */
    public abstract boolean isClosed();
}
