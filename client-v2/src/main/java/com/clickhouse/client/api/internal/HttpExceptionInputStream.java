package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hides tagged exception frames appended to successful HTTP response bodies. A possible frame prefix remains buffered
 * until it is matched or disproved, so callers never observe part of a marker when it crosses source read boundaries.
 */
final class HttpExceptionInputStream extends InputStream {

    private static final byte[] EXCEPTION_MARKER = "\r\n__exception__\r\n".getBytes(StandardCharsets.UTF_8);
    private static final String EXCEPTION_END_MARKER = "\r\n__exception__\r\n";
    private static final int BUFFER_SIZE = 8192;
    private static final int MAX_EXCEPTION_SIZE = 32 * 1024;
    private static final Pattern ERROR_CODE_PATTERN = Pattern.compile("^Code:\\s*(\\d+)\\.");

    private final InputStream source;
    private final String exceptionTag;
    private final int transportStatus;
    private final String queryId;
    private final byte[] exceptionPrefix;
    private final byte[] sourceBuffer = new byte[BUFFER_SIZE];

    private byte[] pending = new byte[BUFFER_SIZE];
    private int pendingStart;
    private int pendingEnd;
    private int scanOffset;
    private boolean sourceDone;
    private RuntimeException terminalException;
    private IOException terminalIOException;

    HttpExceptionInputStream(InputStream source, String exceptionTag, int transportStatus, String queryId) {
        this.source = source;
        this.exceptionTag = exceptionTag;
        this.transportStatus = transportStatus;
        this.queryId = queryId;
        byte[] tagBytes = exceptionTag.getBytes(StandardCharsets.UTF_8);
        this.exceptionPrefix = Arrays.copyOf(EXCEPTION_MARKER, EXCEPTION_MARKER.length + tagBytes.length + 2);
        System.arraycopy(tagBytes, 0, exceptionPrefix, EXCEPTION_MARKER.length, tagBytes.length);
        exceptionPrefix[exceptionPrefix.length - 2] = '\r';
        exceptionPrefix[exceptionPrefix.length - 1] = '\n';
    }

    @Override
    public int read() throws IOException {
        byte[] oneByte = new byte[1];
        int read = read(oneByte, 0, 1);
        return read < 0 ? -1 : oneByte[0] & 0xff;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        if (buffer == null) {
            throw new NullPointerException("buffer");
        }
        if (offset < 0 || length < 0 || length > buffer.length - offset) {
            throw new IndexOutOfBoundsException();
        }
        if (length == 0) {
            return 0;
        }

        while (true) {
            int safeLength = safeLength();
            if (safeLength > 0) {
                int read = Math.min(length, safeLength);
                System.arraycopy(pending, pendingStart, buffer, offset, read);
                pendingStart += read;
                return read;
            }
            if (terminalException != null) {
                throw terminalException;
            }
            if (terminalIOException != null) {
                throw terminalIOException;
            }
            if (sourceDone) {
                return -1;
            }

            fillPending();
            scanPending();
        }
    }

    @Override
    public int available() {
        return safeLength();
    }

    @Override
    public void close() throws IOException {
        source.close();
    }

    private int safeLength() {
        if (sourceDone || terminalException != null || terminalIOException != null) {
            return pendingEnd - pendingStart;
        }
        return Math.max(0, scanOffset - pendingStart);
    }

    private void fillPending() {
        try {
            int read = source.read(sourceBuffer);
            if (read < 0) {
                sourceDone = true;
                scanOffset = pendingEnd;
                return;
            }
            appendPending(sourceBuffer, read);
        } catch (IOException e) {
            sourceDone = true;
            terminalIOException = e;
            scanOffset = pendingEnd;
        }
    }

    private void appendPending(byte[] bytes, int length) {
        compactPending(length);
        System.arraycopy(bytes, 0, pending, pendingEnd, length);
        pendingEnd += length;
    }

    private void compactPending(int additionalLength) {
        int currentLength = pendingEnd - pendingStart;
        if (pending.length - pendingEnd >= additionalLength) {
            return;
        }

        int newLength = Math.max(pending.length * 2, currentLength + additionalLength);
        byte[] compacted = new byte[newLength];
        System.arraycopy(pending, pendingStart, compacted, 0, currentLength);
        scanOffset -= pendingStart;
        pendingStart = 0;
        pendingEnd = currentLength;
        pending = compacted;
    }

    private void scanPending() {
        int exceptionStart = indexOf(pending, scanOffset, pendingEnd, exceptionPrefix);
        if (exceptionStart >= 0) {
            captureException(exceptionStart);
            return;
        }

        int suffixLength = matchingSuffixLength(pending, pendingStart, pendingEnd, exceptionPrefix);
        scanOffset = pendingEnd - suffixLength;
    }

    private void captureException(int exceptionStart) {
        ByteArrayOutputStream exceptionBody = new ByteArrayOutputStream();
        int bodyStart = exceptionStart + exceptionPrefix.length;
        exceptionBody.write(pending, bodyStart, pendingEnd - bodyStart);
        pendingEnd = exceptionStart;
        scanOffset = exceptionStart;

        try {
            while (exceptionBody.size() <= MAX_EXCEPTION_SIZE) {
                int read = source.read(sourceBuffer);
                if (read < 0) {
                    terminalException = parseException(exceptionBody.toByteArray());
                    sourceDone = true;
                    return;
                }
                int remaining = MAX_EXCEPTION_SIZE + 1 - exceptionBody.size();
                exceptionBody.write(sourceBuffer, 0, Math.min(read, remaining));
                if (read > remaining || exceptionBody.size() > MAX_EXCEPTION_SIZE) {
                    terminalException = new ClientException("ClickHouse exception frame exceeds "
                            + MAX_EXCEPTION_SIZE + " bytes");
                    sourceDone = true;
                    return;
                }
            }
        } catch (IOException e) {
            ClientException truncatedFrame = new ClientException(
                    "Failed to finish reading ClickHouse exception frame", parseException(exceptionBody.toByteArray()));
            truncatedFrame.addSuppressed(e);
            terminalException = truncatedFrame;
            sourceDone = true;
        }
    }

    private ServerException parseException(byte[] body) {
        String message = stripTrailer(new String(body, StandardCharsets.UTF_8)).trim();
        Matcher matcher = ERROR_CODE_PATTERN.matcher(message);
        int errorCode = matcher.find() ? Integer.parseInt(matcher.group(1)) : ServerException.CODE_UNKNOWN;
        return new ServerException(errorCode, message, transportStatus, queryId);
    }

    private String stripTrailer(String body) {
        int closingMarker = body.lastIndexOf(EXCEPTION_END_MARKER);
        if (closingMarker < 0) {
            return body;
        }

        String beforeMarker = body.substring(0, closingMarker);
        int trailerStart = beforeMarker.lastIndexOf("\r\n");
        if (trailerStart < 0) {
            return body;
        }

        String trailer = beforeMarker.substring(trailerStart + 2);
        int separator = trailer.indexOf(' ');
        if (separator <= 0 || !trailer.substring(separator + 1).equals(exceptionTag)) {
            return body;
        }
        for (int i = 0; i < separator; i++) {
            if (!Character.isDigit(trailer.charAt(i))) {
                return body;
            }
        }
        return beforeMarker.substring(0, trailerStart);
    }

    private static int indexOf(byte[] data, int from, int to, byte[] pattern) {
        int lastStart = to - pattern.length;
        for (int i = from; i <= lastStart; i++) {
            int j = 0;
            while (j < pattern.length && data[i + j] == pattern[j]) {
                j++;
            }
            if (j == pattern.length) {
                return i;
            }
        }
        return -1;
    }

    private static int matchingSuffixLength(byte[] data, int from, int to, byte[] pattern) {
        int maxLength = Math.min(pattern.length - 1, to - from);
        for (int length = maxLength; length > 0; length--) {
            int suffixStart = to - length;
            int i = 0;
            while (i < length && data[suffixStart + i] == pattern[i]) {
                i++;
            }
            if (i == length) {
                return length;
            }
        }
        return 0;
    }
}
