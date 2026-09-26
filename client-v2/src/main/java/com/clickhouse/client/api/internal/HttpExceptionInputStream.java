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
    private final Runnable onCompleteException;
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
        this(source, exceptionTag, transportStatus, queryId, () -> { });
    }

    HttpExceptionInputStream(InputStream source, String exceptionTag, int transportStatus, String queryId,
                             Runnable onCompleteException) {
        this.source = source;
        this.exceptionTag = exceptionTag;
        this.transportStatus = transportStatus;
        this.queryId = queryId;
        this.onCompleteException = onCompleteException;
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

            fillPending(length);
            scanPending();
        }
    }

    @Override
    public int available() {
        return safeLength();
    }

    @Override
    public void close() throws IOException {
        try {
            source.close();
        } catch (IOException e) {
            if (!(terminalException instanceof ServerException)) {
                throw e;
            }
            terminalException.addSuppressed(e);
        }
    }

    private int safeLength() {
        if (sourceDone || terminalException != null || terminalIOException != null) {
            return pendingEnd - pendingStart;
        }
        return Math.max(0, scanOffset - pendingStart);
    }

    private void fillPending(int requestedLength) {
        try {
            // Reading ahead can drain a small HTTP response and release its pooled connection prematurely.
            int read = source.read(sourceBuffer, 0, Math.min(requestedLength, sourceBuffer.length));
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
                byte[] body = exceptionBody.toByteArray();
                int messageLength = completeMessageLength(body);
                if (messageLength >= 0) {
                    terminalException = parseException(Arrays.copyOf(body, messageLength));
                    sourceDone = true;
                    onCompleteException.run();
                    return;
                }
                int read = source.read(sourceBuffer);
                if (read < 0) {
                    terminalException = new ClientException("Incomplete ClickHouse exception frame", parseException(body));
                    sourceDone = true;
                    return;
                }
                int remaining = MAX_EXCEPTION_SIZE + 1 - exceptionBody.size();
                exceptionBody.write(sourceBuffer, 0, Math.min(read, remaining));
            }
            terminalException = new ClientException("ClickHouse exception frame exceeds " + MAX_EXCEPTION_SIZE + " bytes");
            sourceDone = true;
        } catch (IOException e) {
            ClientException truncatedFrame = new ClientException(
                    "Failed to finish reading ClickHouse exception frame", parseException(exceptionBody.toByteArray()));
            truncatedFrame.addSuppressed(e);
            terminalException = truncatedFrame;
            sourceDone = true;
        }
    }

    private ServerException parseException(byte[] body) {
        String message = new String(body, StandardCharsets.UTF_8).trim();
        Matcher matcher = ERROR_CODE_PATTERN.matcher(message);
        int errorCode = matcher.find() ? Integer.parseInt(matcher.group(1)) : ServerException.CODE_UNKNOWN;
        return new ServerException(errorCode, message, transportStatus, queryId);
    }

    private int completeMessageLength(byte[] body) {
        byte[] suffix = (" " + exceptionTag + EXCEPTION_END_MARKER).getBytes(StandardCharsets.UTF_8);
        int digitsEnd = body.length - suffix.length;
        if (digitsEnd <= 0) {
            return -1;
        }
        for (int i = 0; i < suffix.length; i++) {
            if (body[digitsEnd + i] != suffix[i]) {
                return -1;
            }
        }
        int digitsStart = digitsEnd;
        while (digitsStart > 0 && body[digitsStart - 1] != '\n') {
            digitsStart--;
        }
        if (digitsStart == 0 || digitsStart == digitsEnd) {
            return -1;
        }
        int messageLength = 0;
        for (int i = digitsStart; i < digitsEnd; i++) {
            if (body[i] < '0' || body[i] > '9' || messageLength > MAX_EXCEPTION_SIZE / 10) {
                return -1;
            }
            messageLength = messageLength * 10 + body[i] - '0';
        }
        // The server counts UTF-8 bytes, including the message's final newline, not Java characters.
        return messageLength == digitsStart ? messageLength : -1;
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
