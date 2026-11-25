package com.clickhouse.client.cli;

import com.clickhouse.client.api.ServerException;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Error helpers: the server error code of an exception, and the error text that clickhouse-client writes. */
final class Errors {

    static final int SYNTAX_ERROR = 62;

    private Errors() {
    }

    /** Runs call and removes the ExecutionException wrapper of a future. */
    static <T> T unwrap(Callable<T> call) throws Exception {
        try {
            return call.call();
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    /** A result that does not match the test hint of the statement. */
    static final class HintException extends Exception {
        final int code;

        HintException(String message, int code) {
            super(message);
            this.code = code;
        }
    }

    /** Returns the ServerException in the cause chain, or null. */
    static ServerException serverException(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ServerException) {
                return (ServerException) t;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }

    /**
     * Returns the server error code of an exception: the code of a ServerException in the cause chain, or the
     * vendor code of a SQLException. Returns null for an error that did not come from the server.
     */
    static Integer serverCode(Throwable e) {
        if (e == null) {
            return null;
        }
        ServerException se = serverException(e);
        if (se != null) {
            return se.getCode();
        }
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException && ((SQLException) t).getErrorCode() > 0) {
                return ((SQLException) t).getErrorCode();
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }

    /**
     * Writes a server error the way clickhouse-client does, {@code Code: N. DB::Exception: ... (NAME)}, so that the
     * error name is in the text.
     */
    static String formatServerError(Throwable e, int code) {
        ServerException se = serverException(e);
        String msg = se != null ? se.getMessage() : e.getMessage();
        if (msg == null) {
            msg = e.toString();
        }
        msg = msg.trim();
        String name = ErrorCodes.name(code);
        if (!msg.startsWith("Code:")) {
            msg = "Code: " + code + ". DB::Exception: " + msg;
        }
        if (name != null && !msg.contains("(" + name + ")")) {
            msg += " (" + name + ")";
        }
        return msg;
    }

    /** Returns the message of an exception with the messages of its causes. */
    static String describe(Throwable e) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (sb.length() > 0) {
                sb.append(": ");
            }
            String m = t.getMessage();
            sb.append(t.getClass().getSimpleName());
            if (m != null && !m.isEmpty()) {
                sb.append(" ").append(m);
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return sb.toString();
    }
}
