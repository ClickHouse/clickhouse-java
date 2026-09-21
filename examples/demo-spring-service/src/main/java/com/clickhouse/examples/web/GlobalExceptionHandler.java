package com.clickhouse.examples.web;

import com.clickhouse.examples.telemetry.SignalMetrics;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * Turns malformed or invalid signal payloads into 400 responses and counts them as rejected.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private final SignalMetrics metrics;

    public GlobalExceptionHandler(SignalMetrics metrics) {
        this.metrics = metrics;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> onValidation(MethodArgumentNotValidException ex) {
        metrics.recordRejected(null);
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(e -> e.getField() + " " + e.getDefaultMessage())
                .orElse("invalid payload");
        return badRequest(detail);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> onUnreadable(HttpMessageNotReadableException ex) {
        metrics.recordRejected(null);
        return badRequest("malformed or unrecognized signal payload");
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<Map<String, Object>> onUnsupportedMediaType(HttpMediaTypeNotSupportedException ex) {
        metrics.recordRejected(null);
        return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                .body(Map.of("error", "unsupported media type"));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<Map<String, Object>> onUnsupportedMethod(HttpRequestMethodNotSupportedException ex) {
        metrics.recordRejected(null);
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED)
                .body(Map.of("error", "method not allowed"));
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<Map<String, Object>> onStorageFailure(DataAccessException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "signal storage unavailable"));
    }

    private ResponseEntity<Map<String, Object>> badRequest(String detail) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "bad request", "detail", detail));
    }
}
