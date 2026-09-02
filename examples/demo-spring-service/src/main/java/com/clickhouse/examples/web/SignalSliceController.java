package com.clickhouse.examples.web;

import com.clickhouse.examples.model.SignalSlice;
import com.clickhouse.examples.service.SignalSliceService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Returns one-second mean-value rows for recently reported location signals.
 */
@RestController
@RequestMapping("/api/v1/signals/slices")
public class SignalSliceController {

    private static final Duration MAX_LOOKBACK = Duration.ofHours(1);
    private static final Pattern LOOKBACK_PATTERN = Pattern.compile("([1-9][0-9]*)(s|m|h)");

    private final SignalSliceService signalSliceService;

    public SignalSliceController(SignalSliceService signalSliceService) {
        this.signalSliceService = signalSliceService;
    }

    @GetMapping
    public List<SignalSlice> slices(
            @RequestParam(defaultValue = "10s") String lookback,
            @RequestParam(required = false) UUID locationId) {
        return signalSliceService.findSlices(parseLookback(lookback), locationId);
    }

    private Duration parseLookback(String value) {
        Matcher matcher = LOOKBACK_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw invalidLookback();
        }

        long amount;
        try {
            amount = Long.parseLong(matcher.group(1));
        } catch (NumberFormatException ex) {
            throw invalidLookback();
        }

        Duration duration = switch (matcher.group(2)) {
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            case "h" -> Duration.ofHours(amount);
            default -> throw invalidLookback();
        };
        if (duration.compareTo(MAX_LOOKBACK) > 0) {
            throw invalidLookback();
        }
        return duration;
    }

    private ResponseStatusException invalidLookback() {
        return new ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "lookback must use s, m, or h and cannot exceed 1h");
    }
}
