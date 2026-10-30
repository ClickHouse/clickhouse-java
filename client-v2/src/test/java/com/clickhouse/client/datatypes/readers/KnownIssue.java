package com.clickhouse.client.datatypes.readers;

import com.clickhouse.data.ClickHouseFormat;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A known deviation of readers from {@link ReaderExpectations}. It matches deviations of one kind for the given
 * formats, column types and accessors; a known issue that matches nothing in a full run is reported, so it gets
 * removed once fixed.
 */
final class KnownIssue {

    enum Deviation {
        /** a value was expected, a different value was returned */
        WRONG_VALUE,
        /** a value was expected, an exception was thrown */
        UNEXPECTED_EXCEPTION,
        /** an exception was expected, a value was returned */
        MISSING_EXCEPTION,
        /** an exception was expected, an exception of another type was thrown */
        WRONG_EXCEPTION,
        /** the data set cannot be read at all */
        READ_FAILURE
    }

    private final Deviation deviation;
    private final String description;
    private final Set<ReaderAccessor> accessors;
    private final Pattern types;
    private final Set<ClickHouseFormat> formats;
    private final boolean onNullOnly;
    private final Class<? extends Throwable> thrown;

    private KnownIssue(Deviation deviation, String description, Set<ReaderAccessor> accessors, Pattern types,
            Set<ClickHouseFormat> formats, boolean onNullOnly, Class<? extends Throwable> thrown) {
        this.deviation = deviation;
        this.description = description;
        this.accessors = accessors;
        this.types = types;
        this.formats = formats;
        this.onNullOnly = onNullOnly;
        this.thrown = thrown;
    }

    /**
     * @return issue matching all accessors, types and formats; narrow it with {@link #accessors}, {@link #types}
     *         and {@link #formats}
     */
    static KnownIssue of(Deviation deviation, String description) {
        return new KnownIssue(deviation, description, Collections.emptySet(), null, Collections.emptySet(), false,
                null);
    }

    KnownIssue accessors(ReaderAccessor... accessors) {
        return new KnownIssue(deviation, description, EnumSet.copyOf(Arrays.asList(accessors)), types, formats,
                onNullOnly, thrown);
    }

    /**
     * @param regex pattern matching the whole column type, for example {@code Array\(.*\)}
     */
    KnownIssue types(String regex) {
        return new KnownIssue(deviation, description, accessors, Pattern.compile(regex), formats, onNullOnly,
                thrown);
    }

    KnownIssue formats(ClickHouseFormat... formats) {
        return new KnownIssue(deviation, description, accessors, types, EnumSet.copyOf(Arrays.asList(formats)),
                onNullOnly, thrown);
    }

    /**
     * Restricts the issue to {@code NULL} values, so it does not hide deviations for other values.
     */
    KnownIssue onNullOnly() {
        return new KnownIssue(deviation, description, accessors, types, formats, true, thrown);
    }

    /**
     * Restricts the issue to deviations where the reader throws the given exception.
     */
    KnownIssue throwing(Class<? extends Throwable> thrown) {
        return new KnownIssue(deviation, description, accessors, types, formats, onNullOnly, thrown);
    }

    boolean appliesTo(ClickHouseFormat format) {
        return formats.isEmpty() || formats.contains(format);
    }

    /**
     * @param accessor  accessor, {@code null} for {@link Deviation#READ_FAILURE}
     * @param nullValue whether the deviation happened for a {@code NULL} value
     * @param actual    exception thrown by the reader, {@code null} when it returned a value
     */
    boolean matches(ClickHouseFormat format, String type, ReaderAccessor accessor, Deviation deviation,
            boolean nullValue, Class<? extends Throwable> actual) {
        return this.deviation == deviation && appliesTo(format) && (types == null || types.matcher(type).matches())
                && (accessor == null || accessors.isEmpty() || accessors.contains(accessor))
                && (!onNullOnly || nullValue) && (thrown == null || thrown == actual);
    }

    @Override
    public String toString() {
        return deviation + " " + description + (accessors.isEmpty() ? "" : " " + accessors)
                + (types == null ? "" : " types=" + types) + (formats.isEmpty() ? "" : " formats=" + formats)
                + (onNullOnly ? " on NULL" : "") + (thrown == null ? "" : " throwing " + thrown.getSimpleName());
    }
}
