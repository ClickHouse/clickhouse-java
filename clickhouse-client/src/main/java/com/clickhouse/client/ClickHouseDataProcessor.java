package com.clickhouse.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.clickhouse.client.data.ClickHouseSimpleRecord;

/**
 * This defines a data processor for dealing with serialization and
 * deserialization of one or multiple {@link ClickHouseFormat}. Unlike
 * {@link ClickHouseDeserializer} and {@link ClickHouseSerializer}, which is for
 * specific column or data type, data processor is a combination of both, and it
 * can handle more scenarios like separator between columns and rows.
 */
public abstract class ClickHouseDataProcessor {
    public static final List<ClickHouseColumn> DEFAULT_COLUMNS = Collections
            .singletonList(ClickHouseColumn.of("results", "Nullable(String)"));

    protected static final String ERROR_FAILED_TO_READ = "Failed to read column #%d of %d: %s";
    protected static final String ERROR_FAILED_TO_WRITE = "Failed to write column #%d of %d: %s";
    protected static final String ERROR_REACHED_END_OF_STREAM = "Reached end of the stream when reading column #%d of %d: %s";
    protected static final String ERROR_UNKNOWN_DATA_TYPE = "Unsupported data type: ";

    // not a fan of Java generics :<
    protected static void buildAggMappings(
            Map<ClickHouseAggregateFunction, ClickHouseDeserializer<ClickHouseValue>> deserializers,
            Map<ClickHouseAggregateFunction, ClickHouseSerializer<ClickHouseValue>> serializers,
            ClickHouseDeserializer<ClickHouseValue> d, ClickHouseSerializer<ClickHouseValue> s,
            ClickHouseAggregateFunction... types) {
        for (ClickHouseAggregateFunction t : types) {
            if (deserializers.put(t, d) != null) {
                throw new IllegalArgumentException("Duplicated deserializer of AggregateFunction - " + t.name());
            }
            if (serializers.put(t, s) != null) {
                throw new IllegalArgumentException("Duplicated serializer of AggregateFunction - " + t.name());
            }
        }
    }

    protected static <E extends Enum<E>, T extends ClickHouseValue> void buildMappings(
            Map<E, ClickHouseDeserializer<? extends ClickHouseValue>> deserializers,
            Map<E, ClickHouseSerializer<? extends ClickHouseValue>> serializers,
            ClickHouseDeserializer<T> d, ClickHouseSerializer<T> s, E... types) {
        for (E t : types) {
            if (deserializers.put(t, d) != null) {
                throw new IllegalArgumentException("Duplicated deserializer of: " + t.name());
            }
            if (serializers.put(t, s) != null) {
                throw new IllegalArgumentException("Duplicated serializer of: " + t.name());
            }
        }
    }

    protected final ClickHouseConfig config;
    protected final ClickHouseInputStream input;
    protected final ClickHouseOutputStream output;
    protected final ClickHouseColumn[] columns;
    protected final ClickHouseValue[] templates;
    protected final Map<String, Object> settings;

    protected final Iterator<ClickHouseRecord> records;
    protected final Iterator<ClickHouseValue> values;
    // protected final Object writer;

    /**
     * Column index shared by {@link #read(ClickHouseValue, ClickHouseColumn)},
     * {@link #records()}, and {@link #values()}.
     */
    protected int readPosition;
    /**
     * Column index shared by {@link #write(ClickHouseValue, ClickHouseColumn)}.
     */
    // protected int writePosition;

    /**
     * Factory method to create a record.
     *
     * @return new record
     */
    protected ClickHouseRecord createRecord() {
        return ClickHouseSimpleRecord.of(getColumns(), templates);
    }

    /**
     * Initializes iterator of {@link ClickHouseRecord} for reading values record by
     * record. Usually this should be only called once during instantiation.
     * 
     * @return non-null iterator of {@link ClickHouseRecord}
     */
    protected Iterator<ClickHouseRecord> initRecords() {
        final ClickHouseRecord reusableRecord = createRecord();
        final Supplier<ClickHouseRecord> factory;
        if (config.isReuseValueWrapper()) {
            factory = () -> reusableRecord;
        } else {
            factory = reusableRecord::copy;
        }
        return new Iterator<ClickHouseRecord>() {
            @Override
            public boolean hasNext() {
                try {
                    return input.available() > 0;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public ClickHouseRecord next() {
                ClickHouseRecord currentRecord = factory.get();
                try {
                    readAndFill(currentRecord);
                    readPosition = 0;
                } catch (EOFException e) {
                    if (readPosition == 0) { // end of the stream, which is fine
                        throw new NoSuchElementException("No more record");
                    } else {
                        throw new UncheckedIOException(ClickHouseUtils.format(ERROR_REACHED_END_OF_STREAM,
                                readPosition + 1, columns.length, columns[readPosition]), e);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            ClickHouseUtils.format(ERROR_FAILED_TO_READ, readPosition + 1, columns.length,
                                    columns[readPosition]),
                            e);
                }

                return currentRecord;
            }
        };
    }

    /**
     * Initializes iterator of {@link ClickHouseValue} for reading values one by
     * one. Usually this should be only called once during instantiation.
     * 
     * @return non-null iterator of {@link ClickHouseValue}
     */
    protected Iterator<ClickHouseValue> initValues() {
        if (readPosition != 0) {
            throw new IllegalStateException("This method is supposed to be called once during instantiation");
        }

        return new Iterator<ClickHouseValue>() {
            @Override
            public boolean hasNext() {
                try {
                    return input.available() > 0;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public ClickHouseValue next() {
                ClickHouseColumn column = columns[readPosition];
                ClickHouseValue value = config.isReuseValueWrapper() ? templates[readPosition]
                        : ClickHouseValues.newValue(config, column);
                try {
                    templates[readPosition] = read(value, column);
                    if (++readPosition >= columns.length) {
                        readPosition = 0;
                    }
                } catch (EOFException e) {
                    if (readPosition == 0) { // end of the stream, which is fine
                        throw new NoSuchElementException("No more value");
                    } else {
                        throw new UncheckedIOException(ClickHouseUtils.format(ERROR_REACHED_END_OF_STREAM,
                                readPosition + 1, columns.length, column), e);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            ClickHouseUtils.format(ERROR_FAILED_TO_READ, readPosition + 1, columns.length, column), e);
                }

                return value;
            }
        };
    }

    // protected abstract Object initWriter();

    /**
     * Reads columns(starting from {@code readPosition}) from input stream and fill
     * deserialized data into the given record. This method is only used when
     * iterating through {@link #records()}.
     *
     * @param r non-null record to fill
     * @throws IOException when failed to read columns from input stream
     */
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        for (; readPosition < columns.length; readPosition++) {
            readAndFill(r.getValue(readPosition), columns[readPosition]);
        }
    }

    /**
     * Reads column(at {@code readPosition} from input stream and fill deserialized
     * data into the given value object. This method is mainly used when iterating
     * through {@link #values()}. In default implementation, it's also used in
     * {@link #readAndFill(ClickHouseRecord)} for simplicity.
     *
     * @param value  non-null value object to fill
     * @param column non-null type of the value
     * @throws IOException when failed to read column from input stream
     */
    protected abstract void readAndFill(ClickHouseValue value, ClickHouseColumn column) throws IOException;

    /**
     * Reads columns from input stream. Usually this will be only called once during
     * instantiation.
     *
     * @return non-null list of columns
     * @throws IOException when failed to read columns from input stream
     */
    protected abstract List<ClickHouseColumn> readColumns() throws IOException;

    /**
     * Default constructor.
     *
     * @param config   non-null confinguration contains information like format
     * @param input    input stream for deserialization, can be null when
     *                 {@code output} is available
     * @param output   outut stream for serialization, can be null when
     *                 {@code input} is available
     * @param columns  nullable columns
     * @param settings nullable settings
     * @throws IOException when failed to read columns from input stream
     */
    protected ClickHouseDataProcessor(ClickHouseConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Object> settings)
            throws IOException {
        this.config = ClickHouseChecker.nonNull(config, "config");
        if (input == null && output == null) {
            throw new IllegalArgumentException("One of input and output stream must not be null");
        }

        this.input = input;
        this.output = output;
        if (settings == null || settings.isEmpty()) {
            this.settings = Collections.emptyMap();
        } else {
            Map<String, Object> map = new HashMap<>();
            map.putAll(settings);
            this.settings = Collections.unmodifiableMap(map);
        }

        if (columns == null && input != null) {
            columns = readColumns();
        }

        if (columns == null || columns.isEmpty()) {
            this.columns = ClickHouseColumn.EMPTY_ARRAY;
            this.templates = ClickHouseValues.EMPTY_VALUES;
        } else {
            int len = columns.size();
            int idx = 0;
            List<ClickHouseColumn> list = new ArrayList<>(len);
            this.templates = new ClickHouseValue[len];
            for (ClickHouseColumn column : columns) {
                column.setColumnIndex(idx, len);
                list.add(column);
                if (config.isReuseValueWrapper()) {
                    templates[idx] = ClickHouseValues.newValue(config, column);
                }
                idx++;
            }
            this.columns = list.toArray(ClickHouseColumn.EMPTY_ARRAY);
        }

        if (this.columns.length == 0 || input == null) {
            this.records = Collections.emptyIterator();
            this.values = Collections.emptyIterator();
        } else {
            this.records = ClickHouseChecker.nonNull(initRecords(), "Records");
            this.values = ClickHouseChecker.nonNull(initValues(), "Values");
        }
        // this.writer = this.columns.length == 0 || output == null ? null :
        // initWriter();

        this.readPosition = 0;
        // this.writePosition = 0;
    }

    /**
     * Gets list of columns to process.
     *
     * @return list of columns to process
     */
    public final List<ClickHouseColumn> getColumns() {
        return Collections.unmodifiableList(Arrays.asList(columns));
    }

    /**
     * Returns an iterable collection of records which can be walked through in a
     * foreach-loop. Please pay attention that: 1)
     * {@link java.io.UncheckedIOException} might be thrown when iterating through
     * the collection; and 2) it's not supposed to be called for more than once.
     *
     * @return non-null iterable records
     */
    public final Iterable<ClickHouseRecord> records() {
        if (columns.length == 0) {
            return Collections.emptyList();
        }

        return () -> records;
    }

    /**
     * Returns an iterable collection of values which can be walked through in a
     * foreach-loop. It's slower than {@link #records()}, because the latter
     * reads data in bulk. However, it's particular useful when you're reading large
     * values with limited memory - e.g. a binary field with a few GB bytes.
     * 
     * @return non-null iterable values
     */
    public final Iterable<ClickHouseValue> values() {
        if (columns.length == 0) {
            return Collections.emptyList();
        }

        return () -> values;
    }

    /**
     * Reads deserialized value directly from input stream. Unlike
     * {@link #records()}, which reads multiple values at a time, this method will
     * only read one for each call.
     *
     * @param value  value to update, could be null
     * @param column type of the value, could be null
     * @return updated {@code value} or a new {@link ClickHouseValue} when it is
     *         null
     * @throws IOException when failed to read data from input stream
     */
    public ClickHouseValue read(ClickHouseValue value, ClickHouseColumn column) throws IOException {
        if (input == null) {
            throw new IllegalStateException("No input stream available to read");
        }
        if (column == null) {
            column = columns[readPosition];
        }
        if (value == null) {
            value = config.isReuseValueWrapper() ? templates[readPosition] : ClickHouseValues.newValue(config, column);
        }

        readAndFill(value, column);
        if (++readPosition >= columns.length) {
            readPosition = 0;
        }
        return value;
    }

    /**
     * Writes serialized value to output stream.
     *
     * @param value  non-null value to be serialized
     * @param column non-null type information
     * @throws IOException when failed to write data to output stream
     */
    public abstract void write(ClickHouseValue value, ClickHouseColumn column) throws IOException;
}
