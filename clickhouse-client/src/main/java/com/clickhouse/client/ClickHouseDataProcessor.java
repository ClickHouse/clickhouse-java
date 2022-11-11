package com.clickhouse.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This defines a data processor for dealing with serialization and
 * deserialization of one or multiple {@link ClickHouseFormat}. Unlike
 * {@link ClickHouseDeserializer} and {@link ClickHouseSerializer}, which is for
 * specific column or data type, data processor is a combination of both, and it
 * can handle more scenarios like separator between columns and rows.
 */
public abstract class ClickHouseDataProcessor {
    static final class RecordsIterator implements Iterator<ClickHouseRecord> {
        private final ClickHouseDataProcessor processor;

        RecordsIterator(ClickHouseDataProcessor processor) {
            this.processor = processor;
        }

        @Override
        public boolean hasNext() {
            return processor.hasMoreToRead();
        }

        @Override
        public ClickHouseRecord next() {
            return processor.nextRecord();
        }
    }

    static final class ValuesIterator implements Iterator<ClickHouseValue> {
        private final ClickHouseDataProcessor processor;

        ValuesIterator(ClickHouseDataProcessor processor) {
            this.processor = processor;
        }

        @Override
        public boolean hasNext() {
            return processor.hasMoreToRead();
        }

        @Override
        public ClickHouseValue next() {
            return processor.nextValue();
        }
    }

    public static final List<ClickHouseColumn> DEFAULT_COLUMNS = Collections
            .singletonList(ClickHouseColumn.of("results", "Nullable(String)"));

    protected static final String ERROR_FAILED_TO_READ = "Failed to read column #%d of %d: %s";
    protected static final String ERROR_FAILED_TO_WRITE = "Failed to write column #%d of %d: %s";
    protected static final String ERROR_REACHED_END_OF_STREAM = "Reached end of the stream when reading column #%d of %d: %s";
    protected static final String ERROR_UNKNOWN_DATA_TYPE = "Unsupported data type: ";

    protected final ClickHouseConfig config;
    protected final ClickHouseInputStream input;
    protected final ClickHouseOutputStream output;
    protected final ClickHouseColumn[] columns;
    protected final ClickHouseRecord currentRecord;
    protected final ClickHouseValue[] templates;
    protected final Map<String, Object> settings;

    protected final Iterator<ClickHouseRecord> records;
    protected final Iterator<ClickHouseValue> values;

    protected final ClickHouseDeserializer[] deserializers;
    protected final ClickHouseSerializer[] serializers;

    /**
     * Column index shared by {@link #read(ClickHouseValue)}, {@link #records()},
     * and {@link #values()}.
     */
    protected int readPosition;
    /**
     * Column index shared by {@link #write(ClickHouseValue)}.
     */
    protected int writePosition;

    /**
     * Checks whether there's more to read from input stream.
     *
     * @return true if there's more; false otherwise
     * @throws UncheckedIOException when failed to read data from input stream
     */
    protected boolean hasMoreToRead() throws UncheckedIOException {
        try {
            if (input.available() <= 0) {
                input.close();
                return false;
            }
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * This method calls {@link #readAndFill(ClickHouseRecord)} and resets
     * {@code readPosition} to zero(first column).
     *
     * @return non-null record
     * @throws NoSuchElementException when no more record to read
     * @throws UncheckedIOException   when failed to read data from input stream
     */
    private ClickHouseRecord nextRecord() throws NoSuchElementException, UncheckedIOException {
        final ClickHouseRecord r = config.isReuseValueWrapper() ? currentRecord : currentRecord.copy();
        try {
            readAndFill(r);
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
        return r;
    }

    /**
     * This method calls {@link #readAndFill(ClickHouseValue, ClickHouseColumn)} and
     * updates {@code readPosition} to point to next column.
     *
     * @return non-null value
     * @throws NoSuchElementException when no more value to read
     * @throws UncheckedIOException   when failed to read data from input stream
     */
    private ClickHouseValue nextValue() throws NoSuchElementException, UncheckedIOException {
        ClickHouseColumn column = columns[readPosition];
        ClickHouseValue value = templates[readPosition];
        if (value == null || config.isReuseValueWrapper()) {
            value = column.newValue(config);
        }
        try {
            readAndFill(value);
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

    /**
     * Builds list of steps to deserialize value for the given column.
     *
     * @param column non-null column
     * @return non-null list of steps for deserialization
     */
    protected ClickHouseDeserializer[] buildDeserializeSteps(ClickHouseColumn column) {
        return new ClickHouseDeserializer[0];
    }

    /**
     * Builds list of steps to serialize value for the given column.
     *
     * @param column non-null column
     * @return non-null list of steps for serialization
     */
    protected ClickHouseSerializer[] buildSerializeSteps(ClickHouseColumn column) {
        return new ClickHouseSerializer[0];
    }

    /**
     * Factory method to create a record.
     *
     * @return new record
     */
    protected abstract ClickHouseRecord createRecord();

    /**
     * Initializes iterator of {@link ClickHouseRecord} for reading values record by
     * record. Usually this should be only called once during instantiation.
     * 
     * @return non-null iterator of {@link ClickHouseRecord}
     */
    protected Iterator<ClickHouseRecord> initRecords() {
        if (readPosition != 0) {
            throw new IllegalStateException("initRecords() is supposed to be called once during instantiation");
        }
        return new RecordsIterator(this);
    }

    /**
     * Initializes iterator of {@link ClickHouseValue} for reading values one by
     * one. Usually this should be only called once during instantiation.
     * 
     * @return non-null iterator of {@link ClickHouseValue}
     */
    protected Iterator<ClickHouseValue> initValues() {
        if (readPosition != 0) {
            throw new IllegalStateException("initValues() is supposed to be called once during instantiation");
        }
        return new ValuesIterator(this);
    }

    /**
     * Reads columns(starting from {@code readPosition}) from input stream and fill
     * deserialized data into the given record. This method is only used when
     * iterating through {@link #records()}.
     *
     * @param r non-null record to fill
     * @throws IOException when failed to read columns from input stream
     */
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        for (int i = readPosition, len = columns.length; i < len; i++) {
            readAndFill(r.getValue(i));
            readPosition = i;
        }
        readPosition = 0;
    }

    /**
     * Reads next column(at {@code readPosition} from input stream and fill
     * deserialized data into the given value object. This method is mainly used
     * when iterating through {@link #values()}. In default implementation, it's
     * also used in {@link #readAndFill(ClickHouseRecord)} for simplicity.
     *
     * @param value non-null value object to fill
     * @throws IOException when failed to read column from input stream
     */
    protected void readAndFill(ClickHouseValue value) throws IOException {
        int pos = readPosition;
        ClickHouseValue v = deserializers[pos].deserialize(value, input);
        if (v != value) {
            templates[pos] = v;
        }
        if (++pos >= columns.length) {
            readPosition = 0;
        } else {
            readPosition = pos;
        }
    }

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
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Serializable> settings)
            throws IOException {
        this.config = ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME);
        if (input == null && output == null) {
            throw new IllegalArgumentException("One of input and output stream must not be null");
        }

        this.input = input;
        this.output = output;
        if (settings == null || settings.isEmpty()) {
            this.settings = Collections.emptyMap();
        } else {
            this.settings = Collections.unmodifiableMap(new HashMap<>(settings));
        }

        if (columns == null && input != null) {
            columns = readColumns();
        }

        int colCount = 0;
        if (columns == null || columns.isEmpty()) {
            this.columns = ClickHouseColumn.EMPTY_ARRAY;
            this.templates = ClickHouseValues.EMPTY_VALUES;
        } else {
            colCount = columns.size();
            int idx = 0;
            this.columns = new ClickHouseColumn[colCount];
            this.templates = new ClickHouseValue[colCount];
            for (ClickHouseColumn column : columns) {
                column.setColumnIndex(idx, colCount);
                this.columns[idx] = column;
                if (config.isReuseValueWrapper()) {
                    this.templates[idx] = column.newValue(config);
                }
                idx++;
            }
        }

        if (input == null) {
            this.currentRecord = ClickHouseRecord.EMPTY;
            this.records = Collections.emptyIterator();
            this.values = Collections.emptyIterator();

            this.deserializers = new ClickHouseDeserializer[0];
            this.serializers = new ClickHouseSerializer[colCount];
            for (int i = 0; i < colCount; i++) {
                this.serializers[i] = getSerializer(config, this.columns[i]);
            }
        } else {
            this.currentRecord = createRecord();
            this.records = ClickHouseChecker.nonNull(initRecords(), "Records");
            this.values = ClickHouseChecker.nonNull(initValues(), "Values");

            this.deserializers = new ClickHouseDeserializer[colCount];
            this.serializers = new ClickHouseSerializer[0];
            for (int i = 0; i < colCount; i++) {
                this.deserializers[i] = getDeserializer(config, this.columns[i]);
            }
        }
        // this.writer = this.columns.length == 0 || output == null ? null :
        // initWriter();

        this.readPosition = 0;
        this.writePosition = 0;
    }

    public abstract ClickHouseDeserializer getDeserializer(ClickHouseConfig config, ClickHouseColumn column);

    public final ClickHouseDeserializer[] getDeserializers(ClickHouseConfig config, List<ClickHouseColumn> columns) {
        List<ClickHouseDeserializer> list = new ArrayList<>(columns.size());
        for (ClickHouseColumn column : columns) {
            list.add(getDeserializer(config, column));
        }
        return list.toArray(new ClickHouseDeserializer[0]);
    }

    public abstract ClickHouseSerializer getSerializer(ClickHouseConfig config, ClickHouseColumn column);

    public final ClickHouseSerializer[] getSerializers(ClickHouseConfig config, List<ClickHouseColumn> columns) {
        List<ClickHouseSerializer> list = new ArrayList<>(columns.size());
        for (ClickHouseColumn column : columns) {
            list.add(getSerializer(config, column));
        }
        return list.toArray(new ClickHouseSerializer[0]);
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
     * the collection; and 2) it's not supposed to be called for more than once
     * because the input stream will be closed at the end of reading.
     *
     * @return non-null iterable records
     * @throws UncheckedIOException when failed to access the input stream
     */
    public final Iterable<ClickHouseRecord> records() {
        return () -> records;
    }

    /**
     * Returns an iterable collection of values which can be walked through in a
     * foreach-loop. In general, this is slower than {@link #records()}, because the
     * latter reads data in bulk. However, it's particular useful when you're
     * reading large values with limited memory - e.g. a binary field with a few GB
     * bytes. Similarly, the input stream will be closed at the end of reading.
     *
     * @return non-null iterable values
     * @throws UncheckedIOException when failed to access the input stream
     */
    public final Iterable<ClickHouseValue> values() {
        if (columns.length == 0) {
            return Collections.emptyList();
        }

        return () -> values;
    }

    /**
     * Reads deserialized value of next column(at {@code readPosition}) directly
     * from input stream. Unlike {@link #records()}, which reads multiple values at
     * a time, this method will only read one for each call.
     *
     * @param value value to update, could be null
     * @return updated {@code value} or a new {@link ClickHouseValue} when it is
     *         null
     * @throws IOException when failed to read data from input stream
     */
    public ClickHouseValue read(ClickHouseValue value) throws IOException {
        if (input == null) {
            throw new IllegalStateException("No input stream available to read");
        }
        int len = columns.length;
        int pos = readPosition;
        if (len == 0 || pos >= len) {
            throw new IllegalStateException(
                    ClickHouseUtils.format("No column to read(total=%d, readPosition=%d)", len, pos));
        }
        ClickHouseColumn column = columns[pos];
        if (value == null) {
            value = config.isReuseValueWrapper() ? templates[pos] : column.newValue(config);
        }

        readAndFill(value);
        return value;
    }

    /**
     * Writes serialized value of next column(at {@code readPosition}) to output
     * stream.
     *
     * @param value non-null value to be serialized
     * @throws IOException when failed to write data to output stream
     */
    public void write(ClickHouseValue value) throws IOException {
        if (output == null) {
            throw new IllegalStateException("No output stream available to write");
        }
        int len = columns.length;
        int pos = writePosition;
        if (len == 0 || pos >= len) {
            throw new IllegalStateException(
                    ClickHouseUtils.format("No column to write(total=%d, writePosition=%d)", len, pos));
        }
        ClickHouseColumn column = columns[pos];
        if (value == null) {
            value = config.isReuseValueWrapper() ? templates[pos] : column.newValue(config);
        }
        serializers[pos++].serialize(value, output);
        writePosition = pos >= len ? 0 : pos;
    }
}
