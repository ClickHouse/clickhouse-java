package com.clickhouse.data;

import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.config.ClickHouseRenameMethod;

import java.io.Serializable;
import java.math.RoundingMode;
import java.util.TimeZone;

@Deprecated
public interface ClickHouseDataConfig extends Serializable {
    static class Wrapped implements ClickHouseDataConfig {
        private static final long serialVersionUID = -8358244156373920188L;

        private final ClickHouseDataConfig config;

        public Wrapped(ClickHouseDataConfig config) {
            this.config = ClickHouseChecker.nonNull(config, TYPE_NAME);
        }

        @Override
        public boolean isAsync() {
            return config.isAsync();
        }

        @Override
        public ClickHouseFormat getFormat() {
            return config.getFormat();
        }

        @Override
        public int getBufferQueueVariation() {
            return config.getBufferQueueVariation();
        }

        @Override
        public int getBufferSize() {
            return config.getBufferSize();
        }

        @Override
        public int getMaxBufferSize() {
            return config.getMaxBufferSize();
        }

        @Override
        public int getReadBufferSize() {
            return config.getReadBufferSize();
        }

        @Override
        public int getWriteBufferSize() {
            return config.getWriteBufferSize();
        }

        @Override
        public int getReadTimeout() {
            return config.getReadTimeout();
        }

        @Override
        public int getWriteTimeout() {
            return config.getWriteTimeout();
        }

        @Override
        public ClickHouseBufferingMode getReadBufferingMode() {
            return config.getReadBufferingMode();
        }

        @Override
        public ClickHouseBufferingMode getWriteBufferingMode() {
            return config.getWriteBufferingMode();
        }

        @Override
        public ClickHouseRenameMethod getColumnRenameMethod() {
            return config.getColumnRenameMethod();
        }

        @Override
        public int getMaxMapperCache() {
            return config.getMaxMapperCache();
        }

        @Override
        public int getMaxQueuedBuffers() {
            return config.getMaxQueuedBuffers();
        }

        @Override
        public TimeZone getTimeZoneForDate() {
            return config.getTimeZoneForDate();
        }

        @Override
        public TimeZone getUseTimeZone() {
            return config.getUseTimeZone();
        }

        @Override
        public boolean isReuseValueWrapper() {
            return config.isReuseValueWrapper();
        }

        @Override
        public boolean isUseBinaryString() {
            return config.isUseBinaryString();
        }

        @Override
        public boolean isUseBlockingQueue() {
            return config.isUseBlockingQueue();
        }

        @Override
        public boolean isUseCompilation() {
            return config.isUseCompilation();
        }

        @Override
        public boolean isUseObjectsInArray() {
            return config.isUseObjectsInArray();
        }

        @Override
        public boolean isWidenUnsignedTypes() {
            return config.isWidenUnsignedTypes();
        }
    }

    static final String TYPE_NAME = "DataConfig";

    static final boolean DEFAULT_ASYNC = true;

    static final ClickHouseBufferingMode DEFAULT_BUFFERING_MODE = ClickHouseBufferingMode.RESOURCE_EFFICIENT;

    static final int DEFAULT_BUFFER_SIZE = 8192;
    static final int DEFAULT_READ_BUFFER_SIZE = DEFAULT_BUFFER_SIZE;
    static final int DEFAULT_WRITE_BUFFER_SIZE = DEFAULT_BUFFER_SIZE;
    static final int DEFAULT_MAX_BUFFER_SIZE = 128 * DEFAULT_BUFFER_SIZE;
    static final int DEFAULT_MAX_MAPPER_CACHE = 100;
    static final int DEFAULT_MAX_QUEUED_BUFFERS = 512;
    static final int DEFAULT_BUFFER_QUEUE_VARIATION = 100;

    static final ClickHouseRenameMethod DEFAULT_COLUMN_RENAME_METHOD = ClickHouseRenameMethod.NONE;

    static final ClickHouseFormat DEFAULT_FORMAT = ClickHouseFormat.TabSeparated;

    static final boolean DEFAULT_REUSE_VALUE_WRAPPER = true;
    static final boolean DEFAULT_USE_BINARY_STRING = false;
    static final boolean DEFAULT_USE_BLOCKING_QUEUE = false;
    static final boolean DEFAULT_USE_COMPILATION = false;
    static final boolean DEFAULT_USE_OBJECT_IN_ARRAY = false;
    static final boolean DEFAULT_WIDEN_UNSIGNED_TYPE = false;

    static final int DEFAULT_COMPRESS_LEVEL = -1;
    static final int DEFAULT_READ_COMPRESS_LEVEL = DEFAULT_COMPRESS_LEVEL;
    static final int DEFAULT_WRITE_COMPRESS_LEVEL = DEFAULT_COMPRESS_LEVEL;

    static final int DEFAULT_TIMEOUT = 30 * 1000; // 30 seconds

    static final RoundingMode DEFAULT_ROUNDING_MODE = RoundingMode.DOWN;

    /**
     * Gets buffer size. Same as
     * {@code getBufferSize(bufferSize, DEFAULT_BUFFER_SIZE, DEFAULT_MAX_BUFFER_SIZE)}.
     *
     * @param bufferSize suggested buffer size, zero or negative number is treated
     *                   as {@link #DEFAULT_BUFFER_SIZE}
     * @return buffer size
     */
    static int getBufferSize(int bufferSize) {
        return getBufferSize(bufferSize, DEFAULT_BUFFER_SIZE, DEFAULT_MAX_BUFFER_SIZE);
    }

    /**
     * Gets buffer size.
     *
     * @param bufferSize  suggested buffer size, zero or negative number is treated
     *                    as {@code defaultSize}
     * @param defaultSize default buffer size, zero or negative number is treated as
     *                    {@link #DEFAULT_BUFFER_SIZE}
     * @param maxSize     maximum buffer size, zero or negative number is treated as
     *                    {@link #DEFAULT_MAX_BUFFER_SIZE}
     * @return buffer size
     */
    static int getBufferSize(int bufferSize, int defaultSize, int maxSize) {
        if (maxSize < 1 || maxSize > DEFAULT_MAX_BUFFER_SIZE) {
            maxSize = DEFAULT_MAX_BUFFER_SIZE;
        }
        if (defaultSize < 1) {
            defaultSize = DEFAULT_BUFFER_SIZE;
        } else if (defaultSize > maxSize) {
            defaultSize = maxSize;
        }

        if (bufferSize < 1) {
            return defaultSize;
        }

        return bufferSize > maxSize ? maxSize : bufferSize;
    }

    /**
     * Gets default read buffer size in byte. Same as
     * {@code getBufferSize(DEFAULT_READ_BUFFER_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_MAX_BUFFER_SIZE)}.
     *
     * @return default read buffer size in byte
     */
    static int getDefaultReadBufferSize() {
        return getBufferSize(DEFAULT_READ_BUFFER_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_MAX_BUFFER_SIZE);
    }

    /**
     * Gets data format.
     *
     * @return non-null data format
     */
    default ClickHouseFormat getFormat() {
        return DEFAULT_FORMAT;
    }

    /**
     * Gets number of times the buffer queue is filled up before
     * increasing capacity of buffer queue. Zero or negative value means the queue
     * length is fixed.
     *
     * @return variation
     */
    default int getBufferQueueVariation() {
        return DEFAULT_BUFFER_QUEUE_VARIATION;
    }

    /**
     * Gets buffer size in byte can be used for streaming.
     *
     * @return buffer size in byte
     */
    default int getBufferSize() {
        return DEFAULT_BUFFER_SIZE;
    }

    /**
     * Gets max buffer size in byte can be used for streaming.
     *
     * @return max buffer size in byte
     */
    default int getMaxBufferSize() {
        return DEFAULT_MAX_BUFFER_SIZE;
    }

    /**
     * Gets read buffer size in byte.
     *
     * @return read buffer size in byte
     */
    default int getReadBufferSize() {
        return DEFAULT_READ_BUFFER_SIZE;
    }

    /**
     * Gets write buffer size in byte.
     *
     * @return write buffer size in byte
     */
    default int getWriteBufferSize() {
        return DEFAULT_WRITE_BUFFER_SIZE;
    }

    /**
     * Gets maximum number of mappers can be cached.
     *
     * @return maximum number of mappers can be cached
     */
    default int getMaxMapperCache() {
        return DEFAULT_MAX_MAPPER_CACHE;
    }

    /**
     * Gets maximum number of buffers can be queued for processing.
     *
     * @return maximum number of buffers can be queued
     */
    default int getMaxQueuedBuffers() {
        return DEFAULT_MAX_QUEUED_BUFFERS;
    }

    /**
     * Gets buffering mode for reading.
     *
     * @return non-null buffering mode for reading
     */
    default ClickHouseBufferingMode getReadBufferingMode() {
        return DEFAULT_BUFFERING_MODE;
    }

    /**
     * Gets buffering mode for writing.
     *
     * @return non-null buffering mode for writing
     */
    default ClickHouseBufferingMode getWriteBufferingMode() {
        return DEFAULT_BUFFERING_MODE;
    }

    /**
     * Gets column rename method.
     *
     * @return non-null column rename method
     */
    default ClickHouseRenameMethod getColumnRenameMethod() {
        return DEFAULT_COLUMN_RENAME_METHOD;
    }

    /**
     * Checks whether async call is used.
     *
     * @return true if async call is used; false otherwise
     */
    default boolean isAsync() {
        return DEFAULT_ASYNC;
    }

    /**
     * Checks whether value wrapper {@link ClickHouseValue} should be reused.
     *
     * @return true if value wrapper will be reused; false otherwise
     */
    default boolean isReuseValueWrapper() {
        return DEFAULT_REUSE_VALUE_WRAPPER;
    }

    /**
     * Checks whether binary string is supported.
     *
     * @return true if binary string is supported; false otherwise
     */
    default boolean isUseBinaryString() {
        return DEFAULT_USE_BINARY_STRING;
    }

    /**
     * Checks whether blocking queue(mainly for piped stream) is used or not.
     *
     * @return true if blocking queue is used; false indicates that non-blocking
     *         queue is used(faster but consumed more CPU)
     */
    default boolean isUseBlockingQueue() {
        return DEFAULT_USE_BLOCKING_QUEUE;
    }

    /**
     * Checks whether compilation is used in object mapping and serialization.
     *
     * @return true if compilation is used; false otherwise
     */
    default boolean isUseCompilation() {
        return DEFAULT_USE_COMPILATION;
    }

    /**
     * Checks whether object(instead of primitive) is used in array.
     *
     * @return true if object is used in array; false indicates that primitive type
     *         is used(no auto-boxing and less memory footprint)
     */
    default boolean isUseObjectsInArray() {
        return DEFAULT_USE_OBJECT_IN_ARRAY;
    }

    /**
     * Checks whether widening is enabled for unsigned types, for instance: use
     * {@code long} (instead of {@code int}) in Java to represent {@code UInt32} in
     * ClickHouse.
     * 
     * @return true if widening is enabled; false indicates that same type is shared
     *         by signed and unsigned types(e.g. {@code int} for both {@code Int32}
     *         and {@code UInt32})
     */
    default boolean isWidenUnsignedTypes() {
        return DEFAULT_WIDEN_UNSIGNED_TYPE;
    }

    /**
     * Gets read timeout in milliseconds.
     *
     * @return read time out in milliseconds
     */
    default int getReadTimeout() {
        return DEFAULT_TIMEOUT;
    }

    /**
     * Gets write timeout in milliseconds.
     *
     * @return write time out in milliseconds
     */
    default int getWriteTimeout() {
        return DEFAULT_TIMEOUT;
    }

    /**
     * Gets time zone for date values.
     *
     * @return time zone, could be null
     */
    TimeZone getTimeZoneForDate();

    /**
     * Gets preferred time zone.
     *
     * @return non-null preferred time zone
     */
    TimeZone getUseTimeZone();
}
