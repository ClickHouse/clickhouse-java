package com.clickhouse.data;

import java.util.TimeZone;

import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.config.ClickHouseRenameMethod;

public class ClickHouseTestDataConfig implements ClickHouseDataConfig {

    @Override
    public ClickHouseFormat getFormat() {
        return ClickHouseFormat.TabSeparatedWithNamesAndTypes;
    }

    @Override
    public int getBufferQueueVariation() {
        return 0;
    }

    @Override
    public int getBufferSize() {
        return DEFAULT_BUFFER_SIZE;
    }

    @Override
    public int getMaxBufferSize() {
        return DEFAULT_MAX_BUFFER_SIZE;
    }

    @Override
    public int getReadBufferSize() {
        return DEFAULT_READ_BUFFER_SIZE;
    }

    @Override
    public int getWriteBufferSize() {
        return DEFAULT_WRITE_BUFFER_SIZE;
    }

    @Override
    public int getReadTimeout() {
        return 30000;
    }

    @Override
    public int getWriteTimeout() {
        return 30000;
    }

    @Override
    public ClickHouseBufferingMode getReadBufferingMode() {
        return ClickHouseBufferingMode.RESOURCE_EFFICIENT;
    }

    @Override
    public ClickHouseBufferingMode getWriteBufferingMode() {
        return ClickHouseBufferingMode.RESOURCE_EFFICIENT;
    }

    @Override
    public ClickHouseRenameMethod getColumnRenameMethod() {
        return ClickHouseRenameMethod.NONE;
    }

    @Override
    public int getMaxQueuedBuffers() {
        return 0;
    }

    @Override
    public TimeZone getTimeZoneForDate() {
        return TimeZone.getDefault();
    }

    @Override
    public TimeZone getUseTimeZone() {
        return TimeZone.getDefault();
    }

    @Override
    public boolean isReuseValueWrapper() {
        return true;
    }

    @Override
    public boolean isUseBinaryString() {
        return false;
    }

    @Override
    public boolean isUseBlockingQueue() {
        return false;
    }

    @Override
    public boolean isUseCompilation() {
        return false;
    }

    @Override
    public boolean isUseObjectsInArray() {
        return false;
    }

    @Override
    public boolean isWidenUnsignedTypes() {
        return false;
    }
}
