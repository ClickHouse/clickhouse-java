package com.clickhouse.data.mapper;

import java.util.List;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseRecordMapper;

public interface WrappedMapper {
    ClickHouseRecordMapper get(ClickHouseDataConfig config, List<ClickHouseColumn> columns);
}