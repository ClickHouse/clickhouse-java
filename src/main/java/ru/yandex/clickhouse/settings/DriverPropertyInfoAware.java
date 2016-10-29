package ru.yandex.clickhouse.settings;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public interface DriverPropertyInfoAware {
    DriverPropertyInfo toDriverPropertyInfo(Properties properties);
}
