# ClickHouse Server

ClickHouse has dedicated data types for [Date](https://clickhouse.tech/docs/en/data_types/date) and [DateTime](https://clickhouse.tech/docs/en/data_types/datetime). A DateTime value is usually to be interpreted in the server time zone, but a DateTime value may also be formatted for a different time zone. Note that the explicit time zone per column only affects inserting and displaying values, _not_ the predicates (see one example [here](https://github.com/ClickHouse/ClickHouse/issues/5206)).


# Setting Values

When setting values via the PreparedStatement setter methods, the JDBC driver does not have any knowledge about the target columns. Its job is to serialize any date time values into a textual representation. One of the aims of this driver is to use a serialization format that makes it easy to use ClickHouse Date or DateTime fields from a Java application.

In some cases the driver cannot perform the serialization without referring to a relevant time zone. This is how it works:

If the client supplies a valid  `Calendar` object as optional argument, the driver will use the time zone contained therein (`tz_calendar`).

Two regular time zones are initialized like this:

* `tz_datetime`: value from `com.clickhouse.client.config.ClickHouseClientOption.USE_TIME_ZONE`. If null, either ClickHouse server time zone (`com.clickhouse.client.config.ClickHouseClientOption.USE_SERVER_TIME_ZONE` is `true`) or JVM time zone (else)

* `tz_date`: same as `tz_datetime` if `com.clickhouse.client.config.ClickHouseClientOption.USE_SERVER_TIME_ZONE_FOR_DATES` is `true`, JVM time zone else

The JDBC driver supports all explicit methods, e.g. setDate, setTimestamp etc. with their optional Calendar argument. Providing hints via target SQL type does not have any effect.

The following table illustrates the serialization format for some popular date time data types, which we consider the most convenient. Clients are of course free to take care of serialization themselves by supplying a String or an Integer parameter, optionally using one of the server's utility methods (e.g. [parseDateTimeBestEffort](https://clickhouse.tech/docs/en/query_language/functions/type_conversion_functions/#type_conversion_functions-parsedatetimebesteffort)).

 Method | Format | Relevant time zone |
 ------ | ------ | -------------------
[setDate(int, Date)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setDate-int-java.sql.Date-) | yyyy-MM-dd  | tz_date
[setDate(int, Date, Calendar)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setDate-int-java.sql.Date-java.util.Calendar-) | yyyy-MM-dd | tz_calendar
[setObject(int, Date)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | yyyy-MM-dd | tz_date
[setTime(int, Time)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setTime-int-java.sql.Time-) | HH:mm:ss | tz_datetime
[setTime(int, Time, Calendar)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setTime-int-java.sql.Time-java.util.Calendar-) | HH:mm:ss | tz_calendar 
[setObject(int, Time)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | HH:mm:ss | tz_datetime
[setTimestamp(int, Timestamp)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setTimestamp-int-java.sql.Timestamp-) | yyyy-MM-dd HH:mm:ss | tz_datetime
[setTimestamp(int, Timestamp, Calendar)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setTimestamp-int-java.sql.Timestamp-java.util.Calendar-) | yyyy-MM-dd HH:mm:ss | tz_calendar
[setObject(int, Timestamp)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | yyyy-MM-dd HH:mm:ss | tz_datetime
[setObject(int, LocalTime)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | HH:mm:ss | _none_
[setObject(int, OffsetTime)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | HH:mm:ssZZZZ | _none_
[setObject(int, LocalDate)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | yyyy-MM-dd | _none_
[setObject(int, LocalDateTime)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | yyyy-MM-dd HH:mm:ss | _none_
[setObject(int, OffsetDateTime)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | yyyy-MM-dd HH:mm:ss | tz_datetime
[setObject(int, ZonedDateTime)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | yyyy-MM-dd HH:mm:ss | tz_datetime
[setObject(int, Instant)](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html#setObject-int-java.lang.Object-) | yyyy-MM-dd HH:mm:ss | tz_datetime

# Retrieving Values

When retrieving values via the ResultSet's getter methods, the JDBC driver will try to accomodate for some obvious options. If the underlying data field is of type Date or DateTime, the driver knows the implied time zone. This helps during the interpretation of the values retrieved from the server. Users may configure the driver to use a different time zone when reporting results back to the client (via `tz_date` or`tz_datetime`,  see above).

The methods which take a [Calendar]((https://docs.oracle.com/javase/8/docs/api/java.base/java/util/Calendar.html) argument behave the same as the corresponding methods without such an argument. The API documentation says something like

> This method uses the given calendar to construct an appropriate millisecond value for the _x_ if the underlying database does not store timezone information.

For Date and DateTime fields, the JDBC driver has enough time zone related information available, so these methods would only be relevant for String or other typed fields. There might be valid use cases, but for now we think that adding such an option would make things even more complicated.

Requested Type | Number | Date | DateTime | Other 
---------------| -------|------|----------|--------  
[Date](https://docs.oracle.com/javase/8/docs/api/java/sql/Date.html) | Seconds or milliseconds past epoch truncated to day in relevant time zone | Date in relevant time zone, midnight |  Date time in relevant time zone, rewind to midnight | Try number, date time (with or without offset) truncated to day, date
[Time](https://docs.oracle.com/javase/8/docs/api/java/sql/Time.html) | Local time at 1970-01-01 (e.g. “1337” is “13:37:00” at TZ) | Midnight on 1970-01-01 in relevant time zone | Local time in relevant time zone | Local time in relevant time zone via ISO format or via number, at 1970-01-01
[Timestamp](https://docs.oracle.com/javase/8/docs/api/java/sql/Timestamp.html) | Seconds or milliseconds past epoch | Local date at midnight in relevant time zone | Local date and time in relevant time zone | Number, date time with or without offset
[LocalTime](https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html) | Local time (e.g. "430" is "04:30:00") | Midnight | Local time | ISO format with or without offset, number
[OffsetTime](https://docs.oracle.com/javase/8/docs/api/java/time/OffsetTime.html) | Local time with current (!) offset of relevant time zone | Midnight with offset of relevant time zone on that date | Local time with offset of relevant time zone at value's date | ISO format, number
[LocalDate](https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html) | Seconds or milliseconds past epoch as local date in relevant time zone | Local date | Local date (no conversion) | Local date, local time, number
[LocalDateTime](https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html) | Seconds or milliseconds past epoch as local date time in relevant time zone | Local date midnight | Local date time | Local date time, number 
[OffsetDateTime](https://docs.oracle.com/javase/8/docs/api/java/time/OffsetDateTime.html) |  Seconds or milliseconds past epoch, offset from relevant time zone | Date midnight in relevant time zone | Date time in relevant time zone | Local date time in relevant time zone, ISO formats, number
[ZonedDdateTime](https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html) | Seconds or milliseconds past epoch, offset from relevant time zone | Date midnight in relevant time zone | Date time in relevant time zone | Local date, local date time in relevant time zone, ISO formats, number

# Summary

Life as a developer would be boring without time zones: [xkcd Super Villain Plan](https://xkcd.com/1883) Have fun!

If you think the ClickHouse JDBC driver behaves wrong, please file an issue. Make sure to include some time zone information of your ClickHouse server, the JVM, and the relevant driver settings.