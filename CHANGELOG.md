## 0.8.3

### Improvements
- [client-v2] Support for native LZ4 compression (https://github.com/ClickHouse/clickhouse-java/issues/2274)

### Bug Fixes
- [jdbc-v2] Fixed several issues with reading database metadata in JDBC driver. (https://github.com/ClickHouse/clickhouse-java/issues/2282)
- [jdbc-v2] Fixed settings client name in JDBC. (https://github.com/ClickHouse/clickhouse-java/issues/2233)
- [client-v2] Fixed reading data from columns defined as `Nullable(FixedString(N))`. (https://github.com/ClickHouse/clickhouse-java/issues/2218)
- [jdbc-v2] Fixed SQL parser failure to parse SQL statement with comments (https://github.com/ClickHouse/clickhouse-java/issues/2217)
- [client-v2] Fixed issue with excessive logging (https://github.com/ClickHouse/clickhouse-java/issues/2201)
- [jdbc-v2] Fixed handling IP addresses (https://github.com/ClickHouse/clickhouse-java/issues/2140)
- [jdbc] - Fixed missing LZ4 dependency in shaded package (https://github.com/ClickHouse/clickhouse-java/issues/2275)

## 0.8.2

### Bug Fixes
- [jdbc-v2] - Significantly improved performance of JDBC inserts. (https://github.com/ClickHouse/clickhouse-java/pull/2165)
- [client-v1] - Removed unnecessary object creation and added executor pool configuration (https://github.com/ClickHouse/clickhouse-java/pull/2164)

### Miscellaneous
- [client-v1] - Deprecated the old client, though it is still available. We've not yet set a date for its removal, this more serves as a head's up.

## 0.8.1 

### New Features
- [client-v2] - Added support for Variant Data Type for RowBinary format. Can be read into a POJO or using a reader.
Writing is supported only for POJO. (https://github.com/ClickHouse/clickhouse-java/pull/2130)    
- [client-v2] - Added support for Dynamic Data Type for RowBinary format. Can be read into a POJO or using a reader.
  Writing is supported only for POJO. (https://github.com/ClickHouse/clickhouse-java/pull/2130)
- [client-v2] - Added support for JSON Data Type for RowBinary format. Can be read into a POJO or using a reader.
  Writing is supported only for POJO. (https://github.com/ClickHouse/clickhouse-java/pull/2130)
- [client-v2] - Added support for `ZonedDateTime` in POJO serde. (https://github.com/ClickHouse/clickhouse-java/issues/2117)
- [client-v2] - Added suport for micrometer metrics for Apache HTTP client connection pool. It is now possible to get metrics registered 
in micrometer registry by calling `com.clickhouse.client.api.Client.Builder.registerClientMetrics(registry, groupName)`. (https://github.com/ClickHouse/clickhouse-java/issues/1901) 

### Bug Fixes 
- [client-v2] - Fixed `getTableSchema(tableName, databaseName)` now it doesn't ignore database name. (https://github.com/ClickHouse/clickhouse-java/issues/2139)
- [client-v2] - Fixed `returnGeneratedValues` to not throw an exception. Keep in mind that ClickHouse doesn't support 
returning anything in response for `INSERT` statements. This change was done just to make client usable with certain frameworks. (https://github.com/ClickHouse/clickhouse-java/issues/2121)
- [jdbc-v2] - Fixed changing database by `USE` statement. (https://github.com/ClickHouse/clickhouse-java/issues/2137)
- [jbdc-v2] - Fixed `ResultSetMetadata.getColumnClassName()` to return null instead of throwing exception. This change is done only 
to allow certain frameworks work with the client. (https://github.com/ClickHouse/clickhouse-java/issues/2112)
- [jdbc-v2] - Fixed using statements with `WITH` in JDBC v2. Previously was causing NPE. (https://github.com/ClickHouse/clickhouse-java/issues/2132) 
- [repo] - Fixed failing Test in Windows when attempting to locate files with reserved characters in the file path. (https://github.com/ClickHouse/clickhouse-java/issues/2114)

## 0.8.0

### Highlights
- We've updated `ClickHouseDriver` and `ClickHouseDataSource` to default to using the new (`jdbc-v2`) implementation of the JDBC driver. Setting `clickhouse.jdbc.v1=true` will revert this change.

#### JDBC Changes
- `jdbc-v2` - Removed support for Transaction Support. Early versions of the driver only simulated transaction support, which could have unexpected results.
- `jdbc-v2` - Removed support for Response Column Renaming. `ResultSet` was mutable - for efficiency sake they're now read-only
- `jdbc-v2` - Removed support for Multi-Statement SQL. Multi-statement support was only simulated, now it strictly follows 1:1
- `jdbc-v2` - Removed support for Named Parameters. Not part of the JDBC spec
- `jdbc-v2` - Removed support for Stream-based `PreparedStatement`. Early version of the driver allowed for non-jdbc usage of `PreparedStatement` - if you desire such options, we recommend looking at client-v2.

### New Features
- [client-v2, jdbc-v2] - Added support for Bearer token authentication like JWT. Now it is possible to specify encoded token while
creating a client and change while runtime using `com.clickhouse.client.api.Client.updateBearerToken`. (https://github.com/ClickHouse/clickhouse-java/issues/1834, https://github.com/ClickHouse/clickhouse-java/issues/1988) 
- [client-v2] - Exposed connection pool metrics through Micrometer. It allows to monitor internal connection pool for number of active and leased connections. (https://github.com/ClickHouse/clickhouse-java/issues/1901) 

### Bug Fixes
- [client-v2] - Fixed construction of `User-Agent` header. Prev. implementation uses `class.getPackage().getImplementationVersion()` what returns
incorrect title and version when library is shaded. New implementation uses build time information from resource files generated while build. (https://github.com/ClickHouse/clickhouse-java/issues/2007)
- [client-v2] - Fixed multiple issues with handling connectivity disruption. Socket timeout is unlimited by default. Added retry on timeout. Added more information to exception message.
Please read the issue for more details. (https://github.com/ClickHouse/clickhouse-java/issues/1994)
- [client-v2] - Client doesn't close provided executor anymore letting application close it instead. (https://github.com/ClickHouse/clickhouse-java/issues/1956)
- [client-v2] - Removed unnecessary initialization to make startup time shorter. (https://github.com/ClickHouse/clickhouse-java/issues/2032)  

## 0.7.2 

### New Components 
- `jdbc-v2` - pre-release version of upcoming `clickhouse-jdbc` replacement. Supports basic functionality, works only with `client-v2`.
More information will be available after its release

### Highlights
- [repo] Added shaded packages with requires a set of dependencies. Use `all` classified for `client-v2` full package. 
Use `shaded-all` for old jdbc driver.
- [client-v2] New Data Writer API was added. It allows to gain a control over writing data to low-level output stream. 
This API makes it possible to write compressed data directly to server. See `com.clickhouse.client.api.Client#insert(java.lang.String, com.clickhouse.client.api.DataStreamWriter, com.clickhouse.data.ClickHouseFormat, com.clickhouse.client.api.insert.InsertSettings)`
(https://github.com/ClickHouse/clickhouse-java/pull/2034)


### New Features
- [client-v2] Added ability to specify client name. It means `User-Agent` will be filled with proper information (https://github.com/ClickHouse/clickhouse-java/pull/1948)
- [client-v2] Implemented statement parameters for `queryAll` and `queryRecords` API methods. (https://github.com/ClickHouse/clickhouse-java/pull/1979) 
- [client-v2] Implemented string to number conversion. (https://github.com/ClickHouse/clickhouse-java/pull/2014) 
- [client-v1] Added basic auth support for proxies. Now you can specify username/password when connecting via a proxy that requires it with HttpURLConnection and Apache HttpClient.
- [client-v2] Enum columns can be read as string and number. Previously only as number. Now number matching string constant is returned when get as string. (https://github.com/ClickHouse/clickhouse-java/pull/2028)
- [client-v2] Client will load some server context (timezone, user) right after build. (https://github.com/ClickHouse/clickhouse-java/pull/2029)

### Bug Fixes
- [jdbc] Fixed default value for `result_overflow_mode` setting. (https://github.com/ClickHouse/clickhouse-java/issues/1932)
- [client-v2] Fixed reading float/double values. Previously was prevented by incorrect overflow check. (https://github.com/ClickHouse/clickhouse-java/issues/1954)
- [client-v2] Fixed issue with enabling client compression. Previously flag was ignore in some cases. (https://github.com/ClickHouse/clickhouse-java/issues/1958)
- [client-v2] Fixed issue with reading `Array(UInt64)` because of incorrect class used to create internal array. (https://github.com/ClickHouse/clickhouse-java/issues/1990)
- [client-v2] Fixed ClickHouseLZ4OutputStream issue of sending empty frame when no data left in uncompressed buffer. (https://github.com/ClickHouse/clickhouse-java/issues/1993)
- [client-v2] Fix handling `ConnectTimeoutException` in retry and wrapping logic. (https://github.com/ClickHouse/clickhouse-java/pull/2015)

## 0.7.1-patch1

### Bug Fixes
- [JDBC] Fixed `java.lang.NoClassDefFoundError: com/clickhouse/client/internal/apache/hc/core5/http2/HttpVersionPolicy` (https://github.com/ClickHouse/clickhouse-java/issues/1912) 
- [client-v2] Fixed multiple issues with error message handling. (https://github.com/ClickHouse/clickhouse-java/issues/1906)
- [client-v2] Fixed primitive types conversion. Now client correctly handles numbers to boolean and vice-versa. (https://github.com/ClickHouse/clickhouse-java/issues/1908)

## 0.7.1 

### New Features
- [client-v2] Implemented more friendly number conversion. Now it is possible to convert smaller type to bigger one. 
It is also possible to convert bigger into smaller if value fits into the range. (https://github.com/ClickHouse/clickhouse-java/issues/1852)
- [client-v2] Ported a feature that allows to remember DB roles for a client instance. See `com.clickhouse.client.api.Client#setDBRoles` 
for details. (https://github.com/ClickHouse/clickhouse-java/issues/1832)
- [client-v2] Ported a feature that allows adding comments to a query. 
See `com.clickhouse.client.api.insert.InsertSettings#logComment` and `com.clickhouse.client.api.query.QuerySettings#logComment` 
for details. (https://github.com/ClickHouse/clickhouse-java/issues/1836)
- [client-v2] Added support for SSL Authentication with client certificates. (https://github.com/ClickHouse/clickhouse-java/issues/1837)
- [client-v2] Implemented a way to define a custom matching between a column name and a field in a POJO in `Client#register` method. (https://github.com/ClickHouse/clickhouse-java/issues/1866)
- [client-v1, client-v2] Implemented HTTP Basic authentication and made it a default auth method for HTTP interface. It 
was done to address problem with passwords contianing special and UTF8 characters. New configuration option 
`com.clickhouse.client.http.config.ClickHouseHttpOption.USE_BASIC_AUTHENTICATION` for client v1 is added. For client v2 
use `com.clickhouse.client.api.Client.Builder#useHTTPBasicAuth` method. (https://github.com/ClickHouse/clickhouse-java/issues/1305)


### Dependency Updates
- [client] Bumped org.apache.avro:avro version to 1.11.4 (https://github.com/ClickHouse/clickhouse-java/pull/1855)

### Documentation
- [client] Added links to javadoc for all classes in the README.md (https://github.com/ClickHouse/clickhouse-java/pull/1878)

### Bug Fixes
- [client-v2] Fixed deserializing nullable columns of `Nested` type (https://github.com/ClickHouse/clickhouse-java/issues/1858)
- [client-v2] Fixed dependencies needed for compression to work out of the box (https://github.com/ClickHouse/clickhouse-java/issues/1805)
- [client-v2] Fixed dependency on SNAPSHOT component (https://github.com/ClickHouse/clickhouse-java/issues/1853)
- [client-v2] Fixed using `scale` from a column definition when deserializing DateTime64 values (https://github.com/ClickHouse/clickhouse-java/issues/1851)
- [client-v2] Fixed applying database from insert settings (https://github.com/ClickHouse/clickhouse-java/issues/1868)
- [client-v2] Fixed error handling from server (https://github.com/ClickHouse/clickhouse-java/issues/1874)
- [client-v2] Fixed SerDe for SimpleAggregateFunction columns (https://github.com/ClickHouse/clickhouse-java/pull/1876)
- [client] Fixed handling error from server in response with `200 OK` status. Happens when 
`send_progress_in_http_headers` is requested and query runs for a long time. (https://github.com/ClickHouse/clickhouse-java/issues/1821)
- [jdbc] Fixed incorrect error logging (https://github.com/ClickHouse/clickhouse-java/issues/1827)
- [client-v2] Fixed handling tuples in arrays (https://github.com/ClickHouse/clickhouse-java/issues/1882)
- [client-v2] Fixed passing `insert_duplication_token` through `InsertSettings`. (https://github.com/ClickHouse/clickhouse-java/issues/1877)

## 0.7.0

### Deprecations
- Following deprecated components are removed:
  - clickhouse-cli-client
  - clickhouse-grpc-client

### Important Changes
- [client-v2] New transport layer implementation is used by default. It is still possible to switch back 
using old implementation by setting `com.clickhouse.client.api.Client.Builder#useNewImplementation` to `false`. (https://github.com/ClickHouse/clickhouse-java/pull/1847)

### New Features
- [client-v2] Now there is an easy way to set custom HTTP headers globally for client and per operation. 
See `com.clickhouse.client.api.Client.Builder.httpHeader(java.lang.String, java.lang.String)` for details. (https://github.com/ClickHouse/clickhouse-java/issues/1782)
- [client-v2] Now there is a way to set any server settings globally for client and per operation.
See `com.clickhouse.client.api.Client.Builder.serverSetting(java.lang.String, java.lang.String)` for details. (https://github.com/ClickHouse/clickhouse-java/issues/1782)
- [client-v2] Added support for writing AggregateFunction values (bitmap serialization). !! Reading is not 
supported but will be added in the next release. (https://github.com/ClickHouse/clickhouse-java/pull/1814)
- [r2dbc] Defer connection creation. This allows pool to create a new instance on every subscription, 
instead of always returning the same one. (https://github.com/ClickHouse/clickhouse-java/pull/1810) 

### Performance Improvements
- [client-v2] Improved reading fixed length data like numbers. It is possible to configure readers to 
use pre-allocated buffers to avoid memory allocation for each data row/block. Significantly reduces GC pressure.
See `com.clickhouse.client.api.Client.Builder.allowBinaryReaderToReuseBuffers` for details. (https://github.com/ClickHouse/clickhouse-java/pull/1816)
- [client-v2] New API method introduced to read data directly to a POJO. Deserializers for POJO classes are compiled into 
bytecode (with help of https://asm.ow2.io/ library) and optimized for each schema. It is great performance boost 
because data is read without copying it into temporary structures. Code can be optimized by JVM while runtime as SerDe 
code is implemented without reflection using JVM bytecode. Using bytecode makes handling primitive types without values boxing. (https://github.com/ClickHouse/clickhouse-java/pull/1794, 
https://github.com/ClickHouse/clickhouse-java/pull/1826)
- [client-v2] Optimized reading columns - internally data is read into map of column-values. It is done 
to allow reading same column more than once. Previously map was cleared each row what caused a lot 
internal objects creation. Now values are overridden because schema doesn't change between rows. (https://github.com/ClickHouse/clickhouse-java/pull/1795)

### Documentation
- [client-v2] Added example for Kotlin (https://github.com/ClickHouse/clickhouse-java/pull/1793)
- [doc] Main documentation on official ClickHouse website is updated. Each client has its own page with detailed information now. 
Added documentation for the Client V2. See https://clickhouse.com/docs/en/integrations/java.

### Bug Fixes
- [client-v2] Fix for cases when missing operation metrics were causing NPE. (https://github.com/ClickHouse/clickhouse-java/pull/1846)
- [client-v2] Fix for handling empty result by BinaryFormat readers. (https://github.com/ClickHouse/clickhouse-java/pull/1845)
- [jdbc] Content of an artifact 'clickhouse-jdbc-{version}-all.jar' is fixed and contains all required classes from `clickhouse-client` 
and `clickhouse-data`. (https://github.com/ClickHouse/clickhouse-java/pull/1842)
- [client-v1, jdbc] Endpoints definition parsing fixed to grub properties correctly. Now even properties with key-value 
pairs are supported. (https://github.com/ClickHouse/clickhouse-java/pull/1841, https://github.com/ClickHouse/clickhouse-java/issues/1665)

## 0.6.5

### Deprecations
- Following components will be deprecated and removed in 0.7.0 release:
  - clickhouse-cli-client
  - clickhouse-grpc-client
- Projects cli-client and grpc-client are excluded from release and build.
- No more builds for non-lts Java versions - no more Java 9 release builds.

### Performance Improvements
- [client-v2] `queryAll()` optimized to use less memory (https://github.com/ClickHouse/clickhouse-java/pull/1779)
- [client-v2] `Client.Builder#setClientNetworkBufferSize` introduced to allow increasing a buffer that is used 
to transfer data from socket buffer to application memory. When set to >= of send/receive socket buffer size it
significantly reduces number of system calls and improves performance. (https://github.com/ClickHouse/clickhouse-java/pull/1784)

### New Features
- [client-v2] Client will retry on `NoHttpResponseException` when using Apache HTTP client. 
  It is useful when close/stale connection is leased from connection pool. No client will 
retry one more time instead of failing. `Client.Builder#retryOnFailures` and `Client.Builder#setMaxRetries` were 
introduced to configure client behavior. (https://github.com/ClickHouse/clickhouse-java/pull/1768)

### Bug Fixes
- [client-v2] Correct timezone used when reading DateTime values. Affects how date/datetime values
  are read when `session_timezone` is used (https://github.com/ClickHouse/clickhouse-java/issues/1780)
- [client-v2] Fix reading big integers. Previously was causing incorrect values
  (https://github.com/ClickHouse/clickhouse-java/issues/1786)
  (https://github.com/ClickHouse/clickhouse-java/issues/1776)
- [client-v2] Fix server compressions when using a client instance concurrently 
(https://github.com/ClickHouse/clickhouse-java/pull/1791) 
- [client-v2] Fix reading arrays as list. Also affected reading nested arrays (https://github.com/ClickHouse/clickhouse-java/pull/1800)
- [client-v1] Fix handling summary metadata for write operations. Previously was causing empty metadata 

## 0.6.4

### Deprecations
- Following components will be deprecated and archived in next release:
  - clickhouse-cli-client
  - clickhouse-grpc-client
- No more builds for non-lts Java versions - no more Java 9 release builds.
- Lowest supported Java version will be 11.
  - Java 11 support will be ended before the end of 2023. 
  - It is recommended to use Java 21.

### Important Changes
- [Client-V1] Fix for handling DateTime without timezone when `session_timezone` is set. Now server timezone 
is parsed from server response when present (https://github.com/ClickHouse/clickhouse-java/issues/1464)

### New Features 
- [Client-V1/Apache HTTP] More configuration parameters for connection management. Useful for tuning performance.
(https://github.com/ClickHouse/clickhouse-java/pull/1771)
    - com.clickhouse.client.config.ClickHouseClientOption#CONNECTION_TTL - to configure connection time-to-live
    - com.clickhouse.client.http.config.ClickHouseHttpOption#KEEP_ALIVE_TIMEOUT - to configure keep-alive timeout
    - com.clickhouse.client.http.config.ClickHouseHttpOption#CONNECTION_REUSE_STRATEGY - defines how connection pool behaves.
If `FIFO` is selected then connections are reused in the order they were created. It results in even distribution of connections.
If `LIFO` is selected then connections are reused as soon they are returned to the pool. 
Note: only for `APACHE_HTTP_CLIENT` connection provider. 
    - Additionally switched to using LAX connection pool for Apache Connection Manager to improve performance 
for concurrent requests.
- [Client-V2] Connection pool configuration https://github.com/ClickHouse/clickhouse-java/pull/1766
    - com.clickhouse.client.api.Client.Builder.setConnectionRequestTimeout - to configure connection request timeout.
Important when there are no connections available in the pool to fail fast. 
    - com.clickhouse.client.api.Client.Builder.setMaxConnections - configures how soft limit of connections per host.
Note: Total number of connections is unlimited because in most cases there is one host.
    - com.clickhouse.client.api.Client.Builder.setConnectionTTL - to limit connection live ignoring keep-alive from server.
    - com.clickhouse.client.api.Client.Builder.setConnectionReuseStrategy - to configure how connections are used.
Select FIFO to reuse connections evenly or LIFO (default) to reuse the most recently active connections.
- [Client-V2] All operations are now executed in calling thread to avoid extra threads creation. 
Async operations can be enabled by `com.clickhouse.client.api.Client.Builder.useAsyncRequests` (https://github.com/ClickHouse/clickhouse-java/pull/1767)
- [Client-V2] Content and HTTP native compression is supported now Currently only LZ4 is available. (https://github.com/ClickHouse/clickhouse-java/pull/1761) 
- [Client-V2] HTTPS support added. Required to communicate with ClickHouse Cloud Services.
Client certificates are supported, too. (https://github.com/ClickHouse/clickhouse-java/pull/1753)
- [Client-V2] Added support for HTTP proxy (https://github.com/ClickHouse/clickhouse-java/pull/1748)

### Documentation
- [Client-V2] Spring Demo Service as usage example (https://github.com/ClickHouse/clickhouse-java/pull/1765)
- [Client-V2] Examples for using text based formats (https://github.com/ClickHouse/clickhouse-java/pull/1752)


### Bug Fixes
- [Client-V2] Data is read fully from a stream. Important for Cloud instances (https://github.com/ClickHouse/clickhouse-java/pull/1759)
- [Client-V2] Timezone from a server response is now used to parse DateTime values (https://github.com/ClickHouse/clickhouse-java/pull/1763)
- [Client-V1] Timezone from a server response is now used to parse DateTime values (https://github.com/ClickHouse/clickhouse-java/issues/1464)

## 0.6.3

### Important Changes
- [Client-V1] Changed how `User-Agent` string is generated. Now `ClickHouse-JavaClient` portion is appended in all cases.
It is still possible to set custom product name that will be the first part in `User-Agent` value. 
(https://github.com/ClickHouse/clickhouse-java/issues/1698)

### New Features
- [Client-V1/Apache HTTP] Retry on NoHttpResponseException in Apache HTTP client.
Should be used with causes because it is not always possible to resend request body. 
Behaviour is controlled by `com.clickhouse.client.http.config.ClickHouseHttpOption#AHC_RETRY_ON_FAILURE`.
Works only for Apache HTTP client because based on its specific behavior(https://github.com/ClickHouse/clickhouse-java/pull/1721)
- [Client-V1/Apache HTTP] Connection validation before sending request.
Behaviour is controlled by `com.clickhouse.client.http.config.ClickHouseHttpOption#AHC_VALIDATE_AFTER_INACTIVITY`.
By default, connection is validated after being in the pool for 5 seconds. (https://github.com/ClickHouse/clickhouse-java/pull/1722)

### Bug Fixes
- [Client-V2] Fix parsing endpoint URL to detect HTTPs (https://github.com/ClickHouse/clickhouse-java/issues/1718)
- [Client-V2] Fix handling asynchronous operations. Less extra threads created now. (https://github.com/ClickHouse/clickhouse-java/issues/1691)
- [Client-V2] Fix way of how settings are validated to let unsupported options to be added (https://github.com/ClickHouse/clickhouse-java/issues/1691)
- [Client-V1] Fix getting `localhost` effective IP address (https://github.com/ClickHouse/clickhouse-java/issues/1729)
- [Client-V2] Make client instance closeable to free underlying resource (https://github.com/ClickHouse/clickhouse-java/pull/1733)

## 0.6.2

### New Features
- Describe non-executed SELECT queries in prepared statements to provide metadata (https://github.com/ClickHouse/clickhouse-java/issues/1430)
- Command execution in the client API (https://github.com/ClickHouse/clickhouse-java/pull/1693)
- Added `com.clickhouse.client.ClickHouseResponseSummary#getQueryId()` (https://github.com/ClickHouse/clickhouse-java/issues/1636)
- Added support for SSL for the Client V2
- Added proxy support for Client V2 (https://github.com/ClickHouse/clickhouse-java/pull/1694)
- Added more examples for Client V2 (https://github.com/ClickHouse/clickhouse-java/pull/1709)

## 0.6.1

### New Features
- Alpha version of the new client API. See example https://github.com/ClickHouse/clickhouse-java/tree/main/examples/client-v2. 

### Bug Fixes
- Fix proper handling of ClickHouseResult to release DB connection (https://github.com/ClickHouse/clickhouse-java/issues/1538
- Fix getting response input stream for binary formats.
    - https://github.com/ClickHouse/clickhouse-java/issues/1494
    - https://github.com/ClickHouse/clickhouse-java/issues/1567
    - https://github.com/ClickHouse/clickhouse-java/issues/1475
    - https://github.com/ClickHouse/clickhouse-java/issues/1376
- Exception context remove from a message. Server information not present anymore but available thru `com.clickhouse.client.ClickHouseException#getServer` (https://github.com/ClickHouse/clickhouse-java/issues/1677)

## 0.6.0-patch5

### Bug Fixes
- Roles (memorized by client) can be reset with 'SET ROLE NONE' query (https://github.com/ClickHouse/clickhouse-java/issues/1647)
- MaxBufferSize can be greater than internal MAX value now
- Updated example project to use the latest version of the client

## 0.6.0-patch4

### New Features
- Added possibility to set client ID in `Referer` HTTP Header (https://github.com/ClickHouse/clickhouse-java/issues/1572)
- [HTTP] Persistence of a role after it is set by `SET ROLE <role>`

### Bug Fixes
- Change RowBinaryWithDefaults settings. Output is changed from true to false
- Fix fail over for Apache HTTP client. Connect timeout error is not detected correctly
- Fix password logging in DEBUG (https://github.com/ClickHouse/clickhouse-java/issues/1571)
- Fix handling "NoHttpResponseException" in Apache HTTP client
- Fix param error in ByteUtils#equals in java9 

## 0.6.0-patch3

### Bug Fixes
- Not detecting correctly ClickHouse error code
- Fix JDBC read error - Multidimensional empty errors raise a java.lang.ArrayStoreException
  
## 0.6.0-patch2

### Dependency Updates
- org.apache.commons:commons-compress from 1.23.0 to 1.26.1
- org.postgresql:postgresql from 42.6.0 to 42.6.1
  
## 0.6.0-patch1

### Bug Fixes
- Fix buffering issue caused by decompress flag not to work when working with HTTP Client.

## 0.6.0

### WARNING -- ClickHouse CLI Client deprecation

`clickhouse-cli-client` package is deprecated from version 0.6.0 and it's going to be removed in `0.7.0`. We recommend using [clickhouse-client](https://clickhouse.com/docs/en/interfaces/cli) instead.

### WARNING -- ClickHouse GRPC Client deprecation

`clickhouse-grpc-client` package is deprecated from version 0.6.0 and it's going to be removed in `0.7.0`. We recommend using [HTTP](https://github.com/ClickHouse/clickhouse-java/blob/main/examples/client/src/main/java/com/clickhouse/examples/jdbc/Main.java) protocol instead.

### Breaking Changes

- Remove WEB_CONTEXT support - [#1512](https://github.com/ClickHouse/clickhouse-java/issues/1512)

### New Features

- Add support in RowBinaryWithDefaults [#1508](https://github.com/ClickHouse/clickhouse-java/pull/1508)

### Bug Fixes

- Fix faulty node detection in ClickHouseNodes - [#1595](https://github.com/ClickHouse/clickhouse-java/pull/1511)
- Fix while getting addBatch with an exception drop the all batch [#1373](https://github.com/ClickHouse/clickhouse-java/issues/1373)
- Fix buffering issue caused by decompress flag not to work [#1500](https://github.com/ClickHouse/clickhouse-java/pull/1500)

## 0.5.0

### Breaking Changes

- ClickHouseByteBuffer can no longer be extended
- rename ClickHouseByteUtils methods by removing LE suffix
- change default databaseTerm from schema to catalog
- remove deprecated API load, dump and connect
- remove use_no_proxy settings

### New Features

- Adding new proxy support [#1338](https://github.com/ClickHouse/clickhouse-java/issues/1338)
- Add support for customer socket factory [#1391](https://github.com/ClickHouse/clickhouse-java/issues/1391)
- use VarHandle in JDK 9+ to read/write numbers
- Establish secured connection with custom Trust Store file
- Change default HTTP Client to Apache HTTP client [#1421](https://github.com/ClickHouse/clickhouse-java/issues/1421)

### Bug Fixes

- Java client threw confusing error when query is invalid.
- JDBC Driver correctly processes `AggregateFunction(Nested(...))` columns
- Incorrect parameter position
- Fix testing framework to support secured clickhouse server

## 0.4.6, 2023-05-02

### New Features

- ClickHouseStatement.setMirroredOutput() for dumping ResultSet.
- ClickHouseResponse.records(Class<?>) for object mapping.
- Two new options(use_compilation & max_mapper_cache) reserved for future usage.

### Bug Fixes

- Too many socket fds generated by Apache HttpClient.
- NoClassDefFoundError with clickhouse-apache-http-client-jdbc. [#1319](https://github.com/ClickHouse/clickhouse-java/issues/1319)
- Nested array in tuple array is incorrectly deserialized. [#1324](https://github.com/ClickHouse/clickhouse-java/issues/1324)
- client certificate password exposure in exception. [#1331](https://github.com/ClickHouse/clickhouse-java/issues/1331)

## 0.4.5, 2023-04-25

### Breaking Changes

- refactor data processors and response classes to ensure input stream remain intact before first read:
  - move ClickHouseSimpleRecord to com.clickhouse.data
  - stop reading input stream when instantiating ClickHouseDataProcessor
  - remove createRecord() method in ClickHouseDataProcessor along with some duplicated code

### New Features

- disable SQL rewrite for DELETE statement in ClickHouse 23.3+

### Bug Fixes

- Slow when using Apache Http Client. [#1320](https://github.com/ClickHouse/clickhouse-java/issues/1320)
- ClickHouseResponse.getInputStream may return closed input stream.
- ConcurrentModificationException may occure during deserialization. [#1327](https://github.com/ClickHouse/clickhouse-java/pull/1327)
- ClickHouseSslContextProvider is not customizable. [#1329](https://github.com/ClickHouse/clickhouse-java/issues/1329)

## 0.4.4, 2023-04-17

### Bug Fixes

- flatten plugin 1.4.1 generated non-sense dependencies.

## 0.4.3, 2023-04-17

### New Features

- replace JavaCC21 with CongoCC

### Bug Fixes

- unable to convert empty string to default value when using text-based data format.
- r2dbc driver does not support most client options. [#1299](https://github.com/ClickHouse/clickhouse-java/issues/1299)
- incorrect content from Lz4InputStream when using text-based data format [#48446](https://github.com/ClickHouse/ClickHouse/issues/48446)

## 0.4.2, 2023-03-21

### Breaking Changes

- ClickHouseSqlStatement and \*ParserHandler in JDBC driver were refactored to support `compression` and `infile` in insert statement.

### New Features

- centralized configuration for JDBC driver using custom server setting `custom_jdbc_config`.
- support `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK` in JDBC driver. [#975](https://github.com/ClickHouse/clickhouse-java/issues/975)
- new options for JDBC driver
  - databaseTerm(catalog or schema, defaults to schema) [#1273](https://github.com/ClickHouse/clickhouse-java/issues/1273)
  - externalDatabase(true or false, defaults to true) [#1245](https://github.com/ClickHouse/clickhouse-java/issues/1245)
  - localFile(true of false, defaults to false) - whether to use local file for INFILE and OUTFILE or not

### Bug Fixes

- error while converting Nested values to Java maps.
- incorrect algorithm extracted from PEM. [#1274](https://github.com/ClickHouse/clickhouse-java/issues/1274)
- transaction failure introduced 0.4.0.
- respect node-specific credentials. [#1114](https://github.com/ClickHouse/clickhouse-java/issues/1114)
- USE statement does nothing. [#1160](https://github.com/ClickHouse/clickhouse-java/issues/1160)
- executeBatch does not support on cluster anymore. [#1261](https://github.com/ClickHouse/clickhouse-java/issues/1261)

## 0.4.1, 2023-02-19

### Breaking Changes

- changed option names - [#1203](https://github.com/ClickHouse/clickhouse-java/pull/1203)
  - compress_alogrithm -> compress_algorithm
  - decompress_alogrithm -> decompress_algorithm

### New Features

- added source in shaded jar for IDE friendly. [#1217](https://github.com/ClickHouse/clickhouse-java/pull/1217)
- iterable ClickHouseInputStream, which slightly improved performance of piping. [#1238](https://github.com/ClickHouse/clickhouse-java/pull/1238)
- ClickHouseOutputStream.transferBytes() is deprecated and it will be removed in 0.5.
- read() and write() methods in ClickHouseClient - connect() is deprecated and it will be removed in 0.5.
- make all dependencies of JDBC driver optional, along with configuration for building native binary. [#1247](https://github.com/ClickHouse/clickhouse-java/pull/1247)

### Bug Fixes

- incorrect nested array value. [#1221](https://github.com/ClickHouse/clickhouse-java/issues/1221)
- incorrect nested array values. [#1223](https://github.com/ClickHouse/clickhouse-java/issues/1223)
- potential endless loop when handling batch update error. [#1233](https://github.com/ClickHouse/clickhouse-java/issues/1233)
- exception when deserializing Array(FixedString(2)) from RowBinary. [#1235](https://github.com/ClickHouse/clickhouse-java/issues/1235)
- deserialization failure of Nested data type. [#1240](https://github.com/ClickHouse/clickhouse-java/issues/1240)
  broken serde for Nested, which also impacted JSON. [#1242](https://github.com/ClickHouse/clickhouse-java/issues/1242)
- fix 64bit bitmap serialization issue. [#641](https://github.com/ClickHouse/clickhouse-java/issues/641), [#874](https://github.com/ClickHouse/clickhouse-java/issues/874), [#1141](https://github.com/ClickHouse/clickhouse-java/issues/1141)
- gRPC client may complete execution before closing response stream
- throw StreamCorruptedException instead of generic IOException when deserialization failed (mostly due to server error)

## 0.4.0, 2023-01-19

### Breaking Changes

- refactored `JdbcTypeMapping` to make it extensible. [#1075](https://github.com/ClickHouse/clickhouse-java/pull/1075)
- removed legacy driver `ru.yandex.*`.[#1089](https://github.com/ClickHouse/clickhouse-java/pull/1089)
- removed most deprecated methods and class members
- refactored data processor(for serialization and deserialization) and added new classes for unsigned types. [#1124](https://github.com/ClickHouse/clickhouse-java/pull/1124)
- refactored ClickHouseRequest/ClickHouseInputStream/ClickHouseOutputStream to better support compression. [#1174](https://github.com/ClickHouse/clickhouse-java/pull/1174), [#1189](https://github.com/ClickHouse/clickhouse-java/pull/1189)
- extracted `clickhouse-data` from `clickhouse-client` along with new packages. [#1197](https://github.com/ClickHouse/clickhouse-java/pull/1197)
  ```java
  com.clickhouse.config  // generic configuration
  com.clickhouse.data    // data processing utilities
  com.clickhouse.logging // generic logging utility
  ```
- added jdk17 in pom and changed build instructions
  ```bash
  mvn -Dj8 clean install # for jdk8, it was 'mvn clean install'
  mvn clean install # for jdk17, it was 'mvn -Drelease clean install'
  ```

### New Features

- added R2DBC driver. [#914](https://github.com/ClickHouse/clickhouse-java/pull/914)
- enhanced ClickHouseClient for importing and exporting compressed file. [#1004](https://github.com/ClickHouse/clickhouse-java/pull/1004)
- added new option `custom_settings`. [#1059](https://github.com/ClickHouse/clickhouse-java/pull/1059)
- enhanced `ClickHouseRequestManager` to support query/session ID customization. [#1074](https://github.com/ClickHouse/clickhouse-java/pull/1074)
- added Apache HTTP Client 5 to support socket options. [#1146](https://github.com/ClickHouse/clickhouse-java/pull/1146)
- enhanced `clickhouse-grpc-client` to support request chunking and compression
  - `decompress_alogrithm`(request compression): `BROTLI`[-1,11], `BZ2`, `DEFLATE`[0,9], `GZIP`[-1,9], `LZ4`[0,18], `XZ`[0,9], `ZSTD`[0,22]
  - `compress_alogrithm`(response decompression): `DEFLATE`, `LZ4`, `ZSTD`
    Note: typo will be fixed in v0.4.1.
- enhanced `clickhouse-http-client` to support zstd compression for both request and response
  - `decompress_alogrithm`(request compression): `LZ4`[0,18], `ZSTD`[0,22]
  - `compress_alogrithm`(response decompression): `BROTLI`, `BZ2`, `DEFLATE`, `GZIP`, `LZ4`, `XZ`, `ZSTD`
    Note: typo will be fixed in v0.4.1.
- added stream-based prepared statement. [#1163](https://github.com/ClickHouse/clickhouse-java/pull/1163)
- browser-like client name (`select distinct http_user_agent from system.query_log`). [#1182](https://github.com/ClickHouse/clickhouse-java/pull/1182)
- enhanced `ClickHouseRequest` by treating input stream and `ClickHouseWriter` equally. [#1200](https://github.com/ClickHouse/clickhouse-java/pull/1200)

### Bug Fixes

- not able to cancel query when there's session_id. [#1035](https://github.com/ClickHouse/clickhouse-java/pull/1035)
- overflow error when handling BigInteger and BigDecimal in ClickHouseLongValue. [#1040](https://github.com/ClickHouse/clickhouse-java/pull/1040)
- not handling SimpleAggregateFunction properly. [#1054](https://github.com/ClickHouse/clickhouse-java/pull/1054)
- DELETE FROM was rewritten to ALTER TABLE DELETE even when lightweight deletion was enabled. [#1063](https://github.com/ClickHouse/clickhouse-java/pull/1063)
- forced headless format for inserting. [#1073](https://github.com/ClickHouse/clickhouse-java/pull/1073)
- missing the verb "be" in error messages. [#1137](https://github.com/ClickHouse/clickhouse-java/pull/1137)
- write time field data loss precision. [#1127](https://github.com/ClickHouse/clickhouse-java/pull/1127)
- fixed a few copy-paste error causing problem handling Geo types
