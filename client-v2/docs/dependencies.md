# Module Dependencies

## Abstract
This document describes module dependencies and a reason for their existence. Additionally 
the document keeps historical information about the dependencies.

## Dependencies

### 0.7.1-patch1

 - `com.clickhouse:clickhouse-data:jar:0.7.1-patch1-SNAPSHOT:compile` - Required for reading custom data types. When client-v1 required classes should be moved to client-v2 module this dependency will be removed.
 - `com.clickhouse:clickhouse-client:jar:0.7.1-patch1-SNAPSHOT:compile` - Required because there is an option to use client-v1. Additionally, this dependency is required because a few core classes like `ClickHouseNode` are used in client-v2. When client-v1 is deprecated - all required classes should be moved to client-v2 module and this dependency will be removed.
 - `org.slf4j:slf4j-api:jar:2.0.7:compile` - Most commonly used logging frontend. 
 - `org.apache.commons:commons-compress:jar:1.27.1:compile` - Required for compression. 
 - `org.lz4:lz4-pure-java:jar:1.8.0:compile` - Required for compression.
 - `org.ow2.asm:asm:jar:9.7:compile` - Required for serialization/deserialization.
 - `org.apache.httpcomponents.client5:httpclient5:jar:5.3.1:compile` - only HTTP client that is currently supported. In the future it should be an optional dependency when support for different clients will be added. This client also implements async API that might be used in the future.
 - `com.fasterxml.jackson.core:jackson-core:jar:2.17.2:compile` - used to safely parse summary from ClickHouse.
 - `org.roaringbitmap:RoaringBitmap:jar:0.9.47:compile` - used for serialization/deserialization of aggregate functions. For some reason `clickhouse-data` modules has this dependency as `provided`. 