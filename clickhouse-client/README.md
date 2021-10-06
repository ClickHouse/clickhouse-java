# ClickHouse Java Client

Async Java client for ClickHouse. `clickhouse-client` is an abstract module, so it does not work by itself unless being used together with implementation module like `ckhouse-grpc-client` or `clickhouse-http-client`.

## Quick Start

```java
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.List;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCluster;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseValue;

// declare a server to connect to
ClickHouseNode server = ClickHouseNode.of("server1.domain", ClickHouseProtocol.GRPC, 9100, "my_db");

// execute multiple queries one after another within one session
CompletableFuture<List<ClickHouseResponseSummary>> future = ClickHouseClient.send(server,
    "create database if not exists test",
    "use test", // change current database from my_db to test
    "create table if not exists test_table(s String) engine=Memory",
    "insert into test_table values('1')('2')('3')",
    "select * from test_table limit 1",
    "truncate table test_table",
    "drop table if exists test_table");
// do something else in current thread, and then retrieve summaries
List<ClickHouseResponseSummary> results = future.get();

// declare a cluster
ClickHouseCluster cluster = ClickHouseCluster.builder()
    // defaults to localhost:8123 and http protocol
    .addNode(ClickHouseNode.builder().cluster("cluster1").tags("dc1", "rack1", "for-write").build())
    .addNode(ClickHouseNode.of("1.2.3.4", ClickHouseProtocol.GRPC, 9100, "system", "dc2", "rack2", "for-read"))
    .build();

// issue query against one node via grpc
String sql = "select * from numbers(100)";
try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
    // connect to a node which understands gRPC
    ClickHouseResponse response = client.connect(cluster).query(sql).execute().get()) {
    for (ClickHouseRecord record : response.records()) {
        // Don't cache ClickHouseValue as it's a container object reused among all records
        ClickHouseValue v = record.getValue(0);
        // converts to DateTime64(6)
        LocalDateTime dateTime = v.asDateTime(6);
        // converts to long/int/byte if you want to
        long l = v.asLong();
        int i  = v.asInteger();
        byte b = v.asByte();
    }

    // summary will be fully available after all records being retrieved
    ClickHouseResponseSummary summary = response.getSummary();
}
```
