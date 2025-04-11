# ClickHouse-Java Demo Service 

## About 
This is an example of a Spring Boot application using different ClickHouse-Java clients and features. 


## Usage

This example requires an instance of ClickHouse running locally on in remote server.
Application uses `system.numbers` table to generate dataset of any size. It is very convenient because:
- data is already there
- server generates data as if it was a real table
- no need to change schema or create tables

Run clickhouse instance in docker: 
```
docker run --rm -e CLICKHOUSE_USER=default -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=secret\
 -p 8123:8123 clickhouse/clickhouse-server

```

Create table: 
```
CREATE TABLE ui_events
(
    `id` UUID,
    `timestamp` DateTime64,
    `eventName` String
)
ENGINE = MergeTree
ORDER BY timestamp
```

To run 

```shell
./gradlew bootRun
```

To test

```shell
curl http://localhost:8080/direct/dataset/0?limit=100000
```

JPA Insert 
```shell 
 curl -v -X POST -H "Content-Type: application/json" -d '{"id": "123", "timestamp": "2025-04-07T14:30:00.000Z", "eventName": "Login", "tags": ["security", "activity"]}' http://localhost:8080/events/ui_events
```
JPA Select
```shell
 curl -v -X GET http://localhost:8080/events/ui_events
```

## Features

- [x] Client V2 New Implementation