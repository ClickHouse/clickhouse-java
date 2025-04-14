# ClickHouse-Java Demo Service 

## About 
This is an example of a Spring Boot service using ClickHouse client directly and via JPA. 
Example is an application that requires ClickHouse DB running externally. It can be a Docker or
ClickHouse Cloud instance.

## How to Run

### Initialize DB
Set up a ClickHouse instance.

Example of running with Docker:
```shell
docker run -d --name demo-service-db -e CLICKHOUSE_USER=default -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=secret\
 -p 8123:8123 clickhouse/clickhouse-server

docker ps
# output should be like: 
CONTAINER ID   IMAGE                          COMMAND            CREATED         STATUS         PORTS                                                           NAMES
2abfefc40a84   clickhouse/clickhouse-server   "/entrypoint.sh"   4 seconds ago   Up 2 seconds   9000/tcp, 0.0.0.0:8123->8123/tcp, :::8123->8123/tcp, 9009/tcp   demo-service-db
```

Example how to run client:
```shell
docker exec -it demo-service-db clickhouse-client
```

Create table (needed for JPA example):
```
CREATE TABLE ui_events
(
    `id` String,
    `timestamp` DateTime64,
    `event_name` String, 
    `tags` Array(String)
)
ENGINE = MergeTree
ORDER BY timestamp
```

### Run Demo-Service 

```shell
./gradlew bootRun
```

### Interact with API 

#### Direct Client

To read `limit` number of rows from `system.numbers`: 
```shell
curl http://localhost:8080/direct/dataset/0?limit=100000
```

#### JPA

To insert some data:
```shell 
curl -v -X POST -H "Content-Type: application/json" \
 -d '{"id": "4NAD7B8HH1", "timestamp": "2025-04-07T14:30:00.000Z", "eventName": "Login", "tags": ["security", "activity"]}'\
  http://localhost:8080/events/ui_events
```

To fetch inserted data:
```shell
curl -v -X GET http://localhost:8080/events/ui_events
```