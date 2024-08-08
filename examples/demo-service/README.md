# ClickHouse-Java Demo Service 

## About 
This is an example of a Spring Boot application using different ClickHouse-Java clients and features. 


## Usage

This example requires an instance of ClickHouse running locally on in remote server.
Application uses `system.numbers` table to generate dataset of any size. It is very convenient because:
- data is already there
- server generates data as if it was a real table
- no need to change schema or create tables

To run

```bash
./gradlew bootRun
```

To test

```bash
curl http://localhost:8080/direct/dataset/0?limit=100000
```

## Features

- [x] Client V2 New Implementation