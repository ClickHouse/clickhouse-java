#clickhouse-r2dbc-spring-webflux-sample

This is a sample rest api which will insert clicks and get the list of clicks per day.

In order to run the application;

- Go clickhouse-java/examples/clickhouse-r2dbc-samples/misc/docker and run `docker-compose up -d`
- Execute the table creation sql at clickhouse-java/examples/clickhouse-r2dbc-samples/clickhouse-r2dbc-spring-webflux-sample/src/main/resources/init.sql.
- Import the postman export clickhouse-java/examples/clickhouse-r2dbc-samples/clickhouse-r2dbc-spring-webflux-sample/src/main/resources/postman.
- Run the application by using Application.java.
- Create some clicks by postman and list daily clicks per domain and path.
