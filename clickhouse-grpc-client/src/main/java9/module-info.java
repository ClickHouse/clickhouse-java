module com.clickhouse.client.grpc {
    exports com.clickhouse.client.grpc;
    exports com.clickhouse.client.grpc.config;
    // exports com.clickhouse.client.grpc.impl;

    provides com.clickhouse.client.ClickHouseClient with com.clickhouse.client.grpc.ClickHouseGrpcClient;

    requires java.base;

    requires static grpc.netty.shaded;
    requires static grpc.okhttp;

    requires transitive com.clickhouse.client;
    requires transitive com.google.gson;
    requires transitive com.google.protobuf;
    requires transitive io.grpc;
    // requires transitive grpc.core;
    // requires transitive grpc.protobuf;
    // requires transitive grpc.stub;
}
