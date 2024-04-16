package com.clickhouse.client.api;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseColumn;

import java.io.InputStream;
import java.net.SocketException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.internal.TableSchemaParser;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static java.time.temporal.ChronoUnit.SECONDS;

public class Client {
    public static final int TIMEOUT = 30_000;
    private Set<String> endpoints;
    private Map<String, String> configuration;
    private List<ClickHouseNode> serverNodes = new ArrayList<>();
    private Client(Set<String> endpoints, Map<String,String> configuration) {
        this.endpoints = endpoints;
        this.configuration = configuration;
        this.endpoints.forEach(endpoint -> {
            this.serverNodes.add(ClickHouseNode.of(endpoint, this.configuration));
        });
    }

    public static class Builder {
        private Set<String> endpoints;
        private Map<String, String> configuration;

        public Builder() {
            this.endpoints = new HashSet<>();
            this.configuration = new HashMap<String, String>();
            // TODO: set defaults configuration values
            this.setConnectTimeout(30, SECONDS)
                .setSocketTimeout(2, SECONDS)
                .setSocketRcvbuf(804800)
                .setSocketSndbuf(804800);
        }

        public Builder addEndpoint(String endpoint) {
            // TODO: validate endpoint
            this.endpoints.add(endpoint);
            return this;
        }

        public Builder addEndpoint(Protocol protocol, String host, int port) {
            String endpoint = String.format("%s://%s:%d", protocol.toString().toLowerCase(), host, port);
            this.addEndpoint(endpoint);
            return this;
        }

        public Builder addConfiguration(String key, String value) {
            this.configuration.put(key, value);
            return this;
        }

        public Builder addUsername(String username) {
            this.configuration.put("user", username);
            return this;
        }

        public Builder addPassword(String password) {
            this.configuration.put("password", password);
            return this;
        }
        // SOCKET SETTINGS
        public Builder setConnectTimeout(long size) {
            this.configuration.put("connect_timeout", String.valueOf(size));
            return this;
        }
        public Builder setConnectTimeout(long amount, ChronoUnit unit) {
            this.setConnectTimeout(Duration.of(amount, unit).toMillis());
            return this;
        }

        public Builder setSocketTimeout(long size) {
            this.configuration.put("socket_timeout", String.valueOf(size));
            return this;
        }
        public Builder setSocketTimeout(long amount, ChronoUnit unit) {
            this.setSocketTimeout(Duration.of(amount, unit).toMillis());
            return this;
        }
        public Builder setSocketRcvbuf(long size) {
            this.configuration.put("socket_rcvbuf", String.valueOf(size));
            return this;
        }
        public Builder setSocketRcvbuf(long amount, ChronoUnit unit) {
            this.setSocketRcvbuf(Duration.of(amount, unit).toMillis());
            return this;
        }
        public Builder setSocketSndbuf(long size) {
            this.configuration.put("socket_sndbuf", String.valueOf(size));
            return this;
        }
        public Builder setSocketReuseaddr(boolean value) {
            this.configuration.put("socket_reuseaddr", String.valueOf(value));
            return this;
        }
        public Builder setSocketKeepalive(boolean value) {
            this.configuration.put("socket_keepalive", String.valueOf(value));
            return this;
        }
        public Builder setSocketTcpNodelay(boolean value) {
            this.configuration.put("socket_tcp_nodelay", String.valueOf(value));
            return this;
        }
        public Builder setSocketTcpNodelay(int size) {
            this.configuration.put("socket_linger", String.valueOf(size));
            return this;
        }
        public Client build() {
            // check if endpoint are empty. so can not initiate client
            if (this.endpoints.isEmpty()) {
                throw new IllegalArgumentException("At least one endpoint is required");
            }
            // check if username and password are empty. so can not initiate client?
            return new Client(this.endpoints, this.configuration);
        }
    }

    private ClickHouseNode getServerNode() {
        // TODO: implement load balancing using existing logic
        return this.serverNodes.get(0);
    }

    /**
     * Ping the server to check if it is alive
     * @return true if the server is alive, false otherwise
     */
    public boolean ping() {
        return ping(Client.TIMEOUT);
    }

    /**
     * Ping the server to check if it is alive
     * @param timeout timeout in milliseconds
     * @return true if the server is alive, false otherwise
     */
    public boolean ping(int timeout) {
        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        return clientPing.ping(getServerNode(), timeout);
    }

    /**
     * Register the POJO
     */
    public void register(Class<?> clazz, TableSchema schema) {
        //This is just a placeholder
        //Create a new POJOSerializer with static .serialize(object, columns) methods
    }

    /**
     * Insert data into ClickHouse using a POJO
     */
    public Future<InsertResponse> insert(String tableName,
                                     List<Object> data,
                                     InsertSettings settings,
                                     List<ClickHouseColumn> columns) throws ClickHouseException, SocketException {
        //Lookup the Serializer for the POJO
        //Call the static .serialize method on the POJOSerializer for each object in the list
        return null;//This is just a placeholder
    }

    /**
     * Insert data into ClickHouse using a binary stream
     */
    public Future<InsertResponse> insert(String tableName,
                                     InputStream data,
                                     InsertSettings settings) throws ClickHouseException, SocketException {
        return null;//This is just a placeholder
    }


    /**
     * Sends data query to the server and returns a reference to a result descriptor.
     * Control is returned when server accepted the query and started processing it.
     * <br/>
     * The caller should use {@link ClickHouseParameterizedQuery} to render the `sqlQuery` with parameters.
     *
     *
     * @param sqlQuery - complete SQL query.
     * @param settings
     * @return
     */
    public Future<QueryResponse> query(String sqlQuery, Map<String, Object> qparams, QuerySettings settings) {
        ClickHouseClient clientQuery = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        ClickHouseRequest request = clientQuery.read(getServerNode());
        request.query(sqlQuery, settings.getQueryID());
        // TODO: convert qparams to map[string, string]
        request.params(qparams);
        return CompletableFuture.completedFuture(new QueryResponse(clientQuery.execute(request)));
    }

    public TableSchema getTableSchema(String table, String database) {
        try (ClickHouseClient clientQuery = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
            ClickHouseRequest request = clientQuery.read(getServerNode());
            // XML - because java has a built-in XML parser. Will consider CSV later.
            request.query("DESCRIBE TABLE " + table + " FORMAT " + ClickHouseFormat.TSKV.name());
            TableSchema tableSchema = new TableSchema();
            try {
                return new TableSchemaParser().createFromBinaryResponse(clientQuery.execute(request).get(), table, database);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get table schema", e);
            }
        }
    }
}
