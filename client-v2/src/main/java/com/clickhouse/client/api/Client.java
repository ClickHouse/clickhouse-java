package com.clickhouse.client.api;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.api.data_formats.DataFormat;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public <TDataFormat extends DataFormat> QueryResponse<TDataFormat> query(String sqlQuery, TDataFormat outputFormat,
                                                                             QuerySettings<?>... settings) {
        ClickHouseClient clientQuery = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        ClickHouseRequest request = clientQuery.read(getServerNode());
        request.query(sqlQuery);
        return new QueryResponse(clientQuery.execute(request));
    }
}
