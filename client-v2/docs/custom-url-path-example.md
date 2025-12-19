# Custom URL Path Configuration

## Overview

The custom URL path feature allows you to route requests to different database instances behind a load balancer by configuring a custom path to be appended to the base endpoint URL.

## Use Case

When multiple ClickHouse database instances are behind a load balancer, you may need to route requests to different databases based on URL paths. For example:

- `https://myhost.com:8123/sales/db` → Routes to sales analytics DB
- `https://myhost.com:8123/app/db` → Routes to app stats DB

## Configuration

### Using the Builder API

```java
import com.clickhouse.client.api.Client;

// Configure client with custom URL path
Client client = new Client.Builder()
    .addEndpoint("https://myhost.com:8123")
    .customURLPath("/sales/db")
    .setUsername("default")
    .setPassword("password")
    .setDefaultDatabase("my_database")
    .build();
```

### Using Configuration Properties

```java
import com.clickhouse.client.api.Client;
import java.util.HashMap;
import java.util.Map;

Map<String, String> config = new HashMap<>();
config.put("custom_url_path", "/sales/db");
config.put("user", "default");
config.put("password", "password");
config.put("database", "my_database");

Client client = new Client.Builder()
    .addEndpoint("https://myhost.com:8123")
    .setOptions(config)
    .build();
```

## Important Notes

1. **Database Name Header**: The database name is sent via the `X-ClickHouse-Database` header, not as part of the URL path. This ensures proper database routing even when custom paths are used.

2. **Path Format**: The custom path is appended to the base URL as-is. It's recommended to start with "/" for clarity (e.g., "/sales/db").

3. **Existing Paths**: If your base endpoint URL already contains a path (e.g., "https://myhost.com:8123/api"), the custom path will be concatenated (e.g., "https://myhost.com:8123/api/sales/db").

## Example Scenarios

### Scenario 1: Simple Custom Path

```java
// Base URL: https://myhost.com:8123
// Custom Path: /sales/db
// Result: https://myhost.com:8123/sales/db

Client client = new Client.Builder()
    .addEndpoint("https://myhost.com:8123")
    .customURLPath("/sales/db")
    .setUsername("default")
    .setPassword("password")
    .build();
```

### Scenario 2: Combining with Existing Path

```java
// Base URL: https://myhost.com:8123/api
// Custom Path: /sales/db
// Result: https://myhost.com:8123/api/sales/db

Client client = new Client.Builder()
    .addEndpoint("https://myhost.com:8123/api")
    .customURLPath("/sales/db")
    .setUsername("default")
    .setPassword("password")
    .build();
```

### Scenario 3: Multiple Clients for Different Databases

```java
// Client for sales database
Client salesClient = new Client.Builder()
    .addEndpoint("https://myhost.com:8123")
    .customURLPath("/sales/db")
    .setUsername("default")
    .setPassword("password")
    .setDefaultDatabase("sales")
    .build();

// Client for app stats database
Client appClient = new Client.Builder()
    .addEndpoint("https://myhost.com:8123")
    .customURLPath("/app/db")
    .setUsername("default")
    .setPassword("password")
    .setDefaultDatabase("app_stats")
    .build();
```

## Load Balancer Configuration

When using this feature, ensure your load balancer is configured to route based on URL paths. For example, with nginx:

```nginx
location /sales/db {
    proxy_pass http://sales-clickhouse-backend;
}

location /app/db {
    proxy_pass http://app-clickhouse-backend;
}
```

## Compatibility

- Available in: client-v2 API
- Minimum version: 0.9.4-SNAPSHOT
- Server compatibility: All ClickHouse versions (server-side configuration required for routing)
