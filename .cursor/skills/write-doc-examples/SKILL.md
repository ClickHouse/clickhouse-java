---
name: write-doc-examples
description: Write, rewrite, and format documentation code snippets into reusable, production-ready methods with necessary library imports and clean linting. Use when writing, updating, or formatting Java code examples in integration guides and documentation (such as docs/integration-client.md or docs/integration-jdbc.md).
---

# Write Documentation Code Examples for Production Readiness

This skill guides writing, rewriting, and formatting code snippets in documentation so they can be directly reused or integrated into production Java applications while maintaining stylistic consistency across the document.

## Core Principles

### 1. Preserve Document Context and Style Consistency
Always align with patterns and conventions established earlier in the same document:
- **Consistent Builder / Factory Pattern**: If early sections establish building a client via a method returning `Client.Builder` (e.g., `public Client.Builder createBaseClient()`), subsequent configuration options (auth schemes, TLS, proxy, timeouts) must follow the same pattern rather than reverting to creating full `Client` instances from scratch.
- **Incremental Context**: Sub-sections and options should build consistently on preceding examples (e.g., `public Client createAnalyticsDBClient(Client.Builder baseClient)`).
- **Naming Conventions**: Maintain consistent method, variable, and parameter names (`createBaseClient`, `client`, `schema`, `settings`, `events`) across all snippets in the document.

### 2. Wrap Code with Methods
Never present bare, loose statements floating outside a method. Encapsulate every snippet into a realistic, reusable method:
- Use factory methods or builder helpers for client instantiation (e.g., `public Client.Builder createBaseClient()`, `public Client createAnalyticsDBClient(Client.Builder baseClient)`).
- Use action-specific methods for queries, inserts, and updates (e.g., `public List<Event> readEvents(Client client, TableSchema schema)`, `public void writeEvents(Client client, List<Event> events)`).
- Use clear parameter lists (`Client client`, `TableSchema schema`, `InsertSettings settings`, etc.) and meaningful return types instead of writing top-level procedural scripts.

### 3. Include Only Relevant Library Imports
At the top of the code block, list only the imports that belong to the library:
- **Include**: Classes and interfaces from `com.clickhouse.client.api.*`, `com.clickhouse.data.*`, `com.clickhouse.jdbc.*`, etc.
- **Exclude**: Common standard JDK classes (e.g., `java.util.List`, `java.util.Map`, `java.io.InputStream`, `java.util.concurrent.TimeUnit`) unless needed to avoid ambiguity.
- Keep the import list compact and directly relevant to the snippet.

### 4. Allow Partial Code (Omit Obvious Definitions)
Keep examples focused on library usage:
- **Omit obvious custom classes**: Auxiliary classes, configuration containers, or DTOs (e.g., `AppConfiguration`) do not need full definitions.
- **Include definitions only when structurally important**: Provide the class definition only when its internal fields or annotations are essential to demonstrating the library feature (e.g., showing how POJO fields map to ClickHouse column types).

### 5. Ensure Code is Linted and Production-Grade
- **Resource Management**: Always use `try-with-resources` for closable resources such as `QueryResponse`, `InsertResponse`, and streams.
- **Compatibility**: Ensure code is valid Java 8+ and follows repository patterns.
- **Error & Edge-Case Handling**: Guard against empty inputs, handle or propagate checked exceptions properly, and include comments at extension points (e.g., `// add db specific configuration`).
- **Formatting**: Maintain consistent indentation (4 spaces), balanced braces, and valid Java syntax.

---

## Transformation Workflow

When updating documentation examples:

1. **Scan Prior Context in the Document**: Check how earlier sections structure their examples (e.g., whether client configuration uses `Client.Builder` factory methods).
2. **Identify the Intent**: Determine whether the example demonstrates configuration, querying, inserting, streaming, or error handling.
3. **Encapsulate in a Reusable Method Matching the Document Style**:
   - For client creation/options: Return `Client.Builder` or take `Client.Builder baseClient` if established earlier in the guide.
   - For operations: Accept `Client` (and any required schemas or options) as parameters.
   - For callbacks/streaming: Pass inputs and manage the response lifecycle properly.
4. **Collect Library Imports**: Add all `com.clickhouse.*` imports required by the snippet at the top.
5. **Prune Unnecessary Boilerplate**: Strip out trivial DTO class definitions, keeping only structural POJO models where column mapping is highlighted.
6. **Lint and Format**: Check method signatures, variable types, semicolons, and `try-with-resources` blocks.

---

## Transformation Examples

### Example 1: Client Configuration & Instantiation

**Before (Loose snippet):**
```java
Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .setUsername("default")
    .setPassword("secret")
    .setDefaultDatabase("analytics")
    .build();
```

**After (Reusable production methods with library imports):**
```java
import com.clickhouse.client.api.Client;

public Client.Builder createBaseClient() {
    return new Client.Builder()
        .addEndpoint("http://localhost:8123")
        .setUsername("default")
        .setPassword("secret")
        // set common configuration
        ;
}

public Client createAnalyticsDBClient(Client.Builder baseClient) {
    return baseClient
        .setDefaultDatabase("analytics")
        // add db specific configuration
        .build();
}
```

---

### Example 2: Following Established Document Style in Configuration Variants

When earlier sections establish `createBaseClient()` returning `Client.Builder`, all variant auth/network options maintain that same style:

**Option B (Bearer Auth):**
```java
import com.clickhouse.client.api.Client;

public Client.Builder createBaseClient() {
    return new Client.Builder()
        .addEndpoint("http://localhost:8123")
        .useBearerTokenAuth("my_access_token");
}
```

**Option C (Mutual TLS):**
```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.SSLMode;

public Client.Builder createBaseClient() {
    return new Client.Builder()
        .addEndpoint("https://localhost:8443")
        .useSSLAuthentication(true)
        .setClientCertificate("/path/to/client.crt")
        .setClientKey("/path/to/client.key")
        .setRootCertificate("/path/to/ca.crt")
        .setSSLMode(SSLMode.STRICT);
}
```

---

### Example 3: Runtime Operations with External Configuration

**Before (Loose statement):**
```java
client.updateUserAndPassword("new_user", "new_password");
```

**After (Wrapped method; obvious custom config class omitted):**
```java
void updateClientCredentials(Client client, AppConfiguration appConf) {
    client.updateUserAndPassword(appConf.db_username, appConf.db_password);
}
```

---

### Example 4: POJO Mapping (Registration, Read, Write)

**Before (Script-like sequence):**
```java
TableSchema schema = client.getTableSchema("events");
client.register(Event.class, schema);

List<Event> events = client.queryAll("SELECT * FROM events", Event.class, schema);
client.insert("events", events).get();
```

**After (Structured into definition, registration, read, and write methods):**

1. Structural POJO definition (included because field structure matters for column mapping):
```java
public static class Event {
    public long id;
    public String name;
    public long timestamp;
}
```

2. Registration helper:
```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;

void registerPojoMappings(Client client, Map<Class<?>, String> pojoTables) {
    for (Map.Entry<Class<?>, String> entry : pojoTables.entrySet()) {
        TableSchema schema = client.getTableSchema(entry.getValue());
        client.register(entry.getKey(), schema);
    }
}
```

3. Read method:
```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;

public List<Event> readEvents(Client client, TableSchema schema) {
    return client.queryAll(
        "SELECT id, name, timestamp FROM events",
        Event.class,
        schema);
}
```

4. Write method with response cleanup:
```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;

public void writeEvents(Client client, List<Event> events) throws Exception {
    if (events.isEmpty()) {
        return;
    }

    try (InsertResponse response = client.insert("events", events).get()) {
        // handle response metrics or confirmation
    }
}
```

---

### Example 5: Streaming Query with Binary Format Reader

**Before (Procedural script):**
```java
QuerySettings settings = new QuerySettings()
    .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);

QueryResponse response = client.query("SELECT * FROM events", settings).get();
ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
while (reader.hasNext()) {
    reader.next();
    long id = reader.getLong("id");
}
```

**After (Encapsulated method with try-with-resources and library imports):**
```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;

public void streamEvents(Client client) throws Exception {
    QuerySettings settings = new QuerySettings()
        .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);

    try (QueryResponse response = client.query("SELECT * FROM events", settings)
            .get(30, TimeUnit.SECONDS)) {

        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
        while (reader.hasNext()) {
            reader.next();
            long id = reader.getLong("id");
            String name = reader.getString("name");
            // process row data
        }
    }
}
```

---

### Example 6: Streaming Insert with Callback Writer

**Before (Loose insert callback):**
```java
TableSchema schema = client.getTableSchema("events");
ClickHouseFormat format = ClickHouseFormat.RowBinary;

client.insert("events", out -> {
    RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, format);
    for (Event event : events) {
        writer.setValue("id", event.getId());
        writer.commitRow();
    }
}, format, new InsertSettings()).get();
```

**After (Encapsulated write method with proper response closure):**
```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;

public void writeEventsStream(Client client, TableSchema schema, List<Event> events) throws Exception {
    ClickHouseFormat format = ClickHouseFormat.RowBinary;

    try (InsertResponse response = client.insert("events", out -> {
        RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, format);
        for (Event event : events) {
            writer.setValue("id", event.id);
            writer.setValue("name", event.name);
            writer.commitRow();
        }
    }, format, new InsertSettings()).get()) {
        // handle response metrics
    }
}
```

---

## Validation Checklist

Before finalizing any rewritten documentation example:
- [ ] Does the example follow the structural and naming style established in earlier sections of the document (e.g. `Client.Builder` return type)?
- [ ] Is every code snippet wrapped in a meaningful method (or a builder helper)?
- [ ] Are all library imports (`com.clickhouse.*`) present and accurate?
- [ ] Are redundant JDK imports omitted unless strictly helpful?
- [ ] Are trivial custom classes omitted and only essential structures (e.g. POJO schema mappings) defined?
- [ ] Are closable resources (`QueryResponse`, `InsertResponse`, etc.) properly handled with `try-with-resources`?
- [ ] Is the code syntactically valid and lint-clean?
