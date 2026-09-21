# IoT Ingest Demo Service

A Spring Boot service demonstrating
- General integration of JDBC driver with Spring Data.
- ClickHouse Client (not JDBC) configuration and usage with Spring Boot.
- Different observability options (instrumentation vs client own observability configuration). 

---

## What This Demo Shows

This sample service ingests IoT telemetry (temperature, humidity, pressure, motion, etc.) and demonstrates two complementary ways to work with ClickHouse in Spring Boot:

1. **Spring Data JPA / JDBC** – Standard entity persistence for single-signal API requests (`POST /api/v1/signals`).
2. **Direct ClickHouse Client V2** – High-performance direct API for:
   - **Async Batch Reconciliation** (`POST /api/v1/reconciliation`): High-throughput POJO batch inserts off the main HTTP thread.
   - **Analytical Aggregation Queries** (`GET /api/v1/signals/slices`): Real-time 1-second location window aggregates.

### Key Highlights
- **Zero-Setup Local Database**: Uses **Testcontainers** to automatically start a ClickHouse instance during development and testing—no external database setup required.
- **Full Observability**: Collects OpenTelemetry metrics and traces, exporting them to both ClickHouse and a local **Grafana LGTM** stack (Prometheus, Tempo, Grafana).
- **API Key Authentication**: Simple header-based (`X-API-Key`) security filter.

---

## How to Run

### Prerequisites
- **Java 17+**
- **Docker** (for the Grafana LGTM stack and Testcontainers)

### 1. Start the Observability Stack (Optional)
Start local Prometheus, Tempo, and Grafana containers:

```bash
docker compose up -d
```

### 2. Launch the Application
Run the Spring Boot service. Testcontainers will automatically launch ClickHouse:

```bash
./mvnw spring-boot:test-run
```

The app starts on `http://localhost:8080`, initializes the `iot_signals` and `otel_metrics` tables in ClickHouse, and begins exporting telemetry. Press `Ctrl+C` to stop.

> **Running against an external ClickHouse?** Run `./mvnw spring-boot:run` with `CLICKHOUSE_URL=jdbc:clickhouse://<host>:8123/default`.

### 3. Try the Endpoints

#### Send a single signal (Spring Data JPA)
```bash
curl -i -X POST http://localhost:8080/api/v1/signals \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-key-1' \
  -d '{"deviceId":"sensor-1","locationId":"11111111-1111-1111-1111-111111111111","type":"TEMPERATURE","value":21.5,"unit":"C"}'
```
*Supported types: `TEMPERATURE`, `HUMIDITY`, `PRESSURE`, `MOTION`, `GAS`, `BATTERY`, `LIGHT`.*

#### Check total stored signal count
```bash
curl -s http://localhost:8080/api/v1/signals/count -H 'X-API-Key: dev-key-1'
```

#### Submit a reconciliation batch (ClickHouse Client V2)
```bash
curl -i -X POST http://localhost:8080/api/v1/reconciliation \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-key-1' \
  -d '[
    {"deviceId":"sensor-1","locationId":"11111111-1111-1111-1111-111111111111","signalType":"TEMPERATURE","value":21.5,"unit":"C"},
    {"deviceId":"sensor-2","locationId":"11111111-1111-1111-1111-111111111111","signalType":"HUMIDITY","value":48.0,"unit":"%"}
  ]'
```

#### Query 1-second location aggregation slices
```bash
# Query all locations over the last 10 minutes
curl -s 'http://localhost:8080/api/v1/signals/slices?lookback=10m' \
  -H 'X-API-Key: dev-key-1'

# Query a specific location over the last hour
curl -s 'http://localhost:8080/api/v1/signals/slices?lookback=1h&locationId=11111111-1111-1111-1111-111111111111' \
  -H 'X-API-Key: dev-key-1'
```
*Lookback format: `<number><unit>` where unit is `s` (seconds), `m` (minutes), or `h` (hours). Maximum lookback is `1h`.*

---

## Traffic Generators (`scripts/` Folder)

The `scripts/` folder contains standalone Python scripts (using standard library only) to generate realistic traffic, simulate edge cases, and run load tests.

### 1. HTTP Ingest Traffic (`signal-generator.py`)
Generates single-signal POST requests with optional authentication and payload errors.

```bash
# Generate mixed valid and invalid traffic for 60 seconds
python3 scripts/signal-generator.py --rate 5 --duration 60

# Available profiles: mixed (default), valid, auth, payload, storage
python3 scripts/signal-generator.py --profile valid --rate 20 --duration 60 --quiet
```

### 2. Batch Reconciliation Generator (`reconciliation-generator.py`)
Sends variable-size reconciliation batches to test async ClickHouse Client V2 batch writes.

```bash
python3 scripts/reconciliation-generator.py --rate 2 --duration 60 --min-batch 1 --max-batch 50
```

### 3. Slice Query Generator (`slice-query-generator.py`)
Continuously queries the 1-second slice endpoint to generate read load and trace spans.

```bash
python3 scripts/slice-query-generator.py --rate 5 --duration 60 --lookbacks 10s,1m,10m,1h
```

> **Note**: Omit `--duration` on any script to run continuously until stopped with `Ctrl+C`.

---

## Architecture & Internals

### Data Flow Overview

```
IoT Device / Client
  │
  ├── POST /api/v1/signals ───────▶ ApiKeyAuthFilter ──▶ SignalController ──────▶ Spring Data JPA / JDBC ──▶ ClickHouse (iot_signals)
  │                                    │
  ├── POST /api/v1/reconciliation ────┼─────────────────▶ ReconciliationController ──▶ @Async Client V2 ──────▶ ClickHouse (iot_signals)
  │                                    │
  └── GET /api/v1/signals/slices ─────┴─────────────────▶ SignalSliceController ─────▶ Client V2 Query ────────▶ ClickHouse (iot_signals)
                                       │
                                       ▼
                                OpenTelemetry SDK
                                       │
                                       ├─ Metrics ──────────▶ ClickHouse (otel_metrics)
                                       └─ Metrics & Spans ──▶ Grafana LGTM (Prometheus, Tempo, Grafana)
```

### Component Reference

| Layer / Concern | File / Class | Description |
| :--- | :--- | :--- |
| **HTTP Ingest API** | `web/SignalController` | Serves `POST /api/v1/signals` via Spring Data JPA repository |
| **Async Reconciliation** | `web/ReconciliationController` + `service/ReconciliationService` | Async POJO batch inserts using ClickHouse Client V2 |
| **Slice Aggregations** | `web/SignalSliceController` + `service/SignalSliceService` | Direct SQL analytical queries via Client V2 |
| **Authentication** | `web/ApiKeyAuthFilter` | Intercepts HTTP requests and validates `X-API-Key` |
| **Schema Initialization** | `schema/ClickHouseSchemaInitializer` | Creates `iot_signals` and `otel_metrics` tables on startup |
| **Telemetry & Metrics** | `telemetry/SignalMetrics` + `config/OpenTelemetryConfig` | Captures OTel metrics & traces across HTTP, JPA, and Client V2 |
| **Metrics Exporter** | `telemetry/ClickHouseMetricExporter` | Flushes OTel metrics directly to ClickHouse |

### Inspecting Data in ClickHouse

Connect to the running ClickHouse container (find ID via `docker ps`) to query landed signals and metrics:

```sql
-- View stored signal breakdown by type
SELECT signal_type, count() FROM iot_signals GROUP BY signal_type;

-- View exported OpenTelemetry metrics
SELECT name, attributes['signal.type'] AS type, value, time
FROM otel_metrics
WHERE name = 'iot.signals.received'
ORDER BY time DESC;

-- Total JPA / ClickHouse operations by outcome
SELECT
    attributes['storage.operation'] AS operation,
    attributes['outcome'] AS outcome,
    max(value) AS cumulative_total
FROM otel_metrics
WHERE name = 'iot.storage.operations'
GROUP BY operation, outcome;

-- Average storage operation duration (ms)
SELECT
    attributes['storage.operation'] AS operation,
    attributes['outcome'] AS outcome,
    maxIf(value, type = 'histogram_sum') / maxIf(value, type = 'histogram_count') AS avg_ms
FROM otel_metrics
WHERE name = 'iot.storage.duration'
GROUP BY operation, outcome;
```

### Observability in Grafana

Open Grafana at **[http://localhost:3000](http://localhost:3000)** (no login required):

- **Metrics (Prometheus)**: Inspect `iot.*` metrics or view client HTTP connection pool stats:
  ```promql
  httpcomponents_httpclient_pool_total_connections{httpclient="reconciliation",state="leased"}
  ```
- **Traces (Tempo)**: Filter by `service.name = iot-ingest` to trace incoming HTTP requests down to their ClickHouse database spans.

### Integration Tests

Run integration tests using Maven:

```bash
./mvnw test
```

`SignalIngestionTests` spins up a Testcontainers ClickHouse instance to verify API authentication, validation, persistence, and telemetry exports.

### Configuration Reference

Configure via environment variables or `src/main/resources/application.yml`:

| Property / Env Variable | Default | Description |
| :--- | :--- | :--- |
| `CLICKHOUSE_URL` | `jdbc:clickhouse://localhost:8123/default` | ClickHouse JDBC URL |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / *(empty)* | Database credentials |
| `IOT_AUTH_API_KEYS` | `dev-key-1,dev-key-2` | Allowed API keys (comma-separated) |
| `iot.telemetry.export-interval` | `15s` | Metric flush interval to ClickHouse |
| `OTEL_EXPORTER_OTLP_ENABLED` | `true` | Enable OTLP telemetry exporter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP/gRPC collector endpoint |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | `http://localhost:4318/v1/metrics` | OTLP/HTTP metrics endpoint |
