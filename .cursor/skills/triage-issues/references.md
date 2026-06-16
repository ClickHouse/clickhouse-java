# Description Sizes

**Tiny**
Problem is described in a few general words and mainly in issue title. For
example, "getRow() throws exception", "client hangs", "Array serialization broken", etc.

**Small**
Problem is described with minimal details but there is reference to a project
or functionality. There can be a single stacktrace.

**Medium**
Problem is described with enough details and tells what is broken and how
it should work. There can be additional comments from author.

**Large**
Problem is described with main details and examples. There is some explanation
of the usecase. Sometimes there is a link to external demo project.


# Minimal Issue Details

- Issue type: potential bug, feature request,
- Affected component: language client or JDBC driver
- Affected area: core functionality (data codecs, configuration validation, formats), special functionality (subset of core functionality or very specific feature) or general failure.


# Module Documentation

External documentation (for human reference only — do NOT fetch these during
automated triage; rely on the checked-out source under "Module Sources"):

- java client documentation: https://clickhouse.com/docs/integrations/language-clients/java/client
- JDBC documentation: https://clickhouse.com/docs/integrations/language-clients/java/jdbc
- JDBC working with date/time values: https://clickhouse.com/docs/integrations/language-clients/java/jdbc_date_time_guide


# Module Sources

Local module directories in this checked-out repository (explore these with
Read/Glob/Grep — do not follow external links):

- client v2: `client-v2/`
- JDBC v2: `jdbc-v2/`
- client v1: `clickhouse-http-client/` (also `clickhouse-client/`, `clickhouse-data/`)
- JDBC v1: `clickhouse-jdbc/`

For a structural map (module/package boundaries, `area:*` label → source
location, entry-point classes, and stacktrace → module heuristics) use
[source-map.md](source-map.md). Consult it to locate the affected module/area
before grepping the tree.


# Labels

# Main

* **client-v1**: Use when issue is in old client version. Projects like `clickhouse-client`, `clickhouse-data`, etc.
* **client-api-v2**: Use when issue is in the new client - `client-v2` project.
* **jdbc-v1**: Use when issue is in the old JDBC driver - `clickhouse-jdbc` project.
* **jdbc-v2**: Use when issue is in the new JDBC driver - `jdbc-v2` project.
* **question**: Use when issue is asking question rather then describing a bug.
* **investigating**: Use when more investigation is needed and it is not possible to pin point the problem.

# Area

* **`area:client-insert`**: Use when handling data insertion specifically in the ClickHouse client.
* **`area:client-pojo-serde`**: Use for issues involving the Serialization and Deserialization (SerDe) of Plain Old Java Objects (POJOs).
* **`area:client-read`**: Use when handling data reading specifically in the ClickHouse client.
* **`area:data-type`**: Use for issues related to processing or handling different ClickHouse data types.
* **`area:dependencies`**: Use for pull requests or issues that update, add, or remove a dependency file.
* **`area:docs`**: Use when documentation is missing, incorrect, or needs updating.
* **`area:error-handling`**: Use for tracking issues or improvements related to error and exception handling.
* **`area:format`**: Use for issues handling specific data formats (e.g., JSON, CSV, RowBinary).
* **`area:general`**: Use for general issues that do not neatly fit into any other specific `area:` category.
* **`area:integration`**: Use for integration issues with third-party frameworks, tools, or systems.
* **`area:jdbc-insert`**: Use for handling data insertion issues specifically related to the JDBC driver.
* **`area:jdbc-metadata`**: Use for issues handling JDBC metadata, such as retrieving the type of a column or database properties.
* **`area:jdbc-read`**: Use for reading data issues specifically related to the JDBC driver.
* **`area:network`**: Use for tracking network configuration, connectivity, and I/O-related issues.
* **`area:old-stmt-parsing`**: Use for issues concerning the parsing logic of older SQL statements.
* **`area:packaging`**: Use for issues related to project packaging, builds, or distribution artifacts.
* **`area:sql-parser`**: Use for issues, bugs, or feature requests regarding the custom SQL parser.
