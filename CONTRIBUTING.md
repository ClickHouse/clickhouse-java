## Getting started
ClickHouse-JDBC client is an open-source project, and we welcome any contributions from the community. Please share your ideas, contribute to the codebase, and help us maintain up-to-date documentation.

### Create a fork of the repository and clone it
```bash
git clone https://github.com/[YOUR_USERNAME]/clickhouse-java
cd clickhouse-java
```

### Set up environment
You have installed:
- JDK 8 or JDK 17+
- To build a multi-release jar use JDK 17+ with `~/.m2/toolchains.xml`
    ```xml
    <?xml version="1.0" encoding="UTF8"?>
    <toolchains>
        <toolchain>
            <type>jdk</type>
            <provides>
                <version>17</version>
            </provides>
            <configuration>
                <jdkHome>/usr/lib/jvm/java-17-openjdk</jdkHome>
            </configuration>
        </toolchain>
    </toolchains>
    ```

### Build modules
- JDK 8 Use `mvn -Dj8 -DskipITs clean verify` to compile and generate packages. 
- JDK 17+ Use create a multi-release jar (see [JEP-238](https://openjdk.java.net/jeps/238)) please verify that you added `~/.m2/toolchains.xml` and run `mvn -DskipITs clean verify`


To create a native binary of JDBC driver for evaluation and testing:

- [install GraalVM](https://www.graalvm.org/latest/docs/getting-started/) and optionally [upx](https://upx.github.io/)

- make sure you have [native-image](https://www.graalvm.org/latest/docs/getting-started/#native-image) installed, and then build the native binary

  ```bash
  cd clickhouse-java
  mvn -DskipTests clean install
  cd clickhouse-jdbc
  mvn -DskipTests -Pnative clean package
  # only if you have upx installed
  upx -7 -k target/clickhouse-jdbc-bin
  ```

- run native binary

  ```bash
  # print usage
  ./target/clickhouse-jdbc-bin
  Usage: clickhouse-jdbc-bin [PROPERTIES] <URL> [QUERY] [FILE]
  ...

  # test database connection using JDBC driver
  ./target/clickhouse-jdbc-bin -Dverbose=true 'jdbc:ch:http://localhost'
  Arguments:
    -   url=jdbc:ch:http://localhost
    - query=select 500000000
    -  file=jdbc.out

  Options: action=read, batch=1000, samples=500000000, serde=true, type=, verbose=true
  Processed 1 rows in 85.56 ms (11.69 rows/s)

  # test query performance using Java client
  ./target/clickhouse-jdbc-bin -Dverbose=true -Dtype=uint64 'http://localhost'
  ...
  Processed 500,000,000 rows in 52,491.38 ms (9,525,373.89 rows/s)

  # test same query again using JVM for comparison - don't have GraalVM EE so JIT wins in my case
  java -Dverbose=true -Dtype=uint64 -jar target/clickhouse-jdbc-*-http.jar 'http://localhost'
  ...
  Processed 500,000,000 rows in 25,267.89 ms (19,787,963.94 rows/s)
  ```

## Testing

By default, [docker](https://docs.docker.com/engine/install/) is required to run integration test. Docker image(defaults to `clickhouse/clickhouse-server`) will be pulled from Internet, and containers will be created automatically by [testcontainers](https://www.testcontainers.org/) before testing. To test against specific version of ClickHouse, you can pass parameter like `-DclickhouseVersion=23.3` to Maven.

In the case you don't want to use docker and/or prefer to test against an existing server, please follow instructions below:

- make sure the server can be accessed using default account(user `default` and no password), which has both DDL and DML privileges
- add below two configuration files to the existing server and expose all defaults ports for external access
  - [ports.xml](../../blob/main/clickhouse-client/src/test/resources/containers/clickhouse-server/config.d/ports.xml) - enable all ports
  - and [users.xml](../../blob/main/clickhouse-client/src/test/resources/containers/clickhouse-server/users.d/users.xml) - accounts used for integration test
    Note: you may need to change root element from `clickhouse` to `yandex` when testing old version of ClickHouse.
- make sure ClickHouse binary(usually `/usr/bin/clickhouse`) is available in PATH, as it's required to test `clickhouse-cli-client`
- put `test.properties` under either `~/.clickhouse` or `src/test/resources` of your project, with content like below:
  ```properties
  # ClickHouse server for integration test
  clickhouseServer=x.x.x.x
  # custom HTTP proxy for integration test
  proxyAddress=<host>:<port>
  
  # below properties are only useful for test containers
  #clickhouseVersion=latest
  #clickhouseTimezone=UTC
  #clickhouseImage=clickhouse/clickhouse-server
  #additionalPackages=
  #proxyImage=ghcr.io/shopify/toxiproxy:2.5.0
  ```

### Tooling
We use [TestNG](https://testng.org/) as testing framework and for running ClickHouse Local instance [testcontainers](https://www.testcontainers.org/modules/databases/clickhouse/).

### Running unit tests

Does not require a running ClickHouse server.
Running the maven commands above will trigger the test.

## Benchmark

To benchmark JDBC drivers:

```bash
cd clickhouse-benchmark
mvn clean package
# single thread mode
java -DdbHost=localhost -jar target/benchmarks.jar -t 1 \
    -p client=clickhouse-jdbc -p connection=reuse \
    -p statement=prepared -p type=default Query.selectInt8
```

It's time consuming to run all benchmarks against all drivers using different parameters for comparison. If you just need some numbers to understand performance, please refer to [this](https://github.com/ClickHouse/clickhouse-java/issues/768) and watch [this](https://github.com/ClickHouse/clickhouse-java/issues/928) for more information and updates.
