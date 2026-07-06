# Contribution Guide

## Preface

Libraries in this repository are used in many production systems. Users expect certain level of quality and active support (adding new features, fixing issues). 
It is important to make all changes safe and swiftly. There we ask all contributors for tests and clear description of changes done in a PR. This expedites merge and
quadruples value of your contribution.

This document covers main important things related to the contributing. Here is very brief overview of them:
- (Contribution Process)[#contribution-process] - describes how contribution is handled and what steps we expect. Specially pay attention if you want to implement 
any feature or do big change. 
- (Testing Expectations)[#testing-expectations] - test is requirement for mostly any change and tests have their quality measure, too. We value failure scenario tests
and when tests cover veriaty of data. Here is where help mostly needed because every use-case or domain specific data examples significantly increase test quality.
- (Pull Request Checklist)[#pull-request-checklist] - helps to prepare PR for review and make whole process faster. It is very important to run tests locally - we skip 
PRs if CI is failing and will leave according comment.
- (Restricted Changes)[#restricted-changes] - currently we restrict only changes in CI workflows and everything related to it.
- (Technical Verification)[#technical-verification] - all kinds of instractions to run tests, examples and verifications. 


## Contribution Process

### Issues, Discussion, and Proposals

Small changes do not require an issue or implementation proposal. A clear pull request description and tests are enough, although an issue is appreciated when it provides useful context.

Please open an issue before starting work on a new feature, a large behavior change, or a change that affects public API, configuration, protocol handling, serialization, JDBC/R2DBC behavior, or documented features.

Feature work and big changes must be discussed through an issue, and the implementation proposal should be approved before code changes begin. The proposal does not need to be perfect, but it should describe:

- what problem is being solved
- what user-visible behavior will change
- what compatibility risks exist
- what tests will prove the behavior
- whether documentation, `CHANGELOG.md`, or `docs/features.md` needs to change

Please review our (AI Policy)[AI_POLICY.MD] if you are using AI tools. Special attention should be paid to tests because they are the main guardrails for code changes. 

Some good ideas need time to design and implement. We are open to discussion and welcome collaboration in issues, even when the final implementation is not ready yet.

### Big Contributions

One logical change per PR: a feature, a bug fix, a refactor, or a doc update. Mixed-purpose PRs complicate reviews and increase the likelihood of unintended regressions.

We all know that large PRs (400+ lines of code) are hard to review and it is more likely to miss a bug. Small changes are appreciated and take less time to verify them. If many changes are needed anyway (feature implementation, refactoring), please split them: something can be done as code preparation, cleanup or smaller improvement. It should take less time at sum as this repository is actively maintained. We will ask to split changes of 800+ LOC anyway.

For a new feature, implement the smallest change that solves the problem. Polish, additional configuration, optimization, and extensions can be addressed in the follow-up PRs. 

For self and AI review use following guides:
- `docs/ai-review.md`
- `docs/changes_checklist.md`
- `docs/features.md` for changes that touch `client-v2` or `jdbc-v2`

### Testing Expectations

Tests should verify correct behavior, not only reproduce a specific issue. An issue may describe a symptom, but it does not always define the complete correct behavior. A good test should make the expected success or failure path clear to future maintainers.

Please include negative tests when a change fixes invalid input, error handling, validation, parsing, serialization, or compatibility-sensitive behavior. The test should prove that the failure path produces the correct result or exception, not only that the original bug no longer happens.

Test code is as important as production code. Keep tests compact, readable, and focused on the scenario being verified. Avoid repeating existing coverage unless the new test adds a distinct scenario, edge case, module, type, format, or failure mode. Reduce duplication in test setup and assertions so reviewers can quickly see what behavior is being tested.

We measure test coverage, but coverage percentage is not the only goal. We value tests that cover meaningful scenarios and edge cases. For example, a test that reads a random large integer is useful, but a stronger test also checks boundary values, rounding behavior, invalid values, and the interaction with nullable or nested types when relevant.

### Restricted Changes

Changes to CI workflows are currently restricted. Please discuss CI workflow changes with maintainers before opening a pull request.

## Pull Request Checklist

Please make pull requests review-ready before requesting review. A PR may still take time to review and prepare for merge, especially when it changes public behavior or touches multiple modules.

Steps: 
- Describe all changes in PR briefly so every human can read. There can be additional description appended to the main description.
  - Include link to an issue. If issue is missing please create one.
  - Describe any user-visible behavior especially if it was changed
  - Include compatibility impact
- Run self-review of the code (personally or by AI). This reduces PR time.
- Run tests locally. IMPORTANT: We skip PR that has failed CI and ask to fix it.  
- Update `CHANGELOG.md` shortly with what was the problem and how fixed. Add link to the issue. 
- Update `docs/features.md` when `client-v2` or `jdbc-v2` feature was added, removed, or intentionally behaviour change. This file helps to review code. 

Use `docs/review-template.md` as a reference for what reviewers will look for when assessing important changes.

TBD: add a dockerized development environment section that documents a standard way to run the full local verification suite.

## Technical Verification

Before submitting a pull request, verify the affected modules locally. Prefer targeted Maven commands over full-repository runs when the change is localized.

### 1. Create a Fork and Clone It

```bash
git clone https://github.com/[YOUR_USERNAME]/clickhouse-java
cd clickhouse-java
```

### 2. Set Up the Environment

Install JDK 8 or JDK 17+.

To build a multi-release jar with JDK 17+, configure `~/.m2/toolchains.xml`:

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

### 3. Build and Compile

Use the command that matches your environment:

```bash
# JDK 8
mvn -Dj8 -DskipITs clean verify

# JDK 17+ with toolchains.xml configured
mvn -DskipITs clean verify
```

For targeted module verification, use:

```bash
mvn -pl <module> test
mvn -pl <module> -am test
```

Compile examples or packaging modules when your change affects examples, packaging, public APIs, or user-facing behavior.

### 4. Run Unit and Integration Tests

Unit tests do not require a running ClickHouse server. Relevant unit tests should pass locally before a PR is submitted.

Integration tests usually require [Docker](https://docs.docker.com/engine/install/). The Docker image defaults to `clickhouse/clickhouse-server`, and containers are created automatically by [testcontainers](https://www.testcontainers.org/). To test against a specific ClickHouse version, pass a Maven parameter such as:

```bash
mvn -pl <module> test -DclickhouseVersion=23.3
```

If you do not want to use Docker, or you prefer to test against an existing server:

- make sure the server can be accessed with the default account, user `default` and no password, with both DDL and DML privileges
- add the test server configuration files and expose all default ports:
  - [ports.xml](clickhouse-client/src/test/resources/containers/clickhouse-server/config.d/ports.xml)
  - [users.xml](clickhouse-client/src/test/resources/containers/clickhouse-server/users.d/users.xml)
- make sure the ClickHouse binary, usually `/usr/bin/clickhouse`, is available in `PATH` for `clickhouse-cli-client` tests
- put `test.properties` under either `~/.clickhouse` or the module's `src/test/resources`

Example `test.properties`:

```properties
# ClickHouse server for integration tests
clickhouseServer=x.x.x.x

# Custom HTTP proxy for integration tests
proxyAddress=<host>:<port>

# Properties below are only useful for testcontainers
#clickhouseVersion=latest
#clickhouseTimezone=UTC
#clickhouseImage=clickhouse/clickhouse-server
#additionalPackages=
#proxyImage=ghcr.io/shopify/toxiproxy:2.5.0
```

TBD: document a dockerized development environment for running the standard local test suite.

### 5. Run Coverage Verification

To run tests and generate a code coverage report, use the `coverage` Maven profile. This will execute tests and produce JaCoCo coverage reports.

```bash
mvn clean verify -P coverage
```

For targeted module verification:

```bash
mvn -pl <module> -am clean verify -P coverage
```

The coverage reports will be generated in the `target/site/jacoco-aggregate` or module-specific `target/site/jacoco-ut` directories. You can view the HTML version of the coverage report by opening the `index.html` file in your browser:

- Full repository report: `target/site/jacoco-aggregate/index.html`
- Module-specific report: `<module>/target/site/jacoco-ut/index.html`

### 6. Optional Native Binary Check

To create a native binary of the JDBC driver for evaluation and testing:

- [install GraalVM](https://www.graalvm.org/latest/docs/getting-started/) and optionally [upx](https://upx.github.io/)
- make sure [native-image](https://www.graalvm.org/latest/docs/getting-started/#native-image) is installed
- build and run the native binary

```bash
cd clickhouse-java
mvn -DskipTests clean install
cd clickhouse-jdbc
mvn -DskipTests -Pnative clean package

# print usage
./target/clickhouse-jdbc-bin
```

### 7. Optional Benchmark Check

Run benchmarks when the change may affect performance:

```bash
cd clickhouse-benchmark
mvn clean package

# single thread mode
java -DdbHost=localhost -jar target/benchmarks.jar -t 1 \
    -p client=clickhouse-jdbc -p connection=reuse \
    -p statement=prepared -p type=default Query.selectInt8
```
