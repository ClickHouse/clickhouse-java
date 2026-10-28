# ClickHouse Java Client & JDBC Driver Integration Testing

## Abstract

Integration testing essential in building integrations. Scope of it defines quality of service for end users. Each business case has own test goals along with 
core  ones. This guide will share our vision and some good practices we see worth using. 

Goal integration testing is to verify:
- Systems can communicate 
- Systems can handle failures

Tests have great value as a contract verification tool when built properly. If there are tests for other integrations, we recommend porting them to verify ClickHouse integration.

## General Recommendations

### Test Environment


Environment where tests run defines what can be tested, what factors affecting test results. Environment should serve test purpose, it should be sufficient for 
the test scope. Integration tests is our main focus because they verify intercommunication of two systems. This type of tests can be run on different stages of development and 
in different environments. Following this an important requirement for tests comes - ability to run same tests with local and remote system in similar way. 

Using Testcontainers is very convinient in local or CI test environment and it is very good start. Most features can be verified in this way. But integration is not limited 
to local machine or CI usually thus tests should be able to run against real DB instances. It could be your customer's on-prem installation or ClickHouse Cloud instance. 
Running tests against environment that is close to production is required step for solid integration because production system has much more moving parts than local host. 
Here is a short list of obvious differences: 
- Computation power
- Network connectivity
- Software versions

Tests that can be executed against different target systems may help while troubleshooting. For example, running group of selected integration tests againts production system
may narrow search area. Tests can be a good step in verifying production systems, too. 

Another requirement for good integration tests is to run against ClickHouse cluster. Distributed setup works differently in many ways. Your application may have 
cluster aware logic. ClickHouse has many distributed table engines that work differently than traditional ones. ClickHouse is eventually consistent and this 
property is well observed on cluster. 

**ClickHouse Version**

Make sure to test with different ClickHouse version expecially if your application connects to different on-prem installation. Test with LTS versions first. We recommend test
with most recent LTS versions that reached their end-of-life. It is very good practice to keep DB version inventory up-to-date to understand where to focus in testing. 

**Protocol** 

Test with different protocols if applicable. Most important to verify secure connections even locally. For example, old JRE version may have expired CA certificate 
and even valid SSL certificate will be seen as invalid because signed with new version of CA certificate unknow to JRE.

**Host** 

Design test to be runnable against remote and local environment. ClickHouse can be run within [testcotnainer](https://testcontainers.com/) but external host configuration still useful to run same tests against staging or more complex local setup.

### Test Structure 

Tests tend to increase in number over time and initial structure will help to work with them in the future. For example, organizing tests in suits matching core workflow (like connection, read/write operations) helps to use some of them in smoke tests. More granular tests help to combine them into more complex end-to-end scenarios. It may be useful
for troubleshooting. 

**Test Functionality** 

Write tests for functinality. Tests work as contract in this case. For each new issue investigate what is worng: requirements changed, behavior changed, missed case. 
Single issue only a signal that something is not integrated (not always broken). Making tests only for a single issue usually is not enough to resolve a problem.

## Tests by Scope

Test scope can be different depending on your strategy. Further we will talk about areas we see as important for test coverage and about some their specifics. . 

### Configuration

We test different configurations in our libraries, but tests set values directly. Application configuration passes through additional steps before reaching the library, so verifying that it is correctly accepted by the client is important. We recommend combining configuration tests with core feature tests. Additionally, it is good to have simple client or driver initialization tests with all important configuration parameters set. Client has `com.clickhouse.client.api.Client#getConfiguration()` to retrieve all settings after creation. ClickHouse JDBC Connection provides access to the client via `com.clickhouse.jdbc.ConnectionImpl#getClient`.

Test scopes - what to verify: 
- Default configuration - proper work for your case. 
- Value boundaries - does client allow set extreme value that you have in your design. 
- Invalid values - does application stop them before passing to the client, does client handle them in way you expect.
- ClickHouse settings - are they correctly passed from user to database. 
- Timeouts - do they have effect and passed in correct units (most common mistake).  
- Different types of authentication - important for SSL authentication to verify certificats are accepted. 

### Operations

This section covers testing operations part of your workload. Even we have own tests connecting to ClickHouse Cloud it is still important to test it within 
your application. There are many different parameters and conditions where integration breaks.


#### Connecting 

Test scopes - what to verify: 
- Client need time to establish connection. Some applications has requirement to do it within certain timeout and set `connection_request_timeout`. It is important to test with multiple runs that timeout is long enough - do not fail periodically because connection took a few milliseconds longer. 
- Client has internal connection pool and returns connection only when response objects is closed. Verify that it happens by setting limited number of connections via `max_open_connections` and repeating request more than that times. JDBC keeps internal connection per result set. When data is inserted - connection is closed internally after request is complete. 
- Always do concurrent tests when client used in multi-threaded application. Most common use is backend application processing user request. Default value of `max_open_connections` may be too low for such application.
- Connecting to ClickHouse Cloud - instances can be in idle state. If it is expected - verify that application can handle it.

#### Fetching Data 

These tests relate to any data query operations. It is very useful to have collection of some real production queries to verify complex cases like one using CTE's. 

Test scopes - what to verify: 
- Data types - correct conversion between DB and application data types. Here is important to check well know values and take some random (save random seed somewhere)
- Data formats - format support and correct work. Formats like CSV may have difference in behavior like handling `null`. 
- Failures - verify retries and timeouts along with correct handling of Client exceptions.

#### Loading Data 

These tests relate to any data insert operations. 

Test scopes - what to verify: 
- Data types - see "Fetching Data" 
- Data formats - see "Fetching Data"
- Failure - verify correct handling of failure and correct handling of Client exceptions.
- Deduplication token - verify that correct deduplication token is set in `InsertSettings` and data is deduplicated correctly. 

## Load Testing

This kind of tests are design to understand when system breaks. It used to understand capacity, find bottlenecks, etc. Run load tests on a signle application instance to make load analysis easier. Load should grow gradually till system breaks. This will tell you: 

- What are the limits. 
- Are there problems with your projected load. 
- How application performance degrade with load.
- Estimate needed resources.

Load tests are targeted on understanding whole application throughput. It is important to saturate application with load while tests. Here are a few examples: 
- Application serves different kinds of requests (query different aggregated stats) - load tests should call them simulteniously 
- Application has periodic tasks (sync from another db, cleanup) - load tests should verify scenario when all it happens.

As one might see load tests in these examples targeted to exhaust resources. This kind of tests are important to verify that solution fits your system and design. 
Loading big datasets need memory what is not compressable resource what means that memory can hold only certain amout of data. With Java this problem also complicated by
GC runs because when big chunk of heep with millions of objects should be GC'ed then it will require more CPU time, longer GC pause and pilling up other requests. 