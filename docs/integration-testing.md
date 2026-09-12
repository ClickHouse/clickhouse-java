# ClickHouse Java Client & JDBC Driver Integration Testing

## Abstract

Every integration case has specific requirements. For example, BI tools should verify connectivity, data type mapping, handling timestamps. This guide documents different scenarios we have learned from numerous integrations. 



# General Recommendations 

Tests has a gread values as contract verification tool when build properly. If there are tests for other integrations we recommend porting them to verify ClickHouse integration. 

Design your tests with posibility to run them on staging system and sometimes on production. For example, testing secure connection is important for communication with ClickHouse Cloud. It also helpes to onboard new customers when they want to integrate their ClickHouse instance.  

# Integration Tests

Integration tests are useful on all stages of work: 
- initial development help teams to have exacts scenarios to verify. 
- verify production system is working correctly.
- during library upgrades to understand amount of changes.


## Configuration 

We test different configurations in our libraries but tests set values dirrectly. Application configuration passes a few more step before getting into library so verifying it is accepted by client is important. We recommend join configuration tests with core features. Additionaly it is good to have simple client or driver initialization tests with all important configuration parameters set. Client has `com.clickhouse.client.api.Client#getConfiguration()` to retrieve all settings after creation. ClickHouse JDBC Connection provides access to client via `com.clickhouse.jdbc.ConnectionImpl#getClient`. 


Things to look for: 
- configuration value encoding
- value ranges 

## Operations 

Majority of the tests should cover operation part: 
- connection 
- health checking 
- read/write operations
- error handling 
- failure handling and recovery. 


## Behavior


# Performance Tests

## Load Testing 
