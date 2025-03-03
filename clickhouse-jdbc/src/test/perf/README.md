# Performance Testing using JMH for JDBC driver
Testing of Insert, Select & comparing the performance of JDBC V1 & JDBC V2
Insert testing 
- batch sizes [100, 1000, 10000, 100000]
- data types [int8, int16, int32, int64, float32, float64, string, boolean] (Will be extended to other data types as well)

Note: string payload size is adjustable ["1024", "2048", "4096", "8192" ] 

Select testing query `"SELECT * FROM %s LIMIT %d"`
- limit is adjustable currently use 100 
- retrieval of different data types [int8, int16, int32, int64, float32, float64, string, boolean] (Will be extended to other data types as well)
- separated test for connection part and connection with deserialization part

# how to run 
mvn clean package -DskipTests=true -P performance-testing-jdbc

# output file 
The benchmark generate the output file in jmh-jdbc-results.json 
Use https://jmh.morethan.io/ to visualize the results

