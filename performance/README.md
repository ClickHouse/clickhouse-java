

## DataSetGenerator 

```shell
 mvn exec:java -Dscope=test -Dexec.mainClass="com.clickhouse.com.clickhouse.benchmark.data.ClickHouseDataTypesShort" -input <table_fields.sql> -rows <number_of_rows>
```


## Performance Test

with custom dataset
```shell
mvn test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.clickhouse.com.clickhouse.benchmark.BenchmarkRunner" -Dexec.args="--dataset=file://dataset_1741150759025.csv"
```

