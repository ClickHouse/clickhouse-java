
## JMH Benchmarks


### Dependencies 



### How to Run


#### Generating Dataset

```shell
mvn compile exec:exec -Dexec.executable=java -Dexec.args="-classpath %classpath com.clickhouse.benchmark.data.DataSetGenerator \
-input sample_dataset.sql -name default -rows 10"
```

#### Running Benchmarks 
 
With default settings :
```shell
mvn compile exec:exec
```

With custom measurement iterations: 
```shell
mvn compile exec:exec -Dexec.executable=java -Dexec.args="-classpath %classpath com.clickhouse.benchmark.BenchmarkRunner -m 3"
```

Other options:
- "-d" - dataset name or file path (like `file://default.csv`)
- "-l" - dataset limits to test coma separated (ex.: `-l 10000,10000`)
- "-m" - number of measurement iterations
- "-t" - time in seconds per iteration
- "-b" - benchmark mask coma separated. Ex.: `-b writer,reader,i`. Default : `-b i,q`
  - "all" - Run alpl benchmarks
  - "i" - InsertClient - insert operation benchmarks
  - "q" - QueryClient - query operation benchmarks
  - "ci" - ConcurrentInsertClient - concurrent version of insert benchmarks
  - "cq" - ConcurrentQueryClient - concurrent version of query benchmarks
  - "lz" - Compression - compression related benchmarks
  - "writer" - Serializer - serialization only logic benchmarks
  - "reader" - DeSerilalizer - deserialization only logic benchmarks
  - "mixed" - MixedWorkload 
    