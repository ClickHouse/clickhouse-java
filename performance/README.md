
## JMH Benchmarks


### Dependencies 



### How to Run

With default settings :
```shell
mvn compile exec:exec
```

With custom measurement iterations: 
```shell
mvn compile exec:exec -Dexec.args="-classpath %classpath com.clickhouse.benchmark.BenchmarkRunner -m 3"
```

Other options:
- "-m" - number of measurement iterations
- "-t" - time in seconds per iteration
- "-b" - benchmark mask coma separated. Ex.: `-b writer,reader,i`. Default : `-b i,q`
  - "i" - InsertClient - insert operation benchmarks
  - "q" - QueryClient - query operation benchmarks
  - "ci" - ConcurrentInsertClient - concurrent version of insert benchmarks
  - "cq" - ConcurrentQueryClient - concurrent version of query benchmarks
  - "lz" - Compression - compression related benchmarks
  - "writer" - Serializer - serialization only logic benchmarks
  - "reader" - DeSerilalizer - deserialization only logic benchmarks
  - "mixed" - MixedWorkload 
    