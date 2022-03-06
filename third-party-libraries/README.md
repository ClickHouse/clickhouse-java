# Repacked 3rd Party Libraries

Repacked third party libraries(gRPC and RoaringBitmap) for JPMS support.

## Maven Dependencies

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>io.grpc</artifactId>
    <version>1.1.2</version>
</dependency>
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>org.roaringbitmap</artifactId>
    <version>1.1.2</version>
</dependency>
```

## References

- Enable MRJAR(multi-release jar)
  - Parent POM: https://github.com/meterware/multirelease-parent
    Note: another example https://github.com/apache/maven-compiler-plugin/blob/master/src/it/multirelease-patterns/singleproject-runtime/pom.xml#L102-L108
  - Basics: https://www.baeldung.com/java-multi-release-jar
  - Maven: https://maven.apache.org/plugins/maven-compiler-plugin/multirelease.html
  - Gradle: https://blog.gradle.org/mrjars
  - More to read at https://in.relation.to/2017/02/13/building-multi-release-jars-with-maven/
