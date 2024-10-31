# ClickHouse JDBC driver
THe official JDBC driver for ClickHouse
## Documentation
See the [ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/jdbc) for the full documentation entry.

## Examples
For more example please check [here](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/jdbc).
## Upgrade path
### to 0.3.2+

Please refer to cheatsheet below to upgrade JDBC driver to 0.3.2+.

<table>
<thead>
<tr>
<th>#</th>
<th>Item</th>
<th>&lt;= 0.3.1-patch</th>
<th>&gt;= 0.3.2</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>pom.xml</td>
<td><pre><code class="language-xml">&lt;dependency&gt;
    &lt;groupId&gt;ru.yandex.clickhouse&lt;/groupId&gt;
    &lt;artifactId&gt;clickhouse-jdbc&lt;/artifactId&gt;
    &lt;version&gt;0.3.1-patch&lt;/version&gt;
    &lt;classifier&gt;shaded&lt;/classifier&gt;
    &lt;exclusions&gt;
        &lt;exclusion&gt;
            &lt;groupId&gt;*&lt;/groupId&gt;
            &lt;artifactId&gt;*&lt;/artifactId&gt;
        &lt;/exclusion&gt;
    &lt;/exclusions&gt;
&lt;/dependency&gt;
</code></pre></td>
<td><pre><code class="language-xml">&lt;dependency&gt;
    &lt;groupId&gt;com.clickhouse&lt;/groupId&gt;
    &lt;artifactId&gt;clickhouse-jdbc&lt;/artifactId&gt;
    &lt;version&gt;0.3.2-patch11&lt;/version&gt;
    &lt;classifier&gt;all&lt;/classifier&gt;
    &lt;exclusions&gt;
        &lt;exclusion&gt;
            &lt;groupId&gt;*&lt;/groupId&gt;
            &lt;artifactId&gt;*&lt;/artifactId&gt;
        &lt;/exclusion&gt;
    &lt;/exclusions&gt;
&lt;/dependency&gt;
</code></pre></td>
</tr>
<tr>
<td>2</td>
<td>driver class</td>
<td>ru.yandex.clickhouse.ClickHouseDriver</td>
<td>com.clickhouse.jdbc.ClickHouseDriver</td>
</tr>
<tr>
<td>3</td>
<td>connection string</td>
<td><pre><code class="language-text">jdbc:clickhouse://[user[:password]@]host:port[/database][?parameters]</code></pre></td>
<td><pre><code class="language-text">jdbc:(ch|clickhouse)[:protocol]://endpoint[,endpoint][/database][?parameters][#tags]</code></pre>
<b>endpoint:</b> [protocol://]host[:port][/database][?parameters][#tags]<br/>
<b>protocol:</b> (grpc|grpcs|http|https|tcp|tcps)<br/>
</td>
</tr>
<tr>
<td>4</td>
<td>custom settings</td>
<td><pre><code class="language-java">String jdbcUrl = "jdbc:clickhouse://localhost:8123/default?socket_timeout=6000000"
    // custom server settings
    + "&max_bytes_before_external_group_by=16000000000"
    + "&optimize_aggregation_in_order=0"
    + "&join_default_strictness=ANY"
    + "&join_algorithm=auto"
    + "&max_memory_usage=20000000000"; </code></pre></td>
<td><pre><code class="language-java">String jdbcUrl = "jdbc:clickhouse://localhost/default?socket_timeout=6000000"
    // or properties.setProperty("custom_settings", "a=1,b=2,c=3")
    + "&custom_settings="
    // url encoded settings separated by comma
    + "max_bytes_before_external_group_by%3D16000000000%2C"
    + "optimize_aggregation_in_order%3D0%2C"
    + "join_default_strictness%3DANY%2C"
    + "join_algorithm%3Dauto%2C"
    + "max_memory_usage%3D20000000000"; </code></pre></td>
</tr>
<tr>
<td>5</td>
<td>load balancing</td>
<td><pre><code class="language-java">String connString = "jdbc:clickhouse://server1:8123,server2:8123,server3:8123/database";
BalancedClickhouseDataSource balancedDs = new BalancedClickhouseDataSource(
    connString).scheduleActualization(5000, TimeUnit.MILLISECONDS);
ClickHouseConnection conn = balancedDs.getConnection("default", "");
</code></pre></td>
<td><pre><code class="language-java">String connString = "jdbc:ch://server1,server2,server3/database"
    + "?load_balancing_policy=random&health_check_interval=5000&failover=2";
ClickHouseDataSource ds = new ClickHouseDataSource(connString);
ClickHouseConnection conn = ds.getConnection("default", "");
</code></pre></td>
</tr>
<tr>
<td>6</td>
<td>DateTime</td>
<td><pre><code class="language-java">try (PreparedStatement ps = conn.preparedStatement("insert into mytable(start_datetime, string_value) values(?,?)") {
    ps.setObject(1, LocalDateTime.now());
    ps.setString(2, "value");
    ps.executeUpdate();
}
</code></pre></td>
<td><pre><code class="language-java">try (PreparedStatement ps = conn.preparedStatement("insert into mytable(start_datetime, string_value) values(?,?)") {
    // resolution of DateTime32 or DateTime without scale is 1 second
    ps.setObject(1, LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));
    ps.setString(2, "value");
    ps.executeUpdate();
}
</code></pre></td>
</tr>
<tr>
<td>7</td>
<td>extended API</td>
<td><pre><code class="language-java">ClickHouseStatement sth = connection.createStatement();
sth.write().send("INSERT INTO test.writer", new ClickHouseStreamCallback() {
    @Override
    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
        for (int i = 0; i < 10; i++) {
            stream.writeInt32(i);
            stream.writeString("Name " + i);
        }
    }
}, ClickHouseFormat.RowBinary); // RowBinary or Native are supported
</code></pre></td>
<td><pre><code class="language-java">// 0.3.2
Statement sth = connection.createStatement();
sth.unwrap(ClickHouseRequest.class).write().table("test.writer")
    .format(ClickHouseFormat.RowBinary).data(out -> {
    for (int i = 0; i < 10; i++) {
        // write data into the piped stream in current thread
        BinaryStreamUtils.writeInt32(out, i);
        BinaryStreamUtils.writeString(out, "Name " + i);
    }
}).sendAndWait();

// since 0.4
PreparedStatement ps = connection.preparedStatement("insert into test.writer format RowBinary");
ps.setObject(new ClickHouseWriter() {
@Override
public void write(ClickHouseOutputStream out) throws IOException {
for (int i = 0; i < 10; i++) {
// write data into the piped stream in current thread
BinaryStreamUtils.writeInt32(out, i);
BinaryStreamUtils.writeString(out, "Name " + i);
}
}
});
// ClickHouseWriter will be executed in a separate thread
ps.executeUpdate();
</code></pre></td>

</tr>
</tbody>
</table>
