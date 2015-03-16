package ru.yandex.metrika.clickhouse.config;

/**
 * datasource
 * @author orantius
 * @version $Id$
 * @since 7/12/12
 */
public class ClickHouseSource {
    public static final int DEFAULT_PORT = 8123;

    private String host = "localhost";

    private int port = DEFAULT_PORT;

    private String database;

    public ClickHouseSource() {
    }

    public ClickHouseSource(String host, String database) {
        this.host = host;
        this.database = database;
    }

    public ClickHouseSource(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public ClickHouseSource(String host, int port, String database) {
        this.host = host;
        this.port = port;
        this.database = database;
    }

    public ClickHouseSource(String database) {
        this.database = database;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setDb(String database) {
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDb() {
        return database;
    }

    public String getUrl() {
        return "http://"+host+ ':' +port+ '/';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClickHouseSource source = (ClickHouseSource) o;
        if (database != null ? !database.equals(source.database) : source.database != null) return false;
        return port == source.port && host.equals(source.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        result = 31 * result + (database != null ? database.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClickHouseSource{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                '}';
    }
}
