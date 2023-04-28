package com.clickhouse.jdbc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseByteValue;
import com.clickhouse.data.value.ClickHouseLongValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;

public final class Main {
    static class Options {
        final String action;
        final int batch;
        final boolean output;
        final int samples;
        final boolean serde;
        final String type;
        final boolean verbose;

        final String url;
        final String query;
        final String file;

        final boolean requiresJdbc;

        private Options(String url, String query, String file) {
            action = System.getProperty("action", "read").toLowerCase();
            batch = Integer.getInteger("batch", 1000);
            output = Boolean.getBoolean("output");
            samples = Integer.getInteger("samples", 500000000);
            serde = !"false".equalsIgnoreCase(System.getProperty("serde", ""));
            type = System.getProperty("type", "").toLowerCase();
            verbose = Boolean.getBoolean("verbose");

            this.url = url;
            this.requiresJdbc = url.length() > 5 && "jdbc:".equalsIgnoreCase(url.substring(0, 5));

            if (query == null || query.isEmpty()) {
                this.query = isLoadAction() || isWriteAction() ? getInsertQuery() : getSelectQuery();
            } else {
                this.query = query;
            }
            if (file == null || file.isEmpty()) {
                if (output) {
                    this.file = requiresJdbc ? "jdbc.out" : "java.out";
                } else {
                    this.file = "";
                }
            } else {
                this.file = file;
            }

            if (verbose) {
                println("Arguments:");
                println("  -   url=%s", this.url);
                println("  - query=%s", this.query);
                println("  -  file=%s", this.file);
                println();
                println("Options: action=%s, batch=%d, samples=%d, serde=%s, type=%s, verbose=%s", action, batch,
                        samples, serde, type, verbose);
            }
        }

        int getSamples() {
            final int s;
            if (isMixed() || isTuple() || isNested()) {
                s = samples / 5;
            } else if (isArray()) {
                s = samples / 1000;
            } else if (isJson()) {
                s = samples / 500;
            } else {
                s = samples;
            }
            return s;
        }

        boolean hasFile() {
            return !file.isEmpty();
        }

        boolean isDumpAction() {
            return "dump".equals(action);
        }

        boolean isLoadAction() {
            return "load".equals(action);
        }

        boolean isWriteAction() {
            return "write".equals(action);
        }

        boolean isInt8() {
            return "int8".equals(type);
        }

        boolean isUInt64() {
            return "uint64".equals(type);
        }

        boolean isString() {
            return "string".equals(type);
        }

        boolean isDateTime() {
            return "datetime".equals(type);
        }

        boolean isDecimal() {
            return "decimal".equals(type);
        }

        boolean isMixed() {
            return "mixed".equals(type);
        }

        boolean isArray() {
            return "array".equals(type);
        }

        boolean isTuple() {
            return "tuple".equals(type);
        }

        boolean isNested() {
            return "nested".equals(type);
        }

        boolean isJson() {
            return "json".equals(type);
        }

        List<ClickHouseColumn> getColumns() {
            final List<ClickHouseColumn> columns;
            if (isInt8()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.Int8, false));
            } else if (isUInt64()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.UInt64, false));
            } else if (isString()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.String, false));
            } else if (isDateTime()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.DateTime, false));
            } else if (isDecimal()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.Decimal128, false, 0, 6));
            } else if (isMixed()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.Int8, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.UInt64, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.String, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.DateTime, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.Decimal128, false, 0, 6));
            } else if (isArray()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, "Array(Int32)"));
            } else if (isTuple()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.Tuple, false,
                        ClickHouseColumn.of(null, ClickHouseDataType.Int8, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.UInt64, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.String, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.DateTime, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.Decimal128, false, 0, 6)));
            } else if (isNested()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.Nested, false,
                        ClickHouseColumn.of(null, ClickHouseDataType.Int8, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.UInt64, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.String, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.DateTime, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.Decimal128, false, 0, 6)));
            } else if (isJson()) {
                columns = Arrays.asList(ClickHouseColumn.of(null, ClickHouseDataType.Tuple, false,
                        ClickHouseColumn.of(null, ClickHouseDataType.Int8, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.UInt64, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.String, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.DateTime, false),
                        ClickHouseColumn.of(null, ClickHouseDataType.Decimal128, false, 0, 6)));
            } else {
                columns = null;
            }
            return columns;
        }

        ClickHouseDeserializer getDerializer(ClickHouseConfig config) throws IOException {
            final List<ClickHouseColumn> columns = getColumns();
            if (columns == null || columns.isEmpty()) {
                throw new IllegalStateException("Not column information available for query: " + query);
            }

            final ClickHouseDataProcessor processor = ClickHouseDataStreamFactory.getInstance().getProcessor(config,
                    null, ClickHouseOutputStream.empty(), null, columns);
            final ClickHouseDeserializer[] deserializers = processor.getDeserializers(config, columns);
            return deserializers.length == 1 ? deserializers[0]
                    : ClickHouseDeserializer.of(Arrays.asList(deserializers));
        }

        ClickHouseSerializer getSerializer(ClickHouseConfig config) throws IOException {
            final List<ClickHouseColumn> columns = getColumns();
            if (columns == null || columns.isEmpty()) {
                throw new IllegalStateException("Not column information available for query: " + query);
            }

            final ClickHouseDataProcessor processor = ClickHouseDataStreamFactory.getInstance().getProcessor(config,
                    null, ClickHouseOutputStream.empty(), null, columns);
            final ClickHouseSerializer[] serializers = processor.getSerializers(config, columns);
            return serializers.length == 1 ? serializers[0] : ClickHouseSerializer.of(Arrays.asList(serializers));
        }

        String getSelectQuery() {
            final String selectQuery;
            if (isInt8()) {
                selectQuery = "select number::Int8 v from numbers(%d)";
            } else if (isUInt64()) {
                selectQuery = "select number v from numbers(%d)";
            } else if (isString()) {
                selectQuery = "select toString(number) v from numbers(%d)";
            } else if (isDateTime()) {
                selectQuery = "select toDateTime(number) v from numbers(%d)";
            } else if (isDecimal()) {
                selectQuery = "select toDecimal128(number, 6) v from numbers(%d)";
            } else if (isMixed()) {
                selectQuery = "select number::Int8 a, number b, toString(number) c, toDateTime(number) d, toDecimal128(number, 6) e from numbers(%d)";
            } else if (isArray()) {
                selectQuery = "select range(100000, 101000 + number % 1000) v from numbers(%d)";
            } else if (isTuple()) {
                selectQuery = "select tuple(number::Int8, number, toString(number), toDateTime(number), toDecimal128(number, 6)) v from numbers(%d)";
            } else if (isNested()) {
                selectQuery = "select [(number::Int8, number, toString(number), toDateTime(number), toDecimal128(number, 6))]::Nested(a Int8, b UInt64, c String, d DateTime, e Decimal128(6)) v from numbers(%d)";
            } else if (isJson()) {
                selectQuery = "select (number::Int8, number, toString(number), toDateTime(number), toDecimal128(number, 6), range(1000,1005), [tuple(number, number+1)])::Tuple(a Int8, b UInt64, c String, d DateTime, e Decimal128(6), f Array(UInt16), g Nested(x UInt64, y UInt64)) v from numbers(%d)";
            } else {
                selectQuery = "select %d";
            }
            return String.format(selectQuery, getSamples());
        }

        String getInsertQuery() {
            return type.isEmpty() ? "insert into test_insert" : "insert into test_insert_" + type;
        }
    }

    static class GenericQuery {
        static final ClickHouseFormat defaultFormat = ClickHouseFormat.RowBinaryWithNamesAndTypes;

        protected final Options options;

        protected GenericQuery(Options options) {
            this.options = options;
        }

        final long run() throws ClickHouseException, SQLException {
            final long rows;
            if (options.isDumpAction()) {
                rows = dump();
            } else if (options.isLoadAction()) {
                rows = load(options);
            } else if (options.isWriteAction()) {
                rows = write(options);
            } else {
                rows = read(options);
            }
            return rows;
        }

        long read(ResultSet rs) throws SQLException {
            long count = 0L;
            final int len = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Object obj = null;
                for (int i = 1; i <= len; i++) {
                    // autoboxing to ensure we "got" the value
                    obj = rs.getObject(i);
                }
                if (obj != null) {
                    count++;
                }
            }
            return count;
        }

        long read(ClickHouseResponse response) throws ClickHouseException {
            long count = 0L;
            int len = response.getColumns().size();
            for (ClickHouseRecord r : response.records()) {
                Object obj = null;
                for (int i = 0; i < len; i++) {
                    // autoboxing just for comparison
                    obj = r.getValue(i).asObject();
                }
                if (obj != null) {
                    count++;
                }
            }
            return count;
        }

        long write(Connection conn) throws SQLException {
            throw new UnsupportedOperationException("No idea how to write data for custom query");
        }

        long write(Mutation request) throws ClickHouseException {
            throw new UnsupportedOperationException("No idea how to write data for custom query");
        }

        final long dump() throws ClickHouseException, SQLException {
            final long rows;
            if (options.requiresJdbc) {
                try (ClickHouseConnection conn = new ClickHouseConnectionImpl(options.url)) {
                    ClickHouseRequest<?> request = conn.unwrap(ClickHouseRequest.class).query(options.query);
                    if (!request.getServer().getConfig().hasOption(ClickHouseClientOption.FORMAT)) {
                        request.format(defaultFormat);
                    }
                    request.output(options.file);
                    try (ClickHouseResponse response = request.executeAndWait()) {
                        rows = response.getSummary().getReadRows();
                    }
                }
            } else { // java client
                final ClickHouseNode server = ClickHouseNode.of(options.url);
                try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
                    ClickHouseRequest<?> request = client.read(server).query(options.query);
                    if (!server.getConfig().hasOption(ClickHouseClientOption.FORMAT)) {
                        request.format(defaultFormat);
                    }
                    request.output(options.file);
                    try (ClickHouseResponse response = request.query(options.query).executeAndWait()) {
                        rows = response.getSummary().getReadRows();
                    }
                }
            }
            return rows;
        }

        final long load(Options options) throws ClickHouseException, SQLException {
            final long rows;
            if (options.requiresJdbc) {
                try (ClickHouseConnection conn = new ClickHouseConnectionImpl(options.url)) {
                    ClickHouseFormat format = conn.getConfig().getFormat();
                    if (!conn.unwrap(ClickHouseRequest.class).getServer().getConfig()
                            .hasOption(ClickHouseClientOption.FORMAT)) {
                        format = defaultFormat.defaultInputFormat();
                    }
                    try (PreparedStatement stmt = conn.prepareStatement(options.query + " format " + format.name())) { // NOSONAR
                        stmt.setObject(1, new File(options.file));
                        rows = stmt.executeLargeUpdate();
                    }
                }
            } else { // java client
                final ClickHouseNode server = ClickHouseNode.of(options.url);
                try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
                    Mutation request = client.write(server).data(options.file);
                    if (!server.getConfig().hasOption(ClickHouseClientOption.FORMAT)) {
                        request.format(defaultFormat.defaultInputFormat());
                    }
                    try (ClickHouseResponse response = request
                            .query(options.query + " format " + request.getConfig().getFormat().name())
                            .executeAndWait()) {
                        rows = response.getSummary().getWrittenRows();
                    }
                }
            }
            return rows;
        }

        final long read(Options options) throws ClickHouseException, SQLException {
            final long rows;
            if (options.requiresJdbc) {
                try (ClickHouseConnection conn = new ClickHouseConnectionImpl(options.url);
                        ClickHouseStatement stmt = conn.createStatement()) {
                    if (options.hasFile()) {
                        try {
                            stmt.setMirroredOutput(
                                    !"-".equals(options.file) ? new FileOutputStream(options.file, false) : System.out); // NOSONAR
                        } catch (IOException e) {
                            throw SqlExceptionUtils.clientError(e);
                        }
                    }
                    try (ResultSet rs = stmt.executeQuery(options.query)) {
                        rows = read(rs);
                    }
                }
            } else { // java client
                final ClickHouseNode server = ClickHouseNode.of(options.url);
                try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
                    ClickHouseRequest<?> request = client.read(server).query(options.query);
                    if (!server.getConfig().hasOption(ClickHouseClientOption.FORMAT)) {
                        request.format(defaultFormat);
                    }
                    try (ClickHouseResponse response = request.executeAndWait()) {
                        if (options.hasFile()) {
                            try {
                                response.getInputStream().setCopyToTarget(
                                        !"-".equals(options.file) ? new FileOutputStream(options.file, false)
                                                : System.out); // NOSONAR
                            } catch (IOException e) {
                                throw ClickHouseException.of(e, server);
                            }
                        }
                        rows = read(response);
                    }
                }
            }
            return rows;
        }

        final long write(Options options) throws ClickHouseException, SQLException {
            final long rows;
            if (options.requiresJdbc) {
                try (ClickHouseConnection conn = new ClickHouseConnectionImpl(options.url)) {
                    rows = write(conn);
                }
            } else { // java client
                final ClickHouseNode server = ClickHouseNode.of(options.url);
                try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
                    Mutation request = client.write(server).query(options.query).data(options.file);
                    if (!server.getConfig().hasOption(ClickHouseClientOption.FORMAT)) {
                        request.format(defaultFormat.defaultInputFormat());
                    }
                    rows = write(request);
                }
            }
            return rows;
        }
    }

    static class Int8Query extends GenericQuery {
        Int8Query(Options options) {
            super(options);
        }

        @Override
        long read(ResultSet rs) throws SQLException {
            long count = 0L;
            final int len = rs.getMetaData().getColumnCount();
            byte v = (byte) 0;
            while (rs.next()) {
                for (int i = 1; i <= len; i++) {
                    v = rs.getByte(i);
                }
                count++;
            }
            long lastValue = 0xFFL & v;
            return count >= lastValue ? count : lastValue;
        }

        @Override
        long read(ClickHouseResponse response) throws ClickHouseException {
            long count = 0L;
            byte v = (byte) 0;
            if (options.serde) {
                if (options.verbose) {
                    println("Deserialization: records");
                }
                for (ClickHouseRecord r : response.records()) {
                    // only one column
                    v = r.getValue(0).asByte();
                    count++;
                }
            } else {
                if (options.verbose) {
                    println("Deserialization: readByte");
                }
                try (ClickHouseInputStream in = response.getInputStream()) {
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        v = in.readByte();
                        count++;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            long lastValue = 0xFFL & v;
            return count >= lastValue ? count : lastValue;
        }

        @Override
        long write(Connection conn) throws SQLException {
            try (PreparedStatement stmt = conn.prepareStatement(options.query)) {
                final int batchSize = options.batch;
                long count = 0L;
                long rows = 0L;
                for (long i = 0, len = options.getSamples(); i < len; i++) {
                    stmt.setByte(1, (byte) i);
                    stmt.addBatch();
                    if ((count = (i + 1) % batchSize) == 0L) {
                        rows += stmt.executeLargeBatch().length;
                    }
                }
                if (count > 0L) {
                    rows += stmt.executeLargeBatch().length;
                }
                return rows;
            }
        }

        @Override
        long write(Mutation request) throws ClickHouseException {
            try (ClickHouseResponse response = request.data(o -> {
                if (options.serde) {
                    ClickHouseConfig config = request.getConfig();
                    ClickHouseSerializer serializer = options.getSerializer(config);
                    ClickHouseValue value = ClickHouseByteValue.ofNull();
                    if (options.verbose) {
                        println("Serialization: %s -> %s", serializer, value);
                    }
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        serializer.serialize(value.update(i), o);
                    }
                } else {
                    if (options.verbose) {
                        println("Serialization: writeByte");
                    }
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        o.writeByte((byte) i);
                    }
                }
            }).executeAndWait()) {
                return response.getSummary().getWrittenRows();
            }
        }
    }

    static class UInt64Query extends GenericQuery {
        UInt64Query(Options options) {
            super(options);
        }

        @Override
        long read(ResultSet rs) throws SQLException {
            long count = 0L;
            final int len = rs.getMetaData().getColumnCount();
            long v = 0L;
            while (rs.next()) {
                for (int i = 1; i <= len; i++) {
                    v = rs.getLong(i);
                }
                count++;
            }
            return count >= v ? count : v;
        }

        @Override
        long read(ClickHouseResponse response) throws ClickHouseException {
            long count = 0L;
            long v = 0L;
            if (options.serde) {
                if (options.verbose) {
                    println("Deserialization: records");
                }
                for (ClickHouseRecord r : response.records()) {
                    // only one column
                    v = r.getValue(0).asLong();
                    count++;
                }
            } else {
                if (options.verbose) {
                    println("Deserialization: readByte");
                }
                try (ClickHouseInputStream in = response.getInputStream()) {
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        v = in.readBuffer(8).asLong();
                        count++;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return count >= v ? count : v;
        }

        @Override
        long write(Connection conn) throws SQLException {
            try (PreparedStatement stmt = conn.prepareStatement(options.query)) {
                final int batchSize = options.batch;
                long count = 0L;
                long rows = 0L;
                for (long i = 0, len = options.getSamples(); i < len; i++) {
                    stmt.setLong(1, i);
                    stmt.addBatch();
                    if ((count = (i + 1) % batchSize) == 0L) {
                        rows += stmt.executeLargeBatch().length;
                    }
                }
                if (count > 0L) {
                    rows += stmt.executeLargeBatch().length;
                }
                return rows;
            }
        }

        @Override
        long write(Mutation request) throws ClickHouseException {
            try (ClickHouseResponse response = request.data(o -> {
                if (options.serde) {
                    ClickHouseConfig config = request.getConfig();
                    ClickHouseSerializer serializer = options.getSerializer(config);
                    ClickHouseValue value = ClickHouseLongValue.ofUnsignedNull();
                    if (options.verbose) {
                        println("Serialization: %s -> %s", serializer, value);
                    }
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        serializer.serialize(value.update(i), o);
                    }
                } else {
                    if (options.verbose) {
                        println("Serialization: writeLong");
                    }
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        BinaryStreamUtils.writeUnsignedInt64(o, i);
                    }
                }
            }).executeAndWait()) {
                return response.getSummary().getWrittenRows();
            }
        }
    }

    static class StringQuery extends GenericQuery {
        StringQuery(Options options) {
            super(options);
        }

        @Override
        long read(ResultSet rs) throws SQLException {
            long count = 0L;
            final int len = rs.getMetaData().getColumnCount();
            String v = null;
            while (rs.next()) {
                for (int i = 1; i <= len; i++) {
                    v = rs.getString(i);
                }
                count++;
            }
            return v != null ? count : 0L;
        }

        @Override
        long read(ClickHouseResponse response) throws ClickHouseException {
            long count = 0L;
            String v = null;
            if (options.serde) {
                if (options.verbose) {
                    println("Deserialization: records");
                }
                for (ClickHouseRecord r : response.records()) {
                    // only one column
                    v = r.getValue(0).asString();
                    count++;
                }
            } else {
                if (options.verbose) {
                    println("Deserialization: readByte");
                }
                try (ClickHouseInputStream in = response.getInputStream()) {
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        v = in.readUnicodeString();
                        count++;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return v != null ? count : 0L;
        }

        @Override
        long write(Connection conn) throws SQLException {
            try (PreparedStatement stmt = conn.prepareStatement(options.query)) {
                final int batchSize = options.batch;
                long count = 0L;
                long rows = 0L;
                for (long i = 0, len = options.getSamples(); i < len; i++) {
                    stmt.setString(1, Long.toString(i));
                    stmt.addBatch();
                    if ((count = (i + 1) % batchSize) == 0L) {
                        rows += stmt.executeLargeBatch().length;
                    }
                }
                if (count > 0L) {
                    rows += stmt.executeLargeBatch().length;
                }
                return rows;
            }
        }

        @Override
        long write(Mutation request) throws ClickHouseException {
            try (ClickHouseResponse response = request.data(o -> {
                if (options.serde) {
                    ClickHouseConfig config = request.getConfig();
                    ClickHouseSerializer serializer = options.getSerializer(config);
                    ClickHouseValue value = ClickHouseStringValue.ofNull();
                    if (options.verbose) {
                        println("Serialization: %s -> %s", serializer, value);
                    }
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        serializer.serialize(value.update(i), o);
                    }
                } else {
                    if (options.verbose) {
                        println("Serialization: writeString");
                    }
                    for (long i = 0L, len = options.samples; i < len; i++) {
                        o.writeUnicodeString(Long.toString(i));
                    }
                }
            }).executeAndWait()) {
                return response.getSummary().getWrittenRows();
            }
        }
    }

    private static void println() {
        System.out.println(); // NOSONAR
    }

    private static void println(Object msg, Object... args) {
        if (args == null || args.length == 0) {
            System.out.println(msg); // NOSONAR
        } else {
            System.out.println(String.format(Locale.ROOT, Objects.toString(msg), args)); // NOSONAR
        }
    }

    private static void printUsage() {
        String execFile = "clickhouse-jdbc-bin";
        try {
            File file = Paths.get(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toFile();
            if (file.isFile()) {
                execFile = file.getName();
                if (!Files.isExecutable(file.toPath())) {
                    execFile = "java -jar " + execFile;
                }
            } else {
                execFile = "java -cp " + file.getCanonicalPath() + " " + Main.class.getName();
            }
        } catch (Exception e) {
            // ignore
        }

        final int index = execFile.indexOf(' ');
        println("Usage: %s <URL> [QUERY] [FILE]",
                index > 0 ? (execFile.substring(0, index) + " [PROPERTIES]" + execFile.substring(index))
                        : (execFile + " [PROPERTIES]"));
        println();
        println("Properties: -Dkey=value [-Dkey=value]*");
        println("  action \tAction, one of read(default), write, dump(no deserialization), and load(no serialization)");
        println("  batch  \tBatch size for JDBC writing, defaults to 1000");
        println("  output \tWhether to write raw response into stdout or a file(java.out or jdbc.out), defaults to false");
        println("  samples\tSamples, defaults to 500000000");
        println("  serde  \tWhether to use default serialization/deserializion mechanism in Java client, defaults to true");
        println("  type   \tPredefined QUERY, one of Int8, UInt64, String, Array, Tuple, Nested, and Mixed");
        println("  verbose\tWhether to show logs, defaults to false");
        println();
        println("Examples:");
        println("  -  %s 'https://localhost?sslmode=none' 'select 1' -",
                index > 0 ? (execFile.substring(0, index) + " -Dverbose=true" + execFile.substring(index))
                        : (execFile + " -Dverbose=true"));
        println("  -  %s 'jdbc:ch://user:password@localhost:8123/default' 'select 1' output.file", execFile);
        println("  -  %s 'jdbc:ch:http://node1,node2,node3/default' 'insert into table1' input.file", execFile);
    }

    public static void main(String[] args) throws Exception {
        if ((args == null || args.length < 1) || args.length > 3) {
            printUsage();
            System.exit(0);
        }

        final Options options = new Options(args[0].trim(), args.length > 1 ? args[1].trim() : null,
                args.length > 2 ? args[2].trim() : null);

        final GenericQuery query;
        if (options.isInt8()) {
            query = new Int8Query(options);
        } else if (options.isUInt64()) {
            query = new UInt64Query(options);
        } else if (options.isString()) {
            query = new StringQuery(options);
        } else {
            query = new GenericQuery(options);
        }

        final long startTime = options.verbose ? System.nanoTime() : 0L;
        final long rows = query.run();
        if (options.verbose) {
            long elapsedNanos = System.nanoTime() - startTime;
            println("\nProcessed %,d rows in %,.2f ms (%,.2f rows/s)", rows, elapsedNanos / 1_000_000D,
                    rows * 1_000_000_000D / elapsedNanos);
        }
        System.exit(rows > 0L ? 0 : 1);
    }

    private Main() {
    }
}