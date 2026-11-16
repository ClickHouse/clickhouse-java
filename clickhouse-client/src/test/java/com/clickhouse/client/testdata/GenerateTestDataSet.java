package com.clickhouse.client.testdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Command line entry point writing a {@link TestDataSet} to a file.
 *
 * <p>Each column is given as a ClickHouse column definition {@code "name Type"} or just {@code Type}
 * (named {@code c1}, {@code c2}, ...). Types are ClickHouse type expressions, for example
 * {@code "price Nullable(Decimal(18, 4))"} or {@code "Map(String, Array(UInt8))"}. Run without arguments
 * to see all options and supported types:
 *
 * <pre>
 * mvn -pl clickhouse-client test-compile
 * java -cp clickhouse-client/target/test-classes com.clickhouse.client.testdata.GenerateTestDataSet \
 *     -o data.sql Int32 "s Nullable(String)" "dt DateTime64(3, 'UTC')"
 * </pre>
 */
public final class GenerateTestDataSet {

    public static final String INDEX_RESOURCE = "/datasets/index.tsv";
    public static final String KIND_STORABLE = "storable";
    public static final String KIND_VALUES_ONLY = "values-only";
    private static final String INDEX_HEADER = "name\ttype\tclickhouse_type\tkind";

    /**
     * Storable types supported for pre-generated data sets.
     */
    private static final List<String> STORABLE_TYPES = Collections.unmodifiableList(Arrays.asList(
            // Integers and floating point
            "Int8", "UInt8", "Int16", "UInt16", "Int32", "UInt32", "Int64", "UInt64",
            "Int128", "UInt128", "Int256", "UInt256",
            "Float32", "Float64",
            // Decimals
            "Decimal(9, 2)", "Decimal(18, 6)", "Decimal(38, 0)", "Decimal(76, 38)",
            "Decimal32(4)", "Decimal64(8)", "Decimal128(18)", "Decimal256(38)",
            // Bool and strings
            "Bool", "String", "BinaryString", "FixedString(1)", "FixedString(16)",
            // Dates and time
            "Date", "Date32", "Time", "Time64(3)", "Time64(9)",
            "DateTime('UTC')", "DateTime('America/New_York')",
            "DateTime64(0, 'UTC')", "DateTime64(3, 'UTC')", "DateTime64(6, 'Europe/Berlin')",
            "DateTime64(9, 'Asia/Kolkata')",
            // Network, identifiers and enums
            "IPv4", "IPv6", "UUID", "Enum8", "Enum8All", "Enum8Single", "Enum8Unsorted",
            "Enum16", "Enum16Many", "EnumNearMax8", "EnumNearMax16",
            // Geo
            "Point", "Ring", "LineString", "Polygon", "MultiLineString", "MultiPolygon",
            // Composite
            "Nullable(Int32)", "Nullable(String)", "Nullable(Decimal(18, 6))", "Nullable(DateTime64(3, 'UTC'))",
            "Nullable(UUID)",
            "LowCardinality(String)", "LowCardinality(Nullable(String))", "LowCardinality(FixedString(16))",
            "Array(Int32)", "Array(String)", "Array(Nullable(String))", "Array(Array(Int8))",
            "Array(LowCardinality(String))",
            "Tuple(Int32, String)", "Tuple(a Int64, b Nullable(String), c Array(UInt8))",
            "Map(String, Int32)", "Map(Int8, String)", "Map(String, Array(Nullable(Float64)))",
            "Map(UUID, Tuple(Int32, String))",
            "Nested(id UInt32, name String)"));

    /**
     * Types supported for pre-generated data sets that cannot be stored in a table.
     */
    private static final List<String> VALUES_ONLY_TYPES = Collections.unmodifiableList(Arrays.asList(
            "IntervalNanosecond", "IntervalMicrosecond", "IntervalMillisecond", "IntervalSecond", "IntervalMinute",
            "IntervalHour", "IntervalDay", "IntervalWeek", "IntervalMonth", "IntervalQuarter", "IntervalYear",
            "Nothing", "Array(Nullable(Nothing))"));

    private static final Pattern NAMED_ELEMENT = Pattern.compile(
            "(`(?:[^`\\\\]|\\\\.)*`|[A-Za-z_][A-Za-z0-9_]*)\\s+(\\S.*)",
            Pattern.DOTALL);
    private static final Pattern ENUM_CONSTANT = Pattern.compile("('(?:[^'\\\\]|\\\\.)*')\\s*=\\s*(-?\\d+)",
            Pattern.DOTALL);

    private static final String USAGE = String.join("\n",
            "Usage: GenerateTestDataSet [options] <column>...",
            "",
            "Columns:",
            "  \"name Type\" or Type     ClickHouse column definition; unnamed columns are named c1, c2, ...",
            "",
            "Options:",
            "  -o, --output <file>      output file (default: stdout)",
            "  -f, --format <format>    sql    - settings, CREATE TABLE and INSERT statements (default)",
            "                           values - rows as SQL tuples only",
            "                           tsv    - rows in the TabSeparated format, as ClickHouse writes them",
            "  -t, --table <name>       table name for the sql format (default: test_data)",
            "  -s, --seed <long>        random seed (default: " + TestDataSet.DEFAULT_SEED + ")",
            "  --list-types <kind>      print types supported for pre-generated data sets, one per line, and exit:",
            "                           storable or values-only",
            "  --list-index             print tab-separated index rows (name, type, clickhouse_type, kind) and exit",
            "  -h, --help               print this help",
            "",
            "Types:",
            "  Int8..Int256, UInt8..UInt256, Float32, Float64, Bool,",
            "  Decimal(P[, S]), Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S),",
            "  String, BinaryString (String with arbitrary bytes), FixedString(N),",
            "  Date, Date32, Time, Time64(P), DateTime[('tz')], DateTime64(P[, 'tz']) (default tz: UTC),",
            "  IPv4, IPv6, UUID, Enum8, Enum8All, Enum8Single, Enum8Unsorted, Enum16, Enum16Many,",
            "  EnumNearMax8, EnumNearMax16, Enum('a' = 1, ...), Enum8('a' = 1, ...), Enum16('a' = 1, ...),",
            "  Point, Ring, LineString, Polygon, MultiLineString, MultiPolygon,",
            "  Array(T), Tuple(T, ...), Tuple(name T, ...), Map(K, V), Nested(name T, ...),",
            "  Nullable(T), LowCardinality(T),",
            "  Interval<Unit>, Nothing (values and tsv formats only, not storable)",
            "",
            "Example:",
            "  GenerateTestDataSet -o data.sql Int32 \"s Nullable(String)\" \"dt DateTime64(3, 'UTC')\"");

    private GenerateTestDataSet() {
    }

    public static void main(String[] args) throws IOException {
        String output = null;
        String format = "sql";
        String table = "test_data";
        TestDataSet.Builder builder = TestDataSet.builder();
        int columns = 0;
        try {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "-h":
                    case "--help":
                        System.out.println(USAGE);
                        return;
                    case "-o":
                    case "--output":
                        output = optionValue(args, ++i, arg);
                        break;
                    case "-f":
                    case "--format":
                        format = optionValue(args, ++i, arg);
                        if (!"sql".equals(format) && !"values".equals(format) && !"tsv".equals(format)) {
                            throw new IllegalArgumentException("Unknown format: " + format);
                        }
                        break;
                    case "-t":
                    case "--table":
                        table = optionValue(args, ++i, arg);
                        break;
                    case "-s":
                    case "--seed":
                        builder.seed(Long.parseLong(optionValue(args, ++i, arg)));
                        break;
                    case "--list-types":
                        String kind = optionValue(args, ++i, arg);
                        if (!KIND_STORABLE.equals(kind) && !KIND_VALUES_ONLY.equals(kind)) {
                            throw new IllegalArgumentException("Unknown type kind: " + kind);
                        }
                        for (String type : KIND_STORABLE.equals(kind) ? STORABLE_TYPES : VALUES_ONLY_TYPES) {
                            System.out.println(type);
                        }
                        return;
                    case "--list-index":
                        printIndex(STORABLE_TYPES, KIND_STORABLE);
                        printIndex(VALUES_ONLY_TYPES, KIND_VALUES_ONLY);
                        return;
                    default:
                        if (arg.startsWith("-")) {
                            throw new IllegalArgumentException("Unknown option: " + arg);
                        }
                        columns++;
                        Matcher named = namedElement(arg);
                        if (named != null) {
                            builder.column(elementName(named), parseType(named.group(2)));
                        } else {
                            builder.column("c" + columns, parseType(arg));
                        }
                }
            }
            if (columns == 0) {
                throw new IllegalArgumentException("At least one column is required");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println();
            System.err.println(USAGE);
            System.exit(1);
            return;
        }

        TestDataSet dataSet = builder.build();
        // TSV text is a byte string with UTF-8 already encoded, see DataTypeGenerator.toTsv
        Charset charset = "tsv".equals(format) ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8;
        try (Writer writer = output == null
                ? new OutputStreamWriter(System.out, charset)
                : Files.newBufferedWriter(Paths.get(output), charset)) {
            write(dataSet, format, table, new PrintWriter(writer));
        }
        if (output != null) {
            System.err.println("Wrote " + dataSet + " to " + output);
        }
    }

    static void write(TestDataSet dataSet, String format, String table, PrintWriter out) {
        if ("values".equals(format)) {
            out.println(dataSet.getValuesClause());
        } else if ("tsv".equals(format)) {
            out.print(dataSet.getTsv());
        } else {
            out.println("-- " + dataSet);
            for (Map.Entry<String, String> setting : dataSet.getRequiredSettings().entrySet()) {
                out.println("SET " + setting.getKey() + " = " + setting.getValue() + ";");
            }
            out.println(dataSet.getCreateTableSql(table) + ";");
            out.println(dataSet.getInsertSql(table) + ";");
        }
        out.flush();
    }

    /**
     * Reads the index of pre-generated data sets from {@value #INDEX_RESOURCE}.
     *
     * @return immutable list of entries in index order
     */
    public static List<DataSetEntry> readDataSetIndex() {
        try (InputStream in = GenerateTestDataSet.class.getResourceAsStream(INDEX_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Dataset index resource not found: " + INDEX_RESOURCE);
            }
            return readDataSetIndex(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read dataset index: " + INDEX_RESOURCE, e);
        }
    }

    /**
     * Reads a dataset index file in TSV format ({@code name}, {@code type}, {@code clickhouse_type}, {@code kind}).
     *
     * @param indexFile path to the index file
     * @return immutable list of entries in index order
     * @throws IOException if reading fails
     */
    public static List<DataSetEntry> readDataSetIndex(Path indexFile) throws IOException {
        try (InputStream in = Files.newInputStream(indexFile)) {
            return readDataSetIndex(in);
        }
    }

    /**
     * Reads a dataset index from a UTF-8 TSV stream.
     *
     * @param in input stream to read from
     * @return immutable list of entries in index order
     * @throws IOException if reading fails
     */
    public static List<DataSetEntry> readDataSetIndex(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        String header = reader.readLine();
        if (header == null) {
            throw new IllegalArgumentException("Dataset index is empty");
        }
        if (!INDEX_HEADER.equals(header)) {
            throw new IllegalArgumentException("Invalid dataset index header: " + header);
        }
        List<DataSetEntry> entries = new ArrayList<>();
        String line;
        int lineNumber = 1;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (line.isEmpty()) {
                continue;
            }
            String[] fields = line.split("\t", -1);
            if (fields.length != 4) {
                throw new IllegalArgumentException(
                        "Expected 4 tab-separated fields at line " + lineNumber + ": " + line);
            }
            for (String field : fields) {
                if (field.isEmpty()) {
                    throw new IllegalArgumentException("Empty field at line " + lineNumber + ": " + line);
                }
            }
            if (!KIND_STORABLE.equals(fields[3]) && !KIND_VALUES_ONLY.equals(fields[3])) {
                throw new IllegalArgumentException(
                        "Unknown dataset kind at line " + lineNumber + ": " + fields[3]);
            }
            entries.add(new DataSetEntry(fields[0], fields[1], fields[2], fields[3]));
        }
        return Collections.unmodifiableList(entries);
    }

    /**
     * Returns the generator type expressions for all storable pre-generated data sets listed in
     * {@value #INDEX_RESOURCE}.
     */
    public static List<String> getStorableTypes() {
        return getTypes(KIND_STORABLE);
    }

    /**
     * Returns the generator type expressions for all values-only pre-generated data sets listed in
     * {@value #INDEX_RESOURCE}.
     */
    public static List<String> getValuesOnlyTypes() {
        return getTypes(KIND_VALUES_ONLY);
    }

    private static List<String> getTypes(String kind) {
        List<String> types = new ArrayList<>();
        for (DataSetEntry entry : readDataSetIndex()) {
            if (kind.equals(entry.getKind())) {
                types.add(entry.getType());
            }
        }
        return Collections.unmodifiableList(types);
    }

    static String datasetName(String type) {
        return type.replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_|_$", "");
    }

    private static void printIndex(List<String> types, String kind) {
        for (String type : types) {
            String name = datasetName(type);
            String clickHouseType = parseType(type).getType();
            System.out.println(name + "\t" + type + "\t" + clickHouseType + "\t" + kind);
        }
    }

    /**
     * Entry in {@value #INDEX_RESOURCE} describing one pre-generated data set.
     */
    public static final class DataSetEntry {
        private final String name;
        private final String type;
        private final String clickHouseType;
        private final String kind;

        public DataSetEntry(String name, String type, String clickHouseType, String kind) {
            this.name = Objects.requireNonNull(name, "name");
            this.type = Objects.requireNonNull(type, "type");
            this.clickHouseType = Objects.requireNonNull(clickHouseType, "clickHouseType");
            this.kind = Objects.requireNonNull(kind, "kind");
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public String getClickHouseType() {
            return clickHouseType;
        }

        public String getKind() {
            return kind;
        }

        public boolean isStorable() {
            return KIND_STORABLE.equals(kind);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DataSetEntry)) {
                return false;
            }
            DataSetEntry that = (DataSetEntry) o;
            return name.equals(that.name) && type.equals(that.type)
                    && clickHouseType.equals(that.clickHouseType) && kind.equals(that.kind);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, clickHouseType, kind);
        }

        @Override
        public String toString() {
            return name + " (" + type + " -> " + clickHouseType + ", " + kind + ")";
        }
    }

    /**
     * Parses a ClickHouse type expression into a generator.
     *
     * @param expression type expression, for example {@code Array(Nullable(String))}
     * @return generator of the type
     * @throws IllegalArgumentException when the type is unknown or malformed
     */
    public static DataTypeGenerator parseType(String expression) {
        String type = expression.trim();
        int open = type.indexOf('(');
        String name = open < 0 ? type : type.substring(0, open).trim();
        List<String> args = new ArrayList<>();
        if (open >= 0) {
            if (!type.endsWith(")")) {
                throw new IllegalArgumentException("Missing closing parenthesis: " + expression);
            }
            args = splitArguments(type.substring(open + 1, type.length() - 1));
        }
        switch (name) {
            case "Int8":
                return noArgs(args, type, DataTypeGenerators.int8());
            case "UInt8":
                return noArgs(args, type, DataTypeGenerators.uint8());
            case "Int16":
                return noArgs(args, type, DataTypeGenerators.int16());
            case "UInt16":
                return noArgs(args, type, DataTypeGenerators.uint16());
            case "Int32":
                return noArgs(args, type, DataTypeGenerators.int32());
            case "UInt32":
                return noArgs(args, type, DataTypeGenerators.uint32());
            case "Int64":
                return noArgs(args, type, DataTypeGenerators.int64());
            case "UInt64":
                return noArgs(args, type, DataTypeGenerators.uint64());
            case "Int128":
                return noArgs(args, type, DataTypeGenerators.int128());
            case "UInt128":
                return noArgs(args, type, DataTypeGenerators.uint128());
            case "Int256":
                return noArgs(args, type, DataTypeGenerators.int256());
            case "UInt256":
                return noArgs(args, type, DataTypeGenerators.uint256());
            case "Float32":
                return noArgs(args, type, DataTypeGenerators.float32());
            case "Float64":
                return noArgs(args, type, DataTypeGenerators.float64());
            case "Decimal":
                checkArgCount(args, 1, 2, type);
                return DataTypeGenerators.decimal(intArg(args, 0), args.size() > 1 ? intArg(args, 1) : 0);
            case "Decimal32":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.decimal32(intArg(args, 0));
            case "Decimal64":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.decimal64(intArg(args, 0));
            case "Decimal128":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.decimal128(intArg(args, 0));
            case "Decimal256":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.decimal256(intArg(args, 0));
            case "Bool":
                return noArgs(args, type, DataTypeGenerators.bool());
            case "String":
                return noArgs(args, type, DataTypeGenerators.string());
            case "BinaryString":
                return noArgs(args, type, DataTypeGenerators.binaryString());
            case "FixedString":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.fixedString(intArg(args, 0));
            case "Date":
                return noArgs(args, type, DataTypeGenerators.date());
            case "Date32":
                return noArgs(args, type, DataTypeGenerators.date32());
            case "Time":
                return noArgs(args, type, DataTypeGenerators.time());
            case "Time64":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.time64(intArg(args, 0));
            case "DateTime":
                checkArgCount(args, 0, 1, type);
                return DataTypeGenerators.dateTime(args.isEmpty() ? "UTC" : stringArg(args, 0));
            case "DateTime64":
                checkArgCount(args, 1, 2, type);
                return DataTypeGenerators.dateTime64(intArg(args, 0), args.size() > 1 ? stringArg(args, 1) : "UTC");
            case "IPv4":
                return noArgs(args, type, DataTypeGenerators.ipv4());
            case "IPv6":
                return noArgs(args, type, DataTypeGenerators.ipv6());
            case "UUID":
                return noArgs(args, type, DataTypeGenerators.uuid());
            case "Enum":
                return args.isEmpty() ? DataTypeGenerators.enumNearMax8() : enumeration(name, args);
            case "Enum8":
                return args.isEmpty() ? DataTypeGenerators.enum8() : enumeration(name, args);
            case "Enum8All":
                return noArgs(args, type, DataTypeGenerators.enum8All());
            case "Enum8Single":
                return noArgs(args, type, DataTypeGenerators.enum8Single());
            case "Enum8Unsorted":
                return noArgs(args, type, DataTypeGenerators.enum8Unsorted());
            case "Enum16":
                return args.isEmpty() ? DataTypeGenerators.enum16() : enumeration(name, args);
            case "Enum16Many":
                return noArgs(args, type, DataTypeGenerators.enum16Many());
            case "EnumNearMax8":
                return noArgs(args, type, DataTypeGenerators.enumNearMax8());
            case "EnumNearMax16":
                return noArgs(args, type, DataTypeGenerators.enumNearMax16());
            case "Nothing":
                return noArgs(args, type, DataTypeGenerators.nothing());
            case "Point":
                return noArgs(args, type, DataTypeGenerators.point());
            case "Ring":
                return noArgs(args, type, DataTypeGenerators.ring());
            case "LineString":
                return noArgs(args, type, DataTypeGenerators.lineString());
            case "Polygon":
                return noArgs(args, type, DataTypeGenerators.polygon());
            case "MultiLineString":
                return noArgs(args, type, DataTypeGenerators.multiLineString());
            case "MultiPolygon":
                return noArgs(args, type, DataTypeGenerators.multiPolygon());
            case "Array":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.array(parseType(args.get(0)));
            case "Tuple":
                return tuple(args, type);
            case "Map":
                checkArgCount(args, 2, 2, type);
                return DataTypeGenerators.map(parseType(args.get(0)), parseType(args.get(1)));
            case "Nested":
                return DataTypeGenerators.nested(namedElements(args, type));
            case "Nullable":
                checkArgCount(args, 1, 1, type);
                if ("Nothing".equals(args.get(0))) {
                    return DataTypeGenerators.nothing();
                }
                return DataTypeGenerators.nullable(parseType(args.get(0)));
            case "LowCardinality":
                checkArgCount(args, 1, 1, type);
                return DataTypeGenerators.lowCardinality(parseType(args.get(0)));
            default:
                if (name.startsWith("Interval")) {
                    return noArgs(args, type, DataTypeGenerators.interval(name.substring("Interval".length())));
                }
                throw new IllegalArgumentException("Unknown type: " + expression);
        }
    }

    private static String optionValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + option);
        }
        return args[index];
    }

    private static DataTypeGenerator noArgs(List<String> args, String type, DataTypeGenerator generator) {
        checkArgCount(args, 0, 0, type);
        return generator;
    }

    private static void checkArgCount(List<String> args, int min, int max, String type) {
        if (args.size() < min || args.size() > max) {
            throw new IllegalArgumentException("Wrong number of type arguments: " + type);
        }
    }

    private static int intArg(List<String> args, int index) {
        try {
            return Integer.parseInt(args.get(index));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected a number: " + args.get(index));
        }
    }

    private static String stringArg(List<String> args, int index) {
        String arg = args.get(index);
        if (arg.length() < 2 || arg.charAt(0) != '\'' || arg.charAt(arg.length() - 1) != '\'') {
            throw new IllegalArgumentException("Expected a quoted string: " + arg);
        }
        return unquote(arg);
    }

    private static DataTypeGenerator enumeration(String type, List<String> args) {
        Map<String, Integer> constants = new LinkedHashMap<>();
        for (String arg : args) {
            Matcher constant = ENUM_CONSTANT.matcher(arg);
            if (!constant.matches()) {
                throw new IllegalArgumentException("Expected 'name' = value: " + arg);
            }
            constants.put(unquote(constant.group(1)), Integer.parseInt(constant.group(2)));
        }
        return DataTypeGenerators.enumeration(type, constants, false);
    }

    private static DataTypeGenerator tuple(List<String> args, String type) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("Tuple must have at least one element: " + type);
        }
        if (namedElement(args.get(0)) != null) {
            return DataTypeGenerators.namedTuple(namedElements(args, type));
        }
        DataTypeGenerator[] elements = new DataTypeGenerator[args.size()];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = parseType(args.get(i));
        }
        return DataTypeGenerators.tuple(elements);
    }

    private static Map<String, DataTypeGenerator> namedElements(List<String> args, String type) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("Type must have at least one element: " + type);
        }
        Map<String, DataTypeGenerator> elements = new LinkedHashMap<>();
        for (String arg : args) {
            Matcher named = namedElement(arg);
            if (named == null) {
                throw new IllegalArgumentException("Expected 'name Type': " + arg);
            }
            String name = elementName(named);
            if (elements.put(name, parseType(named.group(2))) != null) {
                throw new IllegalArgumentException("Duplicate element name: " + name);
            }
        }
        return elements;
    }

    /**
     * @return matcher with the name in group 1 and the type in group 2, or {@code null} when the
     *         definition has no name
     */
    private static Matcher namedElement(String definition) {
        Matcher matcher = NAMED_ELEMENT.matcher(definition.trim());
        return matcher.matches() ? matcher : null;
    }

    private static String elementName(Matcher named) {
        String name = named.group(1);
        return name.charAt(0) == '`' ? unquote(name) : name;
    }

    /**
     * Splits type arguments at top level commas, skipping commas in nested parentheses and quoted strings.
     */
    private static List<String> splitArguments(String args) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean quoted = false;
        for (int i = 0; i < args.length(); i++) {
            char c = args.charAt(i);
            if (quoted) {
                if (c == '\\') {
                    i++;
                } else if (c == '\'') {
                    quoted = false;
                }
            } else if (c == '\'') {
                quoted = true;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                result.add(args.substring(start, i).trim());
                start = i + 1;
            }
            if (depth < 0) {
                throw new IllegalArgumentException("Unbalanced parentheses: " + args);
            }
        }
        if (quoted || depth != 0) {
            throw new IllegalArgumentException("Unbalanced quotes or parentheses: " + args);
        }
        String last = args.substring(start).trim();
        if (!last.isEmpty() || !result.isEmpty()) {
            result.add(last);
        }
        for (String arg : result) {
            if (arg.isEmpty()) {
                throw new IllegalArgumentException("Empty type argument: " + args);
            }
        }
        return result;
    }

    private static String unquote(String literal) {
        StringBuilder value = new StringBuilder(literal.length());
        for (int i = 1; i < literal.length() - 1; i++) {
            char c = literal.charAt(i);
            if (c == '\\' && i + 1 < literal.length() - 1) {
                c = literal.charAt(++i);
            }
            value.append(c);
        }
        return value.toString();
    }
}
