package com.clickhouse.client.testdata;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
            "  -h, --help               print this help",
            "",
            "Types:",
            "  Int8..Int256, UInt8..UInt256, Float32, Float64, Bool,",
            "  Decimal(P[, S]), Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S),",
            "  String, BinaryString (String with arbitrary bytes), FixedString(N),",
            "  Date, Date32, Time, Time64(P), DateTime[('tz')], DateTime64(P[, 'tz']) (default tz: UTC),",
            "  IPv4, IPv6, UUID, Enum8, Enum16, Enum8('a' = 1, ...), Enum16('a' = 1, ...),",
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
     * Parses a ClickHouse type expression into a generator.
     *
     * @param expression type expression, for example {@code Array(Nullable(String))}
     * @return generator of the type
     * @throws IllegalArgumentException when the type is unknown or malformed
     */
    static DataTypeGenerator parseType(String expression) {
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
            case "Enum8":
                return args.isEmpty() ? DataTypeGenerators.enum8() : enumeration(name, args);
            case "Enum16":
                return args.isEmpty() ? DataTypeGenerators.enum16() : enumeration(name, args);
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
