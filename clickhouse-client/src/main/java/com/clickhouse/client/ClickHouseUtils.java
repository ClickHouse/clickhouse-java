package com.clickhouse.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class ClickHouseUtils {
    private static final String HOME_DIR;

    static {
        HOME_DIR = System.getProperty("os.name").toLowerCase().contains("windows")
                ? Paths.get(System.getenv("APPDATA"), "clickhouse").toFile().getAbsolutePath()
                : Paths.get(System.getProperty("user.home"), ".clickhouse").toFile().getAbsolutePath();
    }

    /**
     * Default buffer size.
     */
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    /**
     * Default charset.
     */
    public static final String DEFAULT_CHARSET = StandardCharsets.UTF_8.name();

    /**
     * Maximum buffer size.
     */
    public static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    public static final String VARIABLE_PREFIX = "{{";
    public static final String VARIABLE_SUFFIX = "}}";

    public static String applyVariables(String template, UnaryOperator<String> applyFunc) {
        if (template == null) {
            template = "";
        }

        if (applyFunc == null) {
            return template;
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0, len = template.length(); i < len; i++) {
            int index = template.indexOf(VARIABLE_PREFIX, i);
            if (index != -1) {
                sb.append(template.substring(i, index));

                i = index;
                index = template.indexOf(VARIABLE_SUFFIX, i);

                if (index != -1) {
                    String variable = template.substring(i + VARIABLE_PREFIX.length(), index).trim();
                    String value = applyFunc.apply(variable);
                    if (value == null) {
                        i += VARIABLE_PREFIX.length() - 1;
                        sb.append(VARIABLE_PREFIX);
                    } else {
                        i = index + VARIABLE_SUFFIX.length() - 1;
                        sb.append(value);
                    }
                } else {
                    sb.append(template.substring(i));
                    break;
                }
            } else {
                sb.append(template.substring(i));
                break;
            }
        }

        return sb.toString();
    }

    public static String applyVariables(String template, Map<String, String> variables) {
        return applyVariables(template, variables == null || variables.isEmpty() ? null : variables::get);
    }

    /**
     * Decode given string using {@link URLDecoder} and
     * {@link StandardCharsets#UTF_8}.
     *
     * @param encodedString encoded string
     * @return non-null decoded string
     */
    public static String decode(String encodedString) {
        if (ClickHouseChecker.isNullOrEmpty(encodedString)) {
            return "";
        }

        try {
            return URLDecoder.decode(encodedString, DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            return encodedString;
        }
    }

    /**
     * Encode given string using {@link URLEncoder} and
     * {@link StandardCharsets#UTF_8}.
     *
     * @param str string to encode
     * @return non-null encoded string
     */
    public static String encode(String str) {
        if (ClickHouseChecker.isNullOrEmpty(str)) {
            return "";
        }

        try {
            return URLEncoder.encode(str, DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            return str;
        }
    }

    private static <T> T findFirstService(Class<? extends T> serviceInterface) {
        ClickHouseChecker.nonNull(serviceInterface, "serviceInterface");

        T service = null;

        for (T s : ServiceLoader.load(serviceInterface, ClickHouseUtils.class.getClassLoader())) {
            if (s != null) {
                service = s;
                break;
            }
        }

        return service;
    }

    public static ExecutorService newThreadPool(Object owner, int maxThreads, int maxRequests) {
        return newThreadPool(owner, maxThreads, 0, maxRequests, 0L, true);
    }

    public static ExecutorService newThreadPool(Object owner, int coreThreads, int maxThreads, int maxRequests,
            long keepAliveTimeoutMs, boolean allowCoreThreadTimeout) {
        BlockingQueue<Runnable> queue = maxRequests > 0 ? new ArrayBlockingQueue<>(maxRequests)
                : new LinkedBlockingQueue<>();
        if (coreThreads < 2) {
            coreThreads = 2;
        }
        if (maxThreads < coreThreads) {
            maxThreads = coreThreads;
        }
        if (keepAliveTimeoutMs <= 0L) {
            keepAliveTimeoutMs = allowCoreThreadTimeout ? 1000L : 0L;
        }

        ThreadPoolExecutor pool = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTimeoutMs,
                TimeUnit.MILLISECONDS, queue, new ClickHouseThreadFactory(owner), new ThreadPoolExecutor.AbortPolicy());
        if (allowCoreThreadTimeout) {
            pool.allowCoreThreadTimeOut(true);
        }
        return pool;
    }

    public static boolean isCloseBracket(char ch) {
        return ch == ')' || ch == ']' || ch == '}';
    }

    public static boolean isOpenBracket(char ch) {
        return ch == '(' || ch == '[' || ch == '{';
    }

    public static boolean isQuote(char ch) {
        return ch == '\'' || ch == '`' || ch == '"';
    }

    public static boolean isSeparator(char ch) {
        return ch == ',' || ch == ';';
    }

    /**
     * Escape quotes in given string.
     * 
     * @param str   string
     * @param quote quote to escape
     * @return escaped string
     */
    public static String escape(String str, char quote) {
        if (str == null) {
            return str;
        }

        int len = str.length();
        StringBuilder sb = new StringBuilder(len + 10);

        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (ch == quote || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }

        return sb.toString();
    }

    /**
     * Unescape quoted string.
     * 
     * @param str quoted string
     * @return unescaped string
     */
    public static String unescape(String str) {
        if (ClickHouseChecker.isNullOrEmpty(str)) {
            return str;
        }

        int len = str.length();
        char quote = str.charAt(0);
        if (!isQuote(quote) || quote != str.charAt(len - 1)) { // not a quoted string
            return str;
        }

        StringBuilder sb = new StringBuilder(len = len - 1);
        for (int i = 1; i < len; i++) {
            char ch = str.charAt(i);

            if (++i >= len) {
                sb.append(ch);
            } else {
                char nextChar = str.charAt(i);
                if (ch == '\\' || (ch == quote && nextChar == quote)) {
                    sb.append(nextChar);
                } else {
                    sb.append(ch);
                    i--;
                }
            }
        }

        return sb.toString();
    }

    /**
     * Wrapper of {@code String.format(Locale.ROOT, ...)}.
     *
     * @param template string to format
     * @param args     arguments used in substitution
     * @return formatted string
     */
    public static String format(String template, Object... args) {
        return String.format(Locale.ROOT, template, args);
    }

    /**
     * Normalizes given directory by appending back slash if it does exist.
     *
     * @param dir original directory
     * @return normalized directory
     */
    public static String normalizeDirectory(String dir) {
        if (dir == null || dir.isEmpty()) {
            return "./";
        }

        return dir.charAt(dir.length() - 1) == '/' ? dir : dir.concat("/");
    }

    private static int readJsonArray(String json, List<Object> array, int startIndex, int len) {
        StringBuilder builder = new StringBuilder();

        // skip the first bracket
        for (int i = startIndex + 1; i < len; i++) {
            char ch = json.charAt(i);
            if (Character.isWhitespace(ch) || ch == ',' || ch == ':') {
                continue;
            } else if (ch == '"') {
                i = readUnescapedJsonString(json, builder, i, len) - 1;
                array.add(builder.toString());
                builder.setLength(0);
            } else if (ch == '-' || (ch >= '0' && ch <= '9')) {
                List<Object> list = new ArrayList<>(1);
                i = readJsonNumber(json, list, i, len) - 1;
                array.add(list.get(0));
                builder.setLength(0);
            } else if (ch == '{') {
                Map<String, Object> map = new LinkedHashMap<>();
                i = readJsonObject(json, map, i, len) - 1;
                array.add(map);
            } else if (ch == '[') {
                List<Object> list = new LinkedList<>();
                i = readJsonArray(json, list, i, len) - 1;
                array.add(list.toArray(new Object[0]));
            } else if (ch == ']') {
                return i + 1;
            } else {
                List<Object> list = new ArrayList<>(1);
                i = readJsonConstants(json, list, i, len) - 1;
                array.add(list.get(0));
            }
        }

        return len;
    }

    private static int readJsonConstants(String json, List<Object> value, int startIndex, int len) {
        String c = "null";
        if (json.indexOf(c, startIndex) == startIndex) {
            value.add(null);
        } else if (json.indexOf(c = "false", startIndex) == startIndex) {
            value.add(Boolean.FALSE);
        } else if (json.indexOf(c = "true", startIndex) == startIndex) {
            value.add(Boolean.TRUE);
        } else {
            throw new IllegalArgumentException(format("Expect one of 'null', 'false', 'true' but we got '%s'",
                    json.substring(startIndex, Math.min(startIndex + 5, len))));
        }

        return startIndex + c.length();
    }

    private static int readJsonNumber(String json, List<Object> value, int startIndex, int len) {
        int endIndex = len;

        StringBuilder builder = new StringBuilder().append(json.charAt(startIndex));

        boolean hasDot = false;
        boolean hasExp = false;
        // add first digit
        for (int i = startIndex + 1; i < len; i++) {
            char n = json.charAt(i);
            if (n >= '0' && n <= '9') {
                builder.append(n);
            } else if (!hasDot && n == '.') {
                hasDot = true;
                builder.append(n);
            } else if (!hasExp && (n == 'e' || n == 'E')) {
                hasDot = true;
                hasExp = true;
                builder.append(n);
                if (i + 1 < len) {
                    char next = json.charAt(i + 1);
                    if (next == '+' || next == '-') {
                        builder.append(next);
                        i++;
                    }
                }

                boolean hasNum = false;
                for (int j = i + 1; j < len; j++) {
                    char next = json.charAt(j);
                    if (next >= '0' && next <= '9') {
                        hasNum = true;
                        builder.append(next);
                    } else {
                        if (!hasNum) {
                            throw new IllegalArgumentException("Expect number after exponent at " + i);
                        }
                        endIndex = j + 1;
                        break;
                    }
                }
                break;
            } else {
                endIndex = i;
                break;
            }
        }

        if (hasDot) {
            if (hasExp || builder.length() >= 21) {
                value.add(new BigDecimal(builder.toString()));
            } else if (builder.length() >= 11) {
                value.add(Double.parseDouble(builder.toString()));
            } else {
                value.add(Float.parseFloat(builder.toString()));
            }
        } else {
            if (hasExp || builder.length() >= 19) {
                value.add(new BigInteger(builder.toString()));
            } else if (builder.length() >= 10) {
                value.add(Long.parseLong(builder.toString()));
            } else {
                value.add(Integer.parseInt(builder.toString()));
            }
        }

        return endIndex;
    }

    private static int readJsonObject(String json, Map<String, Object> object, int startIndex, int len) {
        StringBuilder builder = new StringBuilder();

        String key = null;
        // skip the first bracket
        for (int i = startIndex + 1; i < len; i++) {
            char ch = json.charAt(i);
            if (Character.isWhitespace(ch) || ch == ',' || ch == ':') {
                continue;
            } else if (ch == '"') {
                i = readUnescapedJsonString(json, builder, i, len) - 1;
                if (key != null) {
                    object.put(key, builder.toString());
                    key = null;
                } else {
                    key = builder.toString();
                }
                builder.setLength(0);
            } else if (ch == '-' || (ch >= '0' && ch <= '9')) {
                if (key == null) {
                    throw new IllegalArgumentException("Key is not available");
                }
                List<Object> list = new ArrayList<>(1);
                i = readJsonNumber(json, list, i, len) - 1;
                object.put(key, list.get(0));
                key = null;
                builder.setLength(0);
            } else if (ch == '{') {
                if (key == null) {
                    throw new IllegalArgumentException("Key is not available");
                }
                Map<String, Object> map = new LinkedHashMap<>();
                i = readJsonObject(json, map, i, len) - 1;
                object.put(key, map);
                key = null;
                builder.setLength(0);
            } else if (ch == '[') {
                if (key == null) {
                    throw new IllegalArgumentException("Key is not available");
                }

                List<Object> list = new LinkedList<>();
                i = readJsonArray(json, list, i, len) - 1;
                key = null;
                object.put(key, list.toArray(new Object[0]));
            } else if (ch == '}') {
                return i + 1;
            } else {
                if (key == null) {
                    throw new IllegalArgumentException("Key is not available");
                }
                List<Object> list = new ArrayList<>(1);
                i = readJsonConstants(json, list, i, len) - 1;
                object.put(key, list.get(0));
                key = null;
            }
        }

        return len;
    }

    private static int readUnescapedJsonString(String json, StringBuilder builder, int startIndex, int len) {
        // skip the first double quote
        for (int i = startIndex + 1; i < len; i++) {
            char c = json.charAt(i);
            if (c == '\\') {
                if (++i < len) {
                    builder.append(json.charAt(i));
                }
            } else if (c == '"') {
                return i + 1;
            } else {
                builder.append(c);
            }
        }

        return len;
    }

    /**
     * Simple and un-protected JSON parser.
     *
     * @param json non-empty JSON string
     * @return object array, Boolean, Number, null, String, or Map
     * @throws IllegalArgumentException when JSON string is null or empty
     */
    public static Object parseJson(String json) {
        if (json == null || json.isEmpty()) {
            throw new IllegalArgumentException("Non-empty JSON string is required");
        }

        boolean hasValue = false;
        Object value = null;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = json.length(); i < len; i++) {
            char ch = json.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            } else if (ch == '{') {
                // read object
                Map<String, Object> map = new LinkedHashMap<>();
                i = readJsonObject(json, map, i, len) - 1;
                hasValue = true;
                value = map;
            } else if (ch == '[') {
                // read array
                List<Object> list = new LinkedList<>();
                i = readJsonArray(json, list, i, len) - 1;
                hasValue = true;
                value = list.toArray(new Object[0]);
            } else if (ch == '"') {
                // read string
                i = readUnescapedJsonString(json, builder, i, len) - 1;
                hasValue = true;
                value = builder.toString();
            } else if (ch == '-' || (ch >= '0' && ch <= '9')) {
                // read number
                List<Object> list = new ArrayList<>(1);
                i = readJsonNumber(json, list, i, len) - 1;
                hasValue = true;
                value = list.get(0);
            } else {
                List<Object> list = new ArrayList<>(1);
                i = readJsonConstants(json, list, i, len) - 1;
                hasValue = true;
                value = list.get(0);
            }

            if (hasValue) {
                break;
            }
        }

        if (!hasValue) {
            throw new IllegalArgumentException("No value extracted from given JSON string");
        }

        return value;
    }

    /**
     * Gets buffer size.
     *
     * @param bufferSize  suggested buffer size, zero or negative number is treated
     *                    as {@code defaultSize}
     * @param defaultSize default buffer size, zero or negative number is treated as
     *                    {@link #DEFAULT_BUFFER_SIZE}
     * @param maxSize     maximum buffer size, zero or negative number is treated as
     *                    {@link #MAX_BUFFER_SIZE}
     * @return buffer size
     */
    public static int getBufferSize(int bufferSize, int defaultSize, int maxSize) {
        if (maxSize < 1 || maxSize > MAX_BUFFER_SIZE) {
            maxSize = MAX_BUFFER_SIZE;
        }
        if (defaultSize < 1) {
            defaultSize = DEFAULT_BUFFER_SIZE;
        } else if (defaultSize > maxSize) {
            defaultSize = maxSize;
        }

        if (bufferSize < 1) {
            return defaultSize;
        }

        return bufferSize > maxSize ? maxSize : bufferSize;
    }

    public static char getCloseBracket(char openBracket) {
        char closeBracket;
        if (openBracket == '(') {
            closeBracket = ')';
        } else if (openBracket == '[') {
            closeBracket = ']';
        } else if (openBracket == '{') {
            closeBracket = '}';
        } else {
            throw new IllegalArgumentException("Unsupported bracket: " + openBracket);
        }

        return closeBracket;
    }

    public static <T> T getService(Class<? extends T> serviceInterface) {
        return getService(serviceInterface, null);
    }

    /**
     * Load service according to given interface using
     * {@link java.util.ServiceLoader}, fallback to given default service or
     * supplier function if not found.
     *
     * @param <T>              type of service
     * @param serviceInterface non-null service interface
     * @param defaultService   optionally default service
     * @return non-null service
     */
    public static <T> T getService(Class<? extends T> serviceInterface, T defaultService) {
        T service = defaultService;
        Exception error = null;

        // load custom implementation if any
        try {
            T s = findFirstService(serviceInterface);
            if (s != null) {
                service = s;
            }
        } catch (Exception t) {
            error = t;
        }

        if (service == null) {
            throw new IllegalStateException(String.format("Failed to get service %s", serviceInterface.getName()),
                    error);
        }

        return service;
    }

    public static <T> T getService(Class<? extends T> serviceInterface, Supplier<T> supplier) {
        T service = null;
        Exception error = null;

        // load custom implementation if any
        try {
            service = findFirstService(serviceInterface);
        } catch (Exception t) {
            error = t;
        }

        // and then try supplier function if no luck
        if (service == null && supplier != null) {
            try {
                service = supplier.get();
            } catch (Exception t) {
                // override the error
                error = t;
            }
        }

        if (service == null) {
            throw new IllegalStateException(String.format("Failed to get service %s", serviceInterface.getName()),
                    error);
        }

        return service;
    }

    /**
     * Search file in current directory, home directory, and then classpath, Get
     * input stream to read the given file.
     *
     * @param file path to the file
     * @return input stream
     * @throws FileNotFoundException when the file does not exists
     */
    public static InputStream getFileInputStream(String file) throws FileNotFoundException {
        Path path = Paths.get(ClickHouseChecker.nonBlank(file, "file"));

        StringBuilder builder = new StringBuilder();
        InputStream in = null;
        if (Files.exists(path)) {
            builder.append(',').append(file);
            in = new FileInputStream(path.toFile());
        } else if (!path.isAbsolute()) {
            path = Paths.get(HOME_DIR, file);

            if (Files.exists(path)) {
                builder.append(',').append(path.toString());
                in = new FileInputStream(path.toFile());
            }
        }

        if (in == null) {
            builder.append(',').append("classpath:").append(file);
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        }

        if (in == null) {
            throw new FileNotFoundException(format("Could not open file from: %s", builder.deleteCharAt(0).toString()));
        }

        return in;
    }

    /**
     * Get output stream for writing a file. Directories and file will be created if
     * they do not exist.
     *
     * @param file path to the file
     * @return output stream
     * @throws IOException when failed to create directories and/or file
     */
    public static OutputStream getFileOutputStream(String file) throws IOException {
        Path path = Paths.get(ClickHouseChecker.nonBlank(file, "file"));

        if (Files.notExists(path)) {
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        }

        return new FileOutputStream(file, false);
    }

    /**
     * Extracts key value pairs from the given string.
     * 
     * @param str string
     * @return non-null map containing extracted key value pairs
     */
    public static Map<String, String> getKeyValuePairs(String str) {
        if (str == null || str.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new LinkedHashMap<>();
        String key = null;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (ch == '\\' && i + 1 < len) {
                ch = str.charAt(++i);
                builder.append(ch);
                continue;
            }

            if (Character.isWhitespace(ch)) {
                if (builder.length() > 0) {
                    builder.append(ch);
                }
            } else if (ch == '=' && key == null) {
                key = builder.toString().trim();
                builder.setLength(0);
            } else if (ch == ',' && key != null) {
                String value = builder.toString().trim();
                builder.setLength(0);
                if (!key.isEmpty() && !value.isEmpty()) {
                    map.put(key, value);
                }
                key = null;
            } else {
                builder.append(ch);
            }
        }

        if (key != null && builder.length() > 0) {
            String value = builder.toString().trim();
            if (!key.isEmpty() && !value.isEmpty()) {
                map.put(key, value);
            }
        }

        return Collections.unmodifiableMap(map);
    }

    public static String getLeadingComment(String sql) {
        if (sql == null || sql.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = sql.length(); i + 1 < len; i++) {
            char ch = sql.charAt(i);
            char nextCh = sql.charAt(i + 1);

            if (ch == '-' && nextCh == '-') {
                int index = skipSingleLineComment(sql, i, len);
                if (index > i + 2) {
                    builder.append(sql.substring(i + 2, index).trim());
                }
                i = index - 1;
            } else if (ch == '/' && nextCh == '*') {
                int index = skipMultiLineComment(sql, i + 2, len);
                if (index > i + 4) {
                    builder.append(sql.substring(i + 2, index - 2).trim());
                }
                i = index - 1;
            } else if (!Character.isWhitespace(ch)) {
                break;
            }

            if (builder.length() > 0) {
                break;
            }
        }

        return builder.toString();
    }

    public static String getProperty(String key, Properties... props) {
        return getProperty(key, null, props);
    }

    public static String getProperty(String key, String defaultValue, Properties... props) {
        String value = null;

        if (props != null) {
            for (Properties p : props) {
                value = p.getProperty(key);
                if (value != null) {
                    break;
                }
            }
        }

        if (value == null) {
            value = System.getProperty(key);
        }

        return value == null ? defaultValue : value;
    }

    /**
     * Skip brackets and content inside with consideration of nested brackets,
     * quoted string and comments.
     *
     * @param args       non-null string to scan
     * @param startIndex start index, optionally index of the opening bracket
     * @param len        end index, usually length of the given string
     * @param bracket    opening bracket supported by {@link #isOpenBracket(char)}
     * @return index next to matched closing bracket
     * @throws IllegalArgumentException when missing closing bracket(s)
     */
    public static int skipBrackets(String args, int startIndex, int len, char bracket) {
        char closeBracket = getCloseBracket(bracket);

        Deque<Character> stack = new ArrayDeque<>();
        for (int i = startIndex + (startIndex < len && args.charAt(startIndex) == bracket ? 1 : 0); i < len; i++) {
            char ch = args.charAt(i);
            if (isQuote(ch)) {
                i = skipQuotedString(args, i, len, ch) - 1;
            } else if (isOpenBracket(ch)) {
                stack.push(closeBracket);
                closeBracket = getCloseBracket(ch);
            } else if (ch == closeBracket) {
                if (stack.isEmpty()) {
                    return i + 1;
                } else {
                    closeBracket = stack.pop();
                }
            } else if (i + 1 < len) {
                char nextChar = args.charAt(i + 1);
                if (ch == '-' && nextChar == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextChar == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                }
            }
        }

        throw new IllegalArgumentException("Missing closing bracket(s): " + stack);
    }

    /**
     * Skip quoted string.
     *
     * @param args       non-null string to scan
     * @param startIndex start index, optionally start of the quoted string
     * @param len        end index, usually length of the given string
     * @param quote      quote supported by {@link #isQuote(char)}
     * @return index next to the other matched quote
     * @throws IllegalArgumentException when missing quote
     */
    public static int skipQuotedString(String args, int startIndex, int len, char quote) {
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (ch == '\\') {
                i++;
            } else if (ch == quote && i > startIndex) {
                if (++i < len && args.charAt(i) == quote) {
                    continue;
                } else {
                    return i;
                }
            }
        }

        throw new IllegalArgumentException("Missing quote: " + quote);
    }

    /**
     * Skip single line comment.
     *
     * @param args       non-null string to scan
     * @param startIndex start index, optionally start of the single line comment
     * @param len        end index, usually length of the given string
     * @return index of start of next line, right after {@code \n}
     */
    public static int skipSingleLineComment(String args, int startIndex, int len) {
        int index = args.indexOf('\n', startIndex);
        return index > startIndex ? index + 1 : len;
    }

    /**
     * Skip nested multi-line comment.
     *
     * @param args       non-null string to scan
     * @param startIndex start index, optionally start of the multi-line comment
     * @param len        end index, usually length of the given string
     * @return index next to end of the outter most multi-line comment
     * @throws IllegalArgumentException when multi-line comment is unclosed
     */
    public static int skipMultiLineComment(String args, int startIndex, int len) {
        int openIndex = args.indexOf("/*", startIndex);
        if (openIndex == startIndex) {
            openIndex = args.indexOf("/*", startIndex + 2);
        }
        int closeIndex = args.indexOf("*/", startIndex);

        if (closeIndex < startIndex) {
            throw new IllegalArgumentException("Unclosed multi-line comment");
        }

        return openIndex < startIndex || openIndex > closeIndex ? closeIndex + 2
                : skipMultiLineComment(args, closeIndex + 2, len);
    }

    /**
     * Skip quoted string, comments, and brackets until seeing one of
     * {@code endChars} or reaching end of the given string.
     *
     * @param args       non-null string to scan
     * @param startIndex start index
     * @param len        end index, usually length of the given string
     * @param endChars   skip characters until seeing one of the specified
     *                   characters or reaching end of the string; '\0' is used when
     *                   it's null or empty
     * @return index of {@code endChar} or {@code len}
     */
    public static int skipContentsUntil(String args, int startIndex, int len, char... endChars) {
        int charLen = endChars != null ? endChars.length : 0;
        if (charLen == 0) {
            endChars = new char[] { '\0' };
            charLen = 1;
        }
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            for (int j = 0; j < charLen; j++) {
                if (ch == endChars[j]) {
                    return i + 1;
                }
            }

            if (isQuote(ch)) {
                i = skipQuotedString(args, i, len, ch) - 1;
            } else if (isOpenBracket(ch)) {
                i = skipBrackets(args, i, len, ch) - 1;
            } else if (i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                }
            }
        }

        return len;
    }

    /**
     * Skip quoted string, comments, and brackets until seeing the {@code keyword}
     * or reaching end of the given string.
     *
     * @param args          non-null string to scan
     * @param startIndex    start index
     * @param len           end index, usually length of the given string
     * @param keyword       keyword, null or empty string means any character
     * @param caseSensitive whether keyword is case sensitive or not
     * @return index of {@code endChar} or {@code len}
     */
    public static int skipContentsUntil(String args, int startIndex, int len, String keyword, boolean caseSensitive) {
        if (keyword == null || keyword.isEmpty()) {
            return Math.min(startIndex + 1, len);
        }

        int k = keyword.length();
        if (k == 1) {
            return skipContentsUntil(args, startIndex, len, keyword.charAt(0));
        }

        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);

            if (isQuote(ch)) {
                i = skipQuotedString(args, i, len, ch) - 1;
            } else if (isOpenBracket(ch)) {
                i = skipBrackets(args, i, len, ch) - 1;
            } else if (i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else if (i + k < len) {
                    int endIndex = i + k;
                    String s = args.substring(i, endIndex);
                    if ((caseSensitive && s.equals(keyword)) || (!caseSensitive && s.equalsIgnoreCase(keyword))) {
                        return endIndex;
                    }
                }
            }
        }

        return len;
    }

    /**
     * Skip quoted string, comments, and brackets until seeing all the given
     * {@code keywords}(with only whitespaces or comments in between) or reaching
     * end of the given string.
     *
     * @param args          non-null string to scan
     * @param startIndex    start index
     * @param len           end index, usually length of the given string
     * @param keywords      keywords, null or empty one means any character
     * @param caseSensitive whether keyword is case sensitive or not
     * @return index of {@code endChar} or {@code len}
     */
    public static int skipContentsUntil(String args, int startIndex, int len, String[] keywords,
            boolean caseSensitive) {
        int k = keywords != null ? keywords.length : 0;
        if (k == 0) {
            return Math.min(startIndex + 1, len);
        }

        int index = skipContentsUntil(args, startIndex, len, keywords[0], caseSensitive);
        for (int j = 1; j < k; j++) {
            String keyword = keywords[j];
            if (keyword == null || keyword.isEmpty()) {
                index++;
                continue;
            } else {
                int klen = keyword.length();
                if (index + klen >= len) {
                    return len;
                }

                for (int i = index; i < len; i++) {
                    String s = args.substring(i, i + klen);
                    if ((caseSensitive && s.equals(keyword)) || (!caseSensitive && s.equalsIgnoreCase(keyword))) {
                        index = i + klen;
                        break;
                    } else {
                        char ch = args.charAt(i);
                        if (Character.isWhitespace(ch)) {
                            continue;
                        } else if (i + 1 < len) {
                            char nextCh = args.charAt(i + 1);
                            if (ch == '-' && nextCh == '-') {
                                i = skipSingleLineComment(args, i + 2, len) - 1;
                            } else if (ch == '/' && nextCh == '*') {
                                i = skipMultiLineComment(args, i + 2, len) - 1;
                            } else {
                                return len;
                            }
                        } else {
                            return len;
                        }
                    }
                }
            }
        }

        return index;
    }

    public static int readNameOrQuotedString(String args, int startIndex, int len, StringBuilder builder) {
        char quote = '\0';
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (ch == '\\') {
                if (++i < len) {
                    builder.append(args.charAt(i));
                }
                continue;
            } else if (isQuote(ch)) {
                if (ch == quote) {
                    if (i + 1 < len && args.charAt(i + 1) == ch) {
                        builder.append(ch);
                        i++;
                        continue;
                    }
                    len = i + 1;
                    break;
                } else if (quote == '\0') {
                    quote = ch;
                } else {
                    builder.append(ch);
                }
            } else if (quote == '\0' && (Character.isWhitespace(ch) || isOpenBracket(ch) || isCloseBracket(ch)
                    || isSeparator(ch) || (i + 1 < len && ((ch == '-' && args.charAt(i + 1) == '-')
                            || (ch == '/' && args.charAt(i + 1) == '*'))))) {
                if (builder.length() > 0) {
                    len = i;
                    break;
                }
            } else {
                builder.append(ch);
            }
        }

        return len;
    }

    public static int readEnumValues(String args, int startIndex, int len, Map<String, Integer> values) {
        String name = null;
        StringBuilder builder = new StringBuilder();
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            } else if (ch == '\'') {
                i = readNameOrQuotedString(args, i, len, builder);
                name = builder.toString();
                builder.setLength(0);

                int index = args.indexOf('=', i);
                if (index >= i) {
                    for (i = index + 1; i < len; i++) {
                        ch = args.charAt(i);
                        if (Character.isWhitespace(ch)) {
                            continue;
                        } else if (ch >= '0' && ch <= '9') {
                            builder.append(ch);
                        } else if (ch == ',') {
                            values.put(name, Integer.parseInt(builder.toString()));
                            builder.setLength(0);
                            name = null;
                            break;
                        } else if (ch == ')') {
                            values.put(name, Integer.parseInt(builder.toString()));
                            return i + 1;
                        } else {
                            throw new IllegalArgumentException("Invalid character when reading enum");
                        }
                    }

                    continue;
                } else {
                    throw new IllegalArgumentException("Expect = after enum value but not found");
                }
            } else {
                throw new IllegalArgumentException("Invalid enum declaration");
            }
        }

        return len;
    }

    public static List<String> readValueArray(String args, int startIndex, int len) {
        List<String> list = new LinkedList<>();
        readValueArray(args, startIndex, len, list::add);
        return list.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(list);
    }

    public static int readValueArray(String args, int startIndex, int len, Consumer<String> func) {
        char closeBracket = ']';
        StringBuilder builder = new StringBuilder();
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (ch == '[') {
                startIndex = i + 1;
                break;
            } else if (Character.isWhitespace(ch)) {
                continue;
            } else if (i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else {
                    startIndex = i;
                    break;
                }
            } else {
                startIndex = i;
                break;
            }
        }

        boolean hasNext = false;
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            } else if (ch == '\'') { // string
                hasNext = false;
                int endIndex = readNameOrQuotedString(args, i, len, builder);
                func.accept(unescape(args.substring(i, endIndex)));
                builder.setLength(0);
                i = endIndex + 1;
            } else if (ch == '[') { // array
                hasNext = false;
                int endIndex = skipContentsUntil(args, i + 1, len, ']');
                func.accept(args.substring(i, endIndex));
                builder.setLength(0);
                i = endIndex;
            } else if (ch == '(') { // tuple
                hasNext = false;
                int endIndex = skipContentsUntil(args, i + 1, len, ')');
                func.accept(args.substring(i, endIndex));
                builder.setLength(0);
                i = endIndex;
            } else if (ch == closeBracket) {
                len = i + 1;
                break;
            } else if (ch == ',') {
                hasNext = true;
                String str = builder.toString();
                func.accept(str.isEmpty() || ClickHouseValues.NULL_EXPR.equalsIgnoreCase(str) ? null : str);
                builder.setLength(0);
            } else if (i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else {
                    builder.append(ch);
                }
            } else {
                builder.append(ch);
            }
        }

        if (hasNext || builder.length() > 0) {
            String str = builder.toString();
            func.accept(str.isEmpty() || ClickHouseValues.NULL_EXPR.equalsIgnoreCase(str) ? null : str);
        }

        return len;
    }

    public static int readParameters(String args, int startIndex, int len, List<String> params) {
        char closeBracket = ')'; // startIndex points to the openning bracket
        Deque<Character> stack = new ArrayDeque<>();
        StringBuilder builder = new StringBuilder();

        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (ch == '(') {
                startIndex = i + 1;
                break;
            } else if (Character.isWhitespace(ch)) {
                continue;
            } else if (i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else {
                    startIndex = i;
                    break;
                }
            } else {
                startIndex = i;
                break;
            }
        }

        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            } else if (isQuote(ch)) {
                builder.append(ch);
                for (int j = i + 1; j < len; j++) {
                    char c = args.charAt(j);
                    i = j;
                    builder.append(c);
                    if (c == ch && args.charAt(j - 1) != '\\') {
                        if (j + 1 < len && args.charAt(j + 1) == ch) {
                            builder.append(ch);
                            i = ++j;
                        } else {
                            break;
                        }
                    }
                }
            } else if (isOpenBracket(ch)) {
                builder.append(ch);
                stack.push(closeBracket);
                closeBracket = getCloseBracket(ch);
            } else if (ch == closeBracket) {
                if (stack.isEmpty()) {
                    len = i + 1;
                    break;
                } else {
                    builder.append(ch);
                    closeBracket = stack.pop();
                }
            } else if (ch == ',') {
                if (!stack.isEmpty()) {
                    builder.append(ch);
                } else {
                    params.add(builder.toString());
                    builder.setLength(0);
                }
            } else if (i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else {
                    builder.append(ch);
                }
            } else {
                builder.append(ch);
            }
        }

        if (builder.length() > 0) {
            params.add(builder.toString());
        }

        return len;
    }

    @SuppressWarnings("unchecked")
    protected static <T> T[] toArray(Class<T> clazz, Collection<T> list) {
        int size = list == null ? 0 : list.size();
        T[] array = (T[]) Array.newInstance(clazz, size);
        if (size > 0) {
            int i = 0;
            for (T t : list) {
                array[i++] = t;
            }
        }
        return array;
    }

    private ClickHouseUtils() {
    }
}
