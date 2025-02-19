package com.clickhouse.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

@Deprecated
public final class ClickHouseUtils {
    private static final boolean IS_UNIX;
    private static final boolean IS_WINDOWS;

    private static final String HOME_DIR;

    static {
        final String osName = System.getProperty("os.name", "");

        // https://github.com/apache/commons-lang/blob/5a3904c8678574a4ddb8502ebbc606be1091fb3f/src/main/java/org/apache/commons/lang3/SystemUtils.java#L1370
        IS_UNIX = osName.startsWith("AIX") || osName.startsWith("HP-UX") || osName.startsWith("OS/400")
                || osName.startsWith("Irix") || osName.startsWith("Linux") || osName.startsWith("LINUX")
                || osName.startsWith("Mac OS X") || osName.startsWith("Solaris") || osName.startsWith("SunOS")
                || osName.startsWith("FreeBSD") || osName.startsWith("OpenBSD") || osName.startsWith("NetBSD");
        IS_WINDOWS = osName.toLowerCase(Locale.ROOT).contains("windows");

        HOME_DIR = IS_WINDOWS
                ? Paths.get(System.getenv("APPDATA"), "clickhouse").toFile().getAbsolutePath()
                : Paths.get(System.getProperty("user.home"), ".clickhouse").toFile().getAbsolutePath();
    }

    /**
     * Default charset.
     */
    public static final String DEFAULT_CHARSET = StandardCharsets.UTF_8.name();

    public static final int MIN_CORE_THREADS = 4;

    public static final CompletableFuture<Void> NULL_FUTURE = CompletableFuture.completedFuture(null);
    public static final Supplier<?> NULL_SUPPLIER = () -> null;

    public static final String VARIABLE_PREFIX = "{{";
    public static final String VARIABLE_SUFFIX = "}}";

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
     * Creates a temporary file. Same as {@code createTempFile(null, null, true)}.
     *
     * @return non-null temporary file
     * @throws IOException when failed to create the temporary file
     */
    public static File createTempFile() throws IOException {
        return createTempFile(null, null, true);
    }

    /**
     * Creates a temporary file with given prefix and suffix. Same as
     * {@code createTempFile(prefix, suffix, true)}.
     *
     * @param prefix prefix, could be null
     * @param suffix suffix, could be null
     * @return non-null temporary file
     * @throws IOException when failed to create the temporary file
     */
    public static File createTempFile(String prefix, String suffix) throws IOException {
        return createTempFile(prefix, suffix, true);
    }

    /**
     * Creates a temporary file with the given prefix and suffix. The file has only
     * read and write access granted to the owner.
     *
     * @param prefix       prefix, null or empty string is taken as {@code "ch"}
     * @param suffix       suffix, null or empty string is taken as {@code ".data"}
     * @param deleteOnExit whether the file be deleted on exit
     * @return non-null temporary file
     * @throws IOException when failed to create the temporary file
     */
    public static File createTempFile(String prefix, String suffix, boolean deleteOnExit) throws IOException {
        if (prefix == null || prefix.isEmpty()) {
            prefix = "ch";
        }
        if (suffix == null || suffix.isEmpty()) {
            suffix = ".data";
        }

        final File f;
        if (IS_UNIX) {
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions
                    .asFileAttribute(PosixFilePermissions.fromString("rw-------"));
            f = Files.createTempFile(prefix, suffix, attr).toFile();
        } else {
            f = Files.createTempFile(prefix, suffix).toFile(); // NOSONAR
            f.setReadable(true, true); // NOSONAR
            f.setWritable(true, true); // NOSONAR
            f.setExecutable(false, false); // NOSONAR
        }

        if (deleteOnExit) {
            f.deleteOnExit();
        }
        return f;
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

    /**
     * Extracts parameters, usually key-value pairs, from the given query string.
     *
     * @param query  non-empty query string
     * @param params mutable map for extracted parameters, a new {@link HashMap}
     *               will be created when it's null
     * @return map with extracted parameters, usually same as {@code params}
     */
    public static Map<String, String> extractParameters(String query, Map<String, String> params) {
        if (params == null) {
            params = new HashMap<>();
        }
        if (ClickHouseChecker.isNullOrEmpty(query)) {
            return params;
        }
        int len = query.length();
        for (int i = 0; i < len; i++) {
            int index = query.indexOf('&', i);
            if (index == i) {
                continue;
            }

            String param;
            if (index < 0) {
                param = query.substring(i);
                i = len;
            } else {
                param = query.substring(i, index);
                i = index;
            }
            index = param.indexOf('=');
            String key;
            String value;
            if (index < 0) {
                key = decode(param);
                if (key.charAt(0) == '!') {
                    key = key.substring(1);
                    value = Boolean.FALSE.toString();
                } else {
                    value = Boolean.TRUE.toString();
                }
            } else {
                key = decode(param.substring(0, index));
                value = decode(param.substring(index + 1));
            }

            // any multi-value option? cluster?
            if (!ClickHouseChecker.isNullOrEmpty(value)) {
                params.put(key, value);
            }
        }
        return params;
    }

    public static <T> T newInstance(String className, Class<T> returnType, Class<?> callerClass) {
        if (className == null || className.isEmpty() || returnType == null) {
            throw new IllegalArgumentException("Non-empty class name and return type are required");
        } else if (callerClass == null) {
            callerClass = returnType;
        }

        try {
            Class<?> clazz = Class.forName(className, false, callerClass.getClassLoader());
            if (!returnType.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(
                        format("Invalid %s class type. Input class should be a superclass of %s.", className,
                                returnType));
            }

            return returnType.cast(clazz.getConstructor().newInstance());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(format("Class %s is not found in the classpath.", className));
        } catch (IllegalAccessException | NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    format("Class %s does not have a public constructor without any argument.", className));
        } catch (InstantiationException | InvocationTargetException e) {
            throw new IllegalArgumentException(format("Error while creating an %s class instance.", className), e);
        }
    }

    /**
     * Gets absolute and normalized path to the given file.
     *
     * @param file non-empty file
     * @return non-null absolute and normalized path to the file
     */
    public static Path getFile(String file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("Non-empty file is required");
        } else if (file.startsWith("~/")) {
            return Paths.get(System.getProperty("user.home"), file.substring(2)).normalize();
        }
        return Paths.get(file).toAbsolutePath().normalize();
    }

    /**
     * Finds files according to the given pattern and path.
     *
     * @param pattern non-empty pattern may or may have syntax prefix as in
     *                {@link java.nio.file.FileSystem#getPathMatcher(String)},
     *                defaults to {@code glob} syntax
     * @param paths   path to search, defaults to current work directory
     * @return non-null list of normalized absolute paths matching the pattern
     * @throws IOException when failed to find files
     */
    public static List<Path> findFiles(String pattern, String... paths) throws IOException {
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("Non-empty pattern is required");
        } else if (pattern.startsWith("~/")) {
            return Collections
                    .singletonList(Paths.get(System.getProperty("user.home"), pattern.substring(2)).normalize());
        }

        if (!pattern.startsWith("glob:") && !pattern.startsWith("regex:")) {
            if (IS_WINDOWS) {
                final String reservedCharsWindows = "<>:\"|?*";
                pattern.chars().anyMatch(
                        value -> {
                            if (value < ' ' || reservedCharsWindows.indexOf(value) != -1) {
                                throw new IllegalArgumentException(String.format("File path contains reserved character <%s>", value));
                            }
                            return false;
                        }
                );
            }
            Path path = Paths.get(pattern);
            if (path.isAbsolute()) {
                return Collections.singletonList(path);
            } else {
                pattern = "glob:" + pattern;
            }
        }

        final Path searchPath;
        if (paths == null || paths.length == 0) {
            searchPath = Paths.get("");
        } else {
            String root = paths[0];
            Path rootPath = root.startsWith("~/")
                    ? Paths.get(System.getProperty("user.home"), root.substring(2)).normalize()
                    : Paths.get(root);
            searchPath = paths.length < 2 ? rootPath
                    : Paths.get(rootPath.toFile().getAbsolutePath(), Arrays.copyOfRange(paths, 1, paths.length))
                            .normalize();
        }

        final List<Path> files = new ArrayList<>();
        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher(pattern);
        Files.walkFileTree(searchPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (matcher.matches(path)) {
                    files.add(path.normalize());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
        return files;
    }

    public static String toJavaByteArrayExpression(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        int len = bytes.length;
        if (len == 0) {
            return "{}";
        }

        String prefix = "(byte)0x";
        StringBuilder builder = new StringBuilder(10 * len).append('{');
        for (int i = 0; i < len; i++) {
            builder.append(prefix).append(String.format("%02X", 0xFF & bytes[i])).append(',');
        }
        builder.setCharAt(builder.length() - 1, '}');
        return builder.toString();
    }

    public static ExecutorService newThreadPool(Object owner, int maxThreads, int maxRequests) {
        return newThreadPool(owner, maxThreads, 0, maxRequests, 0L, true);
    }

    public static ExecutorService newThreadPool(Object owner, int coreThreads, int maxThreads, int maxRequests,
            long keepAliveTimeoutMs, boolean allowCoreThreadTimeout) {
        final BlockingQueue<Runnable> queue;
        if (coreThreads < MIN_CORE_THREADS) {
            coreThreads = MIN_CORE_THREADS;
        }
        if (maxRequests > 0) {
            queue = new ArrayBlockingQueue<>(maxRequests);
            if (maxThreads <= coreThreads) {
                maxThreads = coreThreads * 2;
            }
        } else {
            queue = new LinkedBlockingQueue<>();
            if (maxThreads != coreThreads) {
                maxThreads = coreThreads;
            }
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
     * Removes specific character from the given string.
     *
     * @param str  string to remove character from
     * @param ch   specific character to be removed from the string
     * @param more more characters to be removed
     * @return non-null string without the specific character
     */
    public static String remove(String str, char ch, char... more) {
        if (str == null || str.isEmpty()) {
            return "";
        }

        int l = more == null ? 0 : more.length;
        if (l == 0 && str.indexOf(ch) == -1) {
            return str;
        }

        // deduped array
        char[] chars = new char[1 + l];
        chars[0] = ch;
        int p = 1;
        for (int i = 0; i < l; i++) {
            char c = more[i];
            boolean skip = false;
            for (int j = 0, k = i + 1; j < k; j++) {
                if (chars[j] == c) {
                    skip = true;
                    break;
                }
            }
            if (!skip) {
                chars[p++] = c;
            }
        }

        int len = str.length();
        StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            boolean skip = false;
            for (int j = 0; j < p; j++) {
                if (chars[j] == c) {
                    skip = true;
                    break;
                }
            }
            if (!skip) {
                builder.append(c);
            }
        }
        return builder.toString();
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

            break;
        }

        if (!hasValue) {
            throw new IllegalArgumentException("No value extracted from given JSON string");
        }

        return value;
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

        throw new IllegalArgumentException(
                format("Missing '%s' for '%s' at position %d", closeBracket, bracket, startIndex));
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
     * @param startIndex start index, MUST AFTER the beginning of the multi-line
     *                   comment
     * @param len        end index, usually length of the given string
     * @return index next to end of the outter most multi-line comment
     * @throws IllegalArgumentException when multi-line comment is unclosed
     */
    public static int skipMultiLineComment(String args, int startIndex, int len) {
        int commentLevel = 1;

        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            boolean hasNext = i < len - 1;
            if (ch == '/' && hasNext && args.charAt(i + 1) == '*') {
                i++;
                commentLevel++;
            } else if (ch == '*' && hasNext && args.charAt(i + 1) == '/') {
                i++;
                if (--commentLevel == 0) {
                    return i + 1;
                }
            }
            if (commentLevel <= 0) {
                break;
            }
        }

        throw new IllegalArgumentException("Unclosed multi-line comment");
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
            } else if ((ch == '-' || ch == '/') && i + 1 < len) {
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
                        if ((ch == '-' || ch == '/') && i + 1 < len) {
                            char nextCh = args.charAt(i + 1);
                            if (ch == '-' && nextCh == '-') {
                                i = skipSingleLineComment(args, i + 2, len) - 1;
                            } else if (ch == '/' && nextCh == '*') {
                                i = skipMultiLineComment(args, i + 2, len) - 1;
                            } else {
                                return len;
                            }
                        } else if (!Character.isWhitespace(ch)) {
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
            if (ch == '\'') {
                i = readNameOrQuotedString(args, i, len, builder);
                name = builder.toString();
                builder.setLength(0);

                int index = args.indexOf('=', i);
                if (index >= i) {
                    for (i = index + 1; i < len; i++) {
                        ch = args.charAt(i);
                        if (ch >= '0' && ch <= '9') {
                            builder.append(ch);
                        } else if (ch == ',') {
                            values.put(name, Integer.parseInt(builder.toString()));
                            builder.setLength(0);
                            break;
                        } else if (ch == ')') {
                            values.put(name, Integer.parseInt(builder.toString()));
                            return i + 1;
                        } else if (!Character.isWhitespace(ch)) {
                            throw new IllegalArgumentException("Invalid character when reading enum");
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Expect = after enum value but not found");
                }
            } else if (!Character.isWhitespace(ch)) {
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
            } else if ((ch == '-' || ch == '/') && i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else {
                    startIndex = i;
                    break;
                }
            } else if (!Character.isWhitespace(ch)) {
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
            } else if ((ch == '-' || ch == '/') && i + 1 < len) {
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
        char closeBracket = ')'; // startIndex points to the opening bracket
        Deque<Character> stack = new ArrayDeque<>();
        StringBuilder builder = new StringBuilder();

        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (ch == '(') {
                startIndex = i + 1;
                break;
            } else if ((ch == '-' || ch == '/') && i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else {
                    startIndex = i;
                    break;
                }
            } else if (!Character.isWhitespace(ch)) {
                startIndex = i;
                break;
            }
        }

        boolean expectWs = false;
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (Character.isWhitespace(ch)) {
                if (builder.length() > 0) {
                    for (int j = i + 1; j < len; j++) {
                        ch = args.charAt(j);
                        if (ch == ',' || ch == '=' || ch == '-' || ch == '/' || isOpenBracket(ch)
                                || isCloseBracket(ch)) {
                            i = j - 1;
                            break;
                        } else if (!Character.isWhitespace(ch)) {
                            i = j - 1;
                            if (expectWs) {
                                builder.append(' ');
                            }
                            break;
                        }
                    }
                }
                expectWs = false;
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
                expectWs = false;
            } else if (isOpenBracket(ch)) {
                builder.append(ch);
                stack.push(closeBracket);
                closeBracket = getCloseBracket(ch);
                expectWs = false;
            } else if (ch == closeBracket) {
                if (stack.isEmpty()) {
                    len = i + 1;
                    break;
                } else {
                    builder.append(ch);
                    closeBracket = stack.pop();
                }
                expectWs = false;
            } else if (ch == ',') {
                if (!stack.isEmpty()) {
                    builder.append(ch);
                } else {
                    params.add(builder.toString());
                    builder.setLength(0);
                }
                expectWs = false;
            } else if ((ch == '-' || ch == '/') && i + 1 < len) {
                char nextCh = args.charAt(i + 1);
                if (ch == '-' && nextCh == '-') {
                    i = skipSingleLineComment(args, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = skipMultiLineComment(args, i + 2, len) - 1;
                } else {
                    builder.append(ch);
                }
                expectWs = false;
            } else {
                builder.append(ch);
                expectWs = ch != '=';
            }
        }

        if (builder.length() > 0) {
            params.add(builder.toString());
        }

        return len;
    }

    /**
     * Waits until the flag turns to {@code true} or timed out.
     *
     * @param flag    non-null boolean flag to check
     * @param timeout timeout, negative or zero means forever
     * @param unit    non-null time unit
     * @return true if the flag turns to true within given timeout; false otherwise
     * @throws InterruptedException when thread was interrupted
     */
    public static boolean waitFor(AtomicBoolean flag, long timeout, TimeUnit unit) throws InterruptedException {
        if (flag == null || unit == null) {
            throw new IllegalArgumentException("Non-null flag and time unit required");
        }

        final long timeoutMs = timeout > 0L ? unit.toMillis(timeout) : 0L;
        final long startTime = timeoutMs < 1L ? 0L : System.currentTimeMillis();
        while (!flag.get()) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            } else if (startTime > 0L && System.currentTimeMillis() - startTime >= timeoutMs) {
                return false;
            }
        }
        return true;
    }

    private ClickHouseUtils() {
    }
}
