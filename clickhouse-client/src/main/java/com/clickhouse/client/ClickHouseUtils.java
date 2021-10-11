package com.clickhouse.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class ClickHouseUtils {
    private static final String HOME_DIR;

    static {
        HOME_DIR = System.getProperty("os.name").toLowerCase().contains("windows")
                ? Paths.get(System.getenv("APPDATA"), "clickhouse").toFile().getAbsolutePath()
                : Paths.get(System.getProperty("user.home"), ".clickhouse").toFile().getAbsolutePath();
    }

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
        return applyVariables(template, variables == null || variables.size() == 0 ? null : variables::get);
    }

    private static <T> T findFirstService(Class<? extends T> serviceInterface) {
        ClickHouseChecker.nonNull(serviceInterface, "serviceInterface");

        T service = null;

        for (T s : ServiceLoader.load(serviceInterface)) {
            if (s != null) {
                service = s;
                break;
            }
        }

        return service;
    }

    public static ExecutorService newThreadPool(String owner, int maxThreads, int maxRequests) {
        BlockingQueue<Runnable> queue = maxRequests > 0 ? new ArrayBlockingQueue<>(maxRequests)
                : new LinkedBlockingQueue<>();

        return new ThreadPoolExecutor(1, maxThreads < 1 ? 1 : maxThreads, 0L, TimeUnit.MILLISECONDS, queue,
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, owner);
                        thread.setUncaughtExceptionHandler(null);
                        return thread;
                    }
                }, new ThreadPoolExecutor.AbortPolicy());
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
        int closeIndex = args.indexOf("*/", startIndex);

        if (closeIndex < startIndex) {
            throw new IllegalArgumentException("Unclosed multi-line comment");
        }

        return openIndex < startIndex || openIndex > closeIndex ? closeIndex + 2
                : skipMultiLineComment(args, closeIndex + 2, len);
    }

    /**
     * Skip quoted string, comments, and brackets until seeing {@code endChar} or
     * reaching end of the given string.
     *
     * @param args       non-null string to scan
     * @param startIndex start index
     * @param len        end index, usually length of the given string
     * @param endChar    skip characters until seeing this or reaching end of the
     *                   string
     * @return index of {@code endChar} or {@code len}
     */
    public static int skipContentsUntil(String args, int startIndex, int len, char endChar) {
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if (ch == endChar) {
                return i + 1;
            } else if (isQuote(ch)) {
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
            } else if (quote == '\0'
                    && (Character.isWhitespace(ch) || isOpenBracket(ch) || isCloseBracket(ch) || isSeparator(ch))) {
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

    public static int readParameters(String args, int startIndex, int len, List<String> params) {
        char closeBracket = ')'; // startIndex points to the openning bracket
        Deque<Character> stack = new ArrayDeque<>();
        StringBuilder builder = new StringBuilder();
        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            if ((i == startIndex && ch == '(') || Character.isWhitespace(ch)) {
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
