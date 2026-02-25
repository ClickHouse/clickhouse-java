package com.clickhouse.jdbc.internal.parser.javacc;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class ClickHouseSqlUtils {
    public static final String KEYWORD_GROUP_ALLOWED_ALIASES = "allowed_keyword_aliases";

    private static final Set<String> ALLOWED_KEYWORD_ALIASES = initAllowedKeywordAliases();

    private static Set<String> initAllowedKeywordAliases() {
        return buildKeywordSet(
                "ACCESS", "ACTION", "ADD", "ADMIN", "AFTER", "ALGORITHM", "ALIAS", "ALLOWED_LATENESS", "ALTER",
                "AND", "APPEND", "APPLY", "ASC", "ASCENDING", "ASSUME", "AST", "ASYNC", "ATTACH",
                "AUTHENTICATION", "AUTO_INCREMENT", "AZURE", "BACKUP", "BAGEXPANSION", "BASE_BACKUP",
                "BCRYPT_HASH", "BCRYPT_PASSWORD", "BEGIN", "BIDIRECTIONAL", "BOTH", "BY", "CACHE", "CACHES",
                "CASCADE", "CASE", "CAST", "CHANGE", "CHANGEABLE_IN_READONLY", "CHANGED", "CHAR", "CHARACTER",
                "CHECK", "CLEANUP", "CLEAR", "CLONE", "CLUSTER", "CLUSTERS", "CLUSTER_HOST_IDS", "CN", "CODEC",
                "COLLATE", "COLLECTION", "COLUMN", "COLUMNS", "COMMENT", "COMMIT", "COMPRESSION", "CONNECTIONS",
                "CONST", "CONSTRAINT", "COPY", "CREATE", "CUBE", "CURRENT", "CURRENT_USER", "CURRENTUSER",
                "D", "DATA", "DATABASE", "DATABASES", "DATE", "DAY", "DAYS", "DD", "DDL", "DEALLOCATE",
                "DEDUPLICATE", "DEFAULT", "DEFINER", "DELAY", "DELETE", "DELETED", "DEPENDS", "DESC",
                "DESCENDING", "DESCRIBE", "DETACH", "DETACHED", "DICTIONARIES", "DICTIONARY", "DISK", "DISTINCT",
                "DIV", "DOUBLE_SHA1_HASH", "DOUBLE_SHA1_PASSWORD", "DROP", "EMPTY", "ENABLED", "END", "ENFORCED",
                "ENGINE", "ENGINES", "EPHEMERAL", "ESTIMATE", "EVENT", "EVENTS", "EVERY", "EXCHANGE", "EXECUTE",
                "EXISTS", "EXPLAIN", "EXPRESSION", "EXTENDED", "EXTERNAL", "FAKE", "FALSE", "FETCH", "FIELDS",
                "FILE", "FILES", "FILESYSTEM", "FILL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FORCE", "FOREIGN",
                "FORGET", "FREEZE", "FULLTEXT", "FUNCTION", "FUNCTIONS", "GRANT", "GRANTEES", "GRANTS",
                "GRANULARITY", "GROUPING", "GROUPS", "H", "HASH", "HDFS", "HH", "HIERARCHICAL", "HOST", "HOUR",
                "HOURS", "HTTP", "ID", "IDENTIFIED", "IF", "IGNORE", "IMPLICIT", "IN", "INDEX", "INDEXES",
                "INDICES", "INFILE", "INHERIT", "INJECTIVE", "INSERT", "INTERPOLATE", "INTERVAL", "INVISIBLE",
                "INVOKER", "IP", "IS", "IS_OBJECT_ID", "JWT", "KERBEROS", "KEY", "KEYED", "KEYS", "KILL", "KIND",
                "LARGE", "LAST", "LAYOUT", "LDAP", "LEADING", "LESS", "LEVEL", "LIFETIME", "LIGHTWEIGHT",
                "LIMITS", "LINEAR", "LIST", "LIVE", "LOCAL", "M", "MASK", "MASKING", "MASTER", "MATCH",
                "MATERIALIZE", "MATERIALIZED", "MAX", "MCS", "MEMORY", "MERGES", "METHODS", "METRICS", "MI",
                "MICROSECOND", "MICROSECONDS", "MILLISECOND", "MILLISECONDS", "MIN", "MINUTE", "MINUTES", "MM",
                "MOD", "MODIFY", "MONTH", "MONTHS", "MOVE", "MS", "MUTATION", "N", "NAME", "NAMED", "NANOSECOND",
                "NANOSECONDS", "NEW", "NEXT", "NO", "NO_AUTHENTICATION", "NONE", "NO_PASSWORD", "NS", "NULL",
                "NULLS", "OBJECT", "OPTIMIZE", "OPTION", "OR", "OUTER", "OUTFILE", "OVER", "OVERRIDABLE",
                "OVERRIDE", "PART", "PARTIAL", "PARTITION", "PARTITIONS", "PART_MOVE_TO_SHARD", "PARTS",
                "PATCHES", "PAUSE", "PERIODIC", "PERMANENTLY", "PERMISSIVE", "PERSISTENT", "PIPELINE", "PLAN",
                "PLAINTEXT_PASSWORD", "POLICY", "POPULATE", "PRECEDING", "PRECISION", "PREFIX", "PREPARE",
                "PRIMARY", "PRIORITY", "PRIVILEGES", "PROCESSLIST", "PROFILE", "PROFILES", "PROJECTION",
                "PROTOBUF", "PULL", "Q", "QQ", "QUARTER", "QUARTERS", "QUERY", "QUOTA", "RANDOMIZE",
                "RANDOMIZED", "RANGE", "READ", "READONLY", "REALM", "RECOMPRESS", "RECURSIVE", "REFERENCES",
                "REFRESH", "REGEXP", "REMOVE", "RENAME", "REPLACE", "REPLICATED", "RESET", "RESOURCE", "RESPECT",
                "RESTORE", "RESTRICT", "RESTRICTIVE", "RESUME", "REVOKE", "REWRITE", "ROLE", "ROLES", "ROLLBACK",
                "ROLLUP", "ROW", "ROWS", "S", "S3", "SALT", "SAN", "SCHEME", "SCRAM_SHA256_HASH",
                "SCRAM_SHA256_PASSWORD", "SECOND", "SECONDS", "SECURITY", "SELECT", "SEQUENTIAL", "SERVER",
                "SET", "SETS", "SETTING", "SHA256_HASH", "SHA256_PASSWORD", "SHARD", "SHOW", "SIGNED", "SIMPLE",
                "SKIP", "SNAPSHOT", "SOURCE", "SPATIAL", "SQL", "SQL_TSI_DAY", "SQL_TSI_HOUR",
                "SQL_TSI_MICROSECOND", "SQL_TSI_MILLISECOND", "SQL_TSI_MINUTE", "SQL_TSI_MONTH",
                "SQL_TSI_NANOSECOND", "SQL_TSI_QUARTER", "SQL_TSI_SECOND", "SQL_TSI_WEEK", "SQL_TSI_YEAR", "SS",
                "SSH_KEY", "SSL_CERTIFICATE", "STALENESS", "START", "STATISTIC", "STATISTICS", "STDOUT", "STEP",
                "STORAGE", "STRICT", "STRICTLY_ASCENDING", "SUBPARTITION", "SUBPARTITIONS", "SUSPEND", "SYNC",
                "SYNTAX", "SYSTEM", "TABLE", "TABLES", "TAG", "TAGS", "TEMPORARY", "TEST", "THAN", "THEN",
                "THREAD", "TIES", "TIME", "TIMESTAMP", "TO", "TOP", "TOTALS", "TRACKING", "TRAILING",
                "TRANSACTION", "TREE", "TRIGGER", "TRUE", "TRUNCATE", "TTL", "TYPE", "TYPEOF", "UNBOUNDED",
                "UNDROP", "UNFREEZE", "UNIQUE", "UNLOCK", "UNSET", "UNSIGNED", "UNTIL", "UPDATE", "URL", "USE",
                "USER", "VALID", "VALUES", "VARYING", "VIEW", "VISIBLE", "VOLUME", "WATCH", "WATERMARK", "WEEK",
                "WEEKS", "WHEN", "WITH_ITEMINDEX", "WK", "WORKER", "WORKLOAD", "WRITABLE", "WRITE", "WW",
                "YEAR", "YEARS", "YY", "YYYY", "ZKPATH");
    }

    private static Set<String> buildKeywordSet(String... values) {
        LinkedHashSet<String> keywords = new LinkedHashSet<>();
        if (values != null) {
            Collections.addAll(keywords, values);
        }
        return Collections.unmodifiableSet(keywords);
    }

    public static Set<String> getKeywordGroup(String groupName) {
        return KEYWORD_GROUP_ALLOWED_ALIASES.equals(groupName) ? ALLOWED_KEYWORD_ALIASES : Collections.emptySet();
    }

    public static boolean isQuote(char ch) {
        return ch == '"' || ch == '\'' || ch == '`';
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
        StringBuilder sb = new StringBuilder(len + 10).append(quote);

        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (ch == quote || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }

        return sb.append(quote).toString();
    }

    /**
     * Unescape quoted string.
     * 
     * @param str quoted string
     * @return unescaped string
     */
    public static String unescape(String str) {
        if (str == null || str.isEmpty()) {
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

    private ClickHouseSqlUtils() {
    }
}
