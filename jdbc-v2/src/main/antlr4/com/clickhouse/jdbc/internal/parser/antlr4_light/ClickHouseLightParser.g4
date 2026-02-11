
// Simplified ClickHouse SQL parser.
//
// Design goals:
// - Filter comments (handled by the lexer: /* */, --, #!, #)
// - Detect statement verb (SELECT, SHOW, INSERT, SET, EXPLAIN, etc.)
// - For INSERT: parse table name and optional column list
// - For SET: parse completely (key = value pairs)
// - For everything else: accept any tokens without detailed parsing

parser grammar ClickHouseLightParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level entry point

queryStmt
    : insertStmt SEMICOLON? EOF   # InsertQueryStmt
    | setStmt SEMICOLON? EOF      # SetQueryStmt
    | selectStmt SEMICOLON? EOF   # SelectQueryStmt
    | showStmt SEMICOLON? EOF     # ShowQueryStmt
    | explainStmt SEMICOLON? EOF  # ExplainQueryStmt
    | otherStmt SEMICOLON? EOF    # OtherQueryStmt
    ;

// INSERT - parse table identifier and optional column list, accept rest

insertStmt
    : INSERT INTO FUNCTION identifier LPAREN functionArgs RPAREN columnsClause? restOfQuery # InsertFunctionStmt
    | INSERT INTO TABLE? tableIdentifier columnsClause? restOfQuery                         # InsertTableStmt
    ;

columnsClause
    : LPAREN nestedIdentifier (COMMA nestedIdentifier)* RPAREN
    ;

functionArgs
    : (LPAREN functionArgs RPAREN | ~(LPAREN | RPAREN))*
    ;

// SET - fully parsed

setStmt
    : SET settingExprList
    ;

settingExprList
    : settingExpr (COMMA settingExpr)*
    ;

settingExpr
    : identifier EQ_SINGLE (literal | JDBC_PARAM_PLACEHOLDER)
    ;

// SELECT (including WITH ... SELECT for CTEs) - detect verb, accept rest

selectStmt
    : (WITH | SELECT) restOfQuery
    ;

// SHOW - detect verb, accept rest

showStmt
    : SHOW restOfQuery
    ;

// EXPLAIN - detect verb, accept rest

explainStmt
    : EXPLAIN restOfQuery
    ;

// Any other statement - accept all tokens

otherStmt
    : ~SEMICOLON+
    ;

// Consume all remaining non-semicolon tokens

restOfQuery
    : ~SEMICOLON*
    ;

// Table and column identifiers

tableIdentifier
    : (identifier DOT)? identifier
    ;

nestedIdentifier
    : identifier (DOT identifier)?
    ;

// Literals

literal
    : numberLiteral
    | STRING_LITERAL
    | NULL_SQL
    | JSON_TRUE
    | JSON_FALSE
    ;

numberLiteral
    : (PLUS | DASH)? (
        FLOATING_LITERAL
        | OCTAL_LITERAL
        | DECIMAL_LITERAL
        | HEXADECIMAL_LITERAL
        | INF
        | NAN_SQL
    )
    ;

// Identifiers - all keywords can be used as identifiers

identifier
    : BACKTICK_ID
    | QUOTED_IDENTIFIER
    | IDENTIFIER
    | keyword
    ;

// All keywords (so they can appear as table names, column names, setting names, etc.)

keyword
    : ACCESS | ADD | ADMIN | AFTER | ALIAS | ALL | ALLOW | ALTER | AND | ANTI | ANY
    | ARBITRARY | ARRAY | AS | ASCENDING | ASOF | AST | ASYNC | ASYNCHRONOUS | ATTACH
    | AZURE
    | BACKUP | BCRYPT_HASH | BCRYPT_PASSWORD | BETWEEN | BLOCKING | BOTH | BY
    | CACHE | CACHES | CANCEL | CASE | CAST | CHANGEABLE_IN_READONLY | CHANGED | CHECK
    | CLEANUP | CLEAR | CLIENT | CLUSTER | CLUSTERS | CN | CODEC | COLLATE | COLLECTION
    | COLLECTIONS | COLUMN | COLUMNS | COMMENT | COMPILED | CONDITION | CONFIG | CONNECTIONS
    | CONST | CONSTRAINT | CREATE | CROSS | CUBE | CURRENT | CURRENT_USER | CUSTOM
    | DATABASE | DATABASES | DATE | DAY | DEDUPLICATE | DEFAULT | DEFINER | DELAY | DELETE
    | DESC | DESCENDING | DESCRIBE | DETACH | DICTIONARIES | DICTIONARY | DISK | DISTINCT
    | DISTRIBUTED | DNS | DOUBLE_SHA1_HASH | DOUBLE_SHA1_PASSWORD | DROP
    | ELSE | EMBEDDED | ENABLED | END | ENGINE | ENGINES | ESTIMATE | EVENTS | EXCEPT
    | EXCHANGE | EXISTS | EXPLAIN | EXPRESSION | EXTENDED | EXTRACT
    | FAILPOINT | FETCH | FETCHES | FILE | FILESYSTEM | FILTER | FINAL | FIRST | FLUSH
    | FOLLOWING | FOR | FORMAT | FREEZE | FROM | FULL | FUNCTION | FUNCTIONS | FUZZER
    | GLOBAL | GRANT | GRANTEES | GRANTS | GRANULARITY | GROUP | GRPC
    | HAVING | HDFS | HIERARCHICAL | HIVE | HOST | HOUR | HTTP | HTTPS
    | ID | IDENTIFIED | IF | ILIKE | IMPLICIT | IN | INDEX | INDEXES | INDICES
    | INHERIT | INJECTIVE | INNER | INSERT | INTERVAL | INTO | INTROSPECTION | IP | IS
    | IS_OBJECT_ID
    | JDBC | JEMALLOC | JOIN | JSON_FALSE | JSON_TRUE
    | KAFKA | KERBEROS | KEY | KEYED | KEYS | KILL
    | LAST | LAYOUT | LDAP | LEADING | LEFT | LIFETIME | LIGHTWEIGHT | LIKE | LIMIT | LIMITS
    | LISTEN | LIVE | LOAD | LOADING | LOCAL | LOG | LOGS
    | MANAGEMENT | MARK | MATERIALIZE | MATERIALIZED | MAX | MERGES | METRICS | MIN | MINUTE
    | MMAP | MODEL | MODIFY | MONGO | MONTH | MOVE | MOVES | MUTATION | MYSQL
    | NAME | NAMED | NATS | NO | NONE | NOT | NO_PASSWORD | NULLS | NULL_SQL
    | ODBC | OFFSET | ON | ONLY | OPTIMIZE | OPTION | OR | ORDER | OUTER | OUTFILE | OVER
    | OVERRIDE
    | PAGE | PART | PARTITION | PARTS | PERMISSIVE | PIPELINE | PLAINTEXT_PASSWORD | PLAN
    | POLICIES | POLICY | POPULATE | POSTGRES | POSTGRESQL | PRECEDING | PREWHERE | PRIMARY
    | PROCESSLIST | PROFILE | PROFILES | PROJECTION | PROMETHEUS | PROXY | PULL | PULLING
    | QUARTER | QUERIES | QUERY | QUEUE | QUEUES | QUOTA | QUOTAS
    | RABBITMQ | RANDOMIZED | RANGE | READINESS | READONLY | REALM | REDIS | REDUCE | REFRESH
    | REGEXP | RELOAD | REMOTE | REMOVE | RENAME | REPLACE | REPLICA | REPLICAS | REPLICATED
    | REPLICATION | RESOURCE | RESTART | RESTORE | RESTRICTIVE | REVOKE | RIGHT | ROLE | ROLES
    | ROLLUP | ROW | ROWS
    | S3 | SAMPLE | SCHEMA | SCRAM_SHA256_HASH | SCRAM_SHA256_PASSWORD | SECOND | SECRETS
    | SECURE | SECURITY | SELECT | SEMI | SENDS | SERVER | SET | SETTING | SETTINGS
    | SHA256_HASH | SHA256_PASSWORD | SHARD | SHARDS | SHOW | SHUTDOWN | SOURCE | SOURCES
    | SQLITE | SQL | SSH_KEY | SSL_CERTIFICATE | START | STATISTICS | STOP | STRICT
    | SUBSTRING | SYNC | SYNTAX | SYSTEM
    | TABLE | TABLES | TAG | TCP | TEMPORARY | TEST | THEN | THREAD | TIES | TIMEOUT
    | TIMESTAMP | TO | TOP | TOTALS | TRACKING | TRAILING | TRANSACTION | TREE | TRIM
    | TRUNCATE | TTL | TYPE
    | UNBOUNDED | UNCOMPRESSED | UNDROP | UNFREEZE | UNION | UNLOAD | UNTIL | UPDATE | URL
    | USE | USER | USERS | USING | UUID
    | VALID | VALUES | VIEW | VIEWS | VIRTUAL | VOLUME
    | WAIT | WATCH | WEEK | WHEN | WHERE | WINDOW | WITH | WORKLOAD | WRITABLE
    | YEAR | ZKPATH
    | SUM | AVG
    ;
