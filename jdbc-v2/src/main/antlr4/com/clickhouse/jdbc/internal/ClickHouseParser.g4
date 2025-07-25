
// $antlr-format alignColons hanging, alignSemicolons hanging, alignTrailingComments true, allowShortBlocksOnASingleLine true
// $antlr-format allowShortRulesOnASingleLine false, columnLimit 150, maxEmptyLinesToKeep 1, minEmptyLines 1, reflowComments false, useTab false

parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryStmt
    : query (INTO OUTFILE STRING_LITERAL)? (FORMAT identifierOrNull)? (SEMICOLON)?
    | insertStmt
    ;

query
    : alterStmt  // DDL
    | attachStmt // DDL
    | checkStmt
    | createStmt // DDL
    | describeStmt
    | dropStmt // DDL
    | existsStmt
    | explainStmt
    | killStmt     // DDL
    | optimizeStmt // DDL
    | renameStmt   // DDL
    | selectUnionStmt
    | setStmt
    | setRoleStmt
    | showStmt
    | systemStmt
    | truncateStmt // DDL
    | useStmt
    | watchStmt
    | ctes? selectStmt
    | grantStmt
    ;

// CTE statement
ctes
    : LPAREN? WITH (cteUnboundCol COMMA)* namedQuery (COMMA namedQuery)* RPAREN?
    ;

namedQuery
    : name = identifier (columnAliases)? AS LPAREN query RPAREN
    ;

columnAliases
    : LPAREN identifier (',' identifier)* RPAREN
    ;

cteUnboundCol
    : (literal AS identifier) # CteUnboundColLiteral
    | (QUERY AS identifier) # CteUnboundColParam
    ;

// ALTER statement

alterStmt
    : ALTER TABLE tableIdentifier clusterClause? alterTableClause (COMMA alterTableClause)* # AlterTableStmt
    ;

alterTableClause
    : ADD COLUMN (IF NOT EXISTS)? tableColumnDfnt (AFTER nestedIdentifier)?         # AlterTableClauseAddColumn
    | ADD INDEX (IF NOT EXISTS)? tableIndexDfnt (AFTER nestedIdentifier)?           # AlterTableClauseAddIndex
    | ADD PROJECTION (IF NOT EXISTS)? tableProjectionDfnt (AFTER nestedIdentifier)? # AlterTableClauseAddProjection
    | ATTACH partitionClause (FROM tableIdentifier)?                                # AlterTableClauseAttach
    | CLEAR COLUMN (IF EXISTS)? nestedIdentifier (IN partitionClause)?              # AlterTableClauseClearColumn
    | CLEAR INDEX (IF EXISTS)? nestedIdentifier (IN partitionClause)?               # AlterTableClauseClearIndex
    | CLEAR PROJECTION (IF EXISTS)? nestedIdentifier (IN partitionClause)?          # AlterTableClauseClearProjection
    | COMMENT COLUMN (IF EXISTS)? nestedIdentifier STRING_LITERAL                   # AlterTableClauseComment
    | DELETE WHERE columnExpr                                                       # AlterTableClauseDelete
    | DETACH partitionClause                                                        # AlterTableClauseDetach
    | DROP COLUMN (IF EXISTS)? nestedIdentifier                                     # AlterTableClauseDropColumn
    | DROP INDEX (IF EXISTS)? nestedIdentifier                                      # AlterTableClauseDropIndex
    | DROP PROJECTION (IF EXISTS)? nestedIdentifier                                 # AlterTableClauseDropProjection
    | DROP partitionClause                                                          # AlterTableClauseDropPartition
    | FREEZE partitionClause?                                                       # AlterTableClauseFreezePartition
    | MATERIALIZE INDEX (IF EXISTS)? nestedIdentifier (IN partitionClause)?         # AlterTableClauseMaterializeIndex
    | MATERIALIZE PROJECTION (IF EXISTS)? nestedIdentifier (IN partitionClause)?    # AlterTableClauseMaterializeProjection
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier codecExpr                         # AlterTableClauseModifyCodec
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier COMMENT STRING_LITERAL            # AlterTableClauseModifyComment
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier REMOVE tableColumnPropertyType    # AlterTableClauseModifyRemove
    | MODIFY COLUMN (IF EXISTS)? tableColumnDfnt                                    # AlterTableClauseModify
    | MODIFY ORDER BY columnExpr                                                    # AlterTableClauseModifyOrderBy
    | MODIFY ttlClause                                                              # AlterTableClauseModifyTTL
    | MOVE partitionClause (
        TO DISK STRING_LITERAL
        | TO VOLUME STRING_LITERAL
        | TO TABLE tableIdentifier
    )                                                                 # AlterTableClauseMovePartition
    | REMOVE TTL                                                      # AlterTableClauseRemoveTTL
    | RENAME COLUMN (IF EXISTS)? nestedIdentifier TO nestedIdentifier # AlterTableClauseRename
    | REPLACE partitionClause FROM tableIdentifier                    # AlterTableClauseReplace
    | UPDATE assignmentExprList whereClause                           # AlterTableClauseUpdate
    ;

assignmentExprList
    : assignmentExpr (COMMA assignmentExpr)*
    ;

assignmentExpr
    : nestedIdentifier EQ_SINGLE columnExpr
    ;

tableColumnPropertyType
    : ALIAS
    | CODEC
    | COMMENT
    | DEFAULT
    | MATERIALIZED
    | TTL
    ;

partitionClause
    : PARTITION columnExpr // actually we expect here any form of tuple of literals
    | PARTITION ID STRING_LITERAL
    ;

// ATTACH statement
attachStmt
    : ATTACH DICTIONARY tableIdentifier clusterClause? # AttachDictionaryStmt
    ;

// CHECK statement

checkStmt
    : CHECK TABLE tableIdentifier partitionClause?
    ;

// CREATE statement

createStmt
    : (ATTACH | CREATE) DATABASE (IF NOT EXISTS)? databaseIdentifier clusterClause? engineExpr? # CreateDatabaseStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) DICTIONARY (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? dictionarySchemaClause
        dictionaryEngineClause # CreateDictionaryStmt
    | (ATTACH | CREATE) LIVE VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? (
        WITH TIMEOUT DECIMAL_LITERAL?
    )? destinationClause? tableSchemaClause? subqueryClause # CreateLiveViewStmt
    | (ATTACH | CREATE) MATERIALIZED VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? (
        destinationClause
        | engineClause POPULATE?
    ) subqueryClause # CreateMaterializedViewStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause?
        engineClause? subqueryClause?                                                                                                    # CreateTableStmt
    | (ATTACH | CREATE) (OR REPLACE)? VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? subqueryClause #
        CreateViewStmt
    | CREATE USER ((IF NOT EXISTS) | (OR REPLACE))? userIdentifier (COMMA userIdentifier)* clusterClause?
        userIdentifiedClause?
        userCreateHostClause?
        validUntilClause?
        (DEFAULT ROLE identifier (COMMA identifier)*)?
        (DEFAULT DATABASE identifier | NONE)?
        settingsClause? #CreateUserStmt
    | CREATE ROLE (IF NOT EXISTS | OR REPLACE)? identifier (COMMA identifier)* clusterClause?
        (IN identifier)? settingsClause? #CreateRoleStmt
    | CREATE (ROW)? POLICY (IF NOT EXISTS | OR REPLACE)? identifier clusterClause? ON tableIdentifier
        (IN identifier)? (AS (PERMISSIVE | RESTRICTIVE))? (FOR SELECT)? USING columnExpr
        (TO identifier | ALL | ALL EXCEPT identifier)? # CreatePolicyStmt
    ;

userIdentifier
    : (IDENTIFIER | STRING_LITERAL)
    ;

userIdentifiedClause
    : IDENTIFIED BY literal
    | IDENTIFIED WITH userIdentifiedWithClause validUntilClause? (COMMA userIdentifiedWithClause VALID UNTIL literal)*
    |  NOT IDENTIFIED
    ;

userIdentifiedWithClause
    : (PLAINTEXT_PASSWORD | SHA256_PASSWORD | SHA256_HASH | DOUBLE_SHA1_PASSWORD | DOUBLE_SHA1_HASH | SCRAM_SHA256_PASSWORD |  SCRAM_SHA256_HASH | BCRYPT_PASSWORD | BCRYPT_HASH) BY literal
    | NO_PASSWORD
    | LDAP SERVER literal
    | KERBEROS (REALM literal)?
    | SSL_CERTIFICATE CN literal
    | SSH_KEY BY KEY literal TYPE literal (COMMA KEY literal TYPE literal)*
    | HTTP SERVER literal (SCHEMA literal)?
    ;

userCreateHostClause
    : HOST (userCreateHostDef (COMMA userCreateHostDef)*) | ANY | NONE
    ;

userCreateHostDef
    : LOCAL | NAME literal | REGEXP literal | IP literal | LIKE literal
    ;

userCreateGranteesClause
    : GRANTEES (identifier | STRING_LITERAL | ANY | NONE ) (COMMA (identifier | STRING_LITERAL | ANY | NONE ))*
        (EXCEPT (identifier | STRING_LITERAL) (COMMA (identifier | STRING_LITERAL ))*)
    ;
validUntilClause
    : VALID UNTIL interval
    ;

dictionarySchemaClause
    : LPAREN dictionaryAttrDfnt (COMMA dictionaryAttrDfnt)* RPAREN
    ;

dictionaryAttrDfnt
    : identifier columnTypeExpr
    ;

dictionaryEngineClause
    : dictionaryPrimaryKeyClause?
    ;

dictionaryPrimaryKeyClause
    : PRIMARY KEY columnExprList
    ;

dictionaryArgExpr
    : identifier (identifier (LPAREN RPAREN)? | literal)
    ;

sourceClause
    : SOURCE LPAREN identifier LPAREN dictionaryArgExpr* RPAREN RPAREN
    ;

lifetimeClause
    : LIFETIME LPAREN (
        DECIMAL_LITERAL
        | MIN DECIMAL_LITERAL MAX DECIMAL_LITERAL
        | MAX DECIMAL_LITERAL MIN DECIMAL_LITERAL
    ) RPAREN
    ;

layoutClause
    : LAYOUT LPAREN identifier LPAREN dictionaryArgExpr* RPAREN RPAREN
    ;

rangeClause
    : RANGE LPAREN (MIN identifier MAX identifier | MAX identifier MIN identifier) RPAREN
    ;

dictionarySettingsClause
    : SETTINGS LPAREN settingExprList RPAREN
    ;

clusterClause
    : ON CLUSTER (identifier | STRING_LITERAL)
    ;

uuidClause
    : UUID STRING_LITERAL
    ;

destinationClause
    : TO tableIdentifier
    ;

subqueryClause
    : AS selectUnionStmt
    ;

tableSchemaClause
    : LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN # SchemaDescriptionClause
    | AS tableIdentifier                                       # SchemaAsTableClause
    | AS tableFunctionExpr                                     # SchemaAsFunctionClause
    ;

engineClause
    : engineExpr
    ;

partitionByClause
    : PARTITION BY columnExpr
    ;

primaryKeyClause
    : PRIMARY KEY columnExpr
    ;

sampleByClause
    : SAMPLE BY columnExpr
    ;

ttlClause
    : TTL ttlExpr (COMMA ttlExpr)*
    ;

engineExpr
    : ENGINE EQ_SINGLE? identifierOrNull (LPAREN columnExprList? RPAREN)?
    ;

tableElementExpr
    : tableColumnDfnt                        # TableElementExprColumn
    | CONSTRAINT identifier CHECK columnExpr # TableElementExprConstraint
    | INDEX tableIndexDfnt                   # TableElementExprIndex
    | PROJECTION tableProjectionDfnt         # TableElementExprProjection
    ;

tableColumnDfnt
    : nestedIdentifier columnTypeExpr tableColumnPropertyExpr? (COMMENT STRING_LITERAL)? codecExpr? (
        TTL columnExpr
    )?
    | nestedIdentifier columnTypeExpr? tableColumnPropertyExpr (COMMENT STRING_LITERAL)? codecExpr? (
        TTL columnExpr
    )?
    ;

tableColumnPropertyExpr
    : (DEFAULT | MATERIALIZED | ALIAS) columnExpr
    ;

tableIndexDfnt
    : nestedIdentifier columnExpr TYPE columnTypeExpr GRANULARITY DECIMAL_LITERAL
    ;

tableProjectionDfnt
    : nestedIdentifier projectionSelectStmt
    ;

codecExpr
    : CODEC LPAREN codecArgExpr (COMMA codecArgExpr)* RPAREN
    ;

codecArgExpr
    : identifier (LPAREN columnExprList? RPAREN)?
    ;

ttlExpr
    : columnExpr (DELETE | TO DISK STRING_LITERAL | TO VOLUME STRING_LITERAL)?
    ;

// DESCRIBE statement

describeStmt
    : (DESCRIBE | DESC) TABLE? tableExpr
    ;

// DROP statement

dropStmt
    : (DETACH | DROP) DATABASE (IF EXISTS)? databaseIdentifier clusterClause? # DropDatabaseStmt
    | (DETACH | DROP) (DICTIONARY | TEMPORARY? TABLE | VIEW | ROLE | USER) (IF EXISTS)? tableIdentifier clusterClause? (
        NO DELAY
    )? # DropTableStmt
    ;

// EXISTS statement

existsStmt
    : EXISTS DATABASE databaseIdentifier                             # ExistsDatabaseStmt
    | EXISTS (DICTIONARY | TEMPORARY? TABLE | VIEW)? tableIdentifier # ExistsTableStmt
    ;

// EXPLAIN statement

explainStmt
    : EXPLAIN AST query    # ExplainASTStmt
    | EXPLAIN SYNTAX query # ExplainSyntaxStmt
    ;

// INSERT statement

insertStmt
    : INSERT INTO TABLE? (tableIdentifier | FUNCTION tableFunctionExpr) columnsClause? dataClause
    ;

columnsClause
    : LPAREN nestedIdentifier (COMMA nestedIdentifier)* RPAREN
    ;

dataClause
    : FORMAT identifier                                 # DataClauseFormat
    | VALUES assignmentValues (COMMA assignmentValues)* # DataClauseValues
    | selectUnionStmt SEMICOLON? EOF                    # DataClauseSelect
    ;

assignmentValues
    : LPAREN assignmentValue (COMMA assignmentValue)* RPAREN  # AssignmentValuesList
    | LPAREN RPAREN                                           # AssignmentValuesEmpty
    ;

assignmentValue
    : literal   # InsertRawValue
    | QUERY     # InsertParameter
    | identifier (LPAREN columnExprList? RPAREN)? # InsertParameterFuncExpr
    | LPAREN columnExpr RPAREN # InserParameterExpr
    ;

// KILL statement

killStmt
    : KILL MUTATION clusterClause? whereClause (SYNC | ASYNC | TEST)? # KillMutationStmt
    ;

// OPTIMIZE statement

optimizeStmt
    : OPTIMIZE TABLE tableIdentifier clusterClause? partitionClause? FINAL? DEDUPLICATE?
    ;

// RENAME statement

renameStmt
    : RENAME TABLE tableIdentifier TO tableIdentifier (COMMA tableIdentifier TO tableIdentifier)* clusterClause?
    ;

// PROJECTION SELECT statement

projectionSelectStmt
    : LPAREN withClause? SELECT columnExprList groupByClause? projectionOrderByClause? RPAREN
    ;

// SELECT statement

selectUnionStmt
    : selectStmtWithParens (UNION (ALL|DISTINCT)? selectStmtWithParens)*
    ;

selectStmtWithParens
    : selectStmt
    | LPAREN selectUnionStmt RPAREN
    ;

selectStmt
    : withClause? SELECT DISTINCT? topClause? columnExprList fromClause? arrayJoinClause? windowClause? prewhereClause? whereClause? groupByClause? (
        WITH (CUBE | ROLLUP)
    )? (WITH TOTALS)? havingClause? orderByClause? limitByClause? limitClause? settingsClause?
    ;

withClause
    : WITH columnExprList
    ;

topClause
    : TOP DECIMAL_LITERAL (WITH TIES)?
    ;

fromClause
    : FROM joinExpr
    | FROM identifier LPAREN QUERY RPAREN
    | FROM ctes
    ;

arrayJoinClause
    : (LEFT | INNER)? ARRAY JOIN columnExprList
    ;

windowClause
    : WINDOW identifier AS LPAREN windowExpr RPAREN
    ;

prewhereClause
    : PREWHERE columnExpr
    ;

whereClause
    : WHERE columnExpr
    ;

groupByClause
    : GROUP BY ((CUBE | ROLLUP) LPAREN columnExprList RPAREN | columnExprList)
    ;

havingClause
    : HAVING columnExpr
    ;

orderByClause
    : ORDER BY orderExprList
    ;

projectionOrderByClause
    : ORDER BY columnExprList
    ;

limitByClause
    : LIMIT limitExpr BY columnExprList
    ;

limitClause
    : LIMIT limitExpr (WITH TIES)?
    ;

settingsClause
    : SETTINGS settingExprList
    ;

joinExpr
    : joinExpr (GLOBAL | LOCAL)? joinOp? JOIN joinExpr joinConstraintClause # JoinExprOp
    | joinExpr joinOpCross joinExpr                                         # JoinExprCrossOp
    | tableExpr FINAL? sampleClause?                                        # JoinExprTable
    | LPAREN joinExpr RPAREN                                                # JoinExprParens
    ;

joinOp
    : ((ALL | ANY | ASOF)? INNER | INNER (ALL | ANY | ASOF)? | (ALL | ANY | ASOF)) # JoinOpInner
    | (
        (SEMI | ALL | ANTI | ANY | ASOF)? (LEFT | RIGHT) OUTER?
        | (LEFT | RIGHT) OUTER? (SEMI | ALL | ANTI | ANY | ASOF)?
    )                                                       # JoinOpLeftRight
    | ((ALL | ANY)? FULL OUTER? | FULL OUTER? (ALL | ANY)?) # JoinOpFull
    ;

joinOpCross
    : (GLOBAL | LOCAL)? CROSS JOIN
    | COMMA
    ;

joinConstraintClause
    : ON columnExprList
    | USING LPAREN columnExprList RPAREN
    | USING columnExprList
    ;

sampleClause
    : SAMPLE ratioExpr (OFFSET ratioExpr)?
    ;

limitExpr
    : columnExpr ((COMMA | OFFSET) columnExpr)?
    ;

orderExprList
    : orderExpr (COMMA orderExpr)*
    ;

orderExpr
    : columnExpr (ASCENDING | DESCENDING | DESC)? (NULLS (FIRST | LAST))? (COLLATE STRING_LITERAL)?
    ;

ratioExpr
    : numberLiteral (SLASH numberLiteral)?
    ;

settingExprList
    : settingExpr (COMMA settingExpr)*
    ;

settingExpr
    : identifier EQ_SINGLE literal
    ;

windowExpr
    : winPartitionByClause? winOrderByClause? winFrameClause?
    ;

winPartitionByClause
    : PARTITION BY columnExprList
    ;

winOrderByClause
    : ORDER BY orderExprList
    ;

winFrameClause
    : (ROWS | RANGE) winFrameExtend
    ;

winFrameExtend
    : winFrameBound                           # frameStart
    | BETWEEN winFrameBound AND winFrameBound # frameBetween
    ;

winFrameBound
    : (
        CURRENT ROW
        | UNBOUNDED PRECEDING
        | UNBOUNDED FOLLOWING
        | numberLiteral PRECEDING
        | numberLiteral FOLLOWING
    )
    ;

//rangeClause: RANGE LPAREN (MIN identifier MAX identifier | MAX identifier MIN identifier) RPAREN;

// SET statement

setStmt
    : SET settingExprList
    ;

// SET ROLE statement

setRoleStmt
    : SET (DEFAULT)? ROLE (setRolesList | NONE | ALL (EXCEPT setRolesList)) (TO identifier | CURRENT_USER (COMMA identifier | CURRENT_USER)*)?
    ;

setRolesList
    : identifier (COMMA identifier)*
    ;

grantStmt
    : GRANT clusterClause? ((privilege ON grantTableIdentifier) | (identifier (COMMA identifier)*))
        TO (CURRENT_USER | identifier) (COMMA identifier)*
        (WITH GRANT OPTION)? (WITH REPLACE OPTION)?
    ;

grantTableIdentifier
    : (identifier DOT)? identifier
    | (identifier DOT)? ASTERISK
    | (ASTERISK DOT)? identifier
    | (ASTERISK DOT)? ASTERISK
    ;

privilege
    :
    | ACCESS MANAGEMENT
    | ALLOW SQL SECURITY NONE
    | ROLE ADMIN
    | TABLE ENGINE
    | TRUNCATE
    | UNDROP TABLE
    | NONE
    | BACKUP
    | CLUSTER
    | INSERT
    | INTROSPECTION
    | KILL QUERY
    | KILL TRANSACTION
    | MOVE PARTITION BETWEEN SHARDS
    | NAMED COLLECTION ADMIN
    | ALTER NAMED COLLECTION
    | CREATE NAMED COLLECTION
    | NAMED COLLECTION
    | OPTIMIZE
    | SELECT
    | SET DEFINER
    | alterPrivilege
    | createPrivilege
    | dropPrivilege
    | showPrivilege
    | sourcePrivilege
    | systemPrivilege
    ;

alterPrivilege
    :
    | ALTER QUOTA
    | ALTER ROLE
    | ALTER ROW POLICY
    | ALTER SETTINGS PROFILE
    | ALTER USER
    | ALTER
    | ALTER DATABASE
    | ALTER DATABASE SETTINGS
    | ALTER TABLE
    | ALTER COLUMN
    | ALTER ADD COLUMN
    | ALTER CLEAR COLUMN
    | ALTER COMMENT COLUMN
    | ALTER DROP COLUMN
    | ALTER MATERIALIZE COLUMN
    | ALTER MODIFY COLUMN
    | ALTER RENAME COLUMN
    | ALTER CONSTRAINT
    | ALTER ADD CONSTRAINT
    | ALTER DROP CONSTRAINT
    | ALTER DELETE
    | ALTER FETCH PARTITION
    | ALTER FREEZE PARTITION
    | ALTER INDEX
    | ALTER ADD INDEX
    | ALTER CLEAR INDEX
    | ALTER DROP INDEX
    | ALTER MATERIALIZE INDEX
    | ALTER ORDER BY
    | ALTER SAMPLE BY
    | ALTER MATERIALIZE TTL
    | ALTER MODIFY COMMENT
    | ALTER MOVE PARTITION
    | ALTER PROJECTION
    | ALTER SETTINGS
    | ALTER STATISTICS
    | ALTER ADD STATISTICS
    | ALTER DROP STATISTICS
    | ALTER MATERIALIZE STATISTICS
    | ALTER MODIFY STATISTICS
    | ALTER TTL
    | ALTER UPDATE
    | ALTER VIEW
    | ALTER VIEW MODIFY QUERY
    | ALTER VIEW REFRESH
    | ALTER VIEW MODIFY SQL SECURITY
    ;

createPrivilege
    :CREATE QUOTA
    | CREATE ROLE
    | CREATE ROW POLICY
    | CREATE SETTINGS PROFILE
    | CREATE USER
    | CREATE
    | CREATE ARBITRARY TEMPORARY TABLE
    | CREATE TEMPORARY TABLE
    | CREATE DATABASE
    | CREATE DICTIONARY
    | CREATE FUNCTION
    | CREATE RESOURCE
    | CREATE TABLE
    | CREATE VIEW
    | CREATE WORKLOAD
    ;

dropPrivilege
    : DROP QUOTA
    | DROP ROLE
    | DROP ROW POLICY
    | DROP SETTINGS PROFILE
    | DROP USER
    | DROP
    | DROP DATABASE
    | DROP DICTIONARY
    | DROP FUNCTION
    | DROP RESOURCE
    | DROP TABLE
    | DROP VIEW
    | DROP WORKLOAD
    | DROP NAMED COLLECTION
    ;

showPrivilege
    : SHOW ACCESS
    | SHOW QUOTAS
    | SHOW ROLES
    | SHOW ROW POLICIES
    | SHOW SETTINGS PROFILES
    | SHOW USERS
    | SHOW
    | SHOW COLUMNS
    | SHOW DATABASES
    | SHOW DICTIONARIES
    | SHOW TABLES
    | SHOW FILESYSTEM CACHES
    | SHOW NAMED COLLECTIONS
    | SHOW NAMED COLLECTIONS SECRETS
    ;

sourcePrivilege
    : SOURCES
    | AZURE
    | FILE
    | HDFS
    | HIVE
    | JDBC
    | KAFKA
    | MONGO
    | MYSQL
    | NATS
    | ODBC
    | POSTGRES
    | RABBITMQ
    | REDIS
    | REMOTE
    | S3
    | SQLITE
    | URL
    ;

systemPrivilege
    : SYSTEM
    | SYSTEM CLEANUP
    | SYSTEM DROP CACHE
    | SYSTEM DROP COMPILED EXPRESSION CACHE
    | SYSTEM DROP CONNECTIONS CACHE
    | SYSTEM DROP DISTRIBUTED CACHE
    | SYSTEM DROP DNS CACHE
    | SYSTEM DROP FILESYSTEM CACHE
    | SYSTEM DROP FORMAT SCHEMA CACHE
    | SYSTEM DROP MARK CACHE
    | SYSTEM DROP MMAP CACHE
    | SYSTEM DROP PAGE CACHE
    | SYSTEM DROP PRIMARY INDEX CACHE
    | SYSTEM DROP QUERY CACHE
    | SYSTEM DROP S3 CLIENT CACHE
    | SYSTEM DROP SCHEMA CACHE
    | SYSTEM DROP UNCOMPRESSED CACHE
    | SYSTEM DROP PRIMARY INDEX CACHE
    | SYSTEM DROP REPLICA
    | SYSTEM FAILPOINT
    | SYSTEM FETCHES
    | SYSTEM FLUSH
    | SYSTEM FLUSH ASYNC INSERT QUEUE
    | SYSTEM FLUSH LOGS
    | SYSTEM JEMALLOC
    | SYSTEM KILL QUERY
    | SYSTEM KILL TRANSACTION
    | SYSTEM LISTEN
    | SYSTEM LOAD PRIMARY KEY
    | SYSTEM MERGES
    | SYSTEM MOVES
    | SYSTEM PULLING REPLICATION LOG
    | SYSTEM REDUCE BLOCKING PARTS
    | SYSTEM REPLICATION QUEUES
    | SYSTEM REPLICA READINESS
    | SYSTEM RESTART DISK
    | SYSTEM RESTART REPLICA
    | SYSTEM RESTORE REPLICA
    | SYSTEM RELOAD
    | SYSTEM RELOAD ASYNCHRONOUS METRICS
    | SYSTEM RELOAD CONFIG
    | SYSTEM RELOAD DICTIONARY
    | SYSTEM RELOAD EMBEDDED DICTIONARIES
    | SYSTEM RELOAD FUNCTION
    | SYSTEM RELOAD MODEL
    | SYSTEM RELOAD USERS
    | SYSTEM SENDS
    | SYSTEM DISTRIBUTED SENDS
    | SYSTEM REPLICATED SENDS
    | SYSTEM SHUTDOWN
    | SYSTEM SYNC DATABASE REPLICA
    | SYSTEM SYNC FILE CACHE
    | SYSTEM SYNC FILESYSTEM CACHE
    | SYSTEM SYNC REPLICA
    | SYSTEM SYNC TRANSACTION LOG
    | SYSTEM THREAD FUZZER
    | SYSTEM TTL MERGES
    | SYSTEM UNFREEZE
    | SYSTEM UNLOAD PRIMARY KEY
    | SYSTEM VIEWS
    | SYSTEM VIRTUAL PARTS UPDATE
    | SYSTEM WAIT LOADING PARTS
    ;

// SHOW statements

showStmt
    : SHOW CREATE DATABASE databaseIdentifier                                                                    # showCreateDatabaseStmt
    | SHOW CREATE DICTIONARY tableIdentifier                                                                     # showCreateDictionaryStmt
    | SHOW CREATE TEMPORARY? TABLE? tableIdentifier                                                              # showCreateTableStmt
    | SHOW DATABASES                                                                                             # showDatabasesStmt
    | SHOW DICTIONARIES (FROM databaseIdentifier)?                                                               # showDictionariesStmt
    | SHOW TEMPORARY? TABLES ((FROM | IN) databaseIdentifier)? (LIKE STRING_LITERAL | whereClause)? limitClause? # showTablesStmt
    ;

// SYSTEM statements

systemStmt
    : SYSTEM FLUSH DISTRIBUTED tableIdentifier
    | SYSTEM FLUSH LOGS
    | SYSTEM RELOAD DICTIONARIES
    | SYSTEM RELOAD DICTIONARY tableIdentifier
    | SYSTEM (START | STOP) (DISTRIBUTED SENDS | FETCHES | TTL? MERGES) tableIdentifier
    | SYSTEM (START | STOP) REPLICATED SENDS
    | SYSTEM SYNC REPLICA tableIdentifier
    ;

// TRUNCATE statements

truncateStmt
    : TRUNCATE TEMPORARY? TABLE? (IF EXISTS)? tableIdentifier clusterClause?
    ;

// USE statement

useStmt
    : USE databaseIdentifier
    ;

// WATCH statement

watchStmt
    : WATCH tableIdentifier EVENTS? (LIMIT DECIMAL_LITERAL)?
    ;

// Columns

columnTypeExpr
    : identifier                                                                            # ColumnTypeExprSimple  // UInt64
    | identifier LPAREN identifier columnTypeExpr (COMMA identifier columnTypeExpr)* RPAREN # ColumnTypeExprNested  // Nested
    | identifier LPAREN enumValue (COMMA enumValue)* RPAREN                                 # ColumnTypeExprEnum    // Enum
    | identifier LPAREN columnTypeExpr (COMMA columnTypeExpr)* RPAREN                       # ColumnTypeExprComplex // Array, Tuple
    | identifier LPAREN columnExprList? RPAREN                                              # ColumnTypeExprParam   // FixedString(N)
    ;

columnExprList
    : columnsExpr (COMMA columnsExpr)*
    ;

columnsExpr
    : (tableIdentifier DOT)? ASTERISK # ColumnsExprAsterisk
    | LPAREN selectUnionStmt RPAREN   # ColumnsExprSubquery
    // NOTE: asterisk and subquery goes before |columnExpr| so that we can mark them as multi-column expressions.
    | columnExpr # ColumnsExprColumn
    ;

columnExpr
    : CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END         # ColumnExprCase
    | CAST LPAREN columnExpr AS columnTypeExpr RPAREN                                    # ColumnExprCast
    | columnExpr CAST_OP columnTypeExpr                                                  # ColumnExprCast2
    | DATE STRING_LITERAL                                                                # ColumnExprDate
    | EXTRACT LPAREN interval FROM columnExpr RPAREN                                     # ColumnExprExtract
    | INTERVAL columnExpr interval?                                                      # ColumnExprInterval
    | SUBSTRING LPAREN columnExpr FROM columnExpr (FOR columnExpr)? RPAREN               # ColumnExprSubstring
    | TIMESTAMP STRING_LITERAL                                                           # ColumnExprTimestamp
    | TRIM LPAREN (BOTH | LEADING | TRAILING) STRING_LITERAL FROM columnExpr RPAREN      # ColumnExprTrim
    | identifier (LPAREN columnExprList? RPAREN) OVER LPAREN windowExpr RPAREN           # ColumnExprWinFunction
    | identifier (LPAREN columnExprList? RPAREN) OVER identifier                         # ColumnExprWinFunctionTarget
    | identifier (LPAREN columnExprList? RPAREN)? LPAREN DISTINCT? columnArgList? RPAREN # ColumnExprFunction
    | literal                                                                            # ColumnExprLiteral

    // FIXME(ilezhankin): this part looks very ugly, maybe there is another way to express it
    | columnExpr LBRACKET columnExpr RBRACKET # ColumnExprArrayAccess
    | columnExpr DOT DECIMAL_LITERAL          # ColumnExprTupleAccess
    | DASH columnExpr                         # ColumnExprNegate
    | columnExpr (
        ASTERISK  // multiply
        | SLASH   // divide
        | PERCENT // modulo
    ) columnExpr # ColumnExprPrecedence1
    | columnExpr (
        PLUS     // plus
        | DASH   // minus
        | CONCAT // concat
    ) columnExpr # ColumnExprPrecedence2
    | columnExpr (
        EQ_DOUBLE             // equals
        | EQ_SINGLE           // equals
        | NOT_EQ              // notEquals
        | LE                  // lessOrEquals
        | GE                  // greaterOrEquals
        | LT                  // less
        | GT                  // greater
        | GLOBAL? NOT? IN     // in, notIn, globalIn, globalNotIn
        | NOT? (LIKE | ILIKE) // like, notLike, ilike, notILike
    ) columnExpr                  # ColumnExprPrecedence3
    | columnExpr IS NOT? NULL_SQL # ColumnExprIsNull
    | NOT columnExpr              # ColumnExprNot
    | columnExpr AND columnExpr   # ColumnExprAnd
    | columnExpr OR columnExpr    # ColumnExprOr
    // TODO(ilezhankin): `BETWEEN a AND b AND c` is parsed in a wrong way: `BETWEEN (a AND b) AND c`
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr            # ColumnExprBetween
    | <assoc = right> columnExpr QUERY columnExpr COLON columnExpr # ColumnExprTernaryOp
    | columnExpr (alias | AS identifier)                           # ColumnExprAlias
    | (tableIdentifier DOT)? ASTERISK                              # ColumnExprAsterisk // single-column only
    | LPAREN selectUnionStmt RPAREN                                # ColumnExprSubquery // single-column only
    | LPAREN columnExpr RPAREN                                     # ColumnExprParens   // single-column only
    | LPAREN columnExprList RPAREN                                 # ColumnExprTuple
    | LBRACKET columnExprList? RBRACKET                            # ColumnExprArray
    | columnIdentifier                                             # ColumnExprIdentifier
    | QUERY (CAST_OP identifier)?                                  # ColumnExprParam
    ;

columnArgList
    : columnArgExpr (COMMA columnArgExpr)*
    | QUERY (COMMA QUERY)*
    ;

columnArgExpr
    : columnLambdaExpr
    | columnExpr
    ;

columnLambdaExpr
    : (LPAREN identifier (COMMA identifier)* RPAREN | identifier (COMMA identifier)*) ARROW columnExpr
    ;

columnIdentifier
    : (tableIdentifier DOT)? nestedIdentifier
    ;

nestedIdentifier
    : identifier (DOT identifier)?
    ;

// Tables

tableExpr
    : tableIdentifier                   # TableExprIdentifier
    | tableFunctionExpr                 # TableExprFunction
    | LPAREN selectUnionStmt RPAREN     # TableExprSubquery
    | tableExpr (alias | AS identifier) # TableExprAlias
    ;

tableFunctionExpr
    : identifier LPAREN tableArgList? RPAREN
    ;

tableIdentifier
    : (databaseIdentifier DOT)? identifier
    ;

tableArgList
    : tableArgExpr (COMMA tableArgExpr)*
    ;

tableArgExpr
    : nestedIdentifier
    | tableFunctionExpr
    | literal
    ;

// Databases

databaseIdentifier
    : identifier
    ;

// Basics

floatingLiteral
    : FLOATING_LITERAL
    | DOT (DECIMAL_LITERAL | OCTAL_LITERAL)
    | DECIMAL_LITERAL DOT (DECIMAL_LITERAL | OCTAL_LITERAL)? // can't move this to the lexer or it will break nested tuple access: t.1.2
    ;

numberLiteral
    : (PLUS | DASH)? (
        floatingLiteral
        | OCTAL_LITERAL
        | DECIMAL_LITERAL
        | HEXADECIMAL_LITERAL
        | INF
        | NAN_SQL
    )
    ;

literal
    : numberLiteral
    | STRING_LITERAL
    | NULL_SQL
    ;

interval
    : SECOND
    | MINUTE
    | HOUR
    | DAY
    | WEEK
    | MONTH
    | QUARTER
    | YEAR
    ;

keyword
    // except NULL_SQL, INF, NAN_SQL
    : AFTER
    | ALIAS
    | ALL
    | ALTER
    | AND
    | ANTI
    | ANY
    | ARRAY
    | AS
    | ASCENDING
    | ASOF
    | AST
    | ASYNC
    | ATTACH
    | BETWEEN
    | BOTH
    | BY
    | CASE
    | CAST
    | CHECK
    | CLEAR
    | CLUSTER
    | CODEC
    | COLLATE
    | COLUMN
    | COMMENT
    | CONSTRAINT
    | CREATE
    | CROSS
    | CUBE
    | CURRENT
    | DATABASE
    | DATABASES
    | DATE
    | DEDUPLICATE
    | DEFAULT
    | DELAY
    | DELETE
    | DESCRIBE
    | DESC
    | DESCENDING
    | DETACH
    | DICTIONARIES
    | DICTIONARY
    | DISK
    | DISTINCT
    | DISTRIBUTED
    | DROP
    | ELSE
    | END
    | ENGINE
    | EVENTS
    | EXISTS
    | EXPLAIN
    | EXPRESSION
    | EXTRACT
    | FETCHES
    | FINAL
    | FIRST
    | FLUSH
    | FOR
    | FOLLOWING
    | FOR
    | FORMAT
    | FREEZE
    | FROM
    | FULL
    | FUNCTION
    | GLOBAL
    | GRANULARITY
    | GROUP
    | GRANT
    | HAVING
    | HIERARCHICAL
    | ID
    | IF
    | ILIKE
    | IN
    | INDEX
    | INJECTIVE
    | INNER
    | INSERT
    | INTERVAL
    | INTO
    | IS
    | IS_OBJECT_ID
    | JOIN
    | JSON_FALSE
    | JSON_TRUE
    | KEY
    | KILL
    | LAST
    | LAYOUT
    | LEADING
    | LEFT
    | LIFETIME
    | LIKE
    | LIMIT
    | LIVE
    | LOCAL
    | LOGS
    | MATERIALIZE
    | MATERIALIZED
    | MAX
    | MERGES
    | MIN
    | MODIFY
    | MOVE
    | MUTATION
    | NO
    | NOT
    | NULLS
    | NULL_SQL
    | NAME
    | OFFSET
    | ON
    | OPTIMIZE
    | OR
    | ORDER
    | OUTER
    | OUTFILE
    | OVER
    | PARTITION
    | POPULATE
    | PRECEDING
    | PREWHERE
    | PRIMARY
    | RANGE
    | RELOAD
    | REMOVE
    | RENAME
    | REPLACE
    | REPLICA
    | REPLICATED
    | RIGHT
    | ROLLUP
    | ROW
    | ROWS
    | SAMPLE
    | SELECT
    | SEMI
    | SENDS
    | SET
    | SETTINGS
    | SHOW
    | SOURCE
    | START
    | STOP
    | SUBSTRING
    | SYNC
    | SYNTAX
    | SYSTEM
    | TABLE
    | TABLES
    | TEMPORARY
    | TEST
    | THEN
    | TIES
    | TIMEOUT
    | TIMESTAMP
    | TOTALS
    | TRAILING
    | TRIM
    | TRUNCATE
    | TO
    | TOP
    | TTL
    | TYPE
    | UNBOUNDED
    | UNION
    | UPDATE
    | USE
    | USING
    | USER
    | UUID
    | VALUES
    | VIEW
    | VOLUME
    | WATCH
    | WHEN
    | WHERE
    | WINDOW
    | WITH
    ;

keywordForAlias
    : DATE
    | FIRST
    | ID
    | KEY
    | SOURCE
    | AFTER
    | CASE
    | CLUSTER
    | CURRENT
    | INDEX
    | TABLES
    | TEST
    | VIEW
    | PRIMARY
    | GRANT
    | YEAR
    | DAY
    | MONTH
    | HOUR
    | MINUTE
    | SECOND
    ;

alias
    : IDENTIFIER
    | keywordForAlias
    ; // |interval| can't be an alias, otherwise 'INTERVAL 1 SOMETHING' becomes ambiguous.

identifier
    : IDENTIFIER
    | interval
    | keyword
    ;

identifierOrNull
    : identifier
    | NULL_SQL
    ; // NULL_SQL can be only 'Null' here.

enumValue
    : STRING_LITERAL EQ_SINGLE numberLiteral
    ;
