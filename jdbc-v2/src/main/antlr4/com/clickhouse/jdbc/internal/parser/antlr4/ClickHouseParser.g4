
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
    | undropStmt // DDL
    | existsStmt
    | explainStmt
    | killStmt     // DDL
    | optimizeStmt // DDL
    | renameStmt   // DDL
    | setStmt
    | setRoleStmt
    | showStmt
    | systemStmt
    | truncateStmt // DDL
    | deleteStmt
    | updateStmt
    | useStmt
    | watchStmt
    | selectStmt
    | selectUnionStmt
    | grantStmt
    | revokeStmt
    | exchangeStmt
    | moveStmt
    ;

// DELETE statement

deleteStmt
    : DELETE FROM tableIdentifier clusterClause?  (IN partitionClause)? whereClause?
    ;

// UPDATE statement
updateStmt
    : UPDATE tableIdentifier clusterClause? SET assignmentExprList whereClause?
    ;

// ALTER statement

alterStmt
    : ALTER TABLE tableIdentifier clusterClause? alterTableClause (COMMA alterTableClause)* # AlterTableStmt
    ;

alterTableClause
    : ADD COLUMN (IF NOT EXISTS)? tableColumnDfnt alterTableColumnPosition?         # AlterTableClauseAddColumn
    | ADD INDEX (IF NOT EXISTS)? tableIndexDfnt alterTableColumnPosition?           # AlterTableClauseAddIndex
    | ADD PROJECTION (IF NOT EXISTS)? tableProjectionDfnt alterTableColumnPosition? # AlterTableClauseAddProjection
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
    | ALTER COLUMN (IF EXISTS)? identifier TYPE? columnTypeExpr codecExpr? ttlClause? settingExprList? alterTableColumnPosition? # AlterTableClauseAlterType
    | MODIFY ORDER BY columnExpr                                                    # AlterTableClauseModifyOrderBy
    | MODIFY ttlClause                                                              # AlterTableClauseModifyTTL
    | MODIFY COMMENT literal                                                        # AlterTableClauseModifyComment
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

alterTableColumnPosition
    : (AFTER nestedIdentifier)
    | FIRST
    ;

assignmentExprList
    : assignmentExpr (COMMA assignmentExpr)*
    ;

assignmentExpr
    : nestedIdentifier EQ_SINGLE columnExpr
    | nestedIdentifier EQ_SINGLE JDBC_PARAM_PLACEHOLDER
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
    : ATTACH TABLE (IF NOT EXISTS)? tableIdentifier clusterClause?
    | ATTACH DICTIONARY (IF NOT EXISTS)? tableIdentifier clusterClause?
    | ATTACH DATABASE (IF NOT EXISTS)? databaseIdentifier engineExpr? clusterClause?
    ;

// CHECK statement

checkStmt
    : CHECK TABLE tableIdentifier (PARTITION identifier | PART identifier)? (FORMAT identifier)? settingsClause? # checkTableStmt
    | CHECK ALL TABLES (FORMAT identifier)? settingsClause? # checkAllTablesStmt
    | CHECK GRANT privilege columnsClause? ON grantTableIdentifier  # checkGrantStmt
    ;

// CREATE statement

createStmt
    : CREATE DATABASE (IF NOT EXISTS)? databaseIdentifier clusterClause? engineExpr? # CreateDatabaseStmt
    | (CREATE (OR REPLACE)? | REPLACE) DICTIONARY (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? dictionarySchemaClause
        dictionaryEngineClause sourceClause layoutClause lifetimeClause dictionarySettingsClause? (COMMENT literal)? # CreateDictionaryStmt
    | (ATTACH | CREATE) LIVE VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? (
        WITH TIMEOUT DECIMAL_LITERAL?
    )? destinationClause? tableSchemaClause? subqueryClause # CreateLiveViewStmt
    | (ATTACH | CREATE) MATERIALIZED VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? (
        destinationClause
        | engineClause POPULATE?
    ) subqueryClause # CreateMaterializedViewStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause
        engineClause? subqueryClause?  # CreateTableStmt
    | (ATTACH | CREATE) (OR REPLACE)? VIEW (IF NOT EXISTS)? tableIdentifier alias? uuidClause? clusterClause? tableSchemaClause? subqueryClause #
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
    | CREATE SETTINGS? PROFILE ((IF NOT EXISTS) | (OR REPLACE))? identifier (COMMA identifier)* clusterClause?
        (IN identifier)? ((SETTINGS identifier (EQ_SINGLE literal)? (MIN EQ_SINGLE? literal)? (MAX EQ_SINGLE? literal)?
         (CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY)?)
            | ( INHERIT identifier))? (TO identifier | ALL | ALL EXCEPT identifier)? # createProfileStmt
    | CREATE FUNCTION identifier clusterClause? AS LPAREN (identifier)? (COMMA identifier)? RPAREN ARROW .+? #createFunctionStmt
    | CREATE NAMED COLLECTION (IF NOT EXISTS)? identifier clusterClause? AS nameCollectionKey (COMMA nameCollectionKey)* #createNamedCollectionStmt
    | CREATE QUOTA (IF NOT EXISTS | OR REPLACE)? identifier clusterClause? (IN identifier)?
        (KEYED BY identifier | NOT KEYED)?
        quotaForClause (COMMA quotaForClause)*
        (TO (identifier (COMMA identifier)* | ALL | CURRENT_USER | ALL EXCEPT identifier (COMMA identifier)* ))? # createQuotaStmt
    ;

quotaMaxExpr
    : identifier EQ_SINGLE numberLiteral
    ;

quotaForClause
    : FOR RANDOMIZED? INTERVAL numberLiteral interval (MAX quotaMaxExpr (COMMA quotaMaxExpr)*)+?
    ;

nameCollectionKey
    : (identifier EQ_SINGLE literal (NOT? OVERRIDE)?)
    ;

userIdentifier
    : (BACKTICK_ID | QUOTED_IDENTIFIER | IDENTIFIER | STRING_LITERAL)
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
    : identifier columnTypeExpr ((DEFAULT | EXPRESSION) columnExpr)? (IS_OBJECT_ID|HIERARCHICAL|INJECTIVE)?
    ;

dictionaryEngineClause
    : dictionaryPrimaryKeyClause?
    ;

dictionaryPrimaryKeyClause
    : PRIMARY KEY (identifier) (COMMA identifier)*
    ;

dictionaryArgExpr
    : identifier (identifier (LPAREN RPAREN)? | literal)
    ;

sourceClause
    : SOURCE LPAREN identifier LPAREN settingExprList RPAREN RPAREN
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
    : nestedIdentifier columnTypeExpr (NULL_SQL | NOT NULL_SQL)? tableColumnPropertyExpr? (COMMENT STRING_LITERAL)? codecExpr? (
        TTL columnExpr
    )?
    | nestedIdentifier columnTypeExpr? tableColumnPropertyExpr (COMMENT STRING_LITERAL)? codecExpr? (
        TTL columnExpr
    )?
    ;

tableColumnPropertyExpr
    : (DEFAULT | MATERIALIZED | ALIAS ) columnExpr
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

// MOVE statement
moveStmt
    : MOVE (USER | ROLE | QUOTA | SETTINGS PROFILE | ROW POLICY) identifier TO identifier
    ;

// DESCRIBE statement

describeStmt
    : (DESCRIBE | DESC) TABLE? tableExpr
    ;

// DROP statement

dropStmt
    : (DETACH | DROP) DATABASE (IF EXISTS)? databaseIdentifier clusterClause? SYNC?
    | (DETACH | DROP) (DICTIONARY | TEMPORARY? TABLE | VIEW) (IF EXISTS)? tableIdentifier clusterClause?
        (NO DELAY)? SYNC?
    | (DETACH | DROP) (USER | ROLE | QUOTA | SETTINGS? PROFILE) (IF EXISTS)? identifier clusterClause? (FROM identifier)?
    | (DETACH | DROP) ROW? POLICY (IF EXISTS)? identifier ON grantTableIdentifier (COMMA grantTableIdentifier)* clusterClause? (FROM identifier)?
    | (DETACH | DROP) (FUNCTION | NAMED COLLECTION) (IF EXISTS)? identifier clusterClause?
    ;

undropStmt
    : UNDROP TABLE tableIdentifier uuidClause? clusterClause?
    ;

// EXISTS statement

existsStmt
    : EXISTS DATABASE databaseIdentifier  (INTO OUTFILE filename)? (FORMAT identifier)? # ExistsDatabaseStmt
    | EXISTS (DICTIONARY | TEMPORARY? TABLE | VIEW)? tableIdentifier (INTO OUTFILE filename)? (FORMAT identifier)?  # ExistsTableStmt
    ;

// EXPLAIN statement

explainStmt
    : EXPLAIN (AST | SYNTAX | QUERY TREE | PLAN | PIPELINE | ESTIMATE | TABLE OVERRIDE)? settingExprList? .+?
    | EXPLAIN .+?
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
    | JDBC_PARAM_PLACEHOLDER     # InsertParameter
    | identifier (LPAREN columnExprList? RPAREN)? # InsertParameterFuncExpr
    | LPAREN? columnExpr RPAREN? # InserParameterExpr
    ;

// KILL statement

killStmt
    : KILL MUTATION clusterClause? whereClause (SYNC | ASYNC | TEST)? (FORMAT identifier)? # KillMutationStmt
    | KILL QUERY clusterClause? whereClause (SYNC | ASYNC | TEST)? (FORMAT identifier)? # KillQueryStmt
    ;

// OPTIMIZE statement

optimizeStmt
    : OPTIMIZE TABLE tableIdentifier clusterClause? partitionClause? FINAL? DEDUPLICATE? optimizeByExpr?
    ;

optimizeByExpr
    : BY ASTERISK (EXCEPT LPAREN? (identifier (COMMA identifier)*) RPAREN? )?
    | BY identifier (COMMA identifier)*
    | BY COLUMNS LPAREN literal RPAREN (EXCEPT LPAREN? (identifier (COMMA identifier)*) RPAREN? )?
    ;

// RENAME statement

renameStmt
    : RENAME TABLE tableIdentifier TO tableIdentifier (COMMA tableIdentifier TO tableIdentifier)* clusterClause?
    | RENAME
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
    : cteClause? SELECT DISTINCT? topClause? columnExprList fromClause? arrayJoinClause? windowClause? prewhereClause? whereClause? groupByClause? (
        WITH (CUBE | ROLLUP)
    )? (WITH TOTALS)? havingClause? orderByClause? limitByClause? limitClause? settingsClause?
    ;

withClause
    : WITH columnExprList
    ;

// CTE statement
cteClause
    : WITH (cteUnboundCol | namedQuery) (COMMA (cteUnboundCol | namedQuery))*
    ;


namedQuery
    : identifier (columnAliases)? AS  LPAREN? ( selectStmt | selectStmtWithParens | selectUnionStmt) RPAREN?
    ;

columnAliases
    : LPAREN identifier (',' identifier)* RPAREN
    ;

cteUnboundCol
    : literal AS identifier # CteUnboundColLiteral
    | JDBC_PARAM_PLACEHOLDER AS identifier # CteUnboundColParam
    | LPAREN? columnExpr RPAREN? AS? identifier? # CteUnboundColExpr
    | LPAREN selectStmt RPAREN AS identifier # CteUnboundSubQuery
//    | LPAREN cteStmt? selectStmt RPAREN AS identifier # CteUnboundNestedSelect
    ;

topClause
    : TOP DECIMAL_LITERAL (WITH TIES)?
    ;

fromClause
    : FROM joinExpr
    | FROM tableIdentifier
    | FROM identifier LPAREN JDBC_PARAM_PLACEHOLDER RPAREN
    | FROM selectStmt
    | FROM identifier LPAREN viewParam (COMMA viewParam)?  RPAREN
    | FROM tableFunctionExpr
    ;

viewParam
    : identifier EQ_SINGLE (literal | JDBC_PARAM_PLACEHOLDER)
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

// EXCHANGE statement
exchangeStmt
    : EXCHANGE (TABLES | DICTIONARIES) tableIdentifier AND tableIdentifier clusterClause?
    ;


// SET statement

setStmt
    : SET (identifier | settingExpr)
    ;

// SET ROLE statement

setRoleStmt
    : SET (DEFAULT)? ROLE ( NONE | setRolesList | ALL (EXCEPT setRolesList)) (TO identifier | CURRENT_USER (COMMA identifier | CURRENT_USER)*)?
    ;

setRolesList
    : identifier (COMMA identifier)*
    ;

// GRANT statements

grantStmt
    : GRANT clusterClause? ((identifier (COMMA identifier)*) | (privelegeList ON grantTableIdentifier))
        TO (CURRENT_USER | identifier) (COMMA identifier)*
        (WITH ADMIN OPTION)? (WITH GRANT OPTION)? (WITH REPLACE OPTION)?
    | GRANT CURRENT GRANTS (LPAREN ((privelegeList ON grantTableIdentifier) | (identifier (COMMA identifier)*)) RPAREN)?
        TO (CURRENT_USER | identifier (COMMA identifier)*)
                (WITH GRANT OPTION)? (WITH REPLACE OPTION)?
    ;

// REVOKE statements
revokeStmt
    : REVOKE clusterClause? privelegeList ON grantTableIdentifier
        FROM ((CURRENT_USER | identifier) (COMMA identifier)* | ALL | ALL EXCEPT (CURRENT_USER | identifier) (COMMA identifier)* )
    | REVOKE clusterClause? (ADMIN OPTION FOR)? identifier (COMMA identifier)*
        FROM ((CURRENT_USER | identifier) (COMMA identifier)* | ALL | ALL EXCEPT (CURRENT_USER | identifier) (COMMA identifier)* )
    ;

grantTableIdentifier
    : (identifier DOT)? identifier
    | (identifier DOT)? ASTERISK
    | (ASTERISK DOT)? identifier
    | (ASTERISK DOT)? ASTERISK
    ;

privelegeList
    : columnPrivilege (COMMA columnPrivilege)*
    ;


columnPrivilege
    : privilege (LPAREN identifier (COMMA identifier)* RPAREN)?
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
    : SHOW CREATE? (TEMPORARY? TABLE | DICTIONARY | VIEW | DATABASE) tableIdentifier (INTO OUTFILE literal)? (FORMAT identifier)? # showCreateStmt
    | SHOW DATABASES (NOT? (LIKE | ILIKE) literal) (LIMIT numberLiteral)? (INTO OUTFILE filename)? (FORMAT identifier)? # showDatabasesStmt
    | SHOW FULL? TEMPORARY? TABLES showFromDbClause? (NOT? (LIKE | ILIKE) literal)? (LIMIT numberLiteral)? (INTO OUTFILE filename)? (FORMAT identifier)? # showTablesStmt
    | SHOW EXTENDED? FULL? COLUMNS showFromTableFromDbClause? (NOT? (LIKE | ILIKE) literal)? (LIMIT numberLiteral)? (INTO OUTFILE filename)? (FORMAT identifier)? # showColumnsStmt
    | SHOW DICTIONARIES showFromDbClause? (NOT? (LIKE | ILIKE) literal)? (LIMIT numberLiteral)? (INTO OUTFILE filename)? (FORMAT identifier)? # showDictionariesStmt
    | SHOW EXTENDED? (INDEX | INDEXES | INDICES | KEYS ) (FROM | IN) identifier showFromTableFromDbClause? (WHERE columnExpr)? (INTO OUTFILE filename)? (FORMAT identifier)? # showIndexStmt
    | SHOW PROCESSLIST (INTO OUTFILE filename)? (FORMAT identifier)? # showProcessListStmt
    | SHOW GRANTS (FOR identifier (COMMA identifier)*)? (WITH IMPLICIT)? FINAL? # showGrantsStmt
    | SHOW CREATE USER ((identifier (COMMA identifier)*) | CURRENT_USER) # showCreateUserStmt
    | SHOW CREATE ROLE (identifier (COMMA identifier)*) # showCreateRoleStmt
    | SHOW CREATE ROW? POLICY identifier ON tableIdentifier # showCreatePolicyStmt
    | SHOW CREATE QUOTA ((identifier (COMMA identifier)*) | CURRENT) # showCreateQuotaStmt
    | SHOW CREATE (SETTINGS)? PROFILE identifier (COMMA identifier)* # showCreateProfile
    | SHOW USERS # showUsersStmt
    | SHOW (CURRENT|ENABLED)? ROLES # showRolesStmt
    | SHOW SETTINGS? PROFILES # showProfilesStmt
    | SHOW ROW? POLICIES (ON identifier)? # showPoliciesStmt
    | SHOW QUOTAS # showQuotasStmt
    | SHOW CURRENT? QUOTA # showQuotaStmt
    | SHOW ACCESS # showAccessStmt
    | SHOW CLUSTER identifier # showClusterStmt
    | SHOW CLUSTERS (NOT? (LIKE | ILIKE) literal)? (LIMIT numberLiteral)? (INTO OUTFILE filename)? (FORMAT identifier)? # showClustersStmt
    | SHOW CHANGED? SETTINGS (LIKE | ILIKE) literal # showSettingsStmt
    | SHOW SETTING identifier # showSettingStmt
    | SHOW FILESYSTEM CACHES # showFSCachesStmt
    | SHOW ENGINES (INTO OUTFILE filename)? (FORMAT identifier)? # showEnginesStmt
    | SHOW FUNCTIONS (NOT? (LIKE | ILIKE) literal)? # showFunctionsStmt
    | SHOW MERGES (NOT? (LIKE | ILIKE) literal)? (LIMIT numberLiteral)? (INTO OUTFILE filename)? (FORMAT identifier)? # showMergesStmt
    ;

showFromDbClause
    : ((FROM | IN) identifier)
    ;

showFromTableFromDbClause
    : ((FROM | IN) identifier) showFromDbClause?
    ;

// SYSTEM statements

systemStmt
    : SYSTEM FLUSH DISTRIBUTED tableIdentifier
    | SYSTEM RELOAD DICTIONARIES clusterClause? identifier?
    | SYSTEM RELOAD DICTIONARY tableIdentifier
    | SYSTEM RELOAD MODEL clusterClause? identifier?
    | SYSTEM RELOAD FUNCTIONS clusterClause?
    | SYSTEM RELOAD FUNCTION clusterClause? identifier
    | SYSTEM RELOAD ASYNCHRONOUS METRICS clusterClause?
    | SYSTEM DROP DNS CACHE
    | SYSTEM DROP MARK CACHE
    | SYSTEM DROP REPLICA literal (FROM SHARD literal)? (FROM (TABLE tableIdentifier) | (FROM DATABASE identifier) | (ZKPATH literal))?
    | SYSTEM DROP UNCOMPRESSED CACHE
    | SYSTEM DROP COMPILED EXPRESSION CACHE
    | SYSTEM DROP QUERY CONDITION CACHE
    | SYSTEM DROP QUERY CACHE (TAG literal)?
    | SYSTEM DROP FORMAT SCHEMA CACHE (FOR literal)?
    | SYSTEM FLUSH LOGS
    | SYSTEM RELOAD CONFIG clusterClause?
    | SYSTEM RELOAD USERS clusterClause?
    | SYSTEM SHUTDOWN
    | SYSTEM KILL
    | SYSTEM (START | FLUSH | STOP) (DISTRIBUTED SENDS? | FETCHES | TTL? MERGES) tableIdentifier clusterClause? settingsClause?
    | SYSTEM (START | STOP) LISTEN clusterClause? (QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM literal)
    | SYSTEM (START | STOP) MERGES clusterClause? ((ON VOLUME identifier) | tableIdentifier)?
    | SYSTEM (START | STOP) TTL MERGES clusterClause? tableIdentifier?
    | SYSTEM (START | STOP) MOVES clusterClause? tableIdentifier?
    | SYSTEM UNFREEZE WITH NAME literal
    | SYSTEM WAIT LOADING PARTS clusterClause? tableIdentifier?
    | SYSTEM (START | STOP) FETCHES clusterClause? tableIdentifier?
    | SYSTEM (START | STOP) REPLICATED SENDS clusterClause? tableIdentifier?
    | SYSTEM (START | STOP) REPLICATION QUEUES clusterClause? tableIdentifier?
    | SYSTEM (START | STOP) PULLING REPLICATION LOG clusterClause? tableIdentifier?
    | SYSTEM SYNC REPLICA clusterClause? tableIdentifier? (IF EXISTS)? (STRICT | LIGHTWEIGHT | FROM literal | PULL)?
    | SYSTEM SYNC DATABASE REPLICA identifier
    | SYSTEM RESTART REPLICA clusterClause? tableIdentifier?
    | SYSTEM RESTORE DATABASE? REPLICA identifier clusterClause?
    | SYSTEM RESTART REPLICAS
    | SYSTEM DROP FILESYSTEM CACHE clusterClause?
    | SYSTEM SYNC FILE CACHE clusterClause?
    | SYSTEM (LOAD | UNLOAD) PRIMARY KEY tableIdentifier?
    | SYSTEM REFRESH VIEW tableIdentifier
    | SYSTEM REPLICATED? (START | STOP) ((VIEW tableIdentifier) | VIEWS)
    | SYSTEM CANCEL VIEW tableIdentifier
    | SYSTEM WAIT VIEW tableIdentifier
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
    | identifier (LPAREN columnExprList? RPAREN) FILTER LPAREN whereClause RPAREN        # ColumnExprAgrFuncWithFilter
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
    | <assoc = right> columnExpr JDBC_PARAM_PLACEHOLDER columnExpr COLON columnExpr # ColumnExprTernaryOp
    | columnExpr (alias | AS identifier)                           # ColumnExprAlias
    | (tableIdentifier DOT)? ASTERISK                              # ColumnExprAsterisk // single-column only
    | LPAREN selectUnionStmt RPAREN                                # ColumnExprSubquery // single-column only
    | LPAREN columnExpr RPAREN                                     # ColumnExprParens   // single-column only
    | LPAREN columnExprList RPAREN                                 # ColumnExprTuple
    | LBRACKET columnExprList? RBRACKET                            # ColumnExprArray
    | columnIdentifier                                             # ColumnExprIdentifier
    | JDBC_PARAM_PLACEHOLDER (CAST_OP identifier)?                                  # ColumnExprParam
    | columnExpr REGEXP literal                                    # ColumnExprRegexp
    ;

columnArgList
    : columnArgExpr (COMMA columnArgExpr)*
    | JDBC_PARAM_PLACEHOLDER (COMMA JDBC_PARAM_PLACEHOLDER)*
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
    : databaseIdentifier DOT identifier
    | identifier
    ;

viewIdentifier
    : tableIdentifier
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
    : identifier (DOT identifier)*
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

filename
    : STRING_LITERAL
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
    : ACCESS
    | ADD
    | ADMIN
    | AFTER
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
    | AVG
    | AZURE
    | BACKUP
    | BCRYPT_HASH
    | BCRYPT_PASSWORD
    | BETWEEN
    | BOTH
    | BY
    | CACHE
    | CACHES
    | CASE
    | CAST
    | CHANGEABLE_IN_READONLY
    | CHANGED
    | CHECK
    | CLEANUP
    | CLEAR
    | CLUSTER
    | CLUSTERS
    | CN
    | CODEC
    | COLLATE
    | COLLECTION
    | COLUMN
    | COLUMNS
    | COMMENT
    | CONNECTIONS
    | CONST
    | CONSTRAINT
    | CREATE
    | CROSS
    | CUBE
    | CURRENT
    | CURRENT_USER
    | DATABASE
    | DATABASES
    | DATE
    | DAY
    | DEDUPLICATE
    | DEFAULT
    | DEFINER
    | DELAY
    | DELETE
    | DESC
    | DESCENDING
    | DESCRIBE
    | DETACH
    | DICTIONARIES
    | DICTIONARY
    | DISK
    | DISTINCT
    | DISTRIBUTED
    | DOUBLE_SHA1_HASH
    | DOUBLE_SHA1_PASSWORD
    | DROP
    | ELSE
    | ENABLED
    | END
    | ENGINE
    | ENGINES
    | ESTIMATE
    | EVENTS
    | EXCEPT
    | EXCHANGE
    | EXISTS
    | EXPLAIN
    | EXPRESSION
    | EXTENDED
    | EXTRACT
    | FETCH
    | FETCHES
    | FILE
    | FILESYSTEM
    | FILTER
    | FINAL
    | FIRST
    | FLUSH
    | FOLLOWING
    | FOR
    | FORMAT
    | FREEZE
    | FROM
    | FULL
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GRANT
    | GRANTEES
    | GRANTS
    | GRANULARITY
    | GROUP
    | HAVING
    | HDFS
    | HIERARCHICAL
    | HOST
    | HOUR
    | HTTP
    | ID
    | IDENTIFIED
    | IF
    | ILIKE
    | IMPLICIT
    | IN
    | INDEX
    | INDEXES
    | INDICES
    | INHERIT
    | INJECTIVE
    | INNER
    | INSERT
    | INTERVAL
    | INTO
    | IP
    | IS
    | IS_OBJECT_ID
    | JOIN
    | JSON_FALSE
    | JSON_TRUE
    | KERBEROS
    | KEY
    | KEYED
    | KEYS
    | KILL
    | LAST
    | LAYOUT
    | LDAP
    | LEADING
    | LEFT
    | LIFETIME
    | LIGHTWEIGHT
    | LIKE
    | LIMIT
    | LIMITS
    | LIVE
    | LOCAL
    | LOG
    | LOGS
    | MATERIALIZE
    | MATERIALIZED
    | MAX
    | MERGES
    | METRICS
    | MIN
    | MINUTE
    | MODIFY
    | MONTH
    | MOVE
    | MUTATION
    | NAME
    | NAMED
    | NO
    | NONE
    | NO_PASSWORD
    | NOT
    | NULL_SQL
    | NULLS
    | OFFSET
    | ON
    | ONLY
    | OPTIMIZE
    | OPTION
    | OR
    | ORDER
    | OUTER
    | OUTFILE
    | OVER
    | OVERRIDE
    | PART
    | PARTITION
    | PARTS
    | PERMISSIVE
    | PIPELINE
    | PLAINTEXT_PASSWORD
    | PLAN
    | POLICY
    | POPULATE
    | PRECEDING
    | PREWHERE
    | PRIMARY
    | PROCESSLIST
    | PROFILE
    | PROFILES
    | PROJECTION
    | PULL
    | QUARTER
    | QUERIES
    | QUERY
    | QUOTA
    | RANDOMIZED
    | RANGE
    | READONLY
    | REALM
    | REFRESH
    | REGEXP
    | RELOAD
    | REMOTE
    | REMOVE
    | RENAME
    | REPLACE
    | REPLICA
    | REPLICATED
    | RESOURCE
    | RESTORE
    | RESTRICTIVE
    | REVOKE
    | RIGHT
    | ROLE
    | ROLES
    | ROLLUP
    | ROW
    | ROWS
    | S3
    | SAMPLE
    | SCRAM_SHA256_HASH
    | SCRAM_SHA256_PASSWORD
    | SECOND
    | SECURITY
    | SELECT
    | SEMI
    | SENDS
    | SERVER
    | SET
    | SETTING
    | SETTINGS
    | SHA256_HASH
    | SHA256_PASSWORD
    | SHARD
    | SHOW
    | SOURCE
    | SQL
    | SSH_KEY
    | SSL_CERTIFICATE
    | START
    | STATISTICS
    | STOP
    | STRICT
    | SUBSTRING
    | SUM
    | SYNC
    | SYNTAX
    | SYSTEM
    | TABLE
    | TABLES
    | TAG
    | TEMPORARY
    | TEST
    | THEN
    | THREAD
    | TIES
    | TIMEOUT
    | TIMESTAMP
    | TO
    | TOP
    | TOTALS
    | TRACKING
    | TRAILING
    | TRANSACTION
    | TREE
    | TRIM
    | TRUNCATE
    | TTL
    | TYPE
    | UNBOUNDED
    | UNDROP
    | UNFREEZE
    | UNION
    | UNTIL
    | UPDATE
    | URL
    | USE
    | USER
    | USERS
    | USING
    | UUID
    | VALID
    | VALUES
    | VIEW
    | VOLUME
    | WATCH
    | WEEK
    | WHEN
    | WHERE
    | WINDOW
    | WITH
    | WORKLOAD
    | WRITABLE
    | YEAR
    | ZKPATH
    ;

keywordForAlias
    : ACCESS
    | ADD
    | ADMIN
    | AFTER
    | ALIAS
    | ALTER
    | AND
    | ASCENDING
    | AST
    | ASYNC
    | ATTACH
    | AZURE
    | BACKUP
    | BCRYPT_HASH
    | BCRYPT_PASSWORD
    | BOTH
    | BY
    | CACHE
    | CACHES
    | CASE
    | CAST
    | CHANGEABLE_IN_READONLY
    | CHANGED
    | CHECK
    | CLEANUP
    | CLEAR
    | CLUSTER
    | CLUSTERS
    | CN
    | CODEC
    | COLLATE
    | COLLECTION
    | COLUMN
    | COLUMNS
    | COMMENT
    | CONNECTIONS
    | CONST
    | CONSTRAINT
    | CREATE
    | CUBE
    | CURRENT
    | CURRENT_USER
    | DATABASE
    | DATABASES
    | DATE
    | DAY
    | DEDUPLICATE
    | DEFAULT
    | DEFINER
    | DELAY
    | DELETE
    | DESC
    | DESCENDING
    | DESCRIBE
    | DETACH
    | DICTIONARIES
    | DICTIONARY
    | DISK
    | DISTINCT
    | DOUBLE_SHA1_HASH
    | DOUBLE_SHA1_PASSWORD
    | DROP
    | ENABLED
    | END
    | ENGINE
    | ENGINES
    | ESTIMATE
    | EVENTS
    | EXCHANGE
    | EXISTS
    | EXPLAIN
    | EXPRESSION
    | EXTENDED
    | FETCH
    | FILE
    | FILESYSTEM
    | FILTER
    | FIRST
    | FOLLOWING
    | FOR
    | FREEZE
    | FUNCTION
    | FUNCTIONS
    | GRANT
    | GRANTEES
    | GRANTS
    | GRANULARITY
    | HDFS
    | HIERARCHICAL
    | HOST
    | HOUR
    | HTTP
    | ID
    | IDENTIFIED
    | IF
    | IMPLICIT
    | IN
    | INDEX
    | INDEXES
    | INDICES
    | INHERIT
    | INJECTIVE
    | INSERT
    | INTERVAL
    | IP
    | IS
    | IS_OBJECT_ID
    | KERBEROS
    | KEY
    | KEYED
    | KEYS
    | KILL
    | LAST
    | LAYOUT
    | LDAP
    | LEADING
    | LIFETIME
    | LIGHTWEIGHT
    | LIMITS
    | LIVE
    | LOCAL
    | MATERIALIZE
    | MATERIALIZED
    | MAX
    | MERGES
    | METRICS
    | MIN
    | MINUTE
    | MODIFY
    | MONTH
    | MOVE
    | MUTATION
    | NAME
    | NAMED
    | NO
    | NONE
    | NO_PASSWORD
    | NULL_SQL
    | NULLS
    | OPTIMIZE
    | OPTION
    | OR
    | OUTER
    | OUTFILE
    | OVER
    | OVERRIDE
    | PART
    | PARTITION
    | PARTS
    | PERMISSIVE
    | PIPELINE
    | PLAINTEXT_PASSWORD
    | PLAN
    | POLICY
    | POPULATE
    | PRECEDING
    | PRIMARY
    | PROCESSLIST
    | PROFILE
    | PROFILES
    | PROJECTION
    | PULL
    | QUARTER
    | QUERY
    | QUOTA
    | RANDOMIZED
    | RANGE
    | READONLY
    | REALM
    | REFRESH
    | REGEXP
    | REMOVE
    | RENAME
    | REPLACE
    | REPLICATED
    | RESOURCE
    | RESTORE
    | RESTRICTIVE
    | REVOKE
    | ROLE
    | ROLES
    | ROLLUP
    | ROW
    | ROWS
    | S3
    | SCRAM_SHA256_HASH
    | SCRAM_SHA256_PASSWORD
    | SECOND
    | SECURITY
    | SELECT
    | SERVER
    | SET
    | SETTING
    | SHA256_HASH
    | SHA256_PASSWORD
    | SHARD
    | SHOW
    | SOURCE
    | SQL
    | SSH_KEY
    | SSL_CERTIFICATE
    | START
    | STATISTICS
    | STRICT
    | SYNC
    | SYNTAX
    | SYSTEM
    | TABLE
    | TABLES
    | TAG
    | TEMPORARY
    | TEST
    | THEN
    | THREAD
    | TIES
    | TIMESTAMP
    | TO
    | TOP
    | TOTALS
    | TRACKING
    | TRAILING
    | TRANSACTION
    | TREE
    | TRUNCATE
    | TTL
    | TYPE
    | UNBOUNDED
    | UNDROP
    | UNFREEZE
    | UNTIL
    | UPDATE
    | URL
    | USE
    | USER
    | VALID
    | VALUES
    | VIEW
    | VOLUME
    | WATCH
    | WEEK
    | WHEN
    | WORKLOAD
    | WRITABLE
    | YEAR
    | ZKPATH
    ;

alias
    : IDENTIFIER
    | keywordForAlias
    ;

identifier
    : BACKTICK_ID
    | QUOTED_IDENTIFIER
    | IDENTIFIER
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
