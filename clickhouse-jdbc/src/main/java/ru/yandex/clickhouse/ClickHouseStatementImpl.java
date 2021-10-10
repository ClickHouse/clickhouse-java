package ru.yandex.clickhouse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;

import com.clickhouse.client.data.JsonStreamUtils;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.domain.ClickHouseCompression;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.jdbc.parser.ClickHouseSqlParser;
import ru.yandex.clickhouse.jdbc.parser.ClickHouseSqlStatement;
import ru.yandex.clickhouse.jdbc.parser.StatementType;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;
import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.response.ClickHouseResponseSummary;
import ru.yandex.clickhouse.response.ClickHouseResultSet;
import ru.yandex.clickhouse.response.ClickHouseScrollableResultSet;
import ru.yandex.clickhouse.response.FastByteArrayOutputStream;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseHttpClientBuilder;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;
import ru.yandex.clickhouse.util.Utils;

public class ClickHouseStatementImpl extends ConfigurableApi<ClickHouseStatement> implements ClickHouseStatement {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    protected static class WrappedHttpEntity extends AbstractHttpEntity {
        private final String sql;
        private final HttpEntity entity;

        public WrappedHttpEntity(String sql, HttpEntity entity) {
            this.sql = sql;
            this.entity = Objects.requireNonNull(entity);

            this.chunked = entity.isChunked();
            this.contentEncoding = entity.getContentEncoding();
            this.contentType = entity.getContentType();
        }

        @Override
        public boolean isRepeatable() {
            return entity.isRepeatable();
        }

        @Override
        public long getContentLength() {
            return entity.getContentLength();
        }

        @Override
        public InputStream getContent() throws IOException, IllegalStateException {
            return entity.getContent();
        }

        @Override
        public void writeTo(OutputStream outputStream) throws IOException {
            if (sql != null && !sql.isEmpty()) {
                outputStream.write(sql.getBytes(StandardCharsets.UTF_8));
                outputStream.write('\n');
            }
            
            entity.writeTo(outputStream);
        }

        @Override
        public boolean isStreaming() {
            return entity.isStreaming();
        }
    }

    private final CloseableHttpClient client;

    private final HttpClientContext httpContext;

    protected ClickHouseProperties properties;

    private ClickHouseConnection connection;

    private ClickHouseResultSet currentResult;

    private ClickHouseRowBinaryInputStream currentRowBinaryResult;

    private ClickHouseResponseSummary currentSummary;

    private int currentUpdateCount = -1;

    private int queryTimeout;

    private boolean isQueryTimeoutSet = false;

    private int maxRows;

    private boolean closeOnCompletion;

    private final boolean isResultSetScrollable;

    private volatile String queryId;

    protected ClickHouseSqlStatement[] parsedStmts;

    protected List<ClickHouseSqlStatement> batchStmts;

    /**
     * Current database name may be changed by {@link java.sql.Connection#setCatalog(String)}
     * between creation of this object and query execution, but javadoc does not allow
     * {@code setCatalog} influence on already created statements.
     */
    protected String currentDatabase;

    protected String getQueryId() {
        return queryId;
    }

    protected ClickHouseSqlStatement getLastStatement() {
        ClickHouseSqlStatement stmt = null;

        if (parsedStmts != null && parsedStmts.length > 0) {
            stmt = parsedStmts[parsedStmts.length - 1];
        }

        return Objects.requireNonNull(stmt);
    }

    protected void setLastStatement(ClickHouseSqlStatement stmt) {
        if (parsedStmts != null && parsedStmts.length > 0) {
            parsedStmts[parsedStmts.length - 1] = Objects.requireNonNull(stmt);
        }
    }

    protected ClickHouseSqlStatement[] parseSqlStatements(String sql) throws SQLException {
        parsedStmts = ClickHouseSqlParser.parse(sql, properties);
        
        if (parsedStmts == null || parsedStmts.length == 0) {
            // should never happen
            throw new IllegalArgumentException("Failed to parse given SQL: " + sql);
        }

        return parsedStmts;
    }

    protected ClickHouseSqlStatement parseSqlStatements(
        String sql, ClickHouseFormat preferredFormat, Map<ClickHouseQueryParam, String> additionalDBParams)
        throws SQLException {
        parseSqlStatements(sql);

        // enable session when we have more than one statement
        if (additionalDBParams != null && parsedStmts.length > 1 && properties.getSessionId() == null) {
            additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, UUID.randomUUID().toString());
        }

        ClickHouseSqlStatement lastStmt = getLastStatement();
        ClickHouseSqlStatement formattedStmt = applyFormat(lastStmt, preferredFormat);
        if (formattedStmt != lastStmt) {
            setLastStatement(lastStmt = formattedStmt);
        }

        return lastStmt;
    }

    protected ClickHouseSqlStatement applyFormat(ClickHouseSqlStatement stmt, ClickHouseFormat preferredFormat) {
        if (Objects.requireNonNull(stmt).isQuery() && !stmt.hasFormat()) {
            String sql = stmt.getSQL();
            String format = Objects.requireNonNull(preferredFormat).name();

            Map<String, Integer> positions = new HashMap<>();
            positions.putAll(stmt.getPositions());
            positions.put(ClickHouseSqlStatement.KEYWORD_FORMAT, sql.length());

            sql = new StringBuilder(sql).append("\nFORMAT ").append(format).toString();
            stmt = new ClickHouseSqlStatement(sql, stmt.getStatementType(), 
                stmt.getCluster(), stmt.getDatabase(), stmt.getTable(),
                format, stmt.getOutfile(), stmt.getParameters(), positions);
        }

        return stmt;
    }

    protected Map<ClickHouseQueryParam, String> importAdditionalDBParameters(Map<ClickHouseQueryParam, String> additionalDBParams) {
        if (additionalDBParams == null || additionalDBParams.isEmpty()) {
            additionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);
        } else { // in case the given additionalDBParams is immutable
            additionalDBParams = new EnumMap<>(additionalDBParams);
        }

        return additionalDBParams;
    }

    protected ResultSet updateResult(ClickHouseSqlStatement stmt, InputStream is) throws IOException, ClickHouseException {
        ResultSet rs = null;
        if (stmt.isQuery()) {
            currentUpdateCount = -1;
            currentResult = createResultSet(
                properties.isCompress() ? new ClickHouseLZ4Stream(is) : is, properties.getBufferSize(),
                stmt.getDatabaseOrDefault(properties.getDatabase()),
                stmt.getTable(),
                stmt.hasWithTotals(),
                this,
                getConnection().getTimeZone(),
                properties
            );
            currentResult.setMaxRows(maxRows);
            rs = currentResult;
        } else {
            currentUpdateCount = 0;
            try {
                is.close();
            } catch (IOException e) {
                log.error("can not close stream: {}", e.getMessage());
            }
        }

        return rs;
    }

    protected int executeStatement(
        ClickHouseSqlStatement stmt,
        Map<ClickHouseQueryParam, String> additionalDBParams,
        List<ClickHouseExternalData> externalData,
        Map<String, String> additionalRequestParams) throws SQLException {
        additionalDBParams = importAdditionalDBParameters(additionalDBParams);
        stmt = applyFormat(stmt, ClickHouseFormat.TabSeparatedWithNamesAndTypes);

        try (InputStream is = getInputStream(stmt, additionalDBParams, externalData, additionalRequestParams)) {
            //noinspection StatementWithEmptyBody
        } catch (IOException e) {
            log.error("can not close stream: {}", e.getMessage());
        }

        return currentSummary != null ? (int) currentSummary.getWrittenRows() : 1;
    }

    protected ResultSet executeQueryStatement(ClickHouseSqlStatement stmt,
        Map<ClickHouseQueryParam, String> additionalDBParams,
        List<ClickHouseExternalData> externalData,
        Map<String, String> additionalRequestParams) throws SQLException {
        additionalDBParams = importAdditionalDBParameters(additionalDBParams);
        stmt = applyFormat(stmt, ClickHouseFormat.TabSeparatedWithNamesAndTypes);

        InputStream is = getInputStream(stmt, additionalDBParams, externalData, additionalRequestParams);
        try {
            return updateResult(stmt, is);
        } catch (Exception e) {
            try {
                is.close();
            } catch (IOException ioe) {
                log.error("can not close stream: {}", ioe.getMessage());
            }
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    protected ClickHouseResponse executeQueryClickhouseResponse(
        ClickHouseSqlStatement stmt,
        Map<ClickHouseQueryParam, String> additionalDBParams,
        Map<String, String> additionalRequestParams) throws SQLException {
        additionalDBParams = importAdditionalDBParameters(additionalDBParams);
        stmt = applyFormat(stmt, ClickHouseFormat.JSONCompact);
        
        try (InputStream is = getInputStream(stmt, additionalDBParams, null, additionalRequestParams)) {
            return JsonStreamUtils.readObject(
                properties.isCompress() ? new ClickHouseLZ4Stream(is) : is, ClickHouseResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ClickHouseStatementImpl(CloseableHttpClient client, ClickHouseConnection connection,
                                   ClickHouseProperties properties, int resultSetType) {
        super(null);
        this.client = client;
        this.httpContext = ClickHouseHttpClientBuilder.createClientContext(properties);
        this.connection = connection;
        this.properties = properties == null ? new ClickHouseProperties() : properties;
        this.currentDatabase = this.properties.getDatabase();
        this.isResultSetScrollable = (resultSetType != ResultSet.TYPE_FORWARD_ONLY);

        this.batchStmts = new ArrayList<>();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    @Override
    public ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQuery(sql, additionalDBParams, null);
    }

    @Override
    public ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException {
        return executeQuery(sql, additionalDBParams, externalData, null);
    }

    @Override
    public ResultSet executeQuery(String sql,
                                  Map<ClickHouseQueryParam, String> additionalDBParams,
                                  List<ClickHouseExternalData> externalData,
                                  Map<String, String> additionalRequestParams) throws SQLException {

        // forcibly disable extremes for ResultSet queries
        additionalDBParams = importAdditionalDBParameters(additionalDBParams);
        // FIXME respect the value set in additionalDBParams?
        additionalDBParams.put(ClickHouseQueryParam.EXTREMES, "0");

        parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, additionalDBParams);

        InputStream is = getLastInputStream(additionalDBParams, externalData, additionalRequestParams);
        ClickHouseSqlStatement parsedStmt = getLastStatement();

        try {
            return updateResult(parsedStmt, is);
        } catch (Exception e) {
            try {
                is.close();
            } catch (IOException ioe) {
                log.error("can not close stream: {}", ioe.getMessage());
            }
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(String sql) throws SQLException {
        return executeQueryClickhouseResponse(sql, null);
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseResponse(sql, additionalDBParams, null);
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(String sql,
                                                             Map<ClickHouseQueryParam, String> additionalDBParams,
                                                             Map<String, String> additionalRequestParams) throws SQLException {
        additionalDBParams = importAdditionalDBParameters(additionalDBParams);
        parseSqlStatements(sql, ClickHouseFormat.JSONCompact, additionalDBParams);
        
        try (InputStream is = getLastInputStream(additionalDBParams, null, additionalRequestParams)) {
            return JsonStreamUtils.readObject(
                properties.isCompress() ? new ClickHouseLZ4Stream(is) : is, ClickHouseResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, null);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, additionalDBParams, null);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, Map<String, String> additionalRequestParams) throws SQLException {
        additionalDBParams = importAdditionalDBParameters(additionalDBParams);
        parseSqlStatements(sql, ClickHouseFormat.RowBinaryWithNamesAndTypes, additionalDBParams);

        InputStream is = getLastInputStream(
                additionalDBParams,
                null,
                additionalRequestParams
        );
        ClickHouseSqlStatement parsedStmt = getLastStatement();

        try {
            if (parsedStmt.isQuery()) {
                currentUpdateCount = -1;
                // FIXME get server timezone?
                currentRowBinaryResult = new ClickHouseRowBinaryInputStream(properties.isCompress()
                        ? new ClickHouseLZ4Stream(is) : is, getConnection().getTimeZone(), properties, true);
                return currentRowBinaryResult;
            } else {
                currentUpdateCount = 0;
                try {
                    is.close();
                } catch (IOException e) {
                    log.error("can not close stream: {}", e.getMessage());
                }
                return null;
            }
        } catch (Exception e) {
            try {
                is.close();
            } catch (IOException ioe) {
                log.error("can not close stream: {}", ioe.getMessage());
            }
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        Map<ClickHouseQueryParam, String> additionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);
        parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, additionalDBParams);

        try (InputStream is = getLastInputStream(additionalDBParams, null, null)) {
            //noinspection StatementWithEmptyBody
        } catch (IOException e) {
            log.error("can not close stream: {}", e.getMessage());
        }

        return currentSummary != null ? (int) currentSummary.getWrittenRows() : 1;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // currentResult is stored here. InputString and currentResult will be closed on this.close()
        return executeQuery(sql) != null;
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }

        if (currentRowBinaryResult != null) {
            try {
                currentRowBinaryResult.close();
            } catch (IOException e) {
                log.error("can not close stream: {}", e.getMessage());
            }
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException(String.format("Illegal maxRows value: %d", max));
        }
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        queryTimeout = seconds;
        isQueryTimeoutSet = true;
    }

    @Override
    public void cancel() throws SQLException {
        if (this.queryId == null || isClosed()) {
            return;
        }

       executeQuery(String.format("KILL QUERY WHERE query_id='%s'", queryId));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResult;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        for (ClickHouseSqlStatement s : ClickHouseSqlParser.parse(sql, properties)) {
            this.batchStmts.add(s);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        this.batchStmts = new ArrayList<>();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int len = batchStmts.size();
        int[] results = new int[len];
        for (int i = 0; i < len; i++) {
            results[i] = executeStatement(batchStmts.get(i), null, null, null);
        }
        
        clearBatch();

        return results;
    }

    @Override
    public ClickHouseConnection getConnection() throws ClickHouseException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public ClickHouseResponseSummary getResponseSummary() {
        return currentSummary;
    }

    private InputStream getLastInputStream(
        Map<ClickHouseQueryParam, String> additionalDBParams,
        List<ClickHouseExternalData> externalData,
        Map<String, String> additionalRequestParams) throws ClickHouseException {
        InputStream is = null;
        for (int i = 0, len = parsedStmts.length; i < len; i++) {
            // TODO skip useless queries to reduce network calls and server load
            is = getInputStream(parsedStmts[i], additionalDBParams, externalData, additionalRequestParams);
            // TODO multi-resultset
            if (i + 1 < len) {
                try {
                    is.close();
                } catch (IOException ioe) {
                    log.warn("Failed to close stream: {}", ioe.getMessage());
                }
            }
        }

        return is;
    }

    private InputStream getInputStream(
        ClickHouseSqlStatement parsedStmt,
        Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
        List<ClickHouseExternalData> externalData,
        Map<String, String> additionalRequestParams
    ) throws ClickHouseException {
        String sql = parsedStmt.getSQL();
        boolean ignoreDatabase = parsedStmt.isRecognized() && !parsedStmt.isDML()
            && parsedStmt.containsKeyword("DATABASE");
        if (parsedStmt.getStatementType() == StatementType.USE) {
            currentDatabase = parsedStmt.getDatabaseOrDefault(currentDatabase);
        }
        
        log.debug("Executing SQL: {}", sql);

        additionalClickHouseDBParams = addQueryIdTo(
                additionalClickHouseDBParams == null
                        ? new EnumMap<ClickHouseQueryParam, String>(ClickHouseQueryParam.class)
                        : additionalClickHouseDBParams);

        URI uri = buildRequestUri(
            null,
            externalData,
            additionalClickHouseDBParams,
            additionalRequestParams,
            ignoreDatabase
        );
        log.debug("Request url: {}", uri);


        HttpEntity requestEntity;
        if (externalData == null || externalData.isEmpty()) {
            requestEntity = new StringEntity(sql, StandardCharsets.UTF_8);
        } else {
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();

            ContentType queryContentType = ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), StandardCharsets.UTF_8);
            entityBuilder.addTextBody("query", sql, queryContentType);

            try {
                for (ClickHouseExternalData externalDataItem : externalData) {
                    // clickhouse may return 400 (bad request) when chunked encoding is used with multipart request
                    // so read content to byte array to avoid chunked encoding
                    // TODO do not read stream into memory when this issue is fixed in clickhouse
                    entityBuilder.addBinaryBody(
                        externalDataItem.getName(),
                        Utils.toByteArray(externalDataItem.getContent()),
                        ContentType.APPLICATION_OCTET_STREAM,
                        externalDataItem.getName()
                    );
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            requestEntity = entityBuilder.build();
        }

        requestEntity = applyRequestBodyCompression(requestEntity);

        HttpEntity entity = null;
        try {
            uri = followRedirects(uri);
            HttpPost post = new HttpPost(uri);
            post.setEntity(requestEntity);

            if (parsedStmt.isIdemponent()) {
                httpContext.setAttribute("is_idempotent", Boolean.TRUE);
            } else {
                httpContext.removeAttribute("is_idempotent");
            }
            
            HttpResponse response = client.execute(post, httpContext);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            InputStream is;
            if (entity.isStreaming()) {
                is = entity.getContent();
            } else {
                FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
                entity.writeTo(baos);
                is = baos.convertToInputStream();
            }

            // retrieve response summary
            if (isQueryParamSet(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, additionalClickHouseDBParams, additionalRequestParams)) {
                Header summaryHeader = response.getFirstHeader("X-ClickHouse-Summary");
                currentSummary = summaryHeader != null ? JsonStreamUtils.readObject(summaryHeader.getValue(), ClickHouseResponseSummary.class) : null;
            }

            return is;
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            log.info("Error during connection to {}, reporting failure to data source, message: {}", properties, e.getMessage());
            EntityUtils.consumeQuietly(entity);
            log.info("Error sql: {}", sql);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    URI buildRequestUri(
        String sql,
        List<ClickHouseExternalData> externalData,
        Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
        Map<String, String> additionalRequestParams,
        boolean ignoreDatabase
    ) {
        try {
            List<NameValuePair> queryParams = getUrlQueryParams(
                sql,
                externalData,
                additionalClickHouseDBParams,
                additionalRequestParams,
                ignoreDatabase
            );

            // avoid to reuse query id
            if (additionalClickHouseDBParams != null) {
                additionalClickHouseDBParams.remove(ClickHouseQueryParam.QUERY_ID);
            }

            return new URIBuilder()
                .setScheme(properties.getSsl() ? "https" : "http")
                .setHost(properties.getHost())
                .setPort(properties.getPort())
                .setPath((properties.getPath() == null || properties.getPath().isEmpty() ? "/" : properties.getPath()))
                .setParameters(queryParams)
                .build();
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: {}", e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
    }

    private List<NameValuePair> getUrlQueryParams(
        String sql,
        List<ClickHouseExternalData> externalData,
        Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
        Map<String, String> additionalRequestParams,
        boolean ignoreDatabase
    ) {
        List<NameValuePair> result = new ArrayList<>();

        if (sql != null && !sql.isEmpty()) {
            result.add(new BasicNameValuePair("query", sql));
        }

        if (externalData != null) {
            for (ClickHouseExternalData externalDataItem : externalData) {
                String name = externalDataItem.getName();
                String format = externalDataItem.getFormat();
                String types = externalDataItem.getTypes();
                String structure = externalDataItem.getStructure();

                if (format != null && !format.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_format", format));
                }
                if (types != null && !types.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_types", types));
                }
                if (structure != null && !structure.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_structure", structure));
                }
            }
        }

        Map<ClickHouseQueryParam, String> params = properties.buildQueryParams(true);
        if (!ignoreDatabase) {
            params.put(ClickHouseQueryParam.DATABASE, currentDatabase);
        }

        params.putAll(getAdditionalDBParams());

        if (additionalClickHouseDBParams != null && !additionalClickHouseDBParams.isEmpty()) {
            params.putAll(additionalClickHouseDBParams);
        }

        setStatementPropertiesToParams(params);

        for (Map.Entry<ClickHouseQueryParam, String> entry : params.entrySet()) {
            if (!Utils.isNullOrEmptyString(entry.getValue())) {
                result.add(new BasicNameValuePair(entry.getKey().toString(), entry.getValue()));
            }
        }

        for (Map.Entry<String, String> entry : getRequestParams().entrySet()) {
            if (!Utils.isNullOrEmptyString(entry.getValue())) {
                result.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
        }

        if (additionalRequestParams != null) {
            for (Map.Entry<String, String> entry : additionalRequestParams.entrySet()) {
                if (!Utils.isNullOrEmptyString(entry.getValue())) {
                    result.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
            }
        }

        return result;
    }

    private boolean isQueryParamSet(ClickHouseQueryParam param, Map<ClickHouseQueryParam, String> additionalClickHouseDBParams, Map<String, String> additionalRequestParams) {
        String value = getQueryParamValue(param, additionalClickHouseDBParams, additionalRequestParams);

        return "true".equals(value) || "1".equals(value);
    }

    private String getQueryParamValue(ClickHouseQueryParam param, Map<ClickHouseQueryParam, String> additionalClickHouseDBParams, Map<String, String> additionalRequestParams) {
        if (additionalRequestParams != null && additionalRequestParams.containsKey(param.getKey()) && !Utils.isNullOrEmptyString(additionalRequestParams.get(param.getKey()))) {
            return additionalRequestParams.get(param.getKey());
        }

        if (getRequestParams().containsKey(param.getKey()) && !Utils.isNullOrEmptyString(getRequestParams().get(param.getKey()))) {
            return getRequestParams().get(param.getKey());
        }

        if (additionalClickHouseDBParams != null && additionalClickHouseDBParams.containsKey(param) && !Utils.isNullOrEmptyString(additionalClickHouseDBParams.get(param))) {
            return additionalClickHouseDBParams.get(param);
        }

        if (getAdditionalDBParams().containsKey(param) && !Utils.isNullOrEmptyString(getAdditionalDBParams().get(param))) {
            return getAdditionalDBParams().get(param);
        }

        return properties.asProperties().getProperty(param.getKey());
    }

    private URI followRedirects(URI uri) throws IOException, URISyntaxException {
        if (properties.isCheckForRedirects()) {
            int redirects = 0;
            while (redirects < properties.getMaxRedirects()) {
                HttpGet httpGet = new HttpGet(uri);
                HttpResponse response = client.execute(httpGet, httpContext);
                if (response.getStatusLine().getStatusCode() == 307) {
                    uri = new URI(response.getHeaders("Location")[0].getValue());
                    redirects++;
                    log.info("Redirected to " + uri.getHost());
                } else {
                    break;
                }
            }
        }
        return uri;
    }

    private void setStatementPropertiesToParams(Map<ClickHouseQueryParam, String> params) {
        if (maxRows > 0) {
            params.put(ClickHouseQueryParam.MAX_RESULT_ROWS, String.valueOf(maxRows));
            params.put(ClickHouseQueryParam.RESULT_OVERFLOW_MODE, "break");
        }
        if(isQueryTimeoutSet) {
            params.put(ClickHouseQueryParam.MAX_EXECUTION_TIME, String.valueOf(queryTimeout));
        }
    }


    @Override
    public void sendRowBinaryStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        sendRowBinaryStream(sql, null, callback);
    }

    @Override
    public void sendRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, ClickHouseStreamCallback callback) throws SQLException {
        write().withDbParams(additionalDBParams)
                .send(sql, callback, ClickHouseFormat.RowBinary);
    }

    @Override
    public void sendNativeStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        sendNativeStream(sql, null, callback);
    }

    @Override
    public void sendNativeStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, ClickHouseStreamCallback callback) throws SQLException {
        write().withDbParams(additionalDBParams)
                .send(sql, callback, ClickHouseFormat.Native);
    }

    @Override
    public void sendCSVStream(InputStream content, String table, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        write()
                .table(table)
                .withDbParams(additionalDBParams)
                .data(content)
                .format(ClickHouseFormat.CSV)
                .send();
    }

    @Override
    public void sendCSVStream(InputStream content, String table) throws SQLException {
        sendCSVStream(content, table, null);
    }

    @Override
    public void sendStream(InputStream content, String table) throws SQLException {
        sendStream(content, table, null);
    }

    @Override
    public void sendStream(InputStream content, String table,
                           Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        write()
                .table(table)
                .data(content)
                .withDbParams(additionalDBParams)
                .format(ClickHouseFormat.TabSeparated)
                .send();
    }

    @Deprecated
    public void sendStream(HttpEntity content, String sql) throws ClickHouseException {
        sendStream(content, sql, ClickHouseFormat.TabSeparated, null);
    }

    @Deprecated
    public void sendStream(HttpEntity content, String sql,
                           Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {
        sendStream(content, sql, ClickHouseFormat.TabSeparated, additionalDBParams);
    }

    private void sendStream(HttpEntity content, String sql, ClickHouseFormat format,
                            Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {

        Writer writer = write().format(format).withDbParams(additionalDBParams).sql(sql);
        sendStream(writer, content);
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql,
                              Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        write().data(content).sql(sql).withDbParams(additionalDBParams).send();
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql) throws SQLException {
        write().sql(sql).data(content).send();
    }

    void sendStream(Writer writer, HttpEntity content) throws ClickHouseException {
        HttpEntity entity = null;
        // TODO no parser involved so user can execute arbitray statement here
        try {
            String sql = writer.getSql();
            boolean isContentCompressed = writer.getCompression() != ClickHouseCompression.none;
            URI uri = buildRequestUri(
                isContentCompressed ? sql : null, null, writer.getAdditionalDBParams(), writer.getRequestParams(), false);
            uri = followRedirects(uri);

            content = applyRequestBodyCompression(
                new WrappedHttpEntity(isContentCompressed ? null : sql, content));

            HttpPost httpPost = new HttpPost(uri);

            if (writer.getCompression() != ClickHouseCompression.none) {
                httpPost.addHeader("Content-Encoding", writer.getCompression().name());
            }
            httpPost.setEntity(content);
            HttpResponse response = client.execute(httpPost, httpContext);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            // retrieve response summary
            if (isQueryParamSet(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, writer.getAdditionalDBParams(), writer.getRequestParams())) {
                Header summaryHeader = response.getFirstHeader("X-ClickHouse-Summary");
                currentSummary = summaryHeader != null ? JsonStreamUtils.readObject(summaryHeader.getValue(), ClickHouseResponseSummary.class) : null;
            }
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
    }

    private void checkForErrorAndThrow(HttpEntity entity, HttpResponse response) throws IOException, ClickHouseException {
        StatusLine line = response.getStatusLine();
        if (line.getStatusCode() != HttpURLConnection.HTTP_OK) {
            InputStream messageStream = entity.getContent();
            byte[] bytes = Utils.toByteArray(messageStream);
            if (properties.isCompress()) {
                try {
                    messageStream = new ClickHouseLZ4Stream(new ByteArrayInputStream(bytes));
                    bytes = Utils.toByteArray(messageStream);
                } catch (IOException e) {
                    log.warn("error while read compressed stream {}", e.getMessage());
                }
            }
            EntityUtils.consumeQuietly(entity);
            if (bytes.length == 0) {
                throw ClickHouseExceptionSpecifier.specify(new IllegalStateException(line.toString()), properties.getHost(), properties.getPort());
            } else {
                throw ClickHouseExceptionSpecifier.specify(new String(bytes, StandardCharsets.UTF_8), properties.getHost(), properties.getPort());
            }
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }

    private HttpEntity applyRequestBodyCompression(final HttpEntity entity) {
        if (properties.isDecompress()) {
            return new LZ4EntityWrapper(entity, properties.getMaxCompressBufferSize());
        }
        return entity;
    }

    private ClickHouseResultSet createResultSet(InputStream is, int bufferSize, String db, String table, boolean usesWithTotals,
    		ClickHouseStatement statement, TimeZone timezone, ClickHouseProperties properties) throws IOException {
    	if(isResultSetScrollable) {
    		return new ClickHouseScrollableResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	} else {
    		return new ClickHouseResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	}
    }

    private Map<ClickHouseQueryParam, String> addQueryIdTo(Map<ClickHouseQueryParam, String> parameters) {
        if (this.queryId != null) {
            return parameters;
        }

        String queryId = parameters.get(ClickHouseQueryParam.QUERY_ID);
        if (queryId == null) {
            // TODO perhaps we should use TimeUUID so that it's easy to sort?
            this.queryId = UUID.randomUUID().toString();
            parameters.put(ClickHouseQueryParam.QUERY_ID, this.queryId);
        } else {
            this.queryId = queryId;
        }

        return parameters;
    }

    @Override
    public Writer write() {
        return new Writer(this).withDbParams(getAdditionalDBParams()).options(getRequestParams());
    }
}
