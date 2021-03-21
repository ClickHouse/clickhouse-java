package ru.yandex.clickhouse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(
        ClickHouseStatementImpl.class);

    private final CloseableHttpClient client;

    private final HttpClientContext httpContext;

    private final ClickHouseProperties properties;

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

    protected ClickHouseSqlStatement parsedStmt;

    /*
     * Current database name may be changed by {@link java.sql.Connection#setCatalog(String)}
     * between creation of this object and query execution, but javadoc does not allow
     * {@code setCatalog} influence on already created statements.
     */
    private final String initialDatabase;


    public ClickHouseStatementImpl(CloseableHttpClient client, ClickHouseConnection connection,
                                   ClickHouseProperties properties, int resultSetType) {
        super(null);
        this.client = client;
        this.httpContext = ClickHouseHttpClientBuilder.createClientContext(properties);
        this.connection = connection;
        this.properties = properties == null ? new ClickHouseProperties() : properties;
        this.initialDatabase = this.properties.getDatabase();
        this.isResultSetScrollable = (resultSetType != ResultSet.TYPE_FORWARD_ONLY);
        this.queryId = UUID.randomUUID().toString();
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
    public ResultSet executeQuery(final String sql, final Map<ClickHouseQueryParam, String> additionalDBParams,
        final List<ClickHouseExternalData> externalData, final Map<String, String> additionalRequestParams)
        throws SQLException
    {
        // forcibly disable extremes for ResultSet queries
        // why only in this method???
        Map<ClickHouseQueryParam, String> myAdditionalDBParams =
                additionalDBParams == null || additionalDBParams.isEmpty()
            ? new EnumMap<>(ClickHouseQueryParam.class)
            : new EnumMap<>(additionalDBParams);
        updateStateFromQueryParams(myAdditionalDBParams);
        myAdditionalDBParams.put(ClickHouseQueryParam.EXTREMES, "0");
        parseSingleStatement(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes);
        InputStream is = getInputStream(parsedStmt.getSQL(), myAdditionalDBParams, externalData, additionalRequestParams);
        try {
            if (parsedStmt.isQuery()) {
                currentUpdateCount = -1;
                currentResult = createResultSet(
                        is,
                        properties.getBufferSize(),
                        parsedStmt.getDatabaseOrDefault(properties.getDatabase()),
                        parsedStmt.getTable(),
                        parsedStmt.hasWithTotals(),
                        this,
                        getConnection().getTimeZone(),
                        properties);
                currentResult.setMaxRows(maxRows);
                return currentResult;
            }
            currentUpdateCount = 0;
        } catch (Exception e) {
            try {
                is.close();
            } catch (IOException ioe) {
                throw ClickHouseExceptionSpecifier.specify(ioe, properties.getHost(), properties.getPort());
            }
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
        return null;
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
    public ClickHouseResponse executeQueryClickhouseResponse(final String sql,
        final Map<ClickHouseQueryParam, String> additionalDBParams,
        final Map<String, String> additionalRequestParams) throws SQLException
    {
        parseSingleStatement(sql, ClickHouseFormat.JSONCompact);
        updateStateFromQueryParams(additionalDBParams);
        try (InputStream is = getInputStream(parsedStmt.getSQL(), additionalDBParams, null, additionalRequestParams)) {
            return Jackson.getObjectMapper().readValue(is, ClickHouseResponse.class);
        } catch (IOException e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, null);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql,
        Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException
    {
        return executeQueryClickhouseRowBinaryStream(sql, additionalDBParams, null);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(
        final String sql, Map<ClickHouseQueryParam, String> additionalDBParams,
        Map<String, String> additionalRequestParams) throws SQLException
    {
        parseSingleStatement(sql, ClickHouseFormat.RowBinary);
        updateStateFromQueryParams(additionalDBParams);
        InputStream is = getInputStream(parsedStmt.getSQL(), additionalDBParams, null, additionalRequestParams);
        try {
            if (parsedStmt.isQuery()) {
                currentUpdateCount = -1;
                currentRowBinaryResult = new ClickHouseRowBinaryInputStream(
                    is, getConnection().getTimeZone(), properties);
                return currentRowBinaryResult;
            }
            currentUpdateCount = 0;
            try {
                is.close();
            } catch (IOException e) {
                LOG.error("can not close stream: {}", e.getMessage());
            }
        } catch (Exception e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        parseSingleStatement(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes);
        try (InputStream is = getInputStream(sql, null, null, null)) {
            //noinspection StatementWithEmptyBody
        } catch (IOException e) {
            LOG.error("can not close stream: {}", e.getMessage());
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
                LOG.error("can not close stream: {}", e.getMessage());
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
       executeQuery(
           String.format("KILL QUERY WHERE query_id='%s'", queryId),
           // we need a new query ID for the cancellation query
           Collections.singletonMap(
               ClickHouseQueryParam.QUERY_ID,
               UUID.randomUUID().toString()));
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

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
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

    @Deprecated
    static String clickhousifySql(String sql) {
        return addFormatIfAbsent(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes);
    }

    /**
     * Adding  FORMAT TabSeparatedWithNamesAndTypes if not added
     * adds format only to select queries
     */
    @Deprecated
    private static String addFormatIfAbsent(final String sql, ClickHouseFormat format) {
        String cleanSQL = sql.trim();
        ClickHouseSqlStatement[] statements = ClickHouseSqlParser.parse(cleanSQL, null);
        if (statements == null
            || statements.length != 1
            || !statements[0].isQuery())
        {
            return cleanSQL;
        }
        if (ClickHouseFormat.containsFormat(cleanSQL)) {
            return cleanSQL;
        }
        StringBuilder sb = new StringBuilder();
        int idx = cleanSQL.endsWith(";")
            ? cleanSQL.length() - 1
            : cleanSQL.length();
        sb.append(cleanSQL, 0, idx)
          .append("\nFORMAT ")
          .append(format.name())
          .append(';');
        return sb.toString();
    }

    @Deprecated
    private String extractTableName(String sql) {
        String s = extractDBAndTableName(sql);
        return s.contains(".")
            ? s.substring(s.indexOf(".") + 1)
            : s;
    }

    @Deprecated
    private String extractDBName(String sql) {
        String s = extractDBAndTableName(sql);
        return s.contains(".")
            ? s.substring(0, s.indexOf("."))
            : properties.getDatabase();
    }

    @Deprecated
    private String extractDBAndTableName(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            int fromIndex = withoutStrings.indexOf("from");
            if (fromIndex == -1) {
                fromIndex = withoutStrings.indexOf("FROM");
            }
            if (fromIndex != -1) {
                String fromFrom = withoutStrings.substring(fromIndex);
                String fromTable = fromFrom.substring("from".length()).trim();
                return fromTable.split(" ")[0];
            }
        }
        if (Utils.startsWithIgnoreCase(sql, "desc")) {
            return "system.columns";
        }
        if (Utils.startsWithIgnoreCase(sql, "show")) {
            return "system.tables";
        }
        return "system.unknown";
    }

    @Deprecated
    private boolean extractWithTotals(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            return withoutStrings.toLowerCase(Locale.ROOT).contains(" with totals");
        }
        return false;
    }

    private InputStream getInputStream(
        final String sql,
        final Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
        final List<ClickHouseExternalData> externalData,
        final Map<String, String> additionalRequestParams)
            throws ClickHouseException
    {
        boolean ignoreDatabase = false;
        String mySql =  parsedStmt.getSQL();
        // TODO consider more scenarios like drop, show etc.
        ignoreDatabase = parsedStmt.getStatementType() == StatementType.CREATE
            && parsedStmt.containsKeyword(ClickHouseSqlStatement.KEYWORD_DATABASE);
        LOG.debug("Executing SQL: {}", mySql);

        Map<ClickHouseQueryParam, String> myAdditionalClickHouseDBParams =
            addQueryIdTo(
                additionalClickHouseDBParams == null
                    ? new EnumMap<ClickHouseQueryParam, String>(ClickHouseQueryParam.class)
                    : additionalClickHouseDBParams);

        URI uri;
        if (externalData == null || externalData.isEmpty()) {
            uri = buildRequestUri(
                    null,
                    null,
                    myAdditionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
        } else {
            // write sql in query params when there is external data
            // as it is impossible to pass both external data and sql in body
            // TODO move sql to request body when it is supported in clickhouse
            uri = buildRequestUri(
                    sql,
                    externalData,
                    myAdditionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
        }
        LOG.debug("Request url: {}", uri);


        HttpEntity requestEntity;
        if (externalData == null || externalData.isEmpty()) {
            requestEntity = new StringEntity(sql, StandardCharsets.UTF_8);
        } else {
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();

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
            HttpPost post = new HttpPost(uri);
            if (properties.isCheckForRedirects()) {
                post.setConfig(RequestConfig.custom()
                    .setMaxRedirects(properties.getMaxRedirects())
                    .build());
            }
            post.setEntity(requestEntity);

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
                currentSummary = summaryHeader != null ? Jackson.getObjectMapper().readValue(summaryHeader.getValue(), ClickHouseResponseSummary.class) : null;
            }

            return properties.isCompress()
                ? new ClickHouseLZ4Stream(is)
                : is;
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            LOG.info("Error during connection to {}, reporting failure to data source, message: {}", properties, e.getMessage());
            EntityUtils.consumeQuietly(entity);
            LOG.info("Error sql: {}", sql);
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

            return new URIBuilder()
                .setScheme(properties.getSsl() ? "https" : "http")
                .setHost(properties.getHost())
                .setPort(properties.getPort())
                .setPath((properties.getPath() == null || properties.getPath().isEmpty() ? "/" : properties.getPath()))
                .setParameters(queryParams)
                .build();
        } catch (URISyntaxException e) {
            LOG.error("Mailformed URL: {}", e.getMessage());
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

        if (sql != null) {
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
            params.put(ClickHouseQueryParam.DATABASE, initialDatabase);
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

    @Deprecated
    protected void parseSingleStatement(String sql) throws SQLException {
        this.parsedStmt = null;
        ClickHouseSqlStatement[] stmts = ClickHouseSqlParser.parse(sql, properties);

        if (stmts.length == 1) {
            this.parsedStmt = stmts[0];
        } else {
            this.parsedStmt = new ClickHouseSqlStatement(sql, StatementType.UNKNOWN);
            // throw new SQLException("Multiple statements are not supported.");
        }

        if (this.parsedStmt.isIdemponent()) {
            httpContext.setAttribute("is_idempotent", Boolean.TRUE);
        } else {
            httpContext.removeAttribute("is_idempotent");
        }
    }

    @Deprecated
    private void parseSingleStatement(final String sql, final ClickHouseFormat preferredFormat)
        throws SQLException
    {
        parseSingleStatement(sql);
        if (parsedStmt.isQuery() && !parsedStmt.hasFormat()) {
            String format = preferredFormat.name();
            Map<String, Integer> positions = new HashMap<>();
            positions.putAll(parsedStmt.getPositions());
            positions.put(ClickHouseSqlStatement.KEYWORD_FORMAT, Integer.valueOf(sql.length()));
            String mySql = new StringBuilder(parsedStmt.getSQL())
                .append("\nFORMAT ")
                .append(format)
                .append(';')
                .toString();
            parsedStmt = new ClickHouseSqlStatement(mySql, parsedStmt.getStatementType(),
                parsedStmt.getCluster(), parsedStmt.getDatabase(), parsedStmt.getTable(),
                format, parsedStmt.getOutfile(), parsedStmt.getParameters(), positions);
        }
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

    protected final void sendStream(HttpEntity content, String sql,
        ClickHouseFormat format,
        Map<ClickHouseQueryParam, String> additionalDBParams)
        throws ClickHouseException
    {

        Writer writer = write().format(format).withDbParams(additionalDBParams)
            .sql(sql);
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
        try {

            URI uri = buildRequestUri(writer.getSql(), null, writer.getAdditionalDBParams(), writer.getRequestParams(), false);
            HttpPost httpPost = new HttpPost(uri);
            if (properties.isCheckForRedirects()) {
                httpPost.setConfig(RequestConfig.custom()
                    .setMaxRedirects(properties.getMaxRedirects())
                    .build());
            }
            if (writer.getCompression() != null) {
                httpPost.addHeader("Content-Encoding", writer.getCompression().name());
            }
            httpPost.setEntity(applyRequestBodyCompression(content));
            HttpResponse response = client.execute(httpPost, httpContext);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            // retrieve response summary
            if (isQueryParamSet(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, writer.getAdditionalDBParams(), writer.getRequestParams())) {
                Header summaryHeader = response.getFirstHeader("X-ClickHouse-Summary");
                currentSummary = summaryHeader != null ? Jackson.getObjectMapper().readValue(summaryHeader.getValue(), ClickHouseResponseSummary.class) : null;
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
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
            InputStream messageStream = entity.getContent();
            byte[] bytes = Utils.toByteArray(messageStream);
            if (properties.isCompress()) {
                try {
                    messageStream = new ClickHouseLZ4Stream(new ByteArrayInputStream(bytes));
                    bytes = Utils.toByteArray(messageStream);
                } catch (IOException e) {
                    LOG.warn("error while read compressed stream {}", e.getMessage());
                }
            }
            EntityUtils.consumeQuietly(entity);
            String chMessage = new String(bytes, StandardCharsets.UTF_8);
            throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
        }
    }

    final ClickHouseProperties getProperties() {
        return properties;
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
    	return isResultSetScrollable
    	    ? new ClickHouseScrollableResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties)
    	    : new ClickHouseResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    }

    private Map<ClickHouseQueryParam, String> addQueryIdTo(Map<ClickHouseQueryParam,
        String> parameters)
    {
        if (parameters.containsKey(ClickHouseQueryParam.QUERY_ID)) {
            return parameters;
        }
        // parameters might be non-modifiable
        Map<ClickHouseQueryParam, String> myParams = new EnumMap<>(parameters);
        myParams.put(ClickHouseQueryParam.QUERY_ID, queryId);
        return myParams;
    }

    @Override
    public Writer write() {
        return new Writer(this).withDbParams(getAdditionalDBParams()).options(getRequestParams());
    }

    @Override
    protected ClickHouseStatement getThis() {
        return this;
    }

    private void updateStateFromQueryParams(
        Map<ClickHouseQueryParam, String> additionalQueryParameters)
    {
        if (additionalQueryParameters != null) {
            this.queryId = additionalQueryParameters.getOrDefault(
                ClickHouseQueryParam.QUERY_ID,
                queryId);
        }
    }
}
