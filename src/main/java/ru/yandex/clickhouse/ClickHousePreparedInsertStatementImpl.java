package ru.yandex.clickhouse;

import org.apache.http.impl.client.CloseableHttpClient;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHousePreparedInsertStatementImpl extends ClickHousePreparedAbstractStatementImpl implements ClickHousePreparedStatement {
    private static final Pattern VALUES_PATTERN = Pattern.compile("\\(.*?\\)\\s*VALUES", Pattern.CASE_INSENSITIVE);
    private static final String VALUES = "VALUES";

    private final String[] parameters;
    private final BitSet isHasQuote;
    private final List<byte[]> batchRows;
    private final Query queryWrapper;


    public ClickHousePreparedInsertStatementImpl(CloseableHttpClient client,
                                                 ClickHouseConnection connection,
                                                 ClickHouseProperties properties,
                                                 String sql,
                                                 TimeZone timezone) throws SQLException {

        super(client, connection, properties, timezone);

        this.queryWrapper = new Query(sql);
        this.parameters = this.queryWrapper.getParametersArray();
        this.batchRows = new ArrayList<byte[]>();
        this.isHasQuote = new BitSet(parameters.length);
    }

    @Override
    protected String getSqlQuery() throws SQLException {
        if (parameters.length == 0)
            return queryWrapper.sqlQuery;

        checkParameters(this.parameters);
        return new StringBuilder(queryWrapper.sqlQuery).append(' ')
                .append(VALUES).append(' ')
                .append(queryWrapper.getRow(this))
                .toString();
    }

    private void appendParameter(StringBuilder sb, int index) {
        if (isHasQuote.get(index))
            sb.append("'").append(parameters[index]).append("'");
        else if ("\\N".equals(parameters[index]))
            sb.append("null");
        else
            sb.append(parameters[index]);
    }


    @Override
    public void addBatch() throws SQLException {
        checkParameters(parameters);
        batchRows.add(queryWrapper.getBatchRow(parameters));
        clearParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
        batchRows.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        String insertSql = queryWrapper.sqlQuery;
        BatchHttpEntity entity = new BatchHttpEntity(batchRows);
        sendStream(entity, insertSql);

        clearParameters();
        clearBatch();
        int[] result = new int[queryWrapper.tuples.size()];
        Arrays.fill(result, 1);
        return result;
    }

    @Override
    public void clearParameters() throws SQLException {
        Arrays.fill(parameters, null);
        isHasQuote.clear();
    }


    private static final class Query {
        private static final String ARGUMENT_SYMBOL = "?";
        private static final Pattern ARGUMENT_PATTERN = Pattern.compile("\\((.*?)\\)\\s*");

        private final List<String[]> tuples;
        private final String sqlQuery;
        private final int countOfArguments;


        private Query(String sqlQuery) {
            this.sqlQuery = getQueryWithoutArguments(sqlQuery);
            this.tuples = getTuples(sqlQuery);
            this.countOfArguments = getCountOfArguments(sqlQuery);
        }


        private String getRow(ClickHousePreparedInsertStatementImpl statement) {
            StringBuilder sb = new StringBuilder();

            int index = 0;
            for (int i = 0; i < tuples.size(); i++) {
                sb.append('(');
                String[] row = tuples.get(i);
                for (int j = 0; j < row.length; j++) {
                    String parameter = row[j];
                    if (parameter == null)
                        statement.appendParameter(sb, index++);
                    else
                        sb.append(parameter);

                    if (j != row.length - 1)
                        sb.append(',');
                }
                sb.append(')');
                if (i != tuples.size() - 1)
                    sb.append(',');
            }


            return sb.toString();
        }

        private byte[] getBatchRow(String[] parameters) {
            StringBuilder sb = new StringBuilder();

            int index = 0;
            for (int i = 0; i < tuples.size(); i++) {
                String[] args = tuples.get(i);

                for (int j = 0; j < args.length; j++) {
                    String parameter = args[j];
                    if (parameter == null)
                        sb.append(parameters[index++]);
                    else
                        sb.append(parameter);

                    sb.append(j < args.length - 1 ? '\t' : '\n');
                }
            }


            return sb
                    .toString()
                    .getBytes(StreamUtils.UTF_8);
        }


        private static List<String[]> getTuples(String sql) {
            Matcher matcher = ARGUMENT_PATTERN.matcher(sql);

            ArrayList<String[]> result = new ArrayList<String[]>();
            if (matcher.find())
                while (matcher.find()) {
                    String[] parameters = matcher.group(1).split(",");
                    for (int i = 0; i < parameters.length; i++) {
                        String valueParam = parameters[i].trim();
                        if (ARGUMENT_SYMBOL.equals(valueParam))
                            parameters[i] = null;
                        else
                            parameters[i] = valueParam;
                    }

                    result.add(parameters);
                }

            return result;
        }

        private static String getQueryWithoutArguments(String query) {
            Matcher matcher = VALUES_PATTERN.matcher(query);
            if (matcher.find())
                return query.substring(0, matcher.end() - VALUES.length());
            else
                return query;
        }


        private static int getCountOfArguments(String sql) {
            int count = 0;
            int index = 0;
            while ((index = sql.indexOf(ARGUMENT_SYMBOL, index + 1)) != -1)
                count++;

            return count;
        }

        private String[] getParametersArray() {
            return new String[countOfArguments];
        }
    }

    protected void setParameter(int index, String value, boolean isQuote) {
        parameters[index - 1] = value;
        isHasQuote.set(index - 1, isQuote);
    }
}
