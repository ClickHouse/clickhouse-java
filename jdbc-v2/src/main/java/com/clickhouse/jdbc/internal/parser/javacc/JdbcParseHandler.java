package com.clickhouse.jdbc.internal.parser.javacc;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcParseHandler extends ParseHandler {
    private static final String SETTING_MUTATIONS_SYNC = "mutations_sync";

    private static final JdbcParseHandler INSTANCE;

    static {
        INSTANCE = new JdbcParseHandler(true, true, true);
    };

    public static JdbcParseHandler getInstance() {
        return INSTANCE;
    }

    private final boolean allowLocalFile;
    private final boolean allowLightWeightDelete;
    private final boolean allowLightWeightUpdate;

    private void addMutationSetting(String sql, StringBuilder builder, Map<String, Integer> positions,
            Map<String, String> settings, int index) {
        boolean hasSetting = settings != null && !settings.isEmpty();
        String setting = hasSetting ? settings.get(SETTING_MUTATIONS_SYNC) : null;
        if (setting == null) {
            String keyword = "SETTINGS";
            Integer settingsIndex = positions.get(keyword);

            if (settingsIndex == null) {
                builder.append(sql.substring(index)).append(" SETTINGS mutations_sync=1");
                if (hasSetting) {
                    builder.append(',');
                }
            } else {
                builder.append(sql.substring(index, settingsIndex)).append("SETTINGS mutations_sync=1,")
                        .append(sql.substring(settingsIndex + keyword.length()));
            }
        } else {
            builder.append(sql.substring(index));
        }
    }

    private ClickHouseSqlStatement handleDelete(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String compressAlgorithm, String compressLevel, String format, String file,
            List<Integer> parameters, Map<String, Integer> positions, Map<String, String> settings,
            Set<String> tempTables, boolean funcUsed) {
        StringBuilder builder = new StringBuilder();
        int index = positions.get("DELETE");
        if (index > 0) {
            builder.append(sql.substring(0, index));
        }
        index = positions.get("FROM");
        Integer whereIdx = positions.get("WHERE");
        if (whereIdx != null) {
            builder.append("ALTER TABLE ");
            if (!ClickHouseChecker.isNullOrEmpty(database)) {
                builder.append('`').append(database).append('`').append('.');
            }
            builder.append('`').append(table).append('`').append(" DELETE ");
            addMutationSetting(sql, builder, positions, settings, whereIdx);
        } else {
            builder.append("TRUNCATE TABLE").append(sql.substring(index + 4));
        }
        return new ClickHouseSqlStatement(builder.toString(), stmtType, cluster, database, table, input,
                compressAlgorithm, compressLevel, format, file, parameters, null, settings, null, 0, funcUsed);
    }

    private ClickHouseSqlStatement handleUpdate(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String compressAlgorithm, String compressLevel, String format, String file,
            List<Integer> parameters, Map<String, Integer> positions, Map<String, String> settings,
            Set<String> tempTables, boolean funcUsed) {
        StringBuilder builder = new StringBuilder();
        int index = positions.get("UPDATE");
        if (index > 0) {
            builder.append(sql.substring(0, index));
        }
        builder.append("ALTER TABLE ");
        index = positions.get("SET");
        if (!ClickHouseChecker.isNullOrEmpty(database)) {
            builder.append('`').append(database).append('`').append('.');
        }
        builder.append('`').append(table).append('`').append(" UPDATE"); // .append(sql.substring(index + 3));
        addMutationSetting(sql, builder, positions, settings, index + 3);
        return new ClickHouseSqlStatement(builder.toString(), stmtType, cluster, database, table, input,
                compressAlgorithm, compressLevel, format, file, parameters, null, settings, null, 0, funcUsed);
    }

    private ClickHouseSqlStatement handleInFileForInsertQuery(String sql, StatementType stmtType, String cluster,
                                                              String database, String table, String input, String compressAlgorithm, String compressLevel, String format,
                                                              String file, List<Integer> parameters, Map<String, Integer> positions, Map<String, String> settings,
                                                              Set<String> tempTables, int valueGroups, boolean funcUsed) {
        StringBuilder builder = new StringBuilder(sql.length());
        builder.append(sql.substring(0, positions.get("FROM")));
        Integer index = positions.get("SETTINGS");
        if (index == null || index < 0) {
            index = positions.get("FORMAT");
        }
        if (index != null && index > 0) {
            builder.append(sql.substring(index));
        } else {
            ClickHouseFormat f = ClickHouseFormat.fromFileName(ClickHouseUtils.unescape(file));
            if (f == null) {
                f = (ClickHouseFormat) ClickHouseDefaults.FORMAT.getDefaultValue();
            }
            format = f.name();
            builder.append("FORMAT ").append(format);
        }
        return new ClickHouseSqlStatement(builder.toString(), stmtType, cluster, database, table, input,
                compressAlgorithm, compressLevel, format, file, parameters, null, settings, null, valueGroups, funcUsed);
    }

    private ClickHouseSqlStatement handleOutFileForSelectQuery(String sql, StatementType stmtType, String cluster,
            String database, String table, String input, String compressAlgorithm, String compressLevel, String format,
            String file, List<Integer> parameters, Map<String, Integer> positions, Map<String, String> settings,
            Set<String> tempTables, boolean funcUsed) {
        StringBuilder builder = new StringBuilder(sql.length());
        builder.append(sql.substring(0, positions.get("INTO")));
        Integer index = positions.get("FORMAT");
        if (index != null && index > 0) {
            builder.append(sql.substring(index));
        }
        return new ClickHouseSqlStatement(builder.toString(), stmtType, cluster, database, table, input,
                compressAlgorithm, compressLevel, format, file, parameters, null, settings, null, 0, funcUsed);
    }

    @Override
    public ClickHouseSqlStatement handleStatement(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String compressAlgorithm, String compressLevel, String format, String file,
            List<Integer> parameters, Map<String, Integer> positions, Map<String, String> settings,
            Set<String> tempTables, int valueGroups, boolean funcUsed) {
        boolean hasFile = allowLocalFile && !ClickHouseChecker.isNullOrEmpty(file) && file.charAt(0) == '\'';
        ClickHouseSqlStatement s = null;
        if (stmtType == StatementType.DELETE) {
            s = allowLightWeightDelete ? s
                    : handleDelete(sql, stmtType, cluster, database, table, input, compressAlgorithm, compressLevel,
                            format, file, parameters, positions, settings, tempTables, funcUsed);
        } else if (stmtType == StatementType.UPDATE) {
            s = allowLightWeightUpdate ? s
                    : handleUpdate(sql, stmtType, cluster, database, table, input, compressAlgorithm, compressLevel,
                            format, file, parameters, positions, settings, tempTables, funcUsed);
        } else if (stmtType == StatementType.INSERT && hasFile) {
            s = handleInFileForInsertQuery(sql, stmtType, cluster, database, table, input, compressAlgorithm,
                    compressLevel, format, file, parameters, positions, settings, tempTables, valueGroups, funcUsed);
        } else if (stmtType == StatementType.SELECT && hasFile) {
            s = handleOutFileForSelectQuery(sql, stmtType, cluster, database, table, input, compressAlgorithm,
                    compressLevel, format, file, parameters, positions, settings, tempTables, funcUsed);
        }
        return s;
    }

    private JdbcParseHandler(boolean allowLightWeightDelete, boolean allowLightWeightUpdate, boolean allowLocalFile) {
        this.allowLightWeightDelete = allowLightWeightDelete;
        this.allowLightWeightUpdate = allowLightWeightUpdate;
        this.allowLocalFile = allowLocalFile;
    }
}
