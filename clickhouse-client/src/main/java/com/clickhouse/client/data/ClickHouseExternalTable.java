package com.clickhouse.client.data;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseDeferredValue;
import com.clickhouse.client.ClickHouseFile;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseUtils;

/**
 * "Attached" temporary table.
 */
public class ClickHouseExternalTable {
    public static class Builder {
        private String name;
        private ClickHouseFile file;
        private ClickHouseDeferredValue<InputStream> content;
        private ClickHouseCompression compression;
        private ClickHouseFormat format;
        private List<ClickHouseColumn> columns;
        private boolean asTempTable;

        protected Builder() {
            columns = new LinkedList<>();
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder compression(ClickHouseCompression compression) {
            this.compression = compression;
            return this;
        }

        public Builder content(ClickHouseFile file) {
            this.file = ClickHouseChecker.nonNull(file, "file");
            this.compression = file.getCompressionAlgorithm();
            this.content = ClickHouseDeferredValue.of(file.asInputStream(), InputStream.class);
            if (file.hasFormat()) {
                this.format = file.getFormat();
            }
            return this;
        }

        public Builder content(InputStream content) {
            this.content = ClickHouseDeferredValue.of(ClickHouseChecker.nonNull(content, "content"), InputStream.class);
            return this;
        }

        /**
         * Sets future content.
         *
         * @param content non-null future content
         * @return this builder
         * @deprecated will be removed in v0.3.3, please use
         *             {@link #content(ClickHouseDeferredValue)} instead
         */
        @Deprecated
        public Builder content(CompletableFuture<InputStream> content) {
            this.content = ClickHouseDeferredValue.of(ClickHouseChecker.nonNull(content, "Content"));
            return this;
        }

        /**
         * Sets deferred content.
         *
         * @param content non-null deferred content
         * @return this builder
         */
        public Builder content(ClickHouseDeferredValue<InputStream> content) {
            this.content = ClickHouseChecker.nonNull(content, "Content");
            return this;
        }

        public Builder content(String file) {
            final String fileName = ClickHouseChecker.nonEmpty(file, "File");
            this.content = ClickHouseDeferredValue.of(() -> {
                try {
                    return ClickHouseUtils.getFileInputStream(fileName);
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            });
            return this;
        }

        public Builder format(String format) {
            if (!ClickHouseChecker.isNullOrBlank(format)) {
                this.format = ClickHouseFormat.valueOf(format);
            }
            return this;
        }

        public Builder format(ClickHouseFormat format) {
            this.format = format;
            return this;
        }

        public Builder addColumn(String name, String type) {
            this.columns.add(ClickHouseColumn.of(name, type));
            return this;
        }

        public Builder removeColumn(String name) {
            Iterator<ClickHouseColumn> iterator = columns.iterator();
            while (iterator.hasNext()) {
                ClickHouseColumn c = iterator.next();
                if (c.getColumnName().equals(name)) {
                    iterator.remove();
                }
            }

            return this;
        }

        public Builder removeColumn(ClickHouseColumn column) {
            this.columns.remove(column);
            return this;
        }

        public Builder columns(String columns) {
            return !ClickHouseChecker.isNullOrBlank(columns) ? columns(ClickHouseColumn.parse(columns)) : this;
        }

        public Builder columns(Collection<ClickHouseColumn> columns) {
            if (columns != null) {
                for (ClickHouseColumn c : columns) {
                    this.columns.add(c);
                }
            }
            return this;
        }

        public Builder asTempTable() {
            asTempTable = true;
            return this;
        }

        public Builder asExternalTable() {
            asTempTable = false;
            return this;
        }

        public ClickHouseExternalTable build() {
            return new ClickHouseExternalTable(name, file, content, compression, format, columns, asTempTable);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final String name;
    private final ClickHouseFile file;
    private final ClickHouseDeferredValue<InputStream> content;
    private final Optional<ClickHouseCompression> compression;
    private final ClickHouseFormat format;
    private final List<ClickHouseColumn> columns;
    private final boolean asTempTable;

    private final String structure;

    protected ClickHouseExternalTable(String name, ClickHouseFile file, ClickHouseDeferredValue<InputStream> content,
            ClickHouseCompression compression, ClickHouseFormat format, Collection<ClickHouseColumn> columns,
            boolean asTempTable) {
        this.name = name == null ? "" : name.trim();
        this.file = file != null ? file : ClickHouseFile.NULL;
        this.content = ClickHouseChecker.nonNull(content, "content");
        if (compression == null) {
            compression = ClickHouseCompression.fromFileName(this.name);
            this.compression = Optional.ofNullable(compression == ClickHouseCompression.NONE ? null : compression);
        } else {
            this.compression = Optional.of(compression);
        }
        this.format = format == null ? ClickHouseFormat.TabSeparated : format;

        int size = columns == null ? 0 : columns.size();
        if (size == 0) {
            this.columns = Collections.emptyList();
            this.structure = "";
        } else {
            StringBuilder builder = new StringBuilder();
            List<ClickHouseColumn> list = new ArrayList<>(size);
            for (ClickHouseColumn c : columns) {
                list.add(c);
                builder.append(c.getColumnName()).append(' ').append(c.getOriginalTypeName()).append(',');
            }
            this.columns = Collections.unmodifiableList(list);
            this.structure = builder.deleteCharAt(builder.length() - 1).toString();
        }

        this.asTempTable = asTempTable;
    }

    public boolean hasName() {
        return !name.isEmpty();
    }

    public String getName() {
        return name;
    }

    public ClickHouseFile getFile() {
        return file;
    }

    public InputStream getContent() {
        return content.get();
    }

    public Optional<ClickHouseCompression> getCompression() {
        return compression;
    }

    public ClickHouseFormat getFormat() {
        return format;
    }

    public List<ClickHouseColumn> getColumns() {
        return columns;
    }

    public boolean isTempTable() {
        return asTempTable;
    }

    public String getStructure() {
        return structure;
    }
}
