package com.clickhouse.data;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * "Attached" temporary table.
 */
@Deprecated
public class ClickHouseExternalTable implements Serializable {
    public static class Builder {
        private String name;
        private ClickHouseDeferredValue<InputStream> content;
        private ClickHouseCompression compression;
        private int compressionLevel;
        private ClickHouseFormat format;
        private List<ClickHouseColumn> columns;
        private boolean asTempTable;

        protected Builder() {
            columns = new LinkedList<>();

            compressionLevel = ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder compression(ClickHouseCompression compression) {
            return compression(compression, ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL);
        }

        public Builder compression(ClickHouseCompression compression, int compressionLevel) {
            this.compression = compression;
            this.compressionLevel = compressionLevel;
            return this;
        }

        public Builder content(ClickHousePassThruStream stream) {
            if (stream == null || !stream.hasInput()) {
                throw new IllegalArgumentException("Non-null pass-thru stream with input is required");
            }

            this.compression = ClickHouseCompression.NONE;
            this.compressionLevel = ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL;
            this.content = ClickHouseDeferredValue.of(stream.getInputStream(), InputStream.class);
            if (stream.hasFormat()) {
                this.format = stream.getFormat();
            }
            return this;
        }

        public Builder content(InputStream content) {
            if (ClickHouseChecker.nonNull(content, "content") instanceof ClickHouseInputStream) {
                return content((ClickHouseInputStream) content);
            }

            this.content = ClickHouseDeferredValue.of(content, InputStream.class);
            return this;
        }

        public Builder content(ClickHouseInputStream input) {
            if (ClickHouseChecker.nonNull(input, ClickHouseInputStream.TYPE_NAME).hasUnderlyingStream()) {
                ClickHousePassThruStream stream = input.getUnderlyingStream();
                this.compression = ClickHouseCompression.NONE;
                this.compressionLevel = ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL;
                this.content = ClickHouseDeferredValue.of(stream.getInputStream(), InputStream.class);
            } else {
                this.content = ClickHouseDeferredValue.of(input, InputStream.class);
            }

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
            return new ClickHouseExternalTable(name, content, compression, compressionLevel, format, columns,
                    asTempTable);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private static final long serialVersionUID = -5395148151046691946L;

    public static final String TYPE_NAME = "ExternalTable";

    private final String name;
    private final ClickHouseDeferredValue<InputStream> content;
    private final ClickHouseCompression compression;
    private final int compressionLevel;
    private final ClickHouseFormat format;
    private final List<ClickHouseColumn> columns;
    private final boolean asTempTable;

    private final String structure;

    protected ClickHouseExternalTable(String name, ClickHouseDeferredValue<InputStream> content,
            ClickHouseCompression compression, int compressionLevel, ClickHouseFormat format,
            Collection<ClickHouseColumn> columns, boolean asTempTable) {
        this.name = name == null ? "" : name.trim();

        if (compression == null) {
            compression = ClickHouseCompression.fromFileName(this.name);
            this.compression = compression == null ? ClickHouseCompression.NONE : compression;
        } else {
            this.compression = compression;
        }
        this.compressionLevel = compressionLevel;
        this.format = format == null ? ClickHouseFormat.TabSeparated : format;

        if (content == null) {
            throw new IllegalArgumentException("Non-null content is required");
        }
        this.content = compression == ClickHouseCompression.NONE ? content
                // unfortunately ClickHouse does not support compressed external data
                : ClickHouseDeferredValue
                        .of(() -> ClickHouseInputStream.of(content.get(),
                                ClickHouseDataConfig.getDefaultReadBufferSize(), this.compression,
                                this.compressionLevel, null));

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

    public InputStream getContent() {
        return content.get();
    }

    public ClickHouseCompression getCompression() {
        return compression;
    }

    public int getCompressionLevel() {
        return compressionLevel;
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
