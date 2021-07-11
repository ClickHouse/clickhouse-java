package ru.yandex.clickhouse.domain;

/**
 * Input / Output formats supported by ClickHouse
 * <p>
 * Note that the sole existence of a format in this enumeration does not mean
 * that its use is supported for any operation with this JDBC driver. When in
 * doubt, just omit any specific format and let the driver take care of it.
 * <p>
 *
 * @see <a href=
 *      "https://clickhouse.yandex/docs/en/interfaces/formats">ClickHouse
 *      Reference Documentation</a>
 *
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public enum ClickHouseFormat {
    Arrow(true, true, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#arrow
    ArrowStream(true, true, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#arrowstream
    Avro(true, true, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#avro
    AvroConfluent(true, false, true, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#avroconfluent
    CapnProto(true, false, true, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#capnproto
    CSV(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#csv
    CSVWithNames(true, true, false, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#csvwithnames
    CustomSeparated(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#customseparated
    JSON(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#json
    JSONAsString(true, false, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#jsonasstring
    JSONCompact(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompact
    JSONCompactEachRow(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompacteachrow
    JSONCompactEachRowWithNamesAndTypes(true, true, false, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompacteachrowwithnamesandtypes
    JSONCompactString(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompactstring
    JSONCompactStringEachRow(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompactstringeachrow
    JSONCompactStringEachRowWithNamesAndTypes(true, true, false, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompactstringeachrowwithnamesandtypes
    JSONEachRow(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoneachrow
    JSONEachRowWithProgress(false, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsoneachrowwithprogress
    JSONString(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#jsonstring
    JSONStringsEachRow(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsonstringseachrow
    JSONStringsEachRowWithProgress(false, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#jsonstringseachrowwithprogress
    LineAsString(true, false, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#lineasstring
    Native(true, true, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#native
    Null(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#null
    ORC(true, false, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#orc
    Parquet(true, true, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#parquet
    Pretty(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#pretty
    PrettyCompact(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#prettycompact
    PrettyCompactMonoBlock(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#prettycompactmonoblock
    PrettyNoEscapes(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#prettynoescapes
    PrettySpace(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#prettyspace
    Protobuf(true, true, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#protobuf
    ProtobufSingle(true, true, true, true, false), // https://clickhouse.tech/docs/en/interfaces/formats/#protobufsingle
    RawBLOB(true, true, true, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#rawblob
    Regexp(true, false, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#regexp
    RowBinary(true, true, true, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#rowbinary
    RowBinaryWithNamesAndTypes(true, true, true, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#rowbinarywithnamesandtypes
    TabSeparated(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#tabseparated
    TabSeparatedRaw(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#tabseparatedraw
    TabSeparatedWithNames(true, true, false, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#tabseparatedwithnames
    TabSeparatedWithNamesAndTypes(true, true, false, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#tabseparatedwithnamesandtypes
    TSV(true, true, false, false, true), // alias of TabSeparated
    TSVRaw(true, true, false, false, true), // alias of TabSeparatedRaw
    TSVWithNames(true, true, false, true, true), // alias of TabSeparatedWithNames
    TSVWithNamesAndTypes(true, true, false, true, true), // alias of TabSeparatedWithNamesAndTypes
    Template(true, true, false, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#template
    TemplateIgnoreSpaces(true, false, false, true, true), // https://clickhouse.tech/docs/en/interfaces/formats/#templateignorespaces
    TSKV(true, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#tskv
    Values(true, true, false, false, true), // https://clickhouse.tech/docs/en/interfaces/formats/#values
    Vertical(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#vertical
    VerticalRaw(false, true, false, false, false), // https://clickhouse.tech/docs/en/interfaces/formats/#verticalraw
    XML(false, true, false, false, false); // https://clickhouse.tech/docs/en/interfaces/formats/#xml

    private boolean input;
    private boolean output;
    private boolean binary;
    private boolean header;
    private boolean rowBased;

    ClickHouseFormat(boolean input, boolean output, boolean binary, boolean header, boolean rowBased) {
        this.input = input;
        this.output = output;
        this.binary = binary;
        this.header = output && header;
        this.rowBased = rowBased;
    }

    public boolean supportsInput() {
        return input;
    }

    public boolean supportsOutput() {
        return output;
    }

    public boolean isBinary() {
        return binary;
    }

    public boolean isText() {
        return !binary;
    }

    public boolean hasHeader() {
        return header;
    }

    /**
     * Check whether the format is row based(e.g. read/write by row), which is a
     * very useful hint on how to process the data.
     *
     * @return true if the format is row based; false otherwise(e.g. column,
     *         document, or structured-object etc.)
     */
    public boolean isRowBased() {
        return rowBased;
    }

    public static boolean containsFormat(String statement) {
        if (statement == null || statement.isEmpty()) {
            return false;
        }
        // TODO: Proper parsing of comments etc.
        String s = statement.replaceAll("[;\\s]", "");
        for (ClickHouseFormat f : values()) {
            if (s.endsWith(f.name())) {
                return true;
            }
        }
        return false;
    }
}
