# Reader Writer API Spec

## Abstract

The client already has a reader and a writer, but the two are not the same shape, and both materialize nested values before the application can touch them. This document specifies a streaming API that is symmetrical for the types that can be both read and written. Some ClickHouse types remain read-only; those limits are listed below.

## Data flow

A value is encoded on one side and decoded on the other. That encoding depends on the ClickHouse type and the server version, not on the application's domain types. An `Int32` is always the same four little-endian bytes. `String`, `Array`, and `Dynamic` each have their own codec.

The application does not have to use the same Java type as the column. It may write a Java `long` into an `Int32`, which is a conversion (and an overflow check), not a different codec.

The format is the container around those codecs. It decides how rows are framed and how nulls and defaults are marked. `RowBinary` and `RowBinaryWithDefaults` are row-oriented: each row is column values in schema order, and each value uses the RowBinary codec. `RowBinaryWithNamesAndTypes` adds a header of names and then types. `Native` is different: values are grouped into column blocks. `Dynamic` adds a type tag in front of the codec payload.

Three layers, in order:

1. **Conversion.** Java value to the canonical form of the ClickHouse type (`long` to `Int32` with a range check, `LocalDate` to a date). This is the public read and write methods.
2. **Format framing.** Null markers and, for `RowBinaryWithDefaults`, the default marker. This is a property of the column (nullable, has a `DEFAULT`) and of the format. It is not part of the type codec. Nested `Nullable` elements use only the inner null byte. The default marker exists only on a top-level column.
3. **Codec.** Canonical bytes: little-endian numbers, varint lengths, dynamic type tags. The codec does not accept an `Object` and does not look at JDBC types.

Today the public API is a buffered, JDBC-style row (`setValue` / `commitRow`, `next` / `getInteger`). That API stays. Connectors that already have a cursor need to push or pull nested values without building the whole container first. That is the streaming API in this document.

## A scope is a sequence of values

A row, an array, a tuple, a map, a nested row, a variant arm, and a dynamic value are the same thing: an ordered sequence of values. The type of the next value comes from the schema (or, inside `Dynamic`, from the runtime type). The same verbs work at every level. Nesting is opening a child scope. Closing the child commits it and returns to the parent.

Types are not repeated on each call. The caller passes a `TableSchema`. For `RowBinaryWithNamesAndTypes` the reader takes the schema from the header. The current column or element type selects the codec, the null marker, and the default marker. That is how format knowledge moves through the layers. `Dynamic` is the exception: the schema only says `Dynamic`, so `beginDynamic` supplies the runtime type.

The API is positional. Columns are written and read in schema order. Named setters stay on the buffered writer. On write, `MATERIALIZED` and `ALIAS` columns are omitted, matching `RowBinaryFormatWriter`. On read, the schema is the columns present in the stream; nothing is omitted.

Closing a scope is explicit (`close()`, or try-with-resources). The next value does not implicitly end an open container. An open child when the parent is closed is an error. Implicit close makes a nested array ambiguous: the next call could be another element or the next column.

```java
void writeData(MyDTO dto, OutputStream out, TableSchema schema) throws IOException {
    RowBinaryStreamWriter writer = new RowBinaryStreamWriter(
            out, schema, ClickHouseFormat.RowBinaryWithDefaults);
    try (ValueWriter row = writer.beginRow()) {
        row.writeInt32(dto.getIntValue1());
        row.writeString(dto.getStrVal()); // null is SQL NULL when the column is Nullable

        try (ValueWriter items = row.beginArray()) { // size unknown: buffer encoded bytes
            for (String li : dto.getList()) {
                items.writeString(li);
            }
        }

        try (ValueWriter matrix = row.beginArray(dto.tableData().size())) { // size known: straight to the stream
            for (String[] line : dto.tableData()) {
                try (ValueWriter cells = matrix.beginArray(line.length)) {
                    for (String col : line) {
                        cells.writeString(col);
                    }
                }
            }
        }
        row.writeDate(dto.getStartDate());
    }
}
```

The reader has the same shape. It does not take a destination collection.

```java
void readData(MyDTO dto, InputStream in, TableSchema schema) throws IOException {
    RowBinaryStreamReader reader = new RowBinaryStreamReader(
            in, schema, ClickHouseFormat.RowBinary);
    if (!reader.hasNext()) {
        return;
    }
    try (ValueReader row = reader.beginRow()) {
        dto.setIntValue1(row.readInt32());
        dto.setStrVal(row.readString());
        List<String> list = new ArrayList<String>();
        try (ValueReader items = row.beginArray()) {
            while (items.hasNext()) {
                list.add(items.readString());
            }
        }
        dto.setList(list);
    }
}
```

`beginArray()` belongs outside the loop. Calling it inside the loop would open a new array for every element.

## Streaming and zero-copy

`Array`, `Map`, `Nested`, and `QBit` are length-prefixed: a varint count, then the elements. `Tuple` has no count; it is N fields in schema order. Because the count comes first, a container whose size is not known yet cannot be written straight to the caller stream.

Two open modes:

- `beginArray(int size)`, `beginMap(int pairs)`, `beginNested(int rows)` with `size >= 0`. The varint is written immediately. Element bytes go to the current stream. `close()` checks the count. This is the path that does not copy the payload.
- `beginArray()`, `beginMap()`, `beginNested()`. Element bytes go to a growable buffer. `close()` writes the varint and then the buffer. The encoded bytes are copied once. A nested unknown-size container is copied again into its parent buffer. Callers who know the size avoid that copy.

Zero-copy rules:

- There is no row `Object[]`. A scalar is encoded when its write method is called.
- `writeString(byte[])` and `writeString(ByteBuffer)` write a varint length and the existing bytes. There is no UTF-8 round-trip. Bytes that are not valid UTF-8 are preserved. A heap `ByteBuffer` is written from its backing array. A direct buffer is copied once into a temporary array.
- Those payload bytes are not copied again when every open ancestor was opened with a known size, or has no length prefix (`Tuple`, a variant arm, a dynamic value, a top-level column). If any ancestor is buffering, the bytes are copied into that buffer. The copy is required because the length sits in front of the bytes.
- `Tuple` never buffers on its own. Only a length-prefixed field inside it does.
- Readers do not allocate the collection. `beginArray()` reads the varint into a remaining count. `readInt32()` returns a primitive. `readStringValue()` returns a `StringValue` over the bytes just read (one `byte[]`, no `String` unless the caller asks). `readString()` decodes UTF-8 and is lossy for invalid UTF-8. `readInto(int[] dst, int off, int len)` fills a caller buffer for an `Array(Int32)`.
- `hasNext()` is the remaining count of the current array, nested rows, or map pairs. After a map key, the next read or write must be the value. `hasNext()` is true only at the start of a pair, so the loop is "key, then value".
- `size()` is the total count read from the varint, or the number of tuple fields or row columns. It is not the number of values still remaining.

`QBit` is a fixed-length array. `beginQBit()` and `writeFloat64Array(double[])` (and the matching primitive helpers) require the length to equal the dimension. An unknown size is rejected. On the wire a `QBit` is still a varint plus elements; that layout is a RowBinary detail, not a reason to treat `QBit` as `Array`.

Bulk helpers (`writeInt32Array(int[])`, and the same for `Int8`, `Int16`, `Int64`, `Float32`, `Float64`, `BFloat16`, and `Bool`) write a whole primitive array as the current value. They use the known-size path. The loop form stays for a cursor source. A nullable element type is written with a non-null marker per element, because a primitive array cannot hold null. A null element still uses the loop and `writeNull()`.

On any API or IO failure the stream is desynchronized. The writer or reader is not reusable. One writer or reader, one thread, one stream. The caller owns the stream and closes it.

`Date` and `Date32` are encoded in UTC, the same as the buffered writer. `DateTime` and `DateTime64` use the column time zone, or UTC when the column has none. A constructor `TimeZone` overrides that default.

## Writer

`com.clickhouse.client.api.data_formats.stream.RowBinaryStreamWriter`

```text
RowBinaryStreamWriter(OutputStream out, TableSchema schema, ClickHouseFormat format)
RowBinaryStreamWriter(OutputStream out, TableSchema schema, ClickHouseFormat format, TimeZone timeZone)
ValueWriter beginRow()
int getRowCount()
ClickHouseFormat getFormat()
```

Formats: `RowBinary` and `RowBinaryWithDefaults` only. There is no header. The `INSERT` statement carries names and types.

`ValueWriter` is implemented by every scope. `close()` commits the scope. A second `close()` does nothing. Production code is Java 8.

Scalars, named for the ClickHouse type. The argument is the canonical Java type. A second overload exists where a wider Java type needs a range check (`writeInt32(long)`). There is no `Object` overload.

- `writeInt8`, `writeInt16`, `writeInt32`, `writeInt64`, `writeInt128`, `writeInt256`
- `writeUInt8`, `writeUInt16`, `writeUInt32`, `writeUInt64`, `writeUInt128`, `writeUInt256`
- `writeFloat32`, `writeFloat64`, `writeBFloat16`, `writeBool`
- `writeString(String)`, `writeString(byte[])`, `writeString(ByteBuffer)`
- `writeFixedString(String)`, `writeFixedString(byte[])` — length is the column precision
- `writeDate(LocalDate)`, `writeDate32(LocalDate)`
- `writeDateTime` for `LocalDateTime`, `ZonedDateTime`, and `Instant`
- `writeDateTime64` for `LocalDateTime` and `ZonedDateTime`
- `writeUUID`, `writeIPv4`, `writeIPv6`, `writeDecimal`
- `writeEnum8` and `writeEnum16` for the numeric wire value or the enum name
- `writeBitmap` for `AggregateFunction(groupBitmap, ...)` only

`writeString(null)` (and the other reference overloads) writes SQL NULL when the current value is a nullable string. A null passed to the wrong type is a type error. `writeNull()` writes SQL NULL for whatever nullable value is current.

`writeDefault()` writes the `RowBinaryWithDefaults` default marker and no body. It is legal only for a top-level column whose default kind is `DEFAULT`. It is not legal inside an array, a tuple, or a map, and it is not legal for `RowBinary`.

Containers:

- `beginArray()`, `beginArray(int size)`
- `beginTuple()`
- `beginMap()`, `beginMap(int pairs)` — keys and values alternate; a map key is not nullable; an odd number of writes is an error
- `beginNested()`, `beginNested(int rows)` — unflattened `Nested(f1 T1, ..., fN TN)` only, wire-identical to `Array(Tuple(T1, ..., TN))`. Each row is a `beginTuple()`. Flattened Nested (`flatten_nested = 1`) is already separate `Array` columns in the schema.
- `beginVariant(int ordinal)` — one value of that alternative, which may itself be a container. Ordinal `0xFF` is `writeNull()` on a `Variant` or `Geometry` value.
- `beginDynamic(ClickHouseColumn runtimeType)` — writes the dynamic type tag, then one value of that type.
- `beginQBit()` — `beginArray(dimension)`.

`LowCardinality(T)` and `SimpleAggregateFunction(func, T)` are written as `T`. A nullable argument of a simple aggregate function still gets the nested null marker.

`row.close()` requires every non-omitted column to have been written. A missing column is an error. It is not silently NULL or DEFAULT. That differs from `RowBinaryFormatWriter`, which treats an unset cell as null. The buffered writer does not change.

A negative size is an error. Unknown size is the no-argument method, not `-1`.

## Reader

`com.clickhouse.client.api.data_formats.stream.RowBinaryStreamReader`

```text
RowBinaryStreamReader(InputStream in, TableSchema schema, ClickHouseFormat format)
RowBinaryStreamReader(InputStream in, TableSchema schema, ClickHouseFormat format, TimeZone timeZone)
boolean hasNext()
ValueReader beginRow()
TableSchema getSchema()
```

Formats:

- `RowBinary`: schema required, no header.
- `RowBinaryWithNames`: schema required. The name header is read and checked against the schema.
- `RowBinaryWithNamesAndTypes`: schema is read from the header. A schema passed by the caller is not used.

`Native` is out of scope. It is column blocks, and `QBit` uses a different bit-plane layout there. Streaming a Native row still means buffering the block.

`hasNext()` peeks one byte. It is false at end of stream. It throws if a row is still open. `beginRow()` throws `EOFException` when there is no row.

`ValueReader` mirrors the writer: `readInt32()`, `readString()`, `readStringValue()`, and the other scalar twins, plus `beginArray()`, `beginTuple()`, `beginMap()`, `beginNested()`, `beginVariant()`, `beginDynamic()`, `beginQBit()`. `beginVariant()` reads the discriminator. `variantOrdinal()` reports it. `beginDynamic()` reads the type tag. `runtimeType()` returns the concrete column. A `Dynamic` `Nothing` value has no body; the scope is already complete and `runtimeType()` is `Nothing`.

`isNull()` consumes a SQL NULL and returns true, or returns false and leaves the body to be read. On a non-nullable `Variant` or `Geometry` it recognizes discriminator `0xFF`. Primitive reads of SQL NULL throw `NullValueException` and consume the null, same as the buffered getters. `skip()` walks the current value without building a `List`, `Map`, or `ArrayValue`. `readInto` fills a caller `int[]` from an `Array(Int32)`.

`close()` requires every column, field, or element the caller is responsible for to have been consumed. Use `skip()` for the rest. `hasNext()` and `size()` behave as described above.

Strings inside containers are returned as `StringValue` from `readStringValue()`, including nested strings. The buffered reader still returns nested strings as `String`.

## Format framing

Top-level column, `RowBinary`:

- Nullable null: one byte `1`, no body.
- Nullable value: one byte `0`, then the body.
- Non-nullable value: body only.
- Non-nullable null: error, except `Dynamic` (encoded as `Nothing`) and `Variant` / `Geometry` (discriminator `0xFF`).

Top-level column, `RowBinaryWithDefaults`. Every column gets a default marker, including columns that have no default expression:

- `writeDefault()` on a `DEFAULT` column: one byte `1`, no body.
- Value: one byte `0` (not a default). If the column is nullable, another `0`, then the body.
- SQL NULL on a nullable column that is not `writeDefault()`: `0`, then `1`, no body.
- SQL NULL on a non-nullable column: error, with the same `Dynamic` and `Variant` exceptions. The "not a default" byte is still written.

Nested `Nullable` (an array element, a tuple field, a map value): one byte, `1` for null and `0` for a value, then the body. No default marker.

## Container encodings

- `Array`: varint count, then elements. A nullable element is a null byte plus the body.
- `Tuple`: fields in order, no count.
- `Map`: varint pair count, then key, value, key, value. Key order is the order of the write calls.
- `Nested` (unflattened): same bytes as `Array(Tuple(...))`.
- `QBit`: same bytes as an array whose length is the dimension.
- `Variant` and `Geometry`: one discriminator byte, then one value. `0xFF` is null. `Geometry` alternatives are `Point`, `Ring`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, and `MultiPoint`. `Point` is a tuple of two `Float64`. `Ring`, `LineString`, and `MultiPoint` are arrays of points. `Polygon` and `MultiLineString` are arrays of rings. `MultiPolygon` is an array of polygons. There is no separate geo API.
- `Dynamic`: type tag (the existing dynamic tag encoding), then one value.
- `JSON` (binary object, not the string form): varint path count, then each path as a string name followed by its value. A dynamic path prefixes the value with a dynamic type tag. A typed path does not. `Nothing` (`0x00`) is a JSON null. Nested objects written with `beginObject` do not have their own count; their leaves are paths of the enclosing document. A path whose value is a JSON document (an element of `Array(JSON)`) is a dynamic `JSON` type tag plus another path list.

## Symmetry limits

These types are not given a new structured write path:

- `AggregateFunction` other than `groupBitmap`. `writeBitmap` / `readBitmap` cover `groupBitmap` only.
- Structured `JSON` is a map of paths, written with `beginJson`. A dynamic path is a name plus a dynamic type tag and a value. A typed path declared on the column is the value only. A nested object opened with `beginObject` is stored as dotted paths (`address.city`) on the same document. `writeNull(path)` writes a JSON null (`Nothing`), which is not the same as leaving the path out. An array of objects is `beginArray(path, JSON column)` and each element is `beginJson`. `writeString` without a path is still the string form of a `JSON` column, used when the server is reading JSON as a string. The streaming reader still reads that string form. Structured documents are read back through the existing binary reader, which returns a map of paths.
- Inside `Dynamic`, payloads whose type tag the codec does not finish: `Tuple`, `Variant`, `Dynamic`, `JSON`, `Nested`, `QBit`, `AggregateFunction`, `SimpleAggregateFunction`, and `Geometry`. `beginDynamic` rejects them, including when they are nested inside an array or a map. Calling the existing tag writer for `Tuple` would emit a partial tag and desynchronize the stream.

`LowCardinality` is the inner type on the RowBinary value path. The low-cardinality flag is not a dictionary.

## Errors

- `IllegalStateException`: wrong verb for the current type, unclosed child, declared size mismatch, extra value, missing column, `writeDefault()` in the wrong place, use after a failure.
- `IllegalArgumentException`: overflow (`writeInt32(long)` and the other ranged overloads), null into a non-nullable value, negative size, `QBit` length, odd map entry, `writeDefault()` on a column without `DEFAULT`.
- `NullValueException`: primitive read of SQL NULL, or `beginVariant()` on discriminator `0xFF`.
- `ClientException`: unsupported `Dynamic` payload, malformed header.
- `IOException`: the stream failed.
- `EOFException`: `beginRow()` when `hasNext()` is false.

After any of these the reader or writer throws `IllegalStateException` on further use. Do not retry the same stream.

## What does not change

`ClickHouseBinaryFormatWriter` and `RowBinaryFormatWriter` stay the buffered, name-addressable API. POJO insert and JDBC keep using them. The streaming types are a separate experimental API, used inside `DataStreamWriter.onOutput`. A retry still calls `onRetry()` and then `onOutput` again; the caller writes the rows again.

The materialized container codecs (`serializeArrayData`, `serializeMapData`, `serializeTupleData`) stay the path that walks a finished `List`, array, or `Map`. The streaming writer does not call them.
