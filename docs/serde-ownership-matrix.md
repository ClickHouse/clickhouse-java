# Serialization ownership matrix

This matrix records which implementation owns each serialization path. It is a
maintenance map, not a promise that every ClickHouse type is supported by every
format. Public support remains documented in `docs/features.md`.

## Stack and format ownership

| Stack | Format | Read owner | Write owner | Type parsing / conversion owner |
|---|---|---|---|---|
| `clickhouse-data` (legacy v1) | RowBinary | `ClickHouseRowBinaryProcessor` and `BinaryDataProcessor` | `ClickHouseRowBinaryProcessor` and `BinaryDataProcessor` | `ClickHouseColumn`, `ClickHouseValue`, and value classes |
| `clickhouse-data` (legacy v1) | TabSeparated / text | `ClickHouseTabSeparatedProcessor` and `TextDataProcessor` | `ClickHouseTabSeparatedProcessor` and `TextDataProcessor` | `ClickHouseColumn`, `ClickHouseValue`, and value classes |
| `client-v2` | RowBinary variants | `RowBinaryFormatReader` / `RowBinaryWithNames*FormatReader` delegate values to `BinaryStreamReader` | `RowBinaryFormatWriter` delegates values to `SerializerUtils`; `RowBinaryFormatSerializer` owns primitive framing | `ClickHouseColumn` supplies metadata; `BinaryStreamReader` and `SerializerUtils` own runtime conversion |
| `client-v2` | Native | `NativeFormatReader` owns block/column layout and delegates scalar values to `BinaryStreamReader` | Not an insert format | `NativeFormatReader` for columnar framing; `BinaryStreamReader` for values |
| `client-v2` | JSONEachRow | `JSONEachRowFormatReader` and the selected `JsonParser` | Server-side HTTP text path; no matching client serializer | `ProcessParser` and `ValueConverters` |

Code that fixes a v2 result decoding problem belongs in `BinaryStreamReader`
(or `NativeFormatReader` when the problem is columnar framing), not in the
legacy `ClickHouseColumn` value path. Formatting a value for a request parameter
belongs in the relevant converter, not in HTTP transport code.

## Type / format / operation matrix

Legend: **owned** means there is a dedicated implementation path; **delegated**
means wrapper semantics are composed from child columns; **limited** means the
path is intentionally partial or version-dependent; **n/a** means the operation
is not provided by that stack/format.

| Type family | Legacy RowBinary read/write | v2 RowBinary read | v2 RowBinary write | v2 Native read | Deterministic coverage anchor |
|---|---|---|---|---|---|
| Fixed-width numeric, Bool, Decimal | owned | owned | owned | owned | `BinaryStreamUtilsTest`, `SerializerUtilsTest`, `BinaryStreamReaderTests` |
| String, FixedString, UUID, IP | owned | owned | owned | owned | binary/string and nested nullable cases in `SerializerUtilsTest` |
| Date, DateTime, Time64 | owned | owned | owned | owned | timezone cases in `BinaryStreamReaderTests` |
| Nullable | delegated to nested serializer | delegated; RowBinary marker is per value | delegated; marker emitted before nested value | delegated; null map is read separately | middle-of-schema cases in `SerializerUtilsTest` |
| Array, Map, Tuple, Nested | delegated recursively | delegated recursively | delegated recursively | format-specific container framing plus delegated values | nested nullable and trailing fixed-width guards in `SerializerUtilsTest` |
| Enum / LowCardinality | owned | owned | owned | owned, with Native dictionary framing | enum width/alignment cases in `BinaryStreamReaderTests` |
| SimpleAggregateFunction | delegated to underlying type | delegated | delegated | delegated | middle-of-schema tests in `SerializerUtilsTest` and `BinaryStreamReaderTests` |
| Variant | n/a in the legacy value stack | owned, including null discriminator | owned; runtime alternative selection | owned, version-dependent server metadata | null and Dynamic-wrapped Variant tests in `BinaryStreamReaderTests`; integration coverage in `DataTypeTests` |
| Dynamic / JSON | n/a in the legacy value stack | owned, server-version dependent | limited and server-version dependent | owned, including runtime type metadata | dynamic type-encoding alignment cases in `BinaryStreamReaderTests` |
| Point/Ring/Polygon families | owned for concrete geometry types | owned for concrete types | owned for concrete types | owned | `ClickHouseRowBinaryProcessorTest` and geometry integration tests |
| Geometry sum type | n/a | owned as Variant-like data | owned, server-version dependent | owned, server-version dependent | geometry integration tests in `DataTypeTests` |
| QBit | n/a | owned | owned | owned with bit-plane framing | checked-in Native golden bytes in `BinaryStreamReaderTests` |
| AggregateFunction state | limited to supported bitmap/state shapes | limited | not generally supported | limited | explicit skipped/negative cases; no general round-trip claim |

## Test rules and known gaps

- Put nullable or variable-width values in the middle of a schema and keep a
  fixed-width field after them; assert the complete row.
- Exercise malformed length prefixes without allocating from untrusted sizes.
- Prefer the live v2 `BinaryStreamReader` path for result decoding regressions.
- Keep golden bytes only when their server version and stripped framing are
  documented beside the fixture.
- General AggregateFunction states, all Variant alternative combinations, and
  all server-version-specific Geometry encodings remain incomplete. Add focused
  rows here when support or deterministic coverage is added.
