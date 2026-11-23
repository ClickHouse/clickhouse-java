# QBit encoding: RowBinary vs Native

`QBit(element_type, dimension)` is a fixed-length vector type (ClickHouse `25.10+`; the `Int8` element
type and the `stride` parameter need ClickHouse `26.7+`). The same
logical value is put on the wire two different ways depending on the output format, which is why
`client-v2` has a dedicated read path for each. This document describes both encodings and the
"problem" the Native path has to solve.

See also: [ClickHouse QBit docs](https://clickhouse.com/docs/sql-reference/data-types/qbit) and the
server serializer `SerializationQBit` (`transposeBits` / `restoreBits`).

## RowBinary — the simple case

Over `RowBinary` formats (`RowBinary`, `RowBinaryWithNamesAndTypes`, …) a `QBit(E, N)` value is
encoded **exactly like `Array(E)`**: a var-int length prefix (always `N`) followed by `N` element
values in order. `client-v2` reads it straight into a Java array of the element type — `float[]` for
`BFloat16`/`Float32`, `double[]` for `Float64` — in `BinaryStreamReader#readQBit`. There is no
transposition; the bytes are the elements. (The shared `Array`-like encoding is an implementation
detail of the wire format, not a type equivalence.)

## Native — the bit-plane transposed layout

Over the `Native` format the server does **not** send the elements contiguously. It stores a `QBit`
column as a nested `Tuple(FixedString(...))` with one field per **bit plane**, and serializes that
tuple **column-major**. Concretely, for a block of `nRows` rows of `QBit(E, N, S)` (the stride `S` is
`N` when the type has no third parameter):

- Let `element_size` be the element bit width: `8` (`Int8`), `16` (`BFloat16`), `32` (`Float32`),
  `64` (`Float64`).
- The `N` elements are split into `N / S` **stride groups** of `S` consecutive elements. A QBit
  without a stride has one group.
- Let `bytesPerPlane = ceil(S / 8)` and `totalBits = bytesPerPlane * 8`.
- Each group has `element_size` **bit planes**, most-significant plane first: plane `p` (0-based) of a
  group carries element bit `element_size - 1 - p` of every element in that group. The planes are
  group-major: the planes of group 0, then the planes of group 1, and so on.
- Each plane is a `FixedString(bytesPerPlane)` per row, and the tuple is column-major, so a plane
  occupies `nRows * bytesPerPlane` contiguous bytes; row `r`'s slice for a plane starts at
  `r * bytesPerPlane`.
- Within a plane row, the bit of element `j` of the group (0-based in the group) is stored MSB-first
  with a `j ^ 7` flip inside each byte: its bit index is `bitIndex = (totalBits - 1) - (j ^ 7)`, i.e.
  byte `bitIndex >> 3`, bit `bitIndex & 7`.

Containers use the usual Native layout around this column:

- `Nullable(QBit(...))`: a null map of `nRows` bytes, then the QBit column of all `nRows` rows. A null
  row also has a vector in the QBit column; its value is not specified, and the reader discards it.
- `Array(QBit(...))`: `nRows` cumulative `UInt64` offsets, then the QBit column of all elements of the
  block (`Array(Nullable(QBit(...)))`: the null map of all elements comes before it).

### Why the client can't reuse the Array path

Because the Native layout is bit-transposed and column-major, the QBit bytes are **not** the
`Array(E)` element bytes RowBinary sends. Feeding them through the per-row/columnar reader would
misread the column and desynchronize the rest of the block (every following column shifts). So the
Native reader must either fully reconstruct the vector or fail loudly.

### Decoding (the inverse transpose)

`BinaryStreamReader#readQBitNative` reverses the transpose, producing the **same** `byte[]`/`float[]`/
`double[]` `readQBit` produces over RowBinary (so a `QBit` round-trips identically through either
format):

1. Read the `element_size * (N / S)` planes, each `nRows * bytesPerPlane` bytes.
2. Precompute, per group element `j`, its byte offset `bitIndex >> 3` and bit mask
   `1 << (bitIndex & 7)` within a plane row (constant across groups, planes and rows).
3. For each row `r`, group `g` and plane `p`, for each group element `j`: if the masked bit is set,
   OR bit `element_size - 1 - p` into the accumulated bits of element `g * S + j`.
4. Reinterpret each element's bits as the value: the low 8 bits as a `byte` for `Int8`,
   `Double.longBitsToDouble` for `Float64`, `Float.intBitsToFloat` for `Float32`, and for `BFloat16`
   widen the 16 stored bits to a `float` by shifting them into the high half (`bits << 16`), matching
   `readBFloat16LE`.

This is the exact inverse of the server's `SerializationQBit::transposeBits`.

## What client-v2 decodes over Native

`client-v2` decodes these `QBit` columns from the Native format (`NativeFormatReader#isNativeDecodableQBit`
selects them):

- `QBit(E, N)` and strided `QBit(E, N, S)`, where `E` is `Int8`, `BFloat16`, `Float32` or `Float64`;
- such a `QBit` in `Nullable`;
- such a `QBit` (or `Nullable` `QBit`) as the element of a one-level `Array`. Each row has the same
  item class as the `RowBinary` read of the array.

Every other shape is **rejected up front** with a clear `ClientException` (rather than misread). Read
it through a `RowBinary` format instead:

- a `QBit` in `LowCardinality` (the server does not create this type);
- a `QBit` inside a `Tuple`, `Map`, `Variant`, multi-level `Array` or another container. The Native
  reader does not decode these containers column-major
  ([#3156](https://github.com/ClickHouse/clickhouse-java/issues/3156));
- a `QBit` with another element type.
