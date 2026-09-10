# QBit encoding: RowBinary vs Native

`QBit(element_type, dimension)` is a fixed-length vector type (ClickHouse `25.10+`). The same
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
column as a nested `Tuple(FixedString(ceil(N/8)))` with one field per **bit plane**, and serializes
that tuple **column-major**. Concretely, for a block of `nRows` rows:

- Let `element_size` be the element bit width: `16` (`BFloat16`), `32` (`Float32`), `64` (`Float64`).
- Let `bytesPerPlane = ceil(N / 8)` and `totalBits = bytesPerPlane * 8`.
- There are `element_size` **bit planes**, most-significant plane first: plane `p` (0-based) carries
  element bit `element_size - 1 - p` of every element.
- Each plane is a `FixedString(bytesPerPlane)` per row, and the tuple is column-major, so plane `p`
  occupies `nRows * bytesPerPlane` contiguous bytes; row `r`'s slice for a plane starts at
  `r * bytesPerPlane`.
- Within a plane row, element `j`'s bit is stored MSB-first with a `j ^ 7` flip inside each byte:
  its bit index is `bitIndex = (totalBits - 1) - (j ^ 7)`, i.e. byte `bitIndex >> 3`, bit
  `bitIndex & 7`.

### Why the client can't reuse the Array path

Because the Native layout is bit-transposed and column-major, the QBit bytes are **not** the
`Array(E)` element bytes RowBinary sends. Feeding them through the per-row/columnar reader would
misread the column and desynchronize the rest of the block (every following column shifts). So the
Native reader must either fully reconstruct the vector or fail loudly.

### Decoding (the inverse transpose)

`BinaryStreamReader#readQBitNative` reverses the transpose, producing the **same** `float[]`/`double[]`
`readQBit` produces over RowBinary (so a `QBit` round-trips identically through either format):

1. Read the `element_size` planes, each `nRows * bytesPerPlane` bytes.
2. Precompute, per element `j`, its byte offset `bitIndex >> 3` and bit mask `1 << (bitIndex & 7)`
   within a plane row (constant across planes and rows).
3. For each row `r` and plane `p`, for each element `j`: if the masked bit is set, OR bit
   `element_size - 1 - p` into element `j`'s accumulated bits.
4. Reinterpret each element's bits as the value: `Double.longBitsToDouble` for `Float64`,
   `Float.intBitsToFloat` for `Float32`, and for `BFloat16` widen the 16 stored bits to a `float` by
   shifting them into the high half (`bits << 16`), matching `readBFloat16LE`.

This is the exact inverse of the server's `SerializationQBit::transposeBits`.

## What client-v2 decodes over Native

`client-v2` decodes a **plain, top-level** `QBit(Float32|Float64|BFloat16, dimension)` column from the
Native format (`NativeFormatReader#isNativeDecodableQBit` selects it). Every other shape is **rejected
up front** with a clear `ClientException` (rather than misread) — read it through a `RowBinary` format
instead:

- **strided** `QBit(element_type, dimension, stride)` — its Native layout has
  `element_size * (dimension / stride)` planes, which this decoder does not reconstruct;
- `QBit` wrapped in **`Nullable`** / **`LowCardinality`**;
- `QBit` **nested** inside another type (e.g. `Array`/`Tuple`/`Map(String, QBit(...))`);
- a `QBit` with any **non-float element type**.
