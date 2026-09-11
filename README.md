# parquet-nv

**Status: NOT IMPLEMENTED — interface only.**

Every public function below is published with its signature and its
effect row, and every body is `todo()`. Installing this package works;
calling it panics with `not implemented`.

## What this is

The Apache Parquet file format over arrow-nv, sans-IO: the footer and
the thrift-compact subset that encodes it, row groups, column chunks,
pages with every encoding the format has, definition and repetition
levels for nested data, statistics read sceptically, and page
compression through the published codecs.

- `pqtype` — physical types, the two annotation systems, and the
  flattening that turns a schema tree into column chunks;
- `pqthrift` — the Thrift compact subset the footer needs, and nothing
  else;
- `pqmeta` — the footer, and statistics as a producer's claim;
- `pqlevel` — definition and repetition levels, and what one integer
  distinguishes;
- `pqencode` — the seven encodings, and which three a writer should
  emit;
- `pqpage` — page headers, and what the two page versions differ in;
- `pqcomp` — three codecs run, three named unavailable, one refused;
- `pqread` — the request machine;
- `pqwrite` — pages, chunks, row groups and a footer into a cursor;
- `pqarrow` — the bridge, where levels become offsets;
- `pqfault` — why, and where: a row group, a column path, a page and a
  byte.

```
novo pkg add parquet-nv
novo pkg build
novo test
```

## The one example that will work

```novo ignore
use pqread

// Three columns of a two-hundred-column file, in four round trips.
fn scan<S: pqread.PqSource[e]>(src: S, len: Int)
    -> Result<[PqEvent], PqFault> [e]
    let r = pqread.with_projection(pqread.reader(len),
                                   ["price", "qty", "ts"])
    pqread.read_all(src, r)
```

## The load-bearing interface: `PqRequest`

```novo ignore
pub enum PqRequest
    PqWantTail(n: Int)
    PqWantRange(at: Int, len: Int, why: PqPurpose)
    PqWantRanges(ranges: [ArrowBuffer], why: PqPurpose)
    PqDone
```

**A Parquet file is not a stream.** Its index is at the end, its columns
are contiguous per row group, and the only reason anybody pays for its
complexity over a row format is that a query reading three columns of
two hundred reads three columns' worth of bytes. A feed-and-drain
reader — the shape flate-nv, csv-nv and arrow-nv's IPC reader all take —
would read the file from the front, which is to say all of it, which is
to say it would throw the format's entire value away.

So this is [`docs/publishing.md`](../../docs/publishing.md#how-a-core-package-takes-bytes-from-its-host)'s
**third** sans-IO shape: the core asks, the host performs. The whole
conversation for a projection over one row group:

```
step   -> PqWantTail(8)            the footer length and the magic
supply
step   -> PqWantRange(at, len)     the footer itself
supply
step   -> PqWantRanges([a, b, c])  one per projected column
supply_many
step   -> PqChunkReady x3
step   -> PqDone
```

Four round trips for a file of any size, and the third is parallel. The
reader performs nothing, so its budget is `[]`; the host performs
everything, so the effects are declared where they are spent; and a
caller reading over a network gets to batch, reorder and parallelise —
which a package that opened files on its behalf could never allow.

Two details the enum carries on purpose:

- **A request says why it is asking.** `PqPurpose` lets a host prefetch
  a whole row group when it sees column requests and cache a footer
  across queries. A bare `(offset, length)` would make every host guess.
- **A host can answer wrongly, and that is a fault.** `supply` with
  bytes from an offset the reader did not ask for is `PqRangeMismatch` —
  a protocol error in the *host*, named as such, because silently
  decoding them would produce values rather than an error.

`read_row_group` and `read_all` drive the machine in order over
`PqSource[e]`, for a caller with no opinion about how the reads happen.
They are a convenience, not a replacement: one round trip at a time is
exactly what a caller over a network should not do, and both exist so
the difference is visible.

## The trait that crosses the layer: `PqSource[e]`

```novo ignore
pub trait PqSource[e]
    fn read_at(self, at: Int, len: Int) -> Result<Bytes, IoError> [e]
```

One method, because one method is what this needs and because it is the
one every host can supply: a local file seeks and reads, an HTTP source
sends a `Range` header, an object store takes an offset and a length in
its own API. The standard library's `Seek[e]` and `Read[e]` are the
wrong shape for the middle one — there is no cursor in an object store
to move — so the trait is declared over the *operation*.

`impl PqSource[io, fs] for File`, `impl PqSource[net] for HttpRange`,
`impl PqSource[] for Bytes` in a test. That last one is what makes a
Parquet reader testable without a filesystem.

## Levels: what one integer distinguishes

A column chunk stores only the leaf values that actually exist — no
placeholders, no structure — plus two streams of small integers:

| | |
| --- | --- |
| **definition level** | how deep the value is *defined*. At the maximum, a value was stored. Below it, the number says *which ancestor* was null or empty. |
| **repetition level** | at which depth this value *continues* a repeated ancestor. Zero starts a new record. |

For `a.b.c` where `a` is optional, `b` is a repeated group and `c` is
optional — max definition 3, max repetition 1 — four different facts are
four definition levels:

```
0   a is null
1   a is present and b is empty
2   b has an element and c is null
3   c has a value
```

One integer distinguishes *"the list is null"*, *"the list is empty"*
and *"the list has one null element"*. That is the distinction every
flat representation loses, and it is why this format can round-trip
nested data at all.

**A record boundary is a repetition level of zero, and nothing else
marks one.** `record_starts` is therefore the load-bearing call of
`pqlevel`: a reader assembling rows counts zeros, and a page whose first
repetition level is not zero is a page whose boundaries cannot be
recovered — `PqFirstRepetitionNotZero`, refused rather than guessed at.

Two things that look like optimisations and are the format: a column
whose maximum level is **zero omits that stream entirely** (a reader
that expected a zero-length stream is four bytes into the values), and a
level's bit width is `ceil(log2(max + 1))`, so a three-deep nullable
column costs two bits per slot.

## Statistics are a producer's claim, not a fact

Every field in `PqStats` was written by whatever wrote the file, and a
reader that skipped a row group because its `max` said so is trusting a
number nothing verified. Three hazards, all of them real files:

- `min`/`max` used **signed** comparison for `BYTE_ARRAY` in older
  writers, so UTF-8 bounds were computed with bytes above `0x7F` sorted
  as negative. `min_value`/`max_value` replaced them with unsigned
  ordering and **both** are in files.
- `null_count` of zero can mean *"no nulls"* or *"not computed"*,
  because the field is optional and absent reads as zero in most thrift
  bindings — so `PqStats` carries presence flags rather than sentinels.
- A NaN makes float bounds meaningless and the format says a writer
  should omit them. Some do not.

So every predicate is named `may_contain`, never `contains`: a false
answer is a promise, a true answer is only *"the bounds do not rule it
out"*, and missing or untrustworthy statistics answer **true** — because
a push-down that answered false on missing statistics silently drops
rows. `stats_are_trustworthy` is the question to ask first, and
`created_by` is carried unparsed because several of these hazards are
producer-specific.

The writer's side of the same argument: `bytes_stats` truncates long
bounds so a footer does not become megabytes, and it **rounds** — a
truncated min down, a truncated max up — because a writer that truncated
without rounding produces bounds that exclude rows in its own file.

## Snappy is the default and it is not on the registry

The reference writers default to `SNAPPY`, and most Parquet files in the
world use it. There is no `snappy-nv`, and this package will not grow a
second copy of one. Two things follow, both in the surface:

1. `PqCodecUnavailable` is an **unavailable** fault, not a corrupt one,
   so a caller reports its own build rather than somebody's file. And
   `unreadable_codecs` answers from the **footer**, before a page is
   read, so the refusal is one message naming a codec.
2. `PqDecompressor` is a pair of **named functions** a caller may
   supply, so a program that has a Snappy implementation reads those
   files through this package without waiting for a row. Named functions
   rather than a closure, because a lambda that becomes a value is
   refused at the embedded tier and because named functions are what
   this grid uses.

**This lane's report asks for a `snappy-nv` row on the grid.** It is the
single largest gap between this package and a reader that opens an
arbitrary Parquet file. brotli-nv is already a row (P2, unpublished) and
gets the same treatment meanwhile.

`LZ4` and `LZ4_RAW` are not the same codec, and that is the other place
a reader silently goes wrong. `LZ4_RAW` is the LZ4 **block** format —
`lz4block.decompress` with the header's uncompressed size as the
capacity. The older `LZ4` id meant the frame format in some writers and
a Hadoop-framed variant in others; the format deprecated it for that
reason, and it is refused by name here, because guessing wrong produces
bytes rather than an error.

## Three names that mean two things each

Parquet has a small vocabulary problem, and all three are places a
hand-written reader is quietly wrong:

| name | on one page | on another |
| --- | --- | --- |
| `RLE` | a **level** stream: four-byte length prefix, then runs | a dictionary **index** stream: one byte of bit width, then runs, **no length prefix** |
| `PLAIN_DICTIONARY` | on a data page, the deprecated `RLE_DICTIONARY` | on a dictionary page, *"the values are PLAIN"* |
| `LZ4` | the frame format, in some writers | a Hadoop-framed variant, in others |

The first is four bytes of error on every dictionary-encoded page in the
world, so `rle_decode_levels` and `rle_decode_indices` are two functions
over one `rle_runs`. The second is `effective_encoding`. The third is
refused.

And booleans are one **bit** in `PLAIN` — a page of a thousand is 125
bytes, and a decoder that read a byte each reads eight times too far.
`fixed_width_bytes` answers `0` for `BOOLEAN` for that reason, with
`is_bit_packed_physical` beside it.

## A writer chooses, and every choice is named

| | |
| --- | --- |
| **v1 data pages** | every reader in existence reads them; v2 was specified in 2016 and is still refused by readers in production |
| **PLAIN, RLE_DICTIONARY, RLE for levels** | the set every reader has. The delta encodings are worth writing when the size difference is measured on real data, which is an implementation's decision |
| **ZSTD as the default codec** | because there is no Snappy. Every reader since 2018 has ZSTD; one from before that does not, and the README says so rather than the file finding out |
| **unsigned-ordering statistics only** | writing the deprecated signed pair as well is how the hazard above got made |

A page header carries its payload's **compressed** size and is written
**before** the payload, so the size must be known first. Two ways out:
a scratch buffer, or a reserved header backfilled. This package takes
the scratch buffer — `PqPageBuf`, sized by `page_scratch_bound` — and
never allocates it. Backfilling would mean writing behind a cursor's own
position, which `Cursor` does not do and which no caller streaming to a
socket could use.

**No dictionary is built here.** A caller supplies the dictionary and
the indices; deciding what to intern is a decision about the data, and a
format library that made it would be choosing a hash table for its
callers. `dictionary_hint` is the arithmetic that says whether it would
have paid.

## The Arrow bridge, where levels become offsets

Parquet is how a column is **stored** and Arrow is how it is **held**,
so this decodes into `ArrowArray` rather than inventing a third
in-memory column nobody else reads. That is what the plan's *"parquet-nv,
with arrow-nv"* means and why arrow-nv is the one structural dependency.

The mapping is not a bijection and every gap is a function rather than a
comment:

- **Parquet has no equivalent** for Arrow's unions, its `Null` type and
  its intervals. `parquet_type_of` refuses them by name.
- **Arrow has no equivalent** for `INT96`, a deprecated 96-bit
  timestamp. It reads as `Timestamp(Nano, "")`, and `int96_is_lossy`
  says when that loses range — before 1677 and after 2262.
- **The shapes disagree.** Parquet's nesting is repetition levels and
  Arrow's is offsets buffers. `levels_to_offsets` and
  `offsets_to_levels` are that translation, published separately from
  the column readers so they can be tested against the specification's
  own example.

A file written by an Arrow writer carries its original Arrow schema
base64-encoded under `ARROW:schema`, because Parquet cannot express a
dictionary's index type or an extension type. `has_arrow_schema` asks
and `arrow_schema_text` answers it **undecoded**: base64 is std.codec's
and the IPC schema message is `arrowipc.read_schema`'s, and doing either
here would be a third implementation of something that already has one.

An empty timestamp zone means the same thing in both formats — a wall
clock, not UTC — which is what makes `PqLogTimestamp(adjusted_to_utc,
unit)` map onto `ArrowTimestamp(unit, zone)` without a guess.

## The layer

`core` — no effects. The reader answers requests, the writer fills a
cursor, and nothing here opens a file, reads a clock or touches a
socket. The three compression dependencies are all `core`, so
`dep-layer` holds at one level.

**No `@tier(embedded)` claim, and none is intended.** A footer is a tree
of boxed structs with a string per column path and a row group is a
growable list. The audit's `core-embedded` row passes as *makes no
device claim*.

Five dependencies, and each one is a named call rather than a
convenience: arrow-nv for the values a decoded column becomes, flate-nv
for `GZIP`, zstd-nv for `ZSTD`, lz4-nv for `LZ4_RAW`, and — through
arrow-nv — calendar-nv for the temporal types. `PqCodecConfig` publishes
how each codec is configured, because every one of those is a decision a
reader can disagree with and none of them is visible from a decompressed
page.

## Where the names come from

Every public type and every enum variant is prefixed `Pq`. The type
names that would otherwise collide across the registry are the obvious
ones — `Schema`, `Column`, `Encoding`, `Statistics`, `Writer`, `Reader`,
`Request` — and struct identity is keyed by name across a whole program,
so all of them are prefixed rather than some.

`PqThriftCursor` is a struct rather than an integer offset because the
compact protocol's field ids are **deltas** from the previous field, so
reading a struct needs state that a free function could not carry.

## The reference implementation

Apache Parquet — the format specification, the thrift definition, and
`parquet-mr` / `parquet-cpp` for the API shape and for the behaviours
this README names as producer-specific. The constants asserted in
`tests/` (`PAR1`, the eight-byte tail, the decimal precision table, the
three-level `LIST` shape's fixed inner names, the level arithmetic) are
the specification's own.

The API subset is what a notebook and a query engine use: the footer
whole, every encoding read, the three a first writer emits, nested data
whole. Left out and named above: the encryption module, bloom filters,
the column index's own writer, `LZO`, and the `LZ4` codec id.

## Status

Interface only. `novo pkg build` is clean, `novo doc` renders, the API
tests under `tests/` are red against `todo()` bodies, and the three
shard rows — `effect-budget`, `dep-layer`, `no-discharge-in-core` — are
green. The first implementation is the `0.1.0` published over this.
