# parquet-nv

Apache Parquet is a file format that stores a table one column at a
time, compressed, with an index of where everything is. It is what a
query that reads three columns of two hundred reads three columns'
worth of bytes for. This package reads and writes it in novo-lang,
against the
[Parquet format specification](https://parquet.apache.org/docs/file-format/)
and its Thrift definition, with `parquet-mr` and `parquet-cpp` as the
reference implementations. A decoded column becomes a value of
[arrow-nv](https://novo-lang.org/packages/arrow-nv).

**Status: NOT IMPLEMENTED — interface only.** Every function is declared
with its full signature, but every body is a `todo()` that panics when
called. The package is published so its design can be reviewed and
depended on before it is implemented. Version 0.1.0 will be the first
working release.

## What a Parquet file is made of

A file ends with a **footer** and eight more bytes: a four-byte footer
length and the four-byte magic `PAR1`. The magic is also the first four
bytes of the file. Everything in the footer is written in **Thrift
compact**, a binary encoding in which a struct is a sequence of fields
and each field's identifier is a difference from the one before it.

The footer holds the **schema** and, for each **row group**, one
**column chunk** per column. A row group is a horizontal slice of the
table. A column chunk is one column's values for one row group, stored
contiguously, which is what makes reading three columns cheap.

A column chunk is a sequence of **pages**. A page has a header and a
payload, and the payload is compressed on its own. A **dictionary page**
holds the distinct values of a chunk; the data pages then store indices
into it.

An **encoding** is how the values in a page are laid down: plainly, as
dictionary indices, as run lengths, or as differences from the value
before.

Parquet stores nested data with two streams of small integers beside the
values.

The **definition level** says how deep a value is defined. At the
maximum, a value was stored. Below it, the number says which ancestor
was null or empty. For a path `a.b.c` where `a` is optional, `b` is a
repeated group and `c` is optional, the maximum is 3 and four facts are
four levels.

| Level | Means |
| --- | --- |
| 0 | `a` is null |
| 1 | `a` is present and `b` is empty |
| 2 | `b` has an element and `c` is null |
| 3 | `c` has a value |

The **repetition level** says at which depth a value continues a
repeated ancestor. Zero starts a new record, and nothing else marks a
record boundary.

**Statistics** are a minimum, a maximum and a null count that whatever
wrote the file recorded. They let a reader skip a row group. They are a
claim, not a measurement. See rule 7.

This package performs no input and no output. It opens no file, holds
no socket and reads no clock.

## Install

```
novo pkg add parquet-nv
```

## Example

```novo
use std.list
use pqmeta
use pqlevel
use pqread

fn main() [io]
    // Every Parquet file ends with a four-byte footer length and the
    // four-byte magic. That is where a reader starts.
    println("${pqmeta.tail_bytes()} bytes of tail")

    // A column three levels deep needs two bits per definition level.
    println("${pqlevel.bit_width(3)} bits per level at depth 3")

    // A reader is a value. It asks for byte ranges and performs none of
    // the reads itself, so a host over a network can batch them.
    let r = pqread.with_projection(pqread.reader(1048576), ["price", "qty", "ts"])

    match pqread.step(r)
        Err(e) => println(e.message())
        Ok(s)  =>
            // The first request is always for the tail.
            match s.request
                PqWantTail(n)            => println("wants the last ${n} bytes")
                PqWantRange(at, len, _)  => println("wants ${len} bytes at ${at}")
                PqWantRanges(ranges, _)  => println("wants ${list.len(ranges)} ranges")
                PqDone                   => println("nothing more is needed")
```

Build and test with `novo pkg build` and `novo test`. Today `novo test`
fails on purpose: every test reaches a
`not implemented: parquet-nv.<module>.<fn>` panic. The tests are the
specification the implementation will have to satisfy.

## What the package contains

| Module | Contents |
| --- | --- |
| `pqtype` | The eight physical types, the two annotation systems, the schema tree, and the flattening that turns it into a list of leaf columns with their maximum levels. |
| `pqthrift` | The Thrift compact subset the footer needs, reading and writing, and nothing else. |
| `pqmeta` | The footer, the row groups, the column chunks, the codecs, the page index, and statistics with the questions worth asking of them. |
| `pqlevel` | Definition and repetition levels: their bit width, reading them out of a page in both page versions, the record boundaries, and writing them. |
| `pqencode` | The encodings: their codes and names, which fits which physical type, reading a value under each, and writing the three a first writer emits. |
| `pqpage` | Page headers in both versions, the dictionary header, the spans a payload occupies, the checksum, and the header writers. |
| `pqcomp` | Running the three compression codecs this build has, naming the ones it does not, refusing the abandoned one, and the hook for a codec the caller supplies. |
| `pqread` | The reader: a value that asks for byte ranges and is fed them, plus the projection, the row group and page selection, and three convenience drivers over a source the caller implements. |
| `pqwrite` | Pages, chunks, row groups and a footer written into a cursor the caller sized, with the statistics helpers and the size bounds. |
| `pqarrow` | The bridge: the type mapping both ways, levels to offsets and back, a chunk or a row group as an Arrow value, and the original Arrow schema a writer may have embedded. |
| `pqfault` | Every reason a read or a write refuses, with where it happened: a row group, a column path, a page and a byte. |

## How to choose an entry point

**The reader asks and the host performs.** `pqread.reader` makes a
reader from the file's length. `pqread.step` answers what it wants next
and any events it produced. `pqread.supply` hands it the bytes of one
range, and `pqread.supply_many` hands it several. A projection over one
row group is four round trips for a file of any size, and the third is
several ranges the host may fetch in parallel.

```text
step   -> PqWantTail(8)            the footer length and the magic
supply
step   -> PqWantRange(at, len)     the footer itself
supply
step   -> PqWantRanges([a, b, c])  one per projected column
supply_many
step   -> PqChunkReady x3
step   -> PqDone
```

A file cannot be read from the front, because its index is at the end
and its columns are contiguous per row group. Reading it forwards would
read all of it, which is the value of the format thrown away.

**`PqSource[e]` is the trait a host implements.** It has one method,
which reads a given length at a given offset. A local file seeks and
reads, an HTTP source sends a range header, an object store passes an
offset and a length to its own interface. The standard library's seek
and read traits are the wrong shape for the last of those, because an
object store has no cursor to move. A test implements it over a buffer
in memory and needs no filesystem.

**`pqread.read_footer_from`, `read_row_group` and `read_all` drive the
machine for you**, one round trip at a time, over a `PqSource[e]`. Take
them when you have no opinion about how the reads happen. Do not take
them over a network, where one round trip at a time is exactly what to
avoid.

**`pqread.ranges_for` and `pqread.coalesce` are for a host that wants to
plan.** The first says which ranges a projection needs and the second
merges ranges separated by less than a gap you choose.

**`pqarrow` is where a decoded column becomes a value you can use.**
Parquet is how a column is stored and Arrow is how it is held, so this
package decodes into Arrow rather than inventing a third in-memory
shape.

## The rules a user needs

1. **A request says why it is asking.** `PqPurpose` distinguishes a
   footer, a page index, a column chunk and a page, so a host can
   prefetch a whole row group when it sees column requests and cache a
   footer between queries.
2. **Supplying bytes from an offset the reader did not ask for is a
   fault in the host.** `PqRangeMismatch` names it, because decoding
   them would produce values rather than an error.
3. **A record boundary is a repetition level of zero, and nothing else
   marks one.** `pqlevel.record_starts` counts them. A page whose first
   repetition level is not zero has boundaries that cannot be recovered,
   and is refused as `PqFirstRepetitionNotZero`.
4. **A column whose maximum level is zero has no level stream at all.**
   Not a zero-length one. A reader that expected four bytes of length is
   four bytes into the values. `pqlevel.stream_is_present` answers which
   it is.
5. **A level's bit width is the number of bits needed to write the
   maximum**, so a three-deep nullable column costs two bits per slot.
   `pqlevel.bit_width` is that arithmetic.
6. **`RLE` means two different layouts.** As a level stream it begins
   with a four-byte length. As a dictionary index stream it begins with
   one byte of bit width and has no length prefix. They are
   `pqencode.rle_decode_levels` and `pqencode.rle_decode_indices`, over
   one run reader. Reading the first where the second was meant is four
   bytes of error on every dictionary-encoded page in the world.
7. **Statistics are a producer's claim.** Every predicate is named
   `may_contain` rather than `contains`: a false answer is a promise and
   a true answer only says the bounds do not rule it out. Missing or
   untrustworthy statistics answer true, because a filter that answered
   false on missing statistics would silently drop rows.
   `pqmeta.stats_are_trustworthy` is the question to ask first, and
   `created_by` is carried unparsed because several of the hazards
   belong to particular writers.

   | Hazard | What happens |
   | --- | --- |
   | Older writers compared byte arrays as signed | UTF-8 bounds sorted bytes above 0x7F as negative. The replacement fields use unsigned ordering, and both are in real files. |
   | A null count of zero | means either no nulls or not computed, because the field is optional. `PqStats` carries presence flags rather than sentinels. |
   | A NaN among floating-point values | makes the bounds meaningless. The format says to omit them. Some writers do not. |

8. **`PLAIN_DICTIONARY` means one thing on a data page and another on a
   dictionary page.** On a data page it is the deprecated spelling of
   the dictionary encoding. On a dictionary page it says the values
   themselves are plain. `pqencode.effective_encoding` takes which kind
   of page it is.
9. **A boolean is one bit under the plain encoding.** A page of a
   thousand is 125 bytes. A decoder that reads a byte each reads eight
   times too far. `pqtype.fixed_width_bytes` answers zero for a boolean,
   with `pqtype.is_bit_packed_physical` beside it.
10. **`LZ4` and `LZ4_RAW` are not the same codec.** `LZ4_RAW` is the LZ4
    block format, decompressed with the header's uncompressed size as
    the capacity. The older `LZ4` identifier meant the frame format in
    some writers and a Hadoop-framed variant in others, and the format
    deprecated it. It is refused by name here, because guessing produces
    bytes rather than an error. `LZO` is refused the same way.
11. **A codec this build cannot run is reported as unavailable, not as
    corruption.** `pqcomp.unreadable_codecs` answers from the footer,
    before any page is read, so the refusal is one message naming a
    codec rather than a failure deep in a column. A caller that has an
    implementation supplies it as a `PqDecompressor`, which is a pair of
    named functions.
12. **A page header carries the compressed size of its payload and is
    written before it.** The size therefore has to be known first, so
    `pqwrite` builds the payload into a scratch buffer the caller
    supplies. `pqwrite.page_scratch_bound` says how large to make it.
    Nothing here allocates it.
13. **No dictionary is built for you.** The caller supplies the
    dictionary and the indices. Deciding what to intern is a decision
    about the data. `pqwrite.dictionary_hint` is the arithmetic that
    says whether it would have paid.
14. **The writer emits version 1 data pages.** Every reader in existence
    reads them. Version 2 was specified in 2016 and is still refused by
    readers in production. Both versions are read.
15. **The writer's default codec is Zstandard**, because Snappy is not
    available here. Every reader since 2018 has Zstandard. One from
    before that does not.
16. **The writer emits only unsigned-ordering statistics.** Writing the
    deprecated signed pair as well is how the hazard in rule 7 was
    created.
17. **A truncated statistic is rounded outwards.** `pqwrite.bytes_stats`
    shortens a long bound so a footer does not grow to megabytes, and
    rounds a minimum down and a maximum up. Truncating without rounding
    produces bounds that exclude rows in the writer's own file.
18. **Parquet and Arrow do not map onto each other exactly.** Arrow's
    unions, its null type and its intervals have no Parquet equivalent
    and `pqarrow.parquet_type_of` refuses them by name. Parquet's
    deprecated 96-bit timestamp has no Arrow equivalent and is read as a
    nanosecond timestamp; `pqarrow.int96_is_lossy` says when that loses
    range, which is before 1677 and after 2262.
19. **An embedded Arrow schema is handed back undecoded.** A file
    written by an Arrow writer carries its original schema base64-encoded
    under the key `ARROW:schema`, because Parquet cannot express a
    dictionary's index type or an extension type.
    `pqarrow.arrow_schema_text` answers the text. Decoding the base64 is
    `std.codec`'s and reading the schema message is arrow-nv's.
20. **A fault says where.** `PqWhere` carries the row group, the column
    path, the page and the byte, and `pqfault.kind_of` says whether the
    file was incomplete, corrupt, or used something this build does not
    have.

## What is not included

- **Snappy.** It is the default of the reference writers and most
  Parquet files in the world use it. There is no Snappy package on the
  registry and this one will not carry a second copy. See rule 11 for
  the way through.
- **Brotli.** Same treatment, for the same reason.
- **`LZO` and the `LZ4` codec identifier.** See rule 10.
- **The encryption module.**
- **Bloom filters.**
- **A writer for the column index.** The reader reads one.
- **A dictionary builder.** See rule 13.
- **A microcontroller build.** A footer is a tree of boxed structs with a
  string per column path, and a row group is a growable list. Nothing here is
  claimed to build for a device with no heap allocator, and there is no
  `tests/embedded_probe.nv`.

## Related packages

- [arrow-nv](https://novo-lang.org/packages/arrow-nv) is the in-memory
  shape a decoded column becomes, and the writer's input. It also brings
  the calendar types Parquet's temporal annotations map onto.
- [flate-nv](https://novo-lang.org/packages/flate-nv),
  [zstd-nv](https://novo-lang.org/packages/zstd-nv) and
  [lz4-nv](https://novo-lang.org/packages/lz4-nv) run the three codecs
  this build has. `PqCodecConfig` publishes how each one is configured,
  because none of those decisions is visible from a decompressed page.
- [dataframe-nv](https://novo-lang.org/packages/dataframe-nv) is the
  table shape a notebook works in, and Parquet is how one is usually
  stored.
- [csv-nv](https://novo-lang.org/packages/csv-nv) is the row-oriented
  alternative: simpler, untyped, and read from the front.

## Tests

```bash
novo test tests/pqtype_tests.nv       #  9 tests: the schema, the levels and the type table
novo test tests/pqread_tests.nv       # 10 tests: the request machine and the refusals
```

The constants are the specification's own: the `PAR1` magic, the
eight-byte tail, the decimal precision table, the fixed inner names of a
three-level list, and the level arithmetic. `parquet-mr` and
`parquet-cpp` are the reference implementations for the interface and
for the producer-specific behaviours rule 7 describes.

The suite asserts that the request sequence for a projection is the four
steps above, that supplying the wrong range is refused as a fault in the
host, that a maximum level of zero means no stream rather than an empty
one, that a level's bit width follows the arithmetic in rule 5, that the
two `RLE` layouts are two functions, and that a boolean is one bit.

The tests compile today and fail at run, each on the
`not implemented: parquet-nv.<module>.<fn>` panic that is its body. That
is the expected state of an interface release. They turn green one at a
time as bodies land.

## Implementation status

| Item | Implemented |
| --- | --- |
| `pqtype.PqPhysical`, `.PqRepetition`, `.PqConverted`, `.PqLogical`, `.PqTimeUnit` | declared |
| `pqtype.PqSchemaElement`, `.PqColumn`, `.PqSchema` | declared |
| `pqthrift.PqThriftType`, `.PqThriftCursor`, `.PqThriftField`, `.PqThriftList`, `.PqThriftWriter` | declared |
| `pqmeta.PqCodec`, `.PqStats`, `.PqPageLocation`, `.PqColumnChunk`, `.PqRowGroup`, `.PqKeyValue`, `.PqFileMeta` | declared |
| `pqlevel.PqLevels`, `pqencode.PqEncoding`, `.PqRleRun` | declared |
| `pqpage.PqPageKind`, `.PqPageHeader`, `.PqDataPage`, `.PqDictionaryHeader` | declared |
| `pqcomp.PqCodecConfig`, `.PqDecompressor`, `pqwrite.PqWriter`, `.PqPageBuf` | declared |
| `pqread.PqPurpose`, `.PqRequest`, `.PqEvent`, `.PqStep`, `.PqReader`, `.PqSource` | declared |
| `pqfault.PqWhere`, `.PqFaultKind`, `.PqFault` | declared |
| `pqtype`'s schema constructors, flattening, leaves and level arithmetic | no |
| `pqtype`'s projection, naming, annotation agreement and checks | no |
| `pqthrift`'s field, integer, string, list and boolean readers | no |
| `pqthrift`'s writer, varint, zigzag, binary and list header | no |
| `pqmeta.magic`, `.tail_bytes`, `.footer_position`, `.read_footer` | no |
| `pqmeta`'s row group, chunk and metadata accessors | no |
| `pqmeta.stats_are_trustworthy`, `.may_contain`, `.may_overlap`, `.is_all_null`, `.bounds_of` | no |
| `pqmeta`'s page index readers, `.pages_that_may_contain`, and the checks | no |
| `pqlevel.bit_width`, `.stream_is_present`, `.levels_of_v1`, `.levels_of_v2` | no |
| `pqlevel`'s accessors, `.record_starts`, `.record_count`, `.absent_at_depth`, `.check_levels` | no |
| `pqlevel.flat_levels`, `.write_definitions`, `.write_repetitions`, `.level_bound` | no |
| `pqencode`'s codes, names, `.effective_encoding` and `.encoding_fits` | no |
| `pqencode`'s plain, run-length, delta and byte-stream-split readers | no |
| `pqencode`'s four plain writers, two run-length writers and `.rle_bound` | no |
| `pqpage`'s three header readers, the span helpers, the checksum and the checks | no |
| `pqpage`'s three header writers and `.header_bound` | no |
| `pqcomp`'s configuration, the decompressor hook, `.can_read`, `.unreadable_codecs` | no |
| `pqcomp.decompress`, `.decompress_into`, `.compress_into`, `.compress_bound`, `.should_store_raw` | no |
| `pqread.reader` and its six options | no |
| `pqread.step`, `.supply`, `.supply_many`, `.footer_of` | no |
| `pqread.pages_of`, `.dictionary_of`, `.read_page`, `.scratch_bound` | no |
| `pqread.ranges_for`, `.coalesce`, `.projected_bytes` | no |
| `pqread.read_row_group`, `.read_footer_from`, `.read_all` | no |
| `pqwrite.writer`, the two metadata options, and the two size bounds | no |
| `pqwrite`'s prologue, row group, five page writers, chunk close and `.finish` | no |
| `pqwrite.int_stats`, `.float_stats`, `.bytes_stats`, `.dictionary_hint`, `.page_buf`, `.built_span` | no |
| `pqarrow`'s type mapping in both directions, `.int96_is_lossy`, `.unmappable_columns` | no |
| `pqarrow.levels_to_offsets`, `.offsets_to_levels` | no |
| `pqarrow.chunk_to_array`, `.row_group_to_batch`, `.array_to_chunk`, `.batch_to_row_group`, `.batch_body_bound` | no |
| `pqarrow`'s four embedded-schema functions | no |
| `pqfault`'s variants, its four location constructors and its five questions | no |

## Licence

Apache-2.0. See `LICENSE`.

<!-- docs/writing-a-readme.md is the style guide for this page. -->
