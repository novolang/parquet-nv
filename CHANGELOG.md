# Changelog

All notable changes to parquet-nv are recorded here. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
package follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with the pre-1.0 rule that a breaking change bumps the MINOR number.

## 0.0.1 — 2026-09-11

The **interface**: every signature and every effect row, and no bodies.
`stability = "draft"`, and the release is recorded `implemented = false`.

### Added

- `pqtype` — the six physical types, both annotation systems with a
  stated precedence, and the flattening that turns a schema tree into
  the column chunks a row group carries.
- `pqthrift` — the Thrift compact subset, with the field-id delta state
  in a value because a free function could not carry it.
- `pqmeta` — the footer, the page index, and statistics as a producer's
  claim with `stats_are_trustworthy` beside them.
- `pqlevel` — definition and repetition levels, `record_starts`, and
  the two streams that are absent rather than empty at level zero.
- `pqencode` — seven encodings read, three named writable, and the two
  framings `RLE` has.
- `pqpage` — the two page versions and what they differ in, with a v2
  page's own `is_compressed` flag.
- `pqcomp` — `GZIP`, `ZSTD` and `LZ4_RAW` run; `SNAPPY`, `BROTLI`,
  `LZO` and the deprecated `LZ4` named; `PqDecompressor` for a caller
  that has one.
- `pqread` — the request machine, `PqSource[e]`, and the range
  arithmetic a host can plan with before driving it.
- `pqwrite` — pages, chunks, row groups and a footer into a cursor the
  caller sized, with every writer choice named.
- `pqarrow` — the bridge, with `levels_to_offsets` published separately
  because it is where a nested conversion is right or wrong.
- `pqfault` — every refusal, naming a row group, a column path, a page
  and a byte, classified incomplete / corrupt / unavailable.

### Known

- **`PqRequest` is the load-bearing interface.** A Parquet file is not
  a stream; a feed-and-drain reader would read all of it and throw the
  format's value away. The core asks, the host performs.
- **A record boundary is a repetition level of zero** and nothing else
  marks one.
- **Statistics are a claim**, so every predicate is `may_contain` and
  missing statistics answer true.
- **Snappy is the format's default and has no package on the registry.**
  A `snappy-nv` row is this package's largest gap; `PqDecompressor` is
  the interim.
- **`RLE`, `PLAIN_DICTIONARY` and `LZ4` each name two things**, and all
  three are handled by name rather than by guess.
- **A writer emits v1 pages, three encodings and ZSTD**, each choice
  argued in the README.
- **No `@tier(embedded)` claim.** A footer is a tree of boxed structs.
- A sibling path dependency on arrow-nv while both are staged; it
  becomes `^0.0.1` before the first publish.
