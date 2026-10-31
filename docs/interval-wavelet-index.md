# Interval Wavelet Index Design

## Background

`tdb-succinct` stores `xdd:dateTimeInterval` values as a binary encoding in the typed dictionary (`TypedDict`). The encoding contains both canonical start/end timestamps and the original `Duration` metadata for round-tripping the input string form.

The current storage supports exact lookup, sorting, and prefix-factored dictionary compression. It does not support efficient interval-specific queries such as point stabbing, overlap, or Allen algebra relations.

## Encoding extension: timezone preservation (prerequisite)

XBRL OIM JSON requires the supplied zone designator to round-trip: `+hh:mm`/`-hh:mm` must come back as supplied, `Z` as `Z`, and a **zone-less** (local-time) endpoint must come back without a designator — local-time-ness is meaningful in OIM and must not be conflated with UTC. The engine previously normalised every endpoint to UTC and lost this distinction.

The encoding therefore grows from 48 to **52 bytes**:

- Bytes 0–47 unchanged: 24-byte sort prefix (`start_seconds`, `start_nanos`, `end_seconds`, `end_nanos`), then `start_type`, `end_type`, `flag`, and the 21-byte `Duration`.
- Bytes 48–51 (new): `start_tz: i16 BE` and `end_tz: i16` — the supplied UTC offset in minutes. The sentinel `INTERVAL_NO_ZONE = i16::MIN` marks a zone-less endpoint.
- Zone semantics: the stored instant remains UTC (comparison and sorting are unaffected — the sort prefix is untouched); the offset is purely lexical metadata. For `start/duration` and `duration/end` forms, the computed endpoint inherits the supplied endpoint's offset. Date-only components keep the UTC convention and carry offset `0`.

Compatibility: **there is no legacy 48-byte support.** `from_lexical` accepts exactly 52 bytes and fails hard with a recognisable message (`invalid DateTimeInterval encoding width`) on any other width. No rewriter is planned — interval storage predates production data; if a real store turns out to hold 48-byte entries, a rewriter that re-encodes the typed dictionary becomes necessary at that point.

## xdd:dateRange

`xdd:dateRange` is currently implemented only in Prolog. It is parsed in `src/core/util/xsd_parser.pl` as `dateRange(date(Y1,M1,D1,Offset1), date(Y2,M2,D2,Offset2))` and stored as a Prolog term `date_range(D1, D2)`. Casting logic is in `src/core/triple/casting.pl`. There is **no specialized indexing or wavelet tree support** for `xdd:dateRange` in the storage layer. If wavelet-tree indexing is extended to date ranges, it would need to be added separately or by reusing the same interval-indexing machinery.

## Design: dictionary-ordered start, dense-rank end tree

The typed dictionary is already a sorted index on interval start. The `DateTimeInterval` encoding leads with a 24-byte sorting prefix — `start_seconds`, `start_nanos`, `end_seconds`, `end_nanos` — so dictionary order is sorted by `(start, end)`, and `dict.id()` returns `IdLookupResult::Closest(position)` for non-matching keys: an insertion point, not only exact hits. A start-side query is therefore a binary search for a boundary position yielding a **contiguous id range**, which is cheaper than any wavelet tree:

- `start <= P` → probe `(start=P, end=MIN)`; ids `[seg_lo, insertion_point)` — every entry with `start == P` sorts after the probe because its real end exceeds `MIN`.
- `start > P` → probe `(start=P, end=MAX)`; ids `[insertion_point, seg_hi)` — the probe sorts after every entry with `start <= P`.
- `start == P` → the range between the two probes.

Only the **end** component needs an index: within the dictionary's `(start, end)` order, end is merely a tiebreaker and cannot be queried independently. For each layer, build one dense-rank wavelet tree keyed by the interval's end timestamp.

The tree uses the full 96-bit time resolution already present in the `DateTimeInterval` encoding: 64 bits for seconds and 32 bits for nanoseconds. The dense-rank approach maps the distinct end timestamps in a layer to a compact rank space `0..m-1`, so the wavelet tree width is `ceil(log2(m))` rather than 96. This is the space-efficient design.

The tree shares the same position order as the typed dictionary, so position `i` refers to the same interval as dictionary id `seg_lo + i`. Compound queries combine the two: the start probe yields a contiguous candidate id range, then a **position-restricted** threshold query on the end tree selects matching positions within it.

## Space

- Current `DateTimeInterval` storage: ~48 bytes per interval.
- One dense-rank end wavelet tree: roughly **2.5–5 bytes per interval** plus one small rank table.
- A full 96-bit alphabet would be ~12 bytes per interval and is not recommended.

## Initial triples functions in `tdb-succinct`

The first functions to expose are point-vs-interval queries. They are the primitives that most triples use and they map directly to wavelet-tree range queries.

| Function | Semantics | Implementation |
|---|---|---|
| `interval_value_at_or_after_start(Interval, Point)` | `Point >= Interval.start` | `start <= Point` — dict probe → contiguous range |
| `interval_value_at_or_before_end(Interval, Point)` | `Point <= Interval.end` | `end >= Point` — threshold query on the end tree |
| `interval_value_between(Interval, Point)` | `Interval.start <= Point <= Interval.end` | start probe range ∩ `end >= Point` restricted to that range |
| `interval_value_before(Interval, Point)` | `Point < Interval.start` | `start > Point` — dict probe → contiguous range |
| `interval_value_after(Interval, Point)` | `Point > Interval.end` | `end < Point` — threshold query on the end tree |
| `interval_contains(Interval, Point)` | `Interval.start <= Point < Interval.end` | Same as `between` with half-open semantics |
| `interval_intersects(Interval, QueryStart, QueryEnd)` | `[QueryStart, QueryEnd)` overlaps the interval | `start < QueryEnd` range ∩ `end >= QueryStart` on the end tree |

## Proposed Rust API

Low-level primitives — start-side queries return contiguous ranges from the dictionary, end-side queries stream from the wavelet tree:

```rust
fn interval_ids_start_le(layer, time) -> Range<u64>; // dict probe
fn interval_ids_start_ge(layer, time) -> Range<u64>; // dict probe
fn interval_ids_end_le(layer, time)   -> impl Iterator<Item = u64>;
fn interval_ids_end_ge(layer, time)   -> impl Iterator<Item = u64>;
```

Higher-level point-vs-interval iterators (implemented by intersecting the low-level streams in Rust):

```rust
fn interval_ids_at_or_after_start(layer, time) -> impl Iterator<Item = u64>;
fn interval_ids_at_or_before_end(layer, time)   -> impl Iterator<Item = u64>;
fn interval_ids_between(layer, time)            -> impl Iterator<Item = u64>;
fn interval_ids_before(layer, time)             -> impl Iterator<Item = u64>;
fn interval_ids_after(layer, time)              -> impl Iterator<Item = u64>;
fn interval_ids_containing(layer, time)         -> impl Iterator<Item = u64>;
fn interval_ids_intersecting(layer, start, end) -> impl Iterator<Item = u64>;
```

Implementing the intersection in Rust avoids materializing full candidate lists in Prolog.

## Mapping to Allen algebra

The point-vs-interval functions above are the primitives for Allen algebra. The remaining interval-vs-interval relations can be implemented in Prolog using the Rust iterators.

| Allen relation | Implementation using `tdb-succinct` primitives |
|---|---|
| `before(a, b)` | `b.start >= a.end` via `interval_ids_start_ge` dict probe |
| `meets(a, b)` | `b.start == a.end` via dict probe range |
| `overlaps(a, b)` | `b.start ∈ (a.start, a.end)` probe range ∩ `b.end > a.end` on the end tree |
| `starts(a, b)` | `a.start == b.start` probe range ∩ `a.end < b.end` on the end tree |
| `during(a, b)` | `b.start < a.start` ∩ `a.end < b.end` on the end tree |
| `finishes(a, b)` | `a.end == b.end` on the end tree ∩ `a.start > b.start` probe |
| `equals(a, b)` | `a.start == b.start` probe range ∩ `a.end == b.end` on the end tree |

## Layering

- **`tdb-succinct`** builds the dense-rank end wavelet tree per layer and exposes the dictionary-probe ranges and streaming iterators.
- **`terminusdb` Prolog** implements the full Allen algebra semantics by calling the Rust iterators and binding results into triples.

## Implementation synthesis

The way forward is clear because the underlying wavelet-tree infrastructure is already in place. The remaining work is the interval-specific query layer, the dense-rank adapter, and the layer-builder integration.

### Existing foundation

| Component | Location | What it provides |
|---|---|---|
| Wavelet tree core | `tdb-succinct/src/wavelettree.rs` | `WaveletTree`, `WaveletLookup`, `build_wavelet_tree_from_iter`, `build_wavelet_tree_from_stream`, `build_wavelet_tree_from_logarray`. |
| Rank/select index | `tdb-succinct/src/bitindex.rs` | `BitIndex` with `O(1)` rank/select. |
| ID map wavelet trees | `terminusdb-store/src/layer/id_map.rs` | Builds node-value and predicate idmap wavelet trees from merged dictionaries. |
| Predicate index wavelet tree | `terminusdb-store/src/layer/builder.rs` | Builds the predicate wavelet tree during `build_indexes`. |
| Layer filenames | `terminusdb-store/src/storage/consts.rs` | Registry of all layer files, including the existing wavelet-tree names. |

### Missing pieces

1. **Dictionary boundary probes** — Craft 48-byte search keys (`start=P, end=MIN` or `end=MAX`, zeroed metadata) and call `id_slice` on the `DateTimeInterval` type segment; `IdLookupResult::Closest` yields the boundary position. Wrap in helpers returning `Range<u64>` for `start <=`, `start >`, and `start ==` probes.
2. **Wavelet tree range/threshold queries** — Add operations that query a position subrange for values `<=`, `>=`, `==`, or within a value range. The current `WaveletTree` only supports whole-tree `lookup` and `decode`. Position-restricted queries are what let a start-probe range constrain the end-tree search.
3. **Dense-rank 96-bit adapter** — Collect distinct end timestamps, sort them, assign compact `u64` ranks, and persist the rank table.
4. **Interval index builder** — Extract `DateTimeInterval` entries from the typed dictionary, map their end timestamps to ranks, and build one wavelet tree per layer.
5. **Layer file registration** — Add new filenames to `consts.rs`, load them in `InternalLayer`, and include them in pack/import/export.
6. **Rust accessor API** — `Range<u64>` for start probes, streaming iterators for threshold and intersection queries.
7. **Prolog FFI and Allen algebra** — Thin predicates that expose the Rust primitives to Prolog.

### Where and when to build the index

The interval index should be built in the same layer-construction phase as the existing idmap and predicate wavelet trees:

- **New commits**: build the end interval wavelet tree when the typed dictionary is finalized.
- **Rollups**: rebuild the interval index from the merged layer data, just like the idmap and predicate trees are rebuilt today.

The implementation would live primarily in `terminusdb-store/src/layer/`, with the generic wavelet-tree extensions in `tdb-succinct`.

### Recommended filenames

Following the existing layer-file convention:

- `interval_end_wavelet_tree_bits.bitarray`
- `interval_end_wavelet_tree_bit_index_blocks.logarray`
- `interval_end_wavelet_tree_bit_index_sblocks.logarray`
- `interval_end_rank_table.bin`

### Data flow at layer build time

1. Scan the typed dictionary for `DateTimeInterval` entries (`type_segment` already isolates them).
2. Collect distinct end timestamps (each is a 96-bit value: 64-bit seconds + 32-bit nanoseconds).
3. Sort the list and assign dense ranks `0..m-1`.
4. Write the rank table to `interval_end_rank_table.bin`.
5. Build the end wavelet tree over the rank sequence.
6. Store the wavelet tree and its bitindex in the layer directory.

### Lazy Rust API

The high-level functions should return iterators that do not materialize full candidate sets:

- `interval_ids_start_le(layer, time)` — `Range<u64>` from a dict probe
- `interval_ids_start_ge(layer, time)` — `Range<u64>` from a dict probe
- `interval_ids_end_le(layer, time)` — stream from the end tree
- `interval_ids_end_ge(layer, time)` — stream from the end tree
- `interval_ids_between(layer, time)`
- `interval_ids_containing(layer, time)`
- `interval_ids_before(layer, time)`
- `interval_ids_after(layer, time)`
- `interval_ids_intersecting(layer, start, end)`

The intersection of two sorted streams is done in Rust, so Prolog only sees the result stream or a count.

### Why this is deferred for now

- The work is well-scoped but non-trivial: roughly **1.5–3 weeks** for a senior Rust developer.
- Existing layers would **not** gain the index automatically; only new commits and rollups would include it.
- The core `WaveletTree` in `tdb-succinct` needs range/threshold query extensions first.

When the time comes, the implementation can proceed in the order listed above, starting with the generic wavelet-tree extensions in `tdb-succinct`.

## Migration and cut-over

There is no store-format version to bump and no migration to run. `terminusdb-store` has no global format marker; "version" in the codebase is only the label/head counter. Layer compatibility is decided per file: `storage/consts.rs` classifies every layer file as required or optional, and optional files load with `map_all_if_exists`. The idmap wavelet trees already use this mechanism — the interval index should follow the same pattern.

### Making the new files optional

- Register the four new filenames in `Filenames`, `FILENAME_ENUM_MAP`, and `SHARED_OPTIONAL_FILES`. They are per-layer files over the shared typed dictionary, like the idmaps.
- Load them with `map_all_if_exists`, yielding `Option`, and add them to `BaseLayerFiles`/`ChildLayerFiles` (or the shared file struct) accordingly.
- Old layers then load unchanged — the index is simply absent. New layers also load on older binaries of the directory backend, which resolves files by name and ignores unknown ones.

### Coverage of existing layers

Layers are immutable, so existing layers never gain the index. Only new commits and rollups produce it. Interval queries therefore need defined behavior on unindexed layers — and on mixed stacks, where some layers carry the index and some do not:

- **Recommended: fall back to a typed-dictionary scan.** The same `DateTimeInterval` scan used at build time can answer the query without the wavelet trees — correct on any store, slower on old layers. Layer stacks combine both paths per layer.
- The alternative is requiring a rollup before interval predicates become available. Simpler, but makes feature availability depend on maintenance state and silently breaks correctness if the check is missed.

To index existing history without waiting for organic rollups, an operator can force a rollup (squash) of the branch. Export/import also rebuilds all layers, but is heavier.

### Fallback cost on unindexed layers

The scan is bounded by `TypedDict::type_segment`: only the contiguous `DateTimeInterval` segment of the value dictionary is read, as fixed-width records in ascending id order. Layers without interval values cost a single segment lookup. The cost is therefore O(k) per query per unindexed layer, where k is the number of interval values in the layer — versus O(log m) rank operations on the wavelet trees. That is negligible for point lookups on interval-poor data, but compounds for interval-vs-interval Allen relations issued per candidate, and for deep layer stacks where every unindexed layer pays it. Interval-heavy datasets — the ones that benefit most from the index — are also the ones where the fallback is slowest, so a forced rollup is worth offering as an explicit migration step rather than relying on organic rollups alone.

### Archive backend constraint

`LayerFileEnum` is part of the archive wire format. `ArchiveFilePresenceHeader` is a `u64` bitfield indexed by enum value, but `SmallBitArray` reserves the LSB — giving **63 usable slots** — and file offsets are addressed by rank over the present bits. Consequently:

- New variants must be **appended** to `LayerFileEnum`, never inserted; insertion shifts rank and corrupts offset resolution for existing archives.
- The enum currently occupies 50 of 63 slots. The four interval files leave nine — a finite resource worth tracking.
- An older binary uploading an indexed layer iterates only the file lists it knows and silently drops the interval files. The layer stays valid — the files are optional — and the index is rebuilt at the next rollup after re-upgrading.
- A pack produced by a new binary cannot be imported by an old **archive-backend** binary: pack import calls `get_file` per tar entry name, and `FILENAME_ENUM_MAP[name]` panics on unknown filenames. Downgrades therefore require re-exporting from a binary that predates the index.

### Forward-tolerant import first

The unknown-filename panic is a general sharp edge for any future layer-file addition, not just this feature, and terminusdb deployments use the archive backend — so it should be fixed ahead of the index. The fix is small: replace `FILENAME_ENUM_MAP[name]` indexing in `get_file`/`file_exists` with `.get(name)` returning `Option`, and have pack import skip unknown files with a warning instead of panicking. Skipping is safe because a file an old binary cannot name is necessarily optional — a new *required* file would break old binaries on layer open regardless. The archive backend cannot represent unknown files in `ArchiveFilePresenceHeader` either, so skipping is the only coherent behavior.

Suggested sequencing: ship tolerant import in **12.0.9**, ship the interval index in **12.1.0**. Downgrading 12.1.0 → 12.0.9 then survives pack imports — interval files are skipped, the index is lost, the layer remains valid. Downgrading to ≤12.0.8 still panics on such packs, which makes **12.0.9 the rollback floor**; the release notes should state this.

### Pack and copy paths

`pack.rs` exports exactly the files listed in the `*_OPTIONAL_FILES` constants. Registering the interval filenames in `SHARED_OPTIONAL_FILES` keeps them in packs; omitting them would silently strip the index on export and import.

## Notes

- TerminusDB layers are immutable, so the wavelet trees are built once at commit time and never updated.
- The dense-rank approach requires sorting timestamps and building a rank table per layer.
- The 96-bit resolution preserves nanosecond precision from the `DateTimeInterval` encoding.
