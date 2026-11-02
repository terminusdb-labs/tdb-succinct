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
- Canonicalisation caveat: the offset *value* round-trips, not the exact designator spelling — `+00:00` and `-00:00` both render as `Z` (the OIM canonical form), matching xsd:dateTime canonicalisation.

Compatibility: `from_lexical` accepts exactly 52 bytes and fails hard with a recognisable message (`invalid DateTimeInterval encoding width`) on any other width. Stored 48-byte entries are upgraded once at layer-open by `TypedDict::upgrade_legacy_intervals` — decoded as the legacy layout and re-encoded with zero zone offsets, matching how they always rendered. Anything else is still a hard error; no rewriter or persisted migration exists.

## Encoding extension: ISO 8601-2 open and unknown endpoints

ISO 8601-2:2019 interval endpoints are supported in the same 52-byte layout — no encoding growth. The endpoint type byte (`start_type`/`end_type`, previously `0 = date`, `1 = dateTime`) gains two kinds:

| Kind | Lexical | Type byte | Sentinel `seconds` | Canonical fields |
|---|---|---|---|---|
| `INTERVAL_COMPONENT_OPEN = 2` | `..` | 2 | start `i64::MIN` (`INTERVAL_NEG_INF`), end `i64::MAX` (`INTERVAL_POS_INF`) | nanos 0, tz `NO_ZONE`, duration `Duration::ZERO`, flag `EXPLICIT` |
| `INTERVAL_COMPONENT_UNKNOWN = 3` | (empty) | 3 | start `i64::MAX` (`INTERVAL_UNKNOWN_START`), end `i64::MIN` (`INTERVAL_UNKNOWN_END`) | same |

Sentinel placement is asymmetric on purpose. Open endpoints are real bounds — they sit at the extreme they represent, so `-inf` sorts first and `+inf` last and every instant-algebra query behaves correctly for free: `start_le` includes open starts, `end_ge` includes open ends, and `../..` satisfies point-containment at any instant. Unknown endpoints have NULL semantics — they never satisfy an instant comparison — so each is parked at the one edge that needs exactly **one cut**:

- Unknown **start** at `i64::MAX`: `start_le`/`lt`/`eq` are clean by ordering (top never satisfies `<=`); `start_ge`/`gt` cap their range at the first unknown-block id.
- Unknown **end** at `i64::MIN` (rank 0): `end_ge`/`gt`/`eq` are clean by ordering; `end_le`/`lt` raise the lower bound to the first rank above the sentinel.

The sentinels remain lexically addressable: `start_eq(INTERVAL_UNKNOWN_START, 0)` returns exactly the unknown-start ids, `end_eq(INTERVAL_UNKNOWN_END, 0)` the unknown-end set. Unknown endpoints are never semantically equal — not even to each other.

Invariants enforced in `from_lexical`/`to_lexical`: sentinel seconds require a non-finite type byte and vice versa (`finite interval endpoint on sentinel`/`open interval endpoint with non-sentinel`), unknown type values fail hard, duration forms (`start/duration`, `duration/end`, `start/end/duration`) carrying a non-finite endpoint are rejected by `parse_iso_interval`, and `/` (both unknown) is legal. Compatibility caveat: stores written with type bytes `2`/`3` render as garbage instants under pre-support readers — unknown *values* in known files, unlike the tolerated unknown *files* case.

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

- Current `DateTimeInterval` storage: ~52 bytes per interval.
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

### Implementation status

Implemented in `tdb-succinct` (branch `xbrl-json-interval-timezone`):

- **Dictionary boundary probes** — `TypedDict::interval_ids_start_{le,lt,gt,ge,eq}` in `src/tfc/interval.rs` return half-open global id `Range<u64>`s. They craft 52-byte search keys (`start=P` plus a minimum or maximum end/metadata tail) and resolve them through `id_slice` on the `DateTimeInterval` segment via `IdLookupResult::Closest`.
- **Position-restricted wavelet queries** — `WaveletTree::positions_in_range` plus `positions_le`/`positions_ge`/`positions_eq` in `src/wavelettree.rs` return sorted positions inside a position window whose values fall in a value range. Bound-search strategy is chosen by window width: narrow value windows probe each candidate value via `lookup` (selective queries stay sub-100µs at 100k scale); wide windows enumerate distinct values via a decode pass. Measured: a ~60-match 1-hour end-date window answers in ~64µs vs ~21.6ms for a decode-and-compare scan; a broad ~83%-selectivity threshold is slower through the index than scanning (result enumeration dominates), so callers should pick the path per expected selectivity.
- **Dense-rank end index** — `IntervalEndIndex` in `src/tfc/interval.rs`: a stride-2 `LogArray` of sorted distinct `(secs, nanos)` end keys plus a `WaveletTree` of per-position ranks. `end_lower_bound`/`end_upper_bound` binary-search the rank table; `positions_end_{le,lt,gt,ge,eq,range}` combine them. `build_interval_end_index` writes the four files from a `TypedDict`; `IntervalEndIndex::from_parts` loads them, deriving the tree width from the rank-table length.

Still missing:

1. **Layer file registration** — Add new filenames to `consts.rs`, load them in `InternalLayer`, and include them in pack/import/export.
2. **Upgrade-on-read for old layers** — `TypedDict::upgrade_legacy_intervals` converts legacy 48-byte entries to 52 bytes in memory (zone fields get offset 0: legacy data always rendered `Z`). Run it once when a layer opens, then build the end index in memory over the upgraded segment. This replaces a scan fallback: no width dispatch on the query path, and old layers get the same index behaviour as new ones.
3. **Rust accessor API** — Wire the probes and index into layer-level entry points returning ids, and intersect start ranges with position-restricted end results.
4. **Prolog FFI and Allen algebra** — Thin predicates that expose the Rust primitives to Prolog.

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
- `interval_end_rank_table.logarray` — stride-2 `(secs, nanos)` pairs, sorted distinct

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

### Upgrade-on-read cost for old layers

Layers written before this index exist in two respects: they may hold 48-byte interval entries, and they always lack the index files. Both are repaired once at layer-open rather than per query:

- `TypedDict::upgrade_legacy_intervals` scans the interval segment once (O(k) length checks); if any entry is 48 bytes it rebuilds the dictionary in memory with every entry re-encoded at 52 bytes. Legacy entries get zero zone offsets — they were stored UTC-normalised and always rendered `Z`, so the upgrade is lossless. A 52-byte dictionary costs only the scan and is cloned through.
- The end index is then built in memory from the upgraded segment — the same `build_interval_end_index` output that a new layer would read from files. `map_all_if_exists` simply finds nothing to load.

Old layers therefore pay one linear pass and one index build at open, then every query takes the indexed path — there is no residual runtime penalty and no per-entry width dispatch. Interval-heavy old layers see a real open-time cost once per process instead of degraded queries forever.

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
