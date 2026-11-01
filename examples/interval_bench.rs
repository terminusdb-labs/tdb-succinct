use bytes::{Bytes, BytesMut};
use std::hint::black_box;
use std::time::Instant;
use tdb_succinct::tfc::decimal::integer_and_fraction_to_storage;
use tdb_succinct::tfc::interval::IntervalEndIndex;
use tdb_succinct::tfc::lexical_interval::{
    decode_lexical_entry, encode_lexical_interval, parse_lexical_interval, render_lexical_interval,
    Component, TZ_NEG, TZ_POS,
};
use tdb_succinct::*;

use rug::{Integer, Rational};

const VARIANTS: &[(&str, &str)] = &[
    ("forever ../..", "../.."),
    ("both_unknown /", "/"),
    (
        "datetime/datetime Z",
        "2025-01-31T10:30:00Z/2025-03-31T15:45:00Z",
    ),
    (
        "datetime/datetime localtime",
        "2025-01-31T10:30:00/2025-03-31T15:45:00",
    ),
    (
        "datetime/datetime tz-offset",
        "2025-01-31T10:30:00+02:00/2025-03-31T15:45:00-05:00",
    ),
    ("datetime/period", "2025-01-31T10:30:00Z/P3M"),
    ("period/datetime", "P3M/2025-03-31T15:45:00Z"),
    ("date/date", "2025-01-01/2025-03-31"),
    ("date/period", "2025-01-01/P3M"),
    ("period/date", "P3M/2025-03-31"),
];

fn time<F: FnMut() -> R, R>(n: usize, mut f: F) -> u128 {
    let start = Instant::now();
    for _ in 0..n {
        black_box(f());
    }
    start.elapsed().as_nanos()
}

// ---------------------------------------------------------------------------
// Rational ordering domain.
//
// An instant is `secs + frac/10^len` seconds from the Unix epoch, where secs is
// an arbitrary-precision integer (chrono's ~±262k-year calendar range does not
// bound the ordering domain) and frac is an arbitrary-precision decimal digit
// string (planck scale ~44 digits and beyond).
//
// Three representations are compared:
//   Rational — rug::Rational (GMP mpq), canonicalised at construction.
//   DecKey   — existing integer_and_fraction_to_storage bytes; memcmp order.
//   Lean     — today's (i64 secs, u32 nanos) tuple; nanos-capped, shown for the
//              "normal time is lean" fast path.
// ---------------------------------------------------------------------------

struct Parts {
    is_neg: bool,
    // magnitude of the seconds floor — for negatives this is |ceil(value)|
    // under the floor convention: -1.5 => is_neg, int "1", frac "5".
    int: &'static str,
    frac_a: &'static str,
    frac_b: &'static str,
}

const TIERS: &[(&str, Parts)] = &[
    (
        "nanos (lean-representable)",
        Parts {
            is_neg: false,
            int: "1738233000",
            frac_a: "123456789",
            frac_b: "123456790",
        },
    ),
    (
        "frac30",
        Parts {
            is_neg: false,
            int: "1738233000",
            frac_a: "123456789012345678901234567890",
            frac_b: "123456789012345678901234567891",
        },
    ),
    (
        "planck44",
        Parts {
            is_neg: false,
            int: "1738233000",
            frac_a: "00000000000000000000000000000000000000000539",
            frac_b: "00000000000000000000000000000000000000000540",
        },
    ),
    (
        "universe-age + frac44",
        Parts {
            is_neg: false,
            int: "435000000000000000",
            frac_a: "12345678901234567890123456789012345678901234",
            frac_b: "12345678901234567890123456789012345678901235",
        },
    ),
    (
        "deep-time 1e30 + frac44",
        Parts {
            is_neg: false,
            int: "1000000000000000000000000000000",
            frac_a: "12345678901234567890123456789012345678901234",
            frac_b: "12345678901234567890123456789012345678901235",
        },
    ),
    (
        "pre-epoch neg + frac30",
        Parts {
            is_neg: true,
            int: "322704000",
            frac_a: "123456789012345678901234567890",
            frac_b: "123456789012345678901234567891",
        },
    ),
];

fn denom(len: usize) -> Integer {
    // 10^len — "1" followed by len zeros.
    format!("1{}", "0".repeat(len)).parse::<Integer>().unwrap()
}

fn make_rational(p: &Parts, frac: &str) -> Rational {
    let int = p.int.parse::<Integer>().unwrap();
    let d = denom(frac.len());
    let f = if frac.is_empty() {
        Integer::from(0u32)
    } else {
        frac.parse::<Integer>().unwrap()
    };
    let num = int * &d + f;
    let signed = if p.is_neg { -num } else { num };
    Rational::from((signed, d))
}

fn make_deckey(p: &Parts, frac: &str) -> Vec<u8> {
    let int = p.int.parse::<Integer>().unwrap();
    // verbatim digits: `.5` and `.50` are distinct literals that order
    // adjacent — equal instants tie-break on spelling
    let frac = if frac.is_empty() { None } else { Some(frac) };
    integer_and_fraction_to_storage(p.is_neg, int, frac)
}

fn make_lean(int: &str, frac: &str) -> (i64, u32) {
    let secs = int.parse::<i64>().unwrap();
    let mut f = frac.to_string();
    f.truncate(9);
    while f.len() < 9 {
        f.push('0');
    }
    (secs, f.parse::<u32>().unwrap())
}

fn rational_order() {
    const N: usize = 100_000;

    println!("=== ordering domain: key materialisation, compare, landing ===");
    println!(
        "{:<26} {:<9} {:>10} {:>10} {:>11} {:>13} {:>12}",
        "tier", "repr", "make ns", "cmp ns", "cmp= ns", "land@100k ns", "sort100k ms"
    );

    for (tier, p) in TIERS {
        // -- Rational ------------------------------------------------------
        let ra = make_rational(p, p.frac_a);
        let rb = make_rational(p, p.frac_b);
        let ra2 = make_rational(p, p.frac_a);

        let make_ns = time(10_000, || make_rational(black_box(p), black_box(p.frac_a))) / 10_000;
        let cmp_ns = time(10_000, || black_box(&ra).cmp(black_box(&rb))) / 10_000;
        let cmpeq_ns = time(10_000, || black_box(&ra).cmp(black_box(&ra2))) / 10_000;

        let mut set: Vec<Rational> = (0..N)
            .map(|i| make_rational(p, &format!("{:0>w$}", i + 1, w = p.frac_a.len())))
            .collect();
        let sort_start = Instant::now();
        set.sort();
        let sort_ms = sort_start.elapsed().as_secs_f64() * 1000.0;
        let probe = make_rational(p, p.frac_a);
        let land_ns = time(10_000, || {
            black_box(set.binary_search(black_box(&probe)).unwrap_or_else(|p| p))
        }) / 10_000;

        println!(
            "{:<26} {:<9} {:>10} {:>10} {:>11} {:>13} {:>12.1}",
            tier, "rational", make_ns, cmp_ns, cmpeq_ns, land_ns, sort_ms
        );

        // -- Decimal storage key -------------------------------------------
        let da = make_deckey(p, p.frac_a);
        let db = make_deckey(p, p.frac_b);
        let da2 = make_deckey(p, p.frac_a);

        let make_ns = time(10_000, || make_deckey(black_box(p), black_box(p.frac_a))) / 10_000;
        let cmp_ns = time(10_000, || black_box(&da).cmp(black_box(&db))) / 10_000;
        let cmpeq_ns = time(10_000, || black_box(&da).cmp(black_box(&da2))) / 10_000;

        let mut set: Vec<Vec<u8>> = (0..N)
            .map(|i| make_deckey(p, &format!("{:0>w$}", i + 1, w = p.frac_a.len())))
            .collect();
        let sort_start = Instant::now();
        set.sort();
        let sort_ms = sort_start.elapsed().as_secs_f64() * 1000.0;
        let probe = make_deckey(p, p.frac_a);
        let land_ns = time(10_000, || {
            black_box(set.binary_search(black_box(&probe)).unwrap_or_else(|p| p))
        }) / 10_000;

        println!(
            "{:<26} {:<9} {:>10} {:>10} {:>11} {:>13} {:>12.1}",
            tier, "deckey", make_ns, cmp_ns, cmpeq_ns, land_ns, sort_ms
        );

        // -- Lean (i64, u32) baseline: only where representable -------------
        if p.int.len() <= 18 && p.frac_a.len() <= 9 {
            let la = make_lean(p.int, p.frac_a);
            let lb = make_lean(p.int, p.frac_b);
            let la2 = la;

            let make_ns =
                time(10_000, || make_lean(black_box(p.int), black_box(p.frac_a))) / 10_000;
            let cmp_ns = time(10_000, || black_box(&la).cmp(black_box(&lb))) / 10_000;
            let cmpeq_ns = time(10_000, || black_box(&la).cmp(black_box(&la2))) / 10_000;

            let mut set: Vec<(i64, u32)> = (0..N)
                .map(|i| make_lean(p.int, &format!("{:09}", i + 1)))
                .collect();
            let sort_start = Instant::now();
            set.sort();
            let sort_ms = sort_start.elapsed().as_secs_f64() * 1000.0;
            let probe = la;
            let land_ns = time(10_000, || {
                black_box(set.binary_search(black_box(&probe)).unwrap_or_else(|p| p))
            }) / 10_000;

            println!(
                "{:<26} {:<9} {:>10} {:>10} {:>11} {:>13} {:>12.1}",
                tier, "lean", make_ns, cmp_ns, cmpeq_ns, land_ns, sort_ms
            );
        }
    }
}

fn main() {
    for &n in &[10_000usize, 100_000] {
        println!("=== {} iterations (lexical format) ===", n);
        println!(
            "{:<28} {:>12} {:>16} {:>14} {:>9}",
            "variant", "parse ns/op", "parse+enc ns/op", "dec+rend ns/op", "entry B"
        );
        for (name, lexical) in VARIANTS {
            // warmup
            let iv = parse_lexical_interval(lexical).unwrap();
            let enc = encode_lexical_interval(&iv);
            black_box(render_lexical_interval(
                &decode_lexical_entry(&enc).unwrap(),
            ));

            let parse_ns =
                time(n, || parse_lexical_interval(black_box(lexical)).unwrap()) / n as u128;
            let parse_enc_ns = time(n, || {
                encode_lexical_interval(&parse_lexical_interval(black_box(lexical)).unwrap())
            }) / n as u128;
            let dec_rend_ns = time(n, || {
                render_lexical_interval(&decode_lexical_entry(black_box(&enc)).unwrap())
            }) / n as u128;

            println!(
                "{:<28} {:>12} {:>16} {:>14} {:>9}",
                name,
                parse_ns,
                parse_enc_ns,
                dec_rend_ns,
                enc.len()
            );
        }
        println!();
    }

    rational_order();

    entry_composition();

    lexical_100k();
}

// ---------------------------------------------------------------------------
// 100k-operations benchmark for the lexical interval format.
//
// Workload: same-timezone hourly windows — the common case the layout is
// tuned for. Measures encode/decode cost, raw + front-coded storage against
// the 52-byte legacy format and plain-text storage, dictionary probe lookup,
// and the wavelet end index vs a naive decode-and-scan.
// ---------------------------------------------------------------------------

/// `n` distinct common-case intervals: 30-minute windows in one timezone,
/// one per minute of the year from 2025-01-01.
fn common_intervals(n: usize) -> Vec<String> {
    const CUM: [u32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let doy_to_md = |doy: u32| -> (u32, u32) {
        let mut mi = 0usize;
        while mi + 1 < 12 && CUM[mi + 1] <= doy {
            mi += 1;
        }
        (mi as u32 + 1, doy - CUM[mi] + 1)
    };
    (0..n as u32)
        .map(|i| {
            let s = i; // minute of year
            let e = i + 30;
            let (sm, sd) = doy_to_md(s / 1440);
            let (em, ed) = doy_to_md(e / 1440);
            format!(
                "2025-{sm:02}-{sd:02}T{:02}:{:02}:00+01:00/2025-{em:02}-{ed:02}T{:02}:{:02}:00+01:00",
                s % 1440 / 60,
                s % 60,
                e % 1440 / 60,
                e % 60
            )
        })
        .collect()
}

/// Per-variant byte accounting: supplied text vs the entry's real fields —
/// component instants (class + decimal int/frac), the period string, and
/// kind/timeshift/flag meta. There is no payload: the spelling is
/// reconstructed from these fields on decode.
fn entry_composition() {
    // (instant bytes incl. class, meta bytes) of one component
    let comp = |c: &Component| -> (usize, usize) {
        match c {
            Component::Finite(f) => {
                let inst =
                    1 + integer_and_fraction_to_storage(f.is_neg, f.int.clone(), f.frac.as_deref())
                        .len();
                // kind (1) + tshift class (1) + offset i16 for ±HH:MM
                let meta = 1 + 1 + usize::from(f.tz_class == TZ_NEG || f.tz_class == TZ_POS) * 2;
                (inst, meta)
            }
            _ => (1, 0), // special classes contribute their class byte only
        }
    };
    println!("=== entry composition (bytes) ===");
    println!(
        "{:<28} {:>7} {:>9} {:>7} {:>9} {:>7}",
        "variant", "text", "instants", "period", "meta+flag", "total"
    );
    for (name, lexical) in VARIANTS {
        let iv = parse_lexical_interval(lexical).unwrap();
        let enc = encode_lexical_interval(&iv);
        let (si, sm) = comp(&iv.start);
        let (ei, em) = comp(&iv.end);
        let meta_flag = sm + em + 1;
        let period = enc.len() - si - ei - meta_flag;
        println!(
            "{:<28} {:>7} {:>9} {:>7} {:>9} {:>7}",
            name,
            lexical.len(),
            si + ei,
            period,
            meta_flag,
            enc.len()
        );
    }
    println!();
}

fn lexical_100k() {
    const N: usize = 100_000;
    let lexicals = common_intervals(N);
    let parsed: Vec<_> = lexicals
        .iter()
        .map(|s| parse_lexical_interval(s).unwrap())
        .collect();
    let entries: Vec<Vec<u8>> = parsed.iter().map(encode_lexical_interval).collect();

    // ---- op cost ------------------------------------------------------------
    let parse_enc_ns = time(10_000, || {
        encode_lexical_interval(&parse_lexical_interval(black_box(&lexicals[7])).unwrap())
    }) / 10_000;
    let dec_rend_ns = time(10_000, || {
        render_lexical_interval(&decode_lexical_entry(black_box(&entries[7])).unwrap())
    }) / 10_000;
    println!("=== lexical format: per-op cost (100k dataset) ===");
    println!("parse+encode {parse_enc_ns} ns/op   decode+render {dec_rend_ns} ns/op\n");

    // ---- storage ------------------------------------------------------------
    let raw: usize = entries.iter().map(|e| e.len()).sum();
    let text: usize = lexicals.iter().map(|s| s.len()).sum();
    let legacy = N * 52;
    println!("=== storage, {N} common-case intervals ===");
    println!(
        "lexical entries {:>9} B ({:.1} B/entry)\nplain text      {:>9} B ({:.1} B/entry)\n52-byte format  {:>9} B (52.0 B/entry)",
        raw,
        raw as f64 / N as f64,
        text,
        text as f64 / N as f64,
        legacy
    );

    // front-coded dictionary, 8-entry blocks — same accounting as
    // build_block_unchecked: head vbyte+entry, then per entry
    // vbyte(shared) + vbyte(suffix_len) + suffix
    let mut sorted = entries.clone();
    sorted.sort();
    sorted.dedup();
    let mut coded = 0usize;
    for chunk in sorted.chunks(8) {
        coded += 1; // control word
        coded += vbyte::encoding_len(chunk[0].len() as u64) + chunk[0].len();
        let mut last = &chunk[0];
        for cur in &chunk[1..] {
            let shared = util::find_common_prefix(last, cur);
            coded += vbyte::encoding_len(shared as u64);
            coded += vbyte::encoding_len((cur.len() - shared) as u64);
            coded += cur.len() - shared;
            last = cur;
        }
    }
    println!(
        "front-coded     {:>9} B ({:.1} B/entry)\n",
        coded,
        coded as f64 / N as f64
    );

    // ---- dictionary probes --------------------------------------------------
    let mut dict_entries: Vec<TypedDictEntry> = sorted
        .iter()
        .map(|e| {
            TypedDictEntry::new(
                Datatype::DateTimeInterval,
                SizedDictEntry::from(Bytes::copy_from_slice(e)),
            )
        })
        .collect();
    dict_entries.sort();
    let mut b = TypedDictBufBuilder::new(
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
    );
    b.add_all(dict_entries.into_iter());
    let (t, o, bo, d) = b.finalize();
    let dict = TypedDict::from_parts(t.freeze(), o.freeze(), bo.freeze(), d.freeze());

    let r = dict.lexical_ids_start_le(1740700000, 0);
    let probes = time(10_000, || {
        black_box(dict.lexical_ids_start_le(black_box(1740700000), 0))
    }) / 10_000;
    println!("=== dictionary probe ===");
    println!(
        "start_le range: special {:?} finite {:?}",
        r.special, r.finite
    );
    println!("probe: {probes} ns\n");

    // ---- end index vs naive scan ---------------------------------------------
    let t0 = Instant::now();
    let idx = IntervalEndIndex::build_lexical(&dict).unwrap();
    let build_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let pos = 0..idx.len() as u64;
    // broad query: ~83% of positions — worst case for the index (it
    // enumerates results one at a time) and best case for the scan.
    let end_q = time(100, || {
        black_box(
            idx.positions_end_le(black_box(pos.clone()), 1746057600, 0)
                .len(),
        )
    }) / 100;
    let scan_q = time(100, || {
        let mut hits = 0usize;
        for e in &sorted {
            let iv = decode_lexical_entry(black_box(e)).unwrap();
            if let Component::Finite(f) = &iv.end {
                // whole-second comparison suffices for timing the scan
                if f.int.to_i64().map_or(false, |s| s <= 1746057600) {
                    hits += 1;
                }
            }
        }
        black_box(hits)
    }) / 100;
    println!("=== end index vs naive scan ({N} entries) ===");
    println!("index build {build_ms:.1} ms");
    println!("broad  (le ~83%): index {end_q} ns   scan {scan_q} ns");

    // selective query: ends inside a single hour — the wavelet tree's case.
    let lo_s = 1739145600i64; // 2025-02-10T00:00:00Z, mid-data
    let hi_s = lo_s + 3600;
    let sel_q = time(1_000, || {
        black_box(
            idx.positions_end_range(black_box(pos.clone()), (lo_s, 0), (hi_s, 0))
                .len(),
        )
    }) / 1_000;
    let sel_scan = time(1_000, || {
        let mut hits = 0usize;
        for e in &sorted {
            let iv = decode_lexical_entry(black_box(e)).unwrap();
            if let Component::Finite(f) = &iv.end {
                if f.int.to_i64().map_or(false, |s| s >= lo_s && s <= hi_s) {
                    hits += 1;
                }
            }
        }
        black_box(hits)
    }) / 1_000;
    println!("select (range 1h): index {sel_q} ns   scan {sel_scan} ns");
}
