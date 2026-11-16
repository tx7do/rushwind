//! The contract's regex engine's cost story, benchmarked: the
//! Thompson NFA simulation is linear in input length × pattern size —
//! the RE2-class guarantee the contract's `Regexp`/`Iregexp` carry.
//! Three input magnitudes (1 KiB / 64 KiB / 1 MiB) against the
//! pattern shapes the filters ride (a literal scan, a character
//! class, an alternation), plus the pathological `(a+)+$` shape that
//! backtracking engines die on — the benchmark's own wall time is the
//! linearity's demonstration.

use criterion::{criterion_group, criterion_main, Criterion};
use rushwind_storage::regex::{is_match, parse};

fn text_of(size: usize, poison: bool) -> String {
    // A lowercase alphabet run; `poison` appends a single non-matching
    // tail (the catastrophic-shape input's b).
    let base = "abcdefghij".repeat(size / 10);
    if poison {
        format!("{base}b")
    } else {
        base
    }
}

fn bench_linear_scan(c: &mut Criterion) {
    let ast = parse("abcdefghij").expect("parses");
    let mut group = c.benchmark_group("regex/linear/literal");
    for size in [1_024, 65_536, 1_048_576] {
        let text = text_of(size, false);
        group.throughput(criterion::Throughput::Bytes(size as u64));
        group.bench_with_input(size.to_string().as_str(), &text, |b, text| {
            b.iter(|| is_match(&ast, text, false))
        });
    }
    group.finish();
}

fn bench_class_scan(c: &mut Criterion) {
    let ast = parse("[a-z]+z").expect("parses");
    let mut group = c.benchmark_group("regex/linear/class");
    for size in [1_024, 65_536, 1_048_576] {
        let text = text_of(size, false);
        group.throughput(criterion::Throughput::Bytes(size as u64));
        group.bench_with_input(size.to_string().as_str(), &text, |b, text| {
            b.iter(|| is_match(&ast, text, false))
        });
    }
    group.finish();
}

fn bench_alternation(c: &mut Criterion) {
    let ast = parse("(alpha|bravo|charlie|delta|echo){2,4}").expect("parses");
    let mut group = c.benchmark_group("regex/linear/alternation");
    for size in [1_024, 65_536, 1_048_576] {
        let text = text_of(size, false);
        group.throughput(criterion::Throughput::Bytes(size as u64));
        group.bench_with_input(size.to_string().as_str(), &text, |b, text| {
            b.iter(|| is_match(&ast, text, false))
        });
    }
    group.finish();
}

fn bench_pathological_shape_stays_linear(c: &mut Criterion) {
    // (a+)+$ — the shape that sends backtracking engines exponential.
    // Under the Thompson simulation the wall time tracks the input
    // size like any other shape (the non-matching tail forces the
    // full scan).
    let ast = parse("(a+)+$").expect("parses");
    let mut group = c.benchmark_group("regex/linear/pathological");
    for size in [1_024, 65_536, 1_048_576] {
        let text = text_of(size, true);
        group.throughput(criterion::Throughput::Bytes(size as u64));
        group.bench_with_input(size.to_string().as_str(), &text, |b, text| {
            b.iter(|| is_match(&ast, text, false))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_linear_scan,
    bench_class_scan,
    bench_alternation,
    bench_pathological_shape_stays_linear
);
criterion_main!(benches);
