use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledStore};
use rand::{
    distributions::{Alphanumeric, Distribution, Uniform},
    thread_rng, Rng,
};
use slog;
use slog::Drain;
use slog_term;
use std::iter;

use sled::Config;
use tempfile::TempDir;

fn long_set_bench(c: &mut Criterion) {
    let mut write_group = c.benchmark_group("write");


    let samples = generate_random_string(100, 1000_0);
    write_group
        .bench_function("long-kvs", |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let decorator = slog_term::TermDecorator::new().build();
                    let drain = slog_term::FullFormat::new(decorator).build().fuse();
                    let drain = std::sync::Mutex::new(drain).fuse();
                    let logger = slog::Logger::root(drain, slog::o!());
                    KvStore::open(temp_dir.path(), logger).unwrap()
                },
                |store| {
                    for keypair in samples.iter() {
                        store
                            .set(keypair.0.to_string(), keypair.1.to_string())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        })
        .bench_function("long-sled", |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    SledStore::new(
                        Config::new()
                            .path(temp_dir)
                            .temporary(true)
                            .flush_every_ms(None)
                            .open()
                            .unwrap(),
                    )
                },
                |store| {
                    for keypair in samples.iter() {
                        store
                            .set(keypair.0.to_string(), keypair.1.to_string())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });

    write_group.finish();
}

fn short_set_bench(c: &mut Criterion) {
    let mut write_group = c.benchmark_group("write");

    let samples = generate_random_string(1000, 20);
    write_group
        .bench_function("kvs", |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let decorator = slog_term::TermDecorator::new().build();
                    let drain = slog_term::FullFormat::new(decorator).build().fuse();
                    let drain = std::sync::Mutex::new(drain).fuse();
                    let logger = slog::Logger::root(drain, slog::o!());
                    KvStore::open(temp_dir.path(), logger).unwrap()
                },
                |store| {
                    for keypair in samples.iter() {
                        store
                            .set(keypair.0.to_string(), keypair.1.to_string())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        })
        .bench_function("sled", |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    SledStore::new(
                        Config::new()
                            .path(temp_dir)
                            .temporary(true)
                            .flush_every_ms(None)
                            .open()
                            .unwrap(),
                    )
                },
                |store| {
                    for keypair in samples.iter() {
                        store
                            .set(keypair.0.to_string(), keypair.1.to_string())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });

    write_group.finish();
}
fn long_get_bench(c: &mut Criterion) {
    let mut read_group = c.benchmark_group("read");
    let samples = generate_random_string(100, 1000_0);

    read_group.bench_function("long-kvs", |b| {
        let temp_dir = TempDir::new().unwrap();
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        let store = KvStore::open(temp_dir.path(), logger).unwrap();
        for keypair in samples.iter() {
            store
                .set(keypair.0.to_string(), keypair.1.to_string())
                .unwrap();
        }
        let mut rng = thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..100);
            store.get(samples[idx].0.to_string()).unwrap();
        })
    });
    read_group.bench_function("long-sled", |b| {
        let temp_dir = TempDir::new().unwrap();
        let store = SledStore::new(
            Config::new()
                .path(temp_dir)
                .temporary(true)
                .flush_every_ms(None)
                .open()
                .unwrap(),
        );
        for keypair in samples.iter() {
            store
                .set(keypair.0.to_string(), keypair.1.to_string())
                .unwrap();
        }
        let mut rng = thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..100);
            store.get(samples[idx].0.to_string()).unwrap();
        })
    });
}

fn short_get_bench(c: &mut Criterion) {
    let mut read_group = c.benchmark_group("read");
    let samples = generate_random_string(1000, 20);

    read_group.bench_function("kvs", |b| {
        let temp_dir = TempDir::new().unwrap();
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        let store = KvStore::open(temp_dir.path(), logger).unwrap();
        for keypair in samples.iter() {
            store
                .set(keypair.0.to_string(), keypair.1.to_string())
                .unwrap();
        }
        let mut rng = thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..1000);
            store.get(samples[idx].0.to_string()).unwrap();
        })
    });
    read_group.bench_function("sled", |b| {
        let temp_dir = TempDir::new().unwrap();
        let store = SledStore::new(
            Config::new()
                .path(temp_dir)
                .temporary(true)
                .flush_every_ms(None)
                .open()
                .unwrap(),
        );
        for keypair in samples.iter() {
            store
                .set(keypair.0.to_string(), keypair.1.to_string())
                .unwrap();
        }
        let mut rng = thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..1000);
            store.get(samples[idx].0.to_string()).unwrap();
        })
    });
}

fn generate_random_string(n: usize, len: usize) -> Vec<(String, String)> {
    let mut rng = thread_rng();
    let len_range = Uniform::from(1..=len);
    let mut samples = vec![];
    for i in 0..n {
        let key_len = len_range.sample(&mut rng);
        let val_len = len_range.sample(&mut rng);
        let key: String = format!(
            "{}{}",
            i,
            iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(key_len)
                .collect::<String>()
        );
        let val: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(val_len)
            .collect();

        samples.push((key, val));
    }

    samples
}

criterion_group!(benches, long_set_bench, short_set_bench, short_get_bench, long_get_bench);
criterion_main!(benches);
