use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledStore};
use rand::{
    distributions::{Alphanumeric, Distribution, Uniform},
    thread_rng, Rng,
};
use std::iter;

use sled::Config;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut write_group = c.benchmark_group("write");

    let samples = generate_random_string(100);
    write_group
        .bench_function("kvs", |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    KvStore::open(temp_dir.path()).unwrap()
                },
                |mut store| {
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
                |mut store| {
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

fn get_bench(c: &mut Criterion) {
    let mut read_group = c.benchmark_group("read");
    let samples = generate_random_string(1000);

    read_group.bench_function("kvs", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();
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
        let mut store = SledStore::new(
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

fn generate_random_string(n: usize) -> Vec<(String, String)> {
    let mut rng = thread_rng();
    let len_range = Uniform::from(1..=1000_00);
    let mut samples = vec![];
    for _ in 0..n {
        let key_len = len_range.sample(&mut rng);
        let val_len = len_range.sample(&mut rng);
        let key: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(key_len)
            .collect();
        let val: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(val_len)
            .collect();

        samples.push((key, val));
    }

    samples
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
