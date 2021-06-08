use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::{thread_pool::*, KvStore, KvsClient, KvsEngine, KvsServer, SledStore};
use rand::{
    distributions::{Alphanumeric, Distribution, Uniform},
    thread_rng, Rng,
};
use std::iter;

use sled::Config;
use tempfile::TempDir;

use crossbeam_channel;
use slog;
use slog::Drain;
use slog_term;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

const WORKLOAD_SIZE: usize = 1000;
const KEYLEN: usize = 10;
const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";
const THREAD_NUMS: [u32; 6] = [1, 2, 4, 8, 16, 32];


pub fn write_shared_queue_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_shared_queue_kvs");
    group.measurement_time(Duration::from_secs(20));

    let temp_dir = TempDir::new().unwrap();
    let logger = get_logger();
    let engine = KvStore::open(temp_dir.path(), logger.clone()).unwrap();

    let data_map = generate_random_string(WORKLOAD_SIZE, KEYLEN);
    for num in THREAD_NUMS.iter() {
        let pool = SharedQueueThreadPool::new(*num, logger.clone()).unwrap();
        let mut server = KvsServer::new(engine.clone(), logger.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, value) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                let mut cli = KvsClient::connect(DEFAULT_ADDRESS).unwrap();
                assert!(
                    cli.set(key.clone(), value.clone()).is_ok(),
                    "client set error"
                );
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn read_shared_queue_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_shared_queue_kvs");
    group.measurement_time(Duration::from_secs(20));

    let temp_dir = TempDir::new().unwrap();
    let logger = get_logger();
    let engine = KvStore::open(temp_dir.path(), logger.clone()).unwrap();

    let data_map = generate_random_string(WORKLOAD_SIZE, KEYLEN);

    for (key, value) in data_map.clone() {
        engine.set(key, value).unwrap();
    }

    for num in THREAD_NUMS.iter() {
        let pool = SharedQueueThreadPool::new(*num, logger.clone()).unwrap();
        let mut server = KvsServer::new(engine.clone(), logger.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, _) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }

                let mut cli = KvsClient::connect(DEFAULT_ADDRESS).unwrap();
                assert!(cli.get(key.clone()).is_ok(), "client get error");
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn write_rayon_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_kvs");
    group.measurement_time(Duration::from_secs(20));

    let temp_dir = TempDir::new().unwrap();
    let logger = get_logger();
    let engine = KvStore::open(temp_dir.path(), logger.clone()).unwrap();

    let data_map = generate_random_string(WORKLOAD_SIZE, KEYLEN);
    for num in THREAD_NUMS.iter() {
        let pool = RayonThreadPool::new(*num, logger.clone()).unwrap();
        let mut server = KvsServer::new(engine.clone(), logger.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, value) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                let mut cli = KvsClient::connect(DEFAULT_ADDRESS).unwrap();
                assert!(
                    cli.set(key.clone(), value.clone()).is_ok(),
                    "client set error"
                );
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn read_rayon_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_kvs");
    group.measurement_time(Duration::from_secs(20));

    let temp_dir = TempDir::new().unwrap();
    let logger = get_logger();
    let engine = KvStore::open(temp_dir.path(), logger.clone()).unwrap();

    let data_map = generate_random_string(WORKLOAD_SIZE, KEYLEN);

    for (key, value) in data_map.clone() {
        engine.set(key, value).unwrap();
    }

    for num in THREAD_NUMS.iter() {
        let pool = SharedQueueThreadPool::new(*num, logger.clone()).unwrap();
        let mut server = KvsServer::new(engine.clone(), logger.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, _) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                let mut cli = KvsClient::connect(DEFAULT_ADDRESS).unwrap();
                assert!(cli.get(key.clone()).is_ok(), "client get error");
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn write_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_sled");
    group.measurement_time(Duration::from_secs(20));

    let temp_dir = TempDir::new().unwrap();
    let logger = get_logger();
    let engine = SledStore::new(
        Config::new()
            .path(temp_dir)
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap(),
    );

    let data_map = generate_random_string(WORKLOAD_SIZE, KEYLEN);
    for num in THREAD_NUMS.iter() {
        let pool = RayonThreadPool::new(*num, logger.clone()).unwrap();
        let mut server = KvsServer::new(engine.clone(), logger.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, value) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                let mut cli = KvsClient::connect(DEFAULT_ADDRESS).unwrap();
                assert!(
                    cli.set(key.clone(), value.clone()).is_ok(),
                    "client set error"
                );
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn read_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_sled");
    group.measurement_time(Duration::from_secs(20));

    let temp_dir = TempDir::new().unwrap();
    let logger = get_logger();
    let engine = SledStore::new(
        Config::new()
            .path(temp_dir)
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap(),
    );

    let data_map = generate_random_string(WORKLOAD_SIZE, KEYLEN);

    for (key, value) in data_map.clone() {
        engine.set(key, value).unwrap();
    }

    for num in THREAD_NUMS.iter() {
        let pool = SharedQueueThreadPool::new(*num, logger.clone()).unwrap();
        let mut server = KvsServer::new(engine.clone(), logger.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, _) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                let mut cli = KvsClient::connect(DEFAULT_ADDRESS).unwrap();
                assert!(cli.get(key.clone()).is_ok(), "client get error");
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
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

fn get_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(drain, slog::o!());

    logger
}

criterion_group!(
    benches,
    write_shared_queue_kvs,
    read_shared_queue_kvs,
    write_rayon_kvs,
    read_rayon_kvs,
    write_rayon_sled,
    read_rayon_sled
);
criterion_main!(benches);
