use bplus_store::api::Db;
use criterion::{Criterion, criterion_group, criterion_main};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn bench_tempdir() -> TempDir {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bench_tmp");
    std::fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap()
}

// ---------------------------------------------------------------------------
// Concurrent writers (OCC retries + reclaim contention)
// ---------------------------------------------------------------------------

fn bench_concurrent_writers(c: &mut Criterion) {
    let per_thread: u64 = 200;

    for num_threads in [2, 4] {
        c.bench_function(
            &format!("contention: {num_threads} writers x {per_thread} puts"),
            |b| {
                b.iter(|| {
                    let dir = bench_tempdir();
                    let db = Arc::new(Db::open(dir.path()).expect("open db"));
                    let tree = Arc::new(
                        db.create_tree::<u64, String>("bench", 64)
                            .expect("create tree"),
                    );
                    let barrier = Arc::new(Barrier::new(num_threads));

                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let tree = Arc::clone(&tree);
                            let barrier = Arc::clone(&barrier);
                            thread::spawn(move || {
                                let base = t as u64 * per_thread;
                                barrier.wait();
                                for i in 0..per_thread {
                                    tree.put(&(base + i), &format!("val_{}", base + i)).unwrap();
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Concurrent readers + writer (mixed contention)
// ---------------------------------------------------------------------------

fn bench_mixed_read_write(c: &mut Criterion) {
    let write_count: u64 = 200;
    let read_count: u64 = 1_000;

    for num_readers in [2, 4] {
        c.bench_function(
            &format!("contention: 1 writer + {num_readers} readers"),
            |b| {
                b.iter(|| {
                    let dir = bench_tempdir();
                    let db = Arc::new(Db::open(dir.path()).expect("open db"));

                    // Pre-populate so readers have data
                    let tree = db.create_tree::<u64, String>("bench", 64).unwrap();
                    let mut txn = tree.txn();
                    for i in 0..read_count {
                        txn.insert(&i, &format!("val_{i}"));
                    }
                    txn.commit().unwrap();

                    let barrier = Arc::new(Barrier::new(num_readers + 1));

                    // Spawn readers
                    let reader_handles: Vec<_> = (0..num_readers)
                        .map(|_| {
                            let db = Arc::clone(&db);
                            let barrier = Arc::clone(&barrier);
                            thread::spawn(move || {
                                let tree = db.open_tree::<u64, String>("bench").unwrap();
                                barrier.wait();
                                for i in 0..read_count {
                                    let _ = tree.get(&i);
                                }
                            })
                        })
                        .collect();

                    // Writer on main-spawned thread
                    let writer_db = Arc::clone(&db);
                    let writer_barrier = Arc::clone(&barrier);
                    let writer = thread::spawn(move || {
                        let tree = writer_db.open_tree::<u64, String>("bench").unwrap();
                        writer_barrier.wait();
                        for i in read_count..read_count + write_count {
                            tree.put(&i, &format!("val_{i}")).unwrap();
                        }
                    });

                    writer.join().unwrap();
                    for h in reader_handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Large batch transaction (exercises clone removal path)
// ---------------------------------------------------------------------------

fn bench_large_batch_txn(c: &mut Criterion) {
    for batch_size in [1_000u64, 5_000] {
        c.bench_function(&format!("contention: batch txn {batch_size} ops"), |b| {
            b.iter(|| {
                let dir = bench_tempdir();
                let db = Db::open(dir.path()).expect("open db");
                let tree = db
                    .create_tree::<u64, String>("bench", 64)
                    .expect("create tree");

                let mut txn = tree.txn();
                for i in 0..batch_size {
                    txn.insert(&i, &format!("value_for_key_{i}"));
                }
                txn.commit().unwrap();
            });
        });
    }
}

criterion_group!(
    name = contention_benches;
    config = Criterion::default().sample_size(10);
    targets =
    bench_concurrent_writers,
    bench_mixed_read_write,
    bench_large_batch_txn,
);
criterion_main!(contention_benches);
