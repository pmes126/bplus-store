use bplus_store::api::Db;
use criterion::{Criterion, criterion_group, criterion_main};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

const N: u64 = 5_000;

fn bench_tempdir() -> TempDir {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bench_tmp");
    std::fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap()
}

fn populated_db() -> (TempDir, Db) {
    let dir = bench_tempdir();
    let db = Db::open(dir.path()).expect("open db");
    let tree = db
        .create_tree::<u64, String>("bench", 64)
        .expect("create tree");
    let mut txn = tree.txn();
    for i in 0..N {
        txn.insert(&i, &format!("val_{i}"));
    }
    txn.commit().unwrap();
    (dir, db)
}

// ---------------------------------------------------------------------------
// Single-threaded pin/unpin via get (baseline)
// ---------------------------------------------------------------------------

fn bench_single_thread_gets(c: &mut Criterion) {
    let (_dir, db) = populated_db();
    let tree = db.open_tree::<u64, String>("bench").unwrap();

    c.bench_function("epoch: single-thread 5k gets", |b| {
        b.iter(|| {
            for i in 0..N {
                tree.get(&i).unwrap();
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Concurrent readers (epoch pin/unpin contention)
// ---------------------------------------------------------------------------

fn bench_concurrent_readers(c: &mut Criterion) {
    for num_threads in [2, 4, 8] {
        let (_dir, db) = populated_db();
        let db = Arc::new(db);

        c.bench_function(
            &format!("epoch: concurrent gets ({num_threads} threads, {N} each)"),
            |b| {
                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(num_threads));
                    let handles: Vec<_> = (0..num_threads)
                        .map(|_| {
                            let db = Arc::clone(&db);
                            let barrier = Arc::clone(&barrier);
                            thread::spawn(move || {
                                let tree =
                                    db.open_tree::<u64, String>("bench").unwrap();
                                barrier.wait();
                                for i in 0..N {
                                    tree.get(&i).unwrap();
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

criterion_group!(
    name = epoch_benches;
    config = Criterion::default().sample_size(20);
    targets =
    bench_single_thread_gets,
    bench_concurrent_readers,
);
criterion_main!(epoch_benches);
