use criterion::{criterion_group, criterion_main, Criterion};
use crate::bplustree::BPlusTree;

fn benchmark_insert(c: &mut Criterion) {
    c.bench_function("insert 1 million keys", |b| {
        b.iter(|| {
            let mut tree = BPlusTree::<u64, String, _>::new(...).unwrap();
            for i in 0..1_000_000 {
                tree.insert(i, format!("val_{}", i)).unwrap();
            }
        });
    });
}

fn benchmark_random_inserts() {
    let mut keys: Vec<u64> = (0..1_000_000).collect();
    keys.shuffle(&mut rand::thread_rng());
    let mut tree = BPlusTree::<u64, String, _>::new(...).unwrap();

    for k in keys {
        tree.insert(k, format!("val_{}", k)).unwrap();
    }
}

criterion_group!(benches, bench_insert);
criterion_main!(benches);
