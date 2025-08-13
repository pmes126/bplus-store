use crate::bplustree::tree::{SharedBPlusTree, BPlusTree, BaseVersion, CommitError};
use crate::bplustree::tree::StagedMetadata;
use crate::bplustree::transaction::WriteTransaction;
use crate::storage::{KeyCodec, ValueCodec};
use crate::tests::common;

use anyhow::Result;
use tempfile::TempDir;
use std::fmt::Debug;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use std::thread;

#[test]
fn commit_happy_path() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    for i in 0..100 {
        trx.insert(i,  format!("value_{}", i)).expect("insert");
    }

    trx.commit().expect("commit");

    let _root_id = tree.get_root_id();
    for i in 0..100 {
        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
    }
}

//#[test]
//fn commit_with_retries() {
//    let dir = TempDir::new().unwrap();
//    let order = 16;
//    let tree = common::make_tree(&dir, order).expect("create tree");
//    let mut trx = WriteTransaction::new(tree.clone());
//fail::enable().unwrap();
//fail::cfg("tree::commit::after_cas_before_flush", "return").unwrap();
//// run the commit
//fail::remove("tree::commit::after_cas_before_flush");
//fail::disable().unwrap();/
//    for i in 0..100 {
//        trx.insert(i, format!("value_{}", i).into()).expect("insert");
//    }
//
//    // Simulate a failure on the first commit attempt
//    tree.inject_commit_failure(true);
//
//    // Retry logic
//    let mut retries = 0;
//    while retries < MAX_COMMIT_RETRIES {
//        match trx.commit() {
//            Ok(_) => break,
//            Err(e) => {
//                if retries == MAX_COMMIT_RETRIES - 1 {
//                    panic!("Failed to commit after {} retries: {}", retries + 1, e);
//                }
//                retries += 1;
//            }
//        }
//    }
//
//    for i in 0..100 {
//        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i).into()));
//    }
//}

#[test]
fn commit_with_random_inserts() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    let mut rng = thread_rng();
    let mut keys: Vec<u64> = (0..100).collect();
    keys.shuffle(&mut rng);

    for &key in &keys {
        trx.insert(key, format!("value_{}", key)).expect("insert");
    }

    trx.commit().expect("commit");

    for &key in &keys {
        assert_eq!(tree.search(&key).expect("get"), Some(format!("value_{}", key)));
    }
}

#[test]
fn contending_parallel_transactions() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    thread::scope(|s| {
        for i in 0..10 {
            let t = tree.clone();
            s.spawn(move || {
                let mut trx = WriteTransaction::new(t);
                for j in 0..100 {
                    let sleep_duration = rand::thread_rng().gen_range(1..10);
                    std::thread::sleep(std::time::Duration::from_millis(sleep_duration));
                    trx.insert(i * 100 + j, format!("value_{}", i * 100 + j)).expect("insert");
                }
                trx.commit().expect("commit");
            });
        }
    });
    for i in 0..1000 {
        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
    }
}
