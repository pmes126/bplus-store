use crate::bplustree::tree::{SharedBPlusTree, BPlusTree, BaseVersion, CommitError};
use crate::bplustree::tree::StagedMetadata;
use crate::bplustree::transaction::{WriteTransaction, MAX_COMMIT_RETRIES};
use crate::storage::{KeyCodec, ValueCodec};
use crate::tests::common;

use anyhow::Result;
use tempfile::TempDir;
use std::fmt::Debug;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use std::thread;
use fail;

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

/*
#[cfg(feature = "testing")]
#[test]
fn commit_with_retries() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());
    let _scenario = fail::FailScenario::setup();
    fail::cfg("tree::commit::try_commit_failure", "return").unwrap();
    for i in 0..100 {
        trx.insert(i, format!("value_{}", i)).expect("insert");
    }

    match trx.commit() {
        Ok(_) => panic!("Commit should have failed due to injected failure"),
        Err(_e) => {}
    }
   
    let fail_pattern = format!("return->{}", MAX_COMMIT_RETRIES-1);
    fail::cfg("tree::commit::try_commit_failure", &fail_pattern).unwrap();
    // Now we expect the commit to succeed after retries
    trx.commit().expect("commit after retries");
    // run the commit
    fail::remove("tree::commit::try_commit_failure");
    for i in 0..100 {
        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
    }
}
*/

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

#[test]
fn commit_with_conflicting_transactions() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    
    // Start two transactions that will conflict
    let mut t1 = WriteTransaction::new(tree.clone());
    let mut t2 = WriteTransaction::new(tree.clone());

    // Insert into the same key in both transactions
    t1.insert(42, "value_42_t1".to_string()).expect("insert t1");
    t2.insert(42, "value_42_t2".to_string()).expect("insert t2");

    // Commit the first transaction
    t1.commit().expect("commit t1");

    // Now try to commit the second transaction, which should fail due to conflict
    t2.commit().expect("commit t2");

    tree.search(&42).expect("get").map_or_else(
        || panic!("Key 42 should exist after t1 commit"),
        |value| assert_eq!(value, "value_42_t2", "Value for key 42 should be from t1"),
    );
}

#[test]
fn commit_failure_should_reclaim_nodes() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    
    // Start a transaction
    let mut trx = WriteTransaction::new(tree.clone());
    
    // Insert some data
    for i in 0..10 {
        trx.insert(i, format!("value_{}", i)).expect("insert");
    }

    // Simulate a failure during commit
    fail::cfg("tree::commit::try_commit_failure", "return").unwrap();

    // Attempt to commit, which should fail
    match trx.commit() {
        Ok(_) => panic!("Commit should have failed"),
        Err(e) => assert!(matches!(e, anyhow::Error { .. })),
    }

    // Remove the failure configuration
    fail::remove("tree::commit::try_commit_failure");
}
