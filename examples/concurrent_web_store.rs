//! Concurrent writers fetch web pages and store them in the database while
//! concurrent readers sample random keys and verify the stored content.
//!
//! Demonstrates multi-threaded use of the embedded API with real network I/O.
//!
//! ```bash
//! cargo run --example concurrent_web_store
//! ```

use bplus_store::api::Db;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

// Tree<K,V> is not Clone, so we wrap it in Arc for sharing across threads.
type SharedTree = Arc<bplus_store::api::Tree<String, Vec<u8>>>;

/// URLs to fetch. Each writer thread picks URLs round-robin.
const URLS: &[&str] = &[
    // httpbin ��� various payload sizes and response formats
    "https://httpbin.org/bytes/128",
    "https://httpbin.org/bytes/256",
    "https://httpbin.org/bytes/512",
    "https://httpbin.org/bytes/1024",
    "https://httpbin.org/get",
    "https://httpbin.org/ip",
    "https://httpbin.org/user-agent",
    "https://httpbin.org/headers",
    "https://httpbin.org/uuid",
    // Public JSON APIs
    "https://jsonplaceholder.typicode.com/posts/1",
    "https://jsonplaceholder.typicode.com/posts/2",
    "https://jsonplaceholder.typicode.com/comments/1",
    "https://jsonplaceholder.typicode.com/users/1",
    "https://jsonplaceholder.typicode.com/todos/1",
    // Plain text / lightweight endpoints
    "https://icanhazip.com",
    "https://ifconfig.me/ip",
    "https://api.ipify.org",
    // Public data APIs
    "https://catfact.ninja/fact",
    "https://uselessfacts.jsph.pl/api/v2/facts/random",
    "https://official-joke-api.appspot.com/random_joke",
    "https://www.boredapi.com/api/activity",
    "https://dog.ceo/api/breeds/image/random",
];

const NUM_WRITERS: usize = 4;
const NUM_READERS: usize = 3;
const WRITES_PER_THREAD: usize = 6;

fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Db::open(dir.path())?;
    let tree: SharedTree = Arc::new(db.create_tree::<String, Vec<u8>>("pages", 64)?);

    let done = Arc::new(AtomicBool::new(false));

    // --- Readers --------------------------------------------------------
    // Spin up reader threads that continuously sample the tree. They may see
    // None (key not yet written) or Some(body) — both are fine. They must
    // never see an error or a corrupted value.
    let readers: Vec<_> = (0..NUM_READERS)
        .map(|rid| {
            let t = Arc::clone(&tree);
            let d = Arc::clone(&done);
            thread::spawn(move || {
                let mut hits = 0u64;
                let mut misses = 0u64;
                while !d.load(Ordering::Relaxed) {
                    for url in URLS {
                        match t.get(&url.to_string()) {
                            Ok(Some(body)) => {
                                assert!(!body.is_empty(), "stored body should not be empty");
                                hits += 1;
                            }
                            Ok(None) => {
                                misses += 1;
                            }
                            Err(e) => panic!("reader {rid} error: {e}"),
                        }
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                (hits, misses)
            })
        })
        .collect();

    // --- Writers --------------------------------------------------------
    // Each writer fetches a subset of URLs and stores the response body.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let writers: Vec<_> = (0..NUM_WRITERS)
        .map(|wid| {
            let t = Arc::clone(&tree);
            let c = client.clone();
            let rt_handle = rt.handle().clone();
            thread::spawn(move || {
                let mut written = 0usize;
                for i in 0..WRITES_PER_THREAD {
                    let url = URLS[(wid * WRITES_PER_THREAD + i) % URLS.len()];
                    let result = rt_handle.block_on(async { c.get(url).send().await });
                    match result {
                        Ok(resp) => {
                            let body = rt_handle.block_on(async { resp.bytes().await }).unwrap();
                            t.put(&url.to_string(), &body.to_vec()).unwrap();
                            written += 1;
                            println!("writer {wid}: stored {} ({} bytes)", url, body.len());
                        }
                        Err(e) => {
                            eprintln!("writer {wid}: fetch failed for {url}: {e}");
                        }
                    }
                }
                written
            })
        })
        .collect();

    // Wait for all writers to finish.
    let total_written: usize = writers.into_iter().map(|h| h.join().unwrap()).sum();

    // Signal readers to stop and collect stats.
    done.store(true, Ordering::Relaxed);
    let mut total_hits = 0u64;
    let mut total_misses = 0u64;
    for h in readers {
        let (hits, misses) = h.join().unwrap();
        total_hits += hits;
        total_misses += misses;
    }

    println!();
    println!("--- results ---");
    println!("writers:       {NUM_WRITERS} threads x {WRITES_PER_THREAD} URLs");
    println!("total stored:  {total_written}");
    println!("reader hits:   {total_hits}");
    println!("reader misses: {total_misses}");
    println!("tree size:     {}", tree.len());

    // Final verification: every URL that was successfully fetched should
    // be readable and non-empty.
    for url in URLS {
        if let Some(body) = tree.get(&url.to_string())? {
            assert!(!body.is_empty(), "{url} body should not be empty");
        }
    }
    println!("final verification passed");

    Ok(())
}
