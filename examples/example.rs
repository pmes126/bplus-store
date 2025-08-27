use bplus_tree::bplustree::{transaction, tree::BPlusTree, tree::SharedBPlusTree};
use bplus_tree::storage::page_store::PageStore;
use bplus_tree::storage::file_store::FileStore;
use tempfile::TempDir;
use tokio::time::{interval, Duration};
use reqwest::Error;

#[tokio::main]
async fn main() {
    let order = 128;
    let dir = TempDir::new().unwrap();
        
    let file_path = dir.path().join("tree.data");

    let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path).unwrap();
    let tree = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order).unwrap();
    let st = SharedBPlusTree::new(tree);
    let mut tx = transaction::WriteTransaction::new(st.clone());

    let mut ticker = interval(Duration::from_secs(2));
    let url = "https://httpbin.org/get";
     for i in 1..=10 {
        ticker.tick().await;

        match fetch_url(url).await {
            Ok(body) => {
                println!("Request #{}:\n{}", i, body);
                tx.insert(i, body).unwrap();
            }
            Err(err) => eprintln!("Error on request #{}: {}", i, err),
        }
    }
    let _ = tx.commit();
    
    let res = st.traverse().unwrap();
    for (k, v) in &res {
        println!("key {:?}, value: {:?}",  k, v)
    }
}

async fn fetch_url(url: &str) -> Result<String, Error> {
    let response = reqwest::get(url).await?;
    let body = response.text().await?;
    Ok(body)
}
