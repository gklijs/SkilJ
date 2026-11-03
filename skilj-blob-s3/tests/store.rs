//! The store's contract (docs/architecture.md §239), against object_store's
//! in-memory backend and, where Docker is reachable, a real S3-compatible
//! server. Skips the latter gracefully without Docker, except in CI.

use futures_util::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use skilj_blob_s3::S3BlobStore;
use skilj_core::blob::BlobStore;
use std::sync::Arc;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{GenericImage, ImageExt};

/// Pinned, so a new release can't change what this test means.
const S3MOCK_VERSION: &str = "4.11.0";

fn chunks(bytes: &[u8]) -> skilj_core::blob::ByteStream<'static> {
    let (a, b) = bytes.split_at(bytes.len() / 2);
    futures_util::stream::iter(vec![Ok(a.to_vec()), Ok(b.to_vec())]).boxed()
}

async fn read_back(store: &S3BlobStore, key: &str) -> Vec<u8> {
    let mut stream = store.get("bc", key).await.unwrap();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend(chunk.unwrap());
    }
    out
}

async fn exercise(store: S3BlobStore, inner: Arc<dyn ObjectStore>) {
    // Past object_store's 10 MiB part size, so a multipart upload happens.
    let bytes = vec![3u8; 11 * 1024 * 1024];
    let stored = store
        .put("bc", "blob-1", chunks(&bytes), bytes.len() as u64)
        .await
        .unwrap();
    assert_eq!(stored.storage_key, "tenant/bc/blob-1");
    assert_eq!(stored.size, bytes.len() as u64);
    assert_eq!(read_back(&store, &stored.storage_key).await, bytes);

    store.delete("bc", &stored.storage_key).await.unwrap();
    assert!(store.get("bc", &stored.storage_key).await.is_err());
    // Something already gone is not an error.
    store.delete("bc", &stored.storage_key).await.unwrap();

    // Too large: refused, and nothing is left behind.
    let err = store
        .put("bc", "blob-2", chunks(&[1u8; 64]), 16)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            skilj_core::Error::EventStore(skilj_core::event_store::Error::BlobTooLarge { .. })
        ),
        "{err:?}"
    );
    let left: Vec<_> = inner
        .list(None)
        .map(|meta| meta.unwrap().location.to_string())
        .collect()
        .await;
    assert_eq!(left, Vec::<String>::new());
}

#[tokio::test]
async fn the_contract_holds_in_memory() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    exercise(S3BlobStore::new(inner.clone(), "/tenant/"), inner).await;
}

#[tokio::test]
async fn the_contract_holds_against_an_s3_compatible_server() {
    let container = GenericImage::new("adobe/s3mock", S3MOCK_VERSION)
        .with_exposed_port(9090.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Started S3MockApplication"))
        .with_env_var(
            "COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS",
            "skilj-test",
        )
        .start()
        .await;
    let container = match container {
        Ok(container) => container,
        Err(e) => {
            eprintln!("skipping: starting the ephemeral S3Mock container failed: {e}");
            return;
        }
    };
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9090).await.unwrap();
    let inner: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{host}:{port}"))
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name("skilj-test")
            .with_access_key_id("any")
            .with_secret_access_key("any")
            .build()
            .unwrap(),
    );
    // Its log line can come before its port answers on a loaded machine.
    let mut answered = false;
    for _ in 0..120 {
        let listed: Vec<_> = inner.list(None).collect().await;
        if listed.iter().all(Result::is_ok) {
            answered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(answered, "S3Mock never answered");
    exercise(S3BlobStore::new(inner.clone(), "tenant"), inner).await;
}
