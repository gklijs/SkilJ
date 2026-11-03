//! A [`BlobStore`] on S3 or anything S3-compatible (docs/architecture.md
//! §239), so large payloads' bytes live outside Postgres. Hand it to
//! `SkiljBuilder::blob_store`.
//!
//! The store only ever sees opaque bytes: a blob with a subject reaches it
//! as sealed frames, so the bucket holds no plaintext of it and
//! `forgetSubject` leaves nothing readable there even before skilj's
//! collection task removes the object. A blob without a subject is stored
//! as it came, so give the bucket the access control and encryption at rest
//! such bytes need.
//!
//! An upload streams through in parts rather than being held in memory, so
//! `SkiljBuilder::max_blob_bytes` can go past the Postgres store's limit. A
//! failed or too-large upload is aborted and leaves nothing behind. Objects
//! are named `<prefix>/<bounded context>/<blob id>`.

#![warn(missing_docs)]

use futures_util::stream::{BoxStream, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use skilj_core::blob::{BlobStore, ByteStream, HashingCounter, StoredBlob};
use skilj_core::encryption::provider::BoxFuture;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

/// See this crate's documentation.
#[derive(Clone)]
pub struct S3BlobStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl S3BlobStore {
    /// A store in `bucket`, configured from the usual `AWS_*` environment
    /// variables (`AWS_ENDPOINT` and `AWS_ALLOW_HTTP` for an S3-compatible
    /// server), with its objects under `prefix`.
    pub fn from_env(bucket: &str, prefix: &str) -> skilj_core::error::Result<Self> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(store_error)?;
        Ok(Self::new(Arc::new(store), prefix))
    }

    /// A store on any configured [`ObjectStore`], with its objects under
    /// `prefix`: an [`AmazonS3Builder`] set up by hand, or another backend.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_string(),
        }
    }

    fn key(&self, bounded_context: &str, id: &str) -> String {
        if self.prefix.is_empty() {
            format!("{bounded_context}/{id}")
        } else {
            format!("{}/{bounded_context}/{id}", self.prefix)
        }
    }
}

fn store_error(error: impl std::fmt::Display) -> skilj_core::Error {
    skilj_core::Error::BlobStore(error.to_string())
}

impl BlobStore for S3BlobStore {
    fn put<'a>(
        &'a self,
        bounded_context: &'a str,
        id: &'a str,
        mut bytes: ByteStream<'a>,
        max_bytes: u64,
    ) -> BoxFuture<'a, skilj_core::error::Result<StoredBlob>> {
        Box::pin(async move {
            let storage_key = self.key(bounded_context, id);
            let mut writer = BufWriter::new(self.store.clone(), Path::from(storage_key.as_str()));
            let mut counter = HashingCounter::new(max_bytes);
            while let Some(chunk) = bytes.next().await {
                let written = match chunk {
                    Ok(chunk) => match counter.update(&chunk) {
                        Ok(()) => writer.write_all(&chunk).await.map_err(store_error),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(store_error(e)),
                };
                if let Err(e) = written {
                    // Nothing an aborted multipart upload could leave is
                    // worth masking the reason it stopped.
                    let _ = writer.abort().await;
                    return Err(e);
                }
            }
            writer.shutdown().await.map_err(store_error)?;
            let (size, sha256) = counter.finish();
            Ok(StoredBlob::new(storage_key, size, sha256))
        })
    }

    fn get<'a>(
        &'a self,
        _bounded_context: &'a str,
        storage_key: &'a str,
    ) -> BoxFuture<'a, skilj_core::error::Result<ByteStream<'static>>> {
        Box::pin(async move {
            let result = self
                .store
                .get(&Path::from(storage_key))
                .await
                .map_err(store_error)?;
            let stream: BoxStream<'static, std::io::Result<Vec<u8>>> = result
                .into_stream()
                .map(|chunk| {
                    chunk
                        .map(|bytes| bytes.to_vec())
                        .map_err(std::io::Error::other)
                })
                .boxed();
            Ok(stream)
        })
    }

    fn delete<'a>(
        &'a self,
        _bounded_context: &'a str,
        storage_key: &'a str,
    ) -> BoxFuture<'a, skilj_core::error::Result<()>> {
        Box::pin(async move {
            match self.store.delete(&Path::from(storage_key)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(store_error(e)),
            }
        })
    }
}
