//! `POST /v1/blobs/{boundedContext}` and `GET /v1/blobs/{boundedContext}/{id}`
//! (issue #132, docs/architecture.md §239).
//!
//! Who may upload: a Role whose grant in the bounded context holds the
//! `blob-upload` capability, or an external event, direct creation or
//! command token whose type declares a blob field, which is what a bridge
//! holds. Who may
//! download is decided record by record: `db::blob_access_for_mapping` and
//! `db::blob_access_for_token`. A refusal is answered as a 404, the same as
//! a blob that doesn't exist, so a blob id says nothing to someone who
//! can't read it.

use crate::error::RestError;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{StreamExt, TryStreamExt};
use serde::Deserialize;
use skilj_core::access_control::{IdpConfig, JwksCache, Role, TokenStatus};
use skilj_core::blob::{BlobField, BlobStore};
use skilj_core::db::{self, Pool};
use skilj_core::encryption::KeyService;
use skilj_core::event_store::{self, BoundedContextStatus};
use skilj_core::shared::{hash_secret, secret_matches};
use std::sync::Arc;

/// The capability a Role's grant needs to upload a blob.
pub const BLOB_UPLOAD_CAPABILITY: &str = "blob-upload";

/// The longest media type accepted.
const MAX_MEDIA_TYPE_LEN: usize = 255;

/// What the blob routes need; see [`router`].
#[derive(Clone)]
pub struct BlobRoutes {
    /// The database.
    pub pool: Pool,
    /// Where the bytes are kept.
    pub store: Arc<dyn BlobStore>,
    /// The most bytes one blob may hold.
    pub max_blob_bytes: u64,
    /// For a blob with a subject; `None` refuses those.
    pub key_service: Option<KeyService>,
    /// Verifies a Role's bearer JWT; `None` allows tokens only.
    pub identity: Option<(IdpConfig, Arc<JwksCache>)>,
}

/// The blob routes, separate from [`crate::router`] so a deployment that
/// doesn't want them needn't mount them.
pub fn router(routes: BlobRoutes) -> Router {
    Router::new()
        .route("/v1/blobs/{bounded_context}", post(upload))
        .route("/v1/blobs/{bounded_context}/{id}", get(download))
        .with_state(routes)
}

/// Who a blob route's `Authorization: Bearer` header says is calling: a
/// JWT has three dot-separated parts, a token (`<id>.<secret>`) two.
enum Caller {
    Role(Role),
    Token { id: String, hashed_secret: String },
}

async fn caller(routes: &BlobRoutes, headers: &HeaderMap) -> Result<Caller, RestError> {
    let header = headers
        .get(header::AUTHORIZATION)
        .ok_or(RestError::MissingCredential)?
        .to_str()
        .map_err(|_| RestError::MalformedCredential)?;
    let credential = header
        .strip_prefix("Bearer ")
        .ok_or(RestError::MalformedCredential)?;
    if credential.matches('.').count() == 2 {
        let (config, cache) = routes
            .identity
            .as_ref()
            .ok_or(RestError::UnrecognisedCredential)?;
        let verified = skilj_core::access_control::verify_jwt(credential, config, cache)
            .await
            .map_err(|_| RestError::UnrecognisedCredential)?;
        let roles = db::active_roles_by_external_subject(&routes.pool, &verified.subject).await?;
        let role =
            skilj_core::access_control::resolve_role_by_external_subject(&verified.subject, &roles)
                .map_err(|_| RestError::UnrecognisedCredential)?
                .clone();
        return Ok(Caller::Role(role));
    }
    let (id, secret) = credential
        .split_once('.')
        .filter(|(id, secret)| !id.is_empty() && !secret.is_empty())
        .ok_or(RestError::MalformedCredential)?;
    Ok(Caller::Token {
        id: id.to_string(),
        hashed_secret: hash_secret(secret),
    })
}

/// The blob fields of the event or command type a writing token is for, if
/// it is an active, correct token of such a kind for this bounded context.
async fn upload_token_fields(
    routes: &BlobRoutes,
    bounded_context: &str,
    id: &str,
    hashed_secret: &str,
) -> Result<Vec<BlobField>, RestError> {
    if let Some(token) = db::get_external_event_token(&routes.pool, id).await? {
        if secret_matches(hashed_secret, &token.secret)
            && token.status == TokenStatus::Active
            && token.event_type.bounded_context.name == bounded_context
        {
            return Ok(token.event_type.blob_fields);
        }
    } else if let Some(token) = db::get_direct_creation_token(&routes.pool, id).await? {
        if secret_matches(hashed_secret, &token.secret)
            && token.status == TokenStatus::Active
            && token.event_type.bounded_context.name == bounded_context
        {
            return Ok(token.event_type.blob_fields);
        }
    } else if let Some(token) = db::get_command_token(&routes.pool, id).await? {
        if secret_matches(hashed_secret, &token.secret)
            && token.status == TokenStatus::Active
            && token.command_type.bounded_context.name == bounded_context
        {
            return Ok(token.command_type.blob_fields);
        }
    }
    Err(RestError::UnrecognisedCredential)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadQuery {
    subject_key: Option<String>,
    subject_value: Option<String>,
}

async fn upload(
    State(routes): State<BlobRoutes>,
    Path(bounded_context): Path<String>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    request: Request,
) -> Result<Response, RestError> {
    let caller = caller(&routes, &headers).await?;
    let context = db::get_bounded_context(&routes.pool, &bounded_context)
        .await?
        .ok_or(RestError::UnrecognisedCredential)?;
    match &caller {
        Caller::Role(role) => {
            let mapping =
                db::get_active_role_access_mapping(&routes.pool, &role.id, &bounded_context)
                    .await?;
            if !mapping.is_some_and(|mapping| {
                skilj_core::access_control::mapping_holds_capability(
                    &mapping,
                    Some(BLOB_UPLOAD_CAPABILITY),
                )
            }) {
                return Err(RestError::UploadNotAllowed);
            }
        }
        Caller::Token { id, hashed_secret } => {
            let fields = upload_token_fields(&routes, &bounded_context, id, hashed_secret).await?;
            let declared = fields.iter().any(|field| match &query.subject_key {
                Some(subject_key) => field.subject_key.as_ref() == Some(subject_key),
                None => field.subject_key.is_none(),
            });
            if !declared {
                return Err(RestError::InvalidRequest(
                    "this token's type declares no blob field for this subject".to_string(),
                ));
            }
        }
    }
    if context.status != BoundedContextStatus::Active {
        return Err(
            skilj_core::error::Error::from(event_store::Error::BoundedContextArchived).into(),
        );
    }
    let media_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= MAX_MEDIA_TYPE_LEN)
        .ok_or_else(|| RestError::InvalidRequest("a blob upload needs a Content-Type".to_string()))?
        .to_string();
    let subject = match (&query.subject_key, &query.subject_value) {
        (None, None) => None,
        (Some(subject_key), Some(subject_value)) => {
            let key_service = routes
                .key_service
                .as_ref()
                .ok_or(skilj_core::error::Error::from(
                    skilj_core::encryption::Error::MasterKeyNotConfigured,
                ))?;
            Some(db::BlobSubject {
                subject_key,
                subject_value,
                key_service,
            })
        }
        _ => {
            return Err(RestError::InvalidRequest(
                "subjectKey and subjectValue go together".to_string(),
            ))
        }
    };
    let bytes = request
        .into_body()
        .into_data_stream()
        .map_ok(|chunk| chunk.to_vec())
        .map_err(std::io::Error::other)
        .boxed();
    let reference = db::upload_blob(
        &routes.pool,
        routes.store.as_ref(),
        &bounded_context,
        &media_type,
        bytes,
        routes.max_blob_bytes,
        subject,
        chrono::Utc::now(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(reference)).into_response())
}

async fn download(
    State(routes): State<BlobRoutes>,
    Path((bounded_context, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, RestError> {
    let caller = caller(&routes, &headers).await?;
    if db::get_bounded_context(&routes.pool, &bounded_context)
        .await?
        .is_none()
    {
        return Err(RestError::BlobNotFound);
    }
    let allowed = match &caller {
        Caller::Role(role) => {
            match db::get_active_role_access_mapping(&routes.pool, &role.id, &bounded_context)
                .await?
            {
                Some(mapping) => {
                    db::blob_access_for_mapping(&routes.pool, &bounded_context, &id, &mapping)
                        .await?
                }
                None => false,
            }
        }
        Caller::Token {
            id: token_id,
            hashed_secret,
        } => {
            let token = db::get_event_read_token(&routes.pool, token_id)
                .await?
                .filter(|token| secret_matches(hashed_secret, &token.secret))
                .ok_or(RestError::UnrecognisedCredential)?;
            token.status == TokenStatus::Active
                && token.event_type.bounded_context.name == bounded_context
                && db::blob_access_for_token(&routes.pool, &bounded_context, &id, &token).await?
        }
    };
    if !allowed {
        return Err(RestError::BlobNotFound);
    }
    let Some((reference, stream)) = db::read_blob(
        &routes.pool,
        routes.store.as_ref(),
        &bounded_context,
        &id,
        routes.key_service.as_ref(),
    )
    .await?
    else {
        return Err(RestError::BlobNotFound);
    };
    let mut response = axum::body::Body::from_stream(stream).into_response();
    let out = response.headers_mut();
    // Never rendered in place: what was uploaded is not this site's content.
    out.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment"),
    );
    out.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    out.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&reference.media_type)
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    out.insert(header::CONTENT_LENGTH, HeaderValue::from(reference.size));
    Ok(response)
}
