//! HTTP surfaces for the clean-room SkilJ: GraphQL (Roles, JWT) and REST
//! (AccessTokens).

pub mod auth;
pub mod graphql;
pub mod rest;

use async_graphql::http::{GraphiQLSource, ALL_WEBSOCKET_PROTOCOLS};
use async_graphql::{Data, Executor};
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde_json::Value;
use skilj_core::Skilj;

use crate::auth::Verifier;
use crate::graphql::{Ctx, SchemaCache};

#[derive(Clone)]
pub struct AppState {
    pub skilj: Skilj,
    pub verifier: Verifier,
    pub schemas: SchemaCache,
}

/// The full router: `/graphql` (POST, GET for GraphiQL), `/graphql/ws`,
/// `/rest/v1/...`, `/health`.
pub fn router(skilj: Skilj, verifier: Verifier) -> Router {
    let state = AppState {
        skilj: skilj.clone(),
        verifier,
        schemas: SchemaCache::default(),
    };
    Router::new()
        .route("/graphql", get(graphiql).post(graphql_post))
        .route("/graphql/ws", get(graphql_ws))
        .route("/health", get(|| async { "ok" }))
        .with_state(state)
        .merge(rest::router(skilj))
}

async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .subscription_endpoint("/graphql/ws")
            .finish(),
    )
}

async fn graphql_post(
    State(st): State<AppState>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let identity = match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        None => None,
        Some(h) => match st.verifier.verify_header(Some(h)).await {
            Ok(i) => Some(i),
            Err(e) => {
                return GraphQLResponse::from(async_graphql::Response::from_errors(vec![
                    async_graphql::ServerError::new(e.to_string(), None),
                ]))
                .into_response()
            }
        },
    };
    let schema = match st.schemas.get(&st.skilj).await {
        Ok(s) => s,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };
    let req = req.into_inner().data(Ctx {
        skilj: st.skilj.clone(),
        identity,
    });
    GraphQLResponse::from(schema.execute(req).await).into_response()
}

async fn graphql_ws(
    State(st): State<AppState>,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> Response {
    let schema = match st.schemas.get(&st.skilj).await {
        Ok(s) => s,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };
    ws.protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| {
            let st2 = st.clone();
            GraphQLWebSocket::new(socket, schema, protocol)
                .on_connection_init(move |payload: Value| async move {
                    // The JWT is presented once, at connection init.
                    let header = payload
                        .get("Authorization")
                        .or_else(|| payload.get("authorization"))
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    let identity = st2
                        .verifier
                        .verify_header(header.as_deref())
                        .await
                        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                    let mut data = Data::default();
                    data.insert(Ctx {
                        skilj: st2.skilj.clone(),
                        identity: Some(identity),
                    });
                    Ok(data)
                })
                .serve()
        })
        .into_response()
}

#[allow(dead_code)]
fn _assert_executor<E: Executor>() {}
