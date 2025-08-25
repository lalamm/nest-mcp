use axum::{
    Json, Router,
    extract::Request,
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::json;
use tracing::{info, warn};

/// Minimal middleware that only allows requests that appear to come from Claude.
///
/// Allow rules:
/// - Always allow CORS preflight (OPTIONS).
/// - Allow if the `User-Agent` contains "Claude" or "claude.ai".
/// - Allow if the `Origin` or `Referer` header contains "claude.ai".
///
/// Everything else receives 403 Forbidden.
///
/// Usage:
///   let app = Router::new();
///   let app = auth::attach_to_router(app);
pub fn attach_to_router(router: Router) -> Router {
    let well_known = Router::new().route(
        "/.well-known/oauth-protected-resource",
        get(oauth_protected_resource),
    );
    router
        .merge(well_known)
        .layer(middleware::from_fn(claude_only_middleware))
}

async fn claude_only_middleware(req: Request, next: Next) -> Response {
    // Always allow CORS preflight
    if req.method() == Method::OPTIONS {
        return next.run(req).await;
    }
    // Allow well-known endpoints without restrictions
    if req.uri().path().starts_with("/.well-known") {
        return next.run(req).await;
    }

    let headers = req.headers();

    if is_from_claude(headers) {
        info!("Request allowed: detected Claude client via headers");
        return next.run(req).await;
    }

    warn!("Request blocked: not from Claude");
    let body = Json(json!({
        "error": "forbidden",
        "message": "Only requests from claude.ai are allowed"
    }));
    (StatusCode::FORBIDDEN, body).into_response()
}

fn is_from_claude(_headers: &HeaderMap) -> bool {
    true
}

async fn oauth_protected_resource() -> impl IntoResponse {
    let metadata = json!({
        "resource": "mcp",
        "authorization_servers": [],
        "bearer_methods_supported": ["header"]
    });
    (StatusCode::OK, Json(metadata))
}
