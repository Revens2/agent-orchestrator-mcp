//! orch-gateway-rs — facade Rust devant l'upstream Python fige (`:8802`).
//!
//! Assemble le routeur `orch_gateway_rs::build_router` (aucune auth upstream
//! requise, isolation systemd comme le Python ; `x-orch-mcp-acteur` injecte
//! apres authentification).
//!
//! Environnement (prefixe `ORCH_GW_RS_*`) :
//! * `ORCH_GW_RS_ISSUER` (defaut issuer prod, HTTPS requis),
//! * `ORCH_GW_RS_UPSTREAM` (defaut `http://127.0.0.1:8802`, loopback requis),
//! * `ORCH_GW_RS_PORT` (defaut `18981` canary ; 8801 a la bascule),
//! * `ORCH_GW_RS_TOKEN` (>= 32 car.) OU `ORCH_GW_RS_TOKEN_FILE`
//!   (defaut `/opt/orch-gateway-rs/.mcp_token`) — fail-closed,
//! * `ORCH_GW_RS_TOKEN_SCOPES` (defaut lecture+ecriture, quoté dans l'unit),
//! * `ORCH_GW_RS_CONSENT_HASH` (empreinte PBKDF2, vide = consentement refuse).
//! * `ORCH_GW_RS_OAUTH_ETAT` (defaut `/srv/orch/data/oauth/etat.json` :
//!   pont READ-ONLY vers le magasin Python, sessions existantes sans
//!   re-consentement ; vide = pont desactive).

use mcp_auth::oauth::OAuthConfig;
use orch_gateway_rs::{
    ISSUER_DEFAULT, PRM_ALIAS, READ_SCOPE, RESOURCE_NAME, RESOURCE_URL, WRITE_SCOPE,
};

/// Charge le Bearer statique DEDIE : variable directe, sinon fichier. Fail-closed.
fn load_static_token() -> Result<String, String> {
    if let Ok(v) = std::env::var("ORCH_GW_RS_TOKEN") {
        let v = v.trim().to_string();
        if !v.is_empty() {
            if v.len() < 32 {
                return Err("ORCH_GW_RS_TOKEN trop court (<32 car.)".to_string());
            }
            return Ok(v);
        }
    }
    let path = std::env::var("ORCH_GW_RS_TOKEN_FILE")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "/opt/orch-gateway-rs/.mcp_token".to_string());
    let raw = std::fs::read_to_string(&path)
        .map_err(|e| format!("token Bearer illisible ({path}) : {e}"))?;
    let tok = raw.trim().to_string();
    if tok.len() < 32 {
        return Err("token Bearer trop court (<32 car.), refuse de demarrer".to_string());
    }
    Ok(tok)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    mcp_observe::init("orch-gateway-rs");

    if std::env::var("ORCH_GW_RS_ISSUER")
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        // `set_var` est `unsafe` sur cette toolchain : init mono-thread.
        unsafe { std::env::set_var("ORCH_GW_RS_ISSUER", ISSUER_DEFAULT) };
    }
    if std::env::var("ORCH_GW_RS_UPSTREAM")
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        unsafe { std::env::set_var("ORCH_GW_RS_UPSTREAM", "http://127.0.0.1:8802") };
    }
    // Pont fichier OAuth (transition) : Python = AS/control-plane, Rust =
    // data-plane. Meme utilisateur UNIX que le Python (fichiers 0600).
    if std::env::var("ORCH_GW_RS_OAUTH_ETAT")
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        unsafe { std::env::set_var("ORCH_GW_RS_OAUTH_ETAT", "/srv/orch/data/oauth/etat.json") };
    }
    // Lecon lot 2 : sans SCOPES explicites, le defaut install ne donne que la
    // lecture. Le canary parite exige lecture+ecriture.
    if std::env::var("ORCH_GW_RS_TOKEN_SCOPES")
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        unsafe {
            std::env::set_var(
                "ORCH_GW_RS_TOKEN_SCOPES",
                format!("{READ_SCOPE} {WRITE_SCOPE}"),
            )
        };
    }
    let token = load_static_token().map_err(|e| {
        tracing::error!("fail-closed: pas de Bearer valide");
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, e)
    })?;
    unsafe { std::env::set_var("ORCH_GW_RS_TOKEN", &token) };

    let env = mcp_gateway::config::from_prefix(
        "ORCH_GW_RS",
        18981,
        READ_SCOPE,
        &[READ_SCOPE, WRITE_SCOPE],
    )?;

    let port = env.port;
    let upstream_log = env.upstream.clone();
    let mount = mcp_gateway::config::filestore_mount(&env);
    let app = orch_gateway_rs::build_router_with_filestore(
        orch_gateway_rs::ServiceConfig {
            upstream: env.upstream,
            static_token: env.static_token,
            static_token_scopes: env.token_scopes,
            oauth: OAuthConfig {
                issuer: env.issuer,
                resource_url: RESOURCE_URL.to_string(),
                resource_name: RESOURCE_NAME.to_string(),
                default_scope: READ_SCOPE.to_string(),
                valid_scopes: vec![READ_SCOPE.to_string(), WRITE_SCOPE.to_string()],
                extra_submit_scopes: vec![WRITE_SCOPE.to_string()],
                consent_hash: env.consent_hash,
                static_client_id: orch_gateway_rs::STATIC_CLIENT_ID.to_string(),
            },
            max_body_bytes: mcp_http::hardening::DEFAULT_MAX_BODY_BYTES,
        },
        mount,
    )?;
    let _ = PRM_ALIAS;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    tracing::info!(
        port,
        upstream = %upstream_log,
        "orch-gateway-rs prete (boucle locale uniquement)"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(mcp_core::lifecycle::shutdown_signal())
        .await?;
    Ok(())
}
