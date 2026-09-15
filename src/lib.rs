//! Bibliotheque partagee `orch-gateway-rs` (contrat + politique + assemblage).
//!
//! Miroir de `orch_gateway` Python : validation Bearer + politique explicite
//! 10 lectures / 6 ecritures / 0 admin + proxy vers l'upstream Python fige
//! (`orch_mcp`, `:8791`, loopback, sans auth — isolation systemd).
//! Metier conserve : CalDAV upstream + OAuth Python.
//!
//! Differences assumees vs `build_gateway` du framework (metier local au
//! depot, jamais dans le framework) :
//! * ere protocolaire duale (`era`) : le corps decide ; enveloppe 2026-07-28
//!   jamais rabaissee (refus -32020 sous en-tete historique), forme historique
//!   sous en-tete moderne rabaissee vers 2025-11-25 ;
//! * JSON strict : cle dupliquee (tout niveau) refusee -32600 ;
//! * allowlist des methodes upstream SDK 2.2.0 (sinon -32601 local) ;
//! * en-tetes MCP dupliques refuses -32020 (HTTP 400) ;
//! * identite acteur : `x-orch-mcp-acteur` (client_id du jeton valide) +
//!   `x-orch-mcp-mode` (`cli`/`oauth`) injectes vers l'upstream ;
//! * `tools/list` illisible jamais relayee (-32603) ; `resultType` 2026-07-28
//!   : `cacheScope` force `private`, `ttlMs` 0 (pas de cache partage) ;
//! * challenge 401 vers l'URL PRM exacte du Python.
//!
//! OAuth JWT/Python vs opaque/Rust : canary via Bearer statique dedie
//! (meme famille que les lots 1-3, bascule gated).

pub mod era;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, Router};

use mcp_auth::bearer::{extract_bearer, json_response, AccessTokenResolver, StaticBearer};
use mcp_auth::filestore::{ChainedResolver, FileStore};
use mcp_auth::oauth::{
    auth_router, protected_resource_router, MemoryStore, OAuthConfig, OAuthState,
};
use mcp_auth::policy::{TablePolicy, ToolClass, ToolPolicy};
use mcp_core::error::{self, codes};

/// Codes JSON-RPC propres a l'upstream orch (miroir `upstream.py`, metier
/// local) : le framework ne connait que PARSE/BATCH/APP.
pub mod codes_orch {
    pub const METHODE_INCONNUE: i64 = -32601;
    pub const INTERNE: i64 = -32603;
    pub const ENTETE: i64 = -32020;
}

/// Outils de lecture (aucune donnee modifiee, 14).
pub const OUTILS_LECTURE: &[&str] = &[
    "agent_job_get",
    "agent_job_list",
    "agent_job_output",
    "agent_job_events",
    "agent_runner_inspect",
    "agent_job_wait",
    "agent_mission_get",
    "agent_mission_wait",
    "agent_runner_list",
    "agent_workspace_list",
    "infra_alert_list",
    "infra_alert_get",
    "agent_question_list",
    "agent_question_get",
];

/// Mutateurs : lancement/annulation d'agents, missions, reponses (6).
pub const OUTILS_ECRITURE: &[&str] = &[
    "agent_job_cancel",
    "agent_job_start",
    "agent_mission_create",
    "agent_mission_retry",
    "agent_mission_validate",
    "agent_question_answer",
];

pub const READ_SCOPE: &str = "orch:lecture";
pub const WRITE_SCOPE: &str = "orch:ecriture";

/// URLs publiques a l'identique du Python (issuer/resource).
pub const ISSUER_DEFAULT: &str = "https://mymcps.duckdns.org/oauth/orch";
pub const RESOURCE_URL: &str = "https://mymcps.duckdns.org/orch/mcp";
pub const RESOURCE_NAME: &str = "Orchestrateur d'agents IA personnels (PC Windows)";
pub const PRM_ALIAS: &str = "/.well-known/oauth-protected-resource/orch/mcp";
/// URL PRM exacte servie par le Python (challenge 401 + document).
pub const PRM_URL: &str =
    "https://mymcps.duckdns.org/.well-known/oauth-protected-resource/orch/mcp";

/// Client statique CLI (mode `cli` ; sinon `oauth`).
pub const STATIC_CLIENT_ID: &str = "orch-cli-statique";

/// Politique fail-closed du service (14R/6W/0admin, inconnu refuse).
pub fn policy() -> TablePolicy {
    let mut entries: Vec<(&str, ToolClass)> = Vec::with_capacity(20);
    for t in OUTILS_LECTURE {
        entries.push((t, ToolClass::Read));
    }
    for t in OUTILS_ECRITURE {
        entries.push((t, ToolClass::Write));
    }
    TablePolicy::new(READ_SCOPE, WRITE_SCOPE, &entries)
}

/// Identite acteur : client_id du jeton valide (tronque a 128, comme Python).
#[derive(Debug, Clone)]
pub struct ClientIdentity(pub String);

/// Configuration d'assemblage (resolue par le binaire).
pub struct ServiceConfig {
    pub upstream: String,
    pub static_token: String,
    pub static_token_scopes: Vec<String>,
    pub oauth: OAuthConfig,
    pub max_body_bytes: usize,
}

#[derive(Clone)]
struct AuthState {
    static_bearer: StaticBearer,
    resolver: Arc<ChainedResolver>,
    required_scopes: Vec<String>,
    prm_url: String,
}

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    upstream: String,
    static_client_id: String,
    policy: Arc<TablePolicy>,
}

fn unauthorized(prm_url: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            axum::http::header::WWW_AUTHENTICATE,
            format!("Bearer resource_metadata=\"{prm_url}\""),
        )],
        "Unauthorized",
    )
        .into_response()
}

/// Middleware local : comme `bearer_middleware` du framework, mais propage en
/// plus l'identite acteur (`ClientIdentity`) pour l'injection
/// `x-orch-mcp-acteur` vers l'upstream.
async fn acteur_middleware(
    State(state): State<AuthState>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(token) = extract_bearer(req.headers()) else {
        return unauthorized(&state.prm_url);
    };
    let (client_id, scopes) = if let Some(found) = state.static_bearer.verify(&token) {
        found
    } else {
        match state.resolver.resolve(&token).await {
            Some(found) => found,
            None => return unauthorized(&state.prm_url),
        }
    };
    if !state.required_scopes.iter().all(|s| scopes.contains(s)) {
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }
    req.extensions_mut()
        .insert(mcp_auth::bearer::TokenScopes(scopes));
    let mut ident = client_id;
    ident.truncate(128);
    req.extensions_mut().insert(ClientIdentity(ident));
    next.run(req).await.into_response()
}

/// Assemble le routeur complet : sante + OAuth + PRM (+ alias) + `/mcp` + 404.
/// Assemble le routeur complet. Sans pont fichier (comportement v0.2).
pub fn build_router(cfg: ServiceConfig) -> Result<Router, mcp_core::error::Error> {
    build_router_full(cfg, None)
}

/// Assemble le routeur avec pont fichier optionnel (sessions OAuth Python
/// existantes acceptées sans re-consentement ; `None` = comme
/// [`build_router`]). Ne touche ni à l'upstream ni au Python (drift `main`
/// préservé : le Python reste control-plane et source de vérité).
pub fn build_router_with_filestore(
    cfg: ServiceConfig,
    mount: Option<mcp_gateway::router::FileStoreMount>,
) -> Result<Router, mcp_core::error::Error> {
    build_router_full(cfg, mount)
}

fn build_router_full(
    cfg: ServiceConfig,
    mount: Option<mcp_gateway::router::FileStoreMount>,
) -> Result<Router, mcp_core::error::Error> {
    let store = Arc::new(MemoryStore::default());
    let file = mount
        .filter(|m| !m.etat_path.trim().is_empty())
        .map(|m| Arc::new(FileStore::new(&m.etat_path)));
    let chained = Arc::new(ChainedResolver::new(Arc::clone(&store), file));
    let oauth_state = OAuthState {
        config: Arc::new(cfg.oauth.clone()),
        store,
    };
    let auth_state = AuthState {
        static_bearer: StaticBearer::new(
            &cfg.static_token,
            STATIC_CLIENT_ID,
            &cfg.static_token_scopes,
        ),
        resolver: chained,
        required_scopes: vec![READ_SCOPE.to_string()],
        prm_url: PRM_URL.to_string(),
    };
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(50)
        .build()
        .map_err(|e| mcp_core::error::Error::Upstream(e.to_string()))?;
    let app_state = AppState {
        client,
        upstream: cfg.upstream.trim_end_matches('/').to_string(),
        static_client_id: STATIC_CLIENT_ID.to_string(),
        policy: Arc::new(policy()),
    };

    let mcp_route = Router::new()
        .route(
            "/mcp",
            get(mcp_handler)
                .post(mcp_handler)
                .delete(mcp_handler)
                .route_layer(middleware::from_fn_with_state(
                    auth_state,
                    acteur_middleware,
                )),
        )
        .with_state(app_state);

    let app = Router::new()
        .merge(mcp_http::health::router("orch-gateway-rs", "/mcp"))
        .merge(auth_router(oauth_state.clone()))
        .merge(protected_resource_router(oauth_state, &[PRM_ALIAS]))
        .merge(mcp_route);

    Ok(mcp_http::hardening::harden(app, cfg.max_body_bytes))
}

use mcp_auth::bearer::TokenScopes;

/// Corps d'erreur JSON-RPC local (statut HTTP parametrable).
fn erreur_jsonrpc(
    id: Option<&serde_json::Value>,
    code: i64,
    message: &str,
    statut: StatusCode,
) -> Response {
    json_response(statut, error::jsonrpc_error(id, code, message))
}

/// Handler `/mcp` : ordre Python (`__call__`) — doublons d'en-tetes, parsing
/// strict, allowlist + politique, ere, puis relais avec acteur.
async fn mcp_handler(State(state): State<AppState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().map(str::to_string);
    let scopes: HashSet<String> = parts
        .extensions
        .get::<TokenScopes>()
        .map(|s| s.0.clone())
        .unwrap_or_default();
    let identite: Option<String> = parts
        .extensions
        .get::<ClientIdentity>()
        .map(|c| c.0.clone());

    // 1. En-tetes MCP dupliques (HTTP 400, avant tout). `get_all` expose les
    // doublons fusionnes par axum : un compteur > 1 = doublon.
    let paires: Vec<(String, String)> = {
        let mut v = Vec::new();
        for name in parts.headers.keys() {
            let nom = name.as_str().to_lowercase();
            for value in parts.headers.get_all(name).iter() {
                v.push((nom.clone(), value.to_str().unwrap_or("").to_string()));
            }
        }
        v
    };
    if let Some(doublon) = era::entete_duplique(&paires) {
        tracing::warn!(doublon = %doublon, "refus_entete");
        return erreur_jsonrpc(
            None,
            codes_orch::ENTETE,
            &format!("en-tete {doublon} duplique (fail-closed)"),
            StatusCode::BAD_REQUEST,
        );
    }

    // 2. Corps : parsing strict + allowlist + politique.
    let mut body_bytes: Option<Vec<u8>> = None;
    let mut donnees: Option<serde_json::Value> = None;
    if matches!(method, Method::POST | Method::PUT | Method::PATCH) {
        let bytes =
            match axum::body::to_bytes(body, mcp_http::hardening::DEFAULT_MAX_BODY_BYTES).await {
                Ok(b) => b,
                Err(_) => {
                    return json_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        error::payload_too_large_body(),
                    );
                }
            };
        if !bytes.is_empty() {
            match era::parse_strict(&bytes) {
                Err(era::StrictError::Duplicate(_)) => {
                    tracing::warn!("refus_parse cle JSON dupliquee");
                    return erreur_jsonrpc(
                        None,
                        codes::BATCH,
                        "cles JSON dupliquees (fail-closed)",
                        StatusCode::OK,
                    );
                }
                Err(era::StrictError::Parse) => {
                    tracing::warn!("refus_parse JSON illisible");
                    return erreur_jsonrpc(
                        None,
                        codes::PARSE,
                        "corps JSON-RPC illisible (fail-closed)",
                        StatusCode::OK,
                    );
                }
                Ok(v) => {
                    if v.is_array() {
                        tracing::warn!("refus_parse batch");
                        return erreur_jsonrpc(
                            None,
                            codes::BATCH,
                            "requetes par lot non prises en charge",
                            StatusCode::OK,
                        );
                    }
                    if let Some(o) = v.as_object() {
                        if let Some(m) = o.get("method").and_then(|m| m.as_str()) {
                            if !era::methode_connue(m) {
                                tracing::warn!("refus_methode");
                                return erreur_jsonrpc(
                                    v.get("id"),
                                    codes_orch::METHODE_INCONNUE,
                                    "methode inconnue ou non autorisee (fail-closed)",
                                    StatusCode::OK,
                                );
                            }
                            if m == "tools/call" {
                                let nom = o
                                    .get("params")
                                    .and_then(|p| p.as_object())
                                    .and_then(|p| p.get("name"))
                                    .and_then(|n| n.as_str());
                                match nom {
                                    Some(n) if !n.is_empty() => {
                                        if let Some(raison) =
                                            state.policy.autoriser_call(n, &scopes)
                                        {
                                            tracing::warn!("refus_politique");
                                            return erreur_jsonrpc(
                                                v.get("id"),
                                                codes::APP,
                                                &raison,
                                                StatusCode::OK,
                                            );
                                        }
                                    }
                                    _ => {
                                        return erreur_jsonrpc(
                                            v.get("id"),
                                            codes::APP,
                                            "tools/call sans nom d'outil (fail-closed)",
                                            StatusCode::OK,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    donnees = Some(v);
                }
            }
            body_bytes = Some(bytes.to_vec());
        }
    }

    // 3. Ere protocolaire (corps d'abord).
    let version = paires
        .iter()
        .find(|(n, _)| n == "mcp-protocol-version")
        .map(|(_, v)| v.clone());
    let classement = era::classer_ere(version.as_deref(), donnees.as_ref());
    if let Some(raison) = classement.refus {
        tracing::warn!("refus_ere");
        return erreur_jsonrpc(
            donnees.as_ref().and_then(|d| d.get("id")),
            codes_orch::ENTETE,
            &raison,
            StatusCode::BAD_REQUEST,
        );
    }
    if classement.rabaissee {
        tracing::info!("rabaissement_ere");
    }

    forward(
        &state,
        ForwardArgs {
            method: &method,
            path: &path,
            query: query.as_deref(),
            paires: &paires,
            version_relayee: classement.relayee,
            body: body_bytes,
            methode_rpc: donnees.as_ref().and_then(|d| {
                d.as_object()
                    .and_then(|o| o.get("method"))
                    .and_then(|m| m.as_str())
                    .map(str::to_string)
            }),
            scopes: &scopes,
            identite,
        },
    )
    .await
}

struct ForwardArgs<'a> {
    method: &'a Method,
    path: &'a str,
    query: Option<&'a str>,
    paires: &'a [(String, String)],
    version_relayee: Option<String>,
    body: Option<Vec<u8>>,
    methode_rpc: Option<String>,
    scopes: &'a HashSet<String>,
    identite: Option<String>,
}

async fn forward(state: &AppState, a: ForwardArgs<'_>) -> Response {
    let method = a.method;
    let path = a.path;
    let query = a.query;
    let paires = a.paires;
    let version_relayee = a.version_relayee;
    let body = a.body;
    let methode_rpc = a.methode_rpc;
    let scopes = a.scopes;
    let identite = a.identite;
    // En-tetes relayes : allowlist + `mcp-param-*`, version remplacee
    // (absente si le classement n'en rend aucune).
    let mut outgoing = axum::http::HeaderMap::new();
    for (nom, valeur) in paires {
        if nom == "mcp-protocol-version" {
            // Version remplacee par le classement (absente si aucune rendue).
            if let Some(v) = version_relayee.as_deref() {
                if let Ok(hv) = v.parse() {
                    outgoing.insert("mcp-protocol-version", hv);
                }
            }
        } else if era::ENTETES_REQUETE.contains(&nom.as_str())
            || nom.starts_with(era::PREFIXE_PARAM)
        {
            if let (Ok(n), Ok(v)) = (
                axum::http::HeaderName::try_from(nom),
                valeur.parse::<axum::http::HeaderValue>(),
            ) {
                outgoing.insert(n, v);
            }
        }
    }

    let url = match query {
        Some(q) if !q.is_empty() => format!("{}{}?{q}", state.upstream, path),
        _ => format!("{}{}", state.upstream, path),
    };
    let mut builder = state.client.request(method.clone(), url);
    for (name, value) in outgoing.iter() {
        builder = builder.header(name, value);
    }
    // Acteur propage APRES authentification (jamais lu depuis l'exterieur).
    if let Some(id) = identite.filter(|s| !s.is_empty()) {
        let mode = if id == state.static_client_id {
            "cli"
        } else {
            "oauth"
        };
        builder = builder.header("x-orch-mcp-acteur", id);
        builder = builder.header("x-orch-mcp-mode", mode);
    }
    if *method == Method::POST {
        builder = builder.timeout(Duration::from_secs(3600));
    }
    if let Some(b) = body {
        builder = builder.body(b);
    }
    let debut = std::time::Instant::now();
    let upstream = match builder.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                "relais echec: {:?}",
                e.to_string().chars().take(32).collect::<String>()
            );
            return json_response(
                StatusCode::BAD_GATEWAY,
                error::bad_gateway_body("HttpError"),
            );
        }
    };
    if debut.elapsed().as_secs_f64() > 30.0 {
        tracing::warn!("relais anormalement lent");
    }
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let out_headers = {
        let mut h = axum::http::HeaderMap::new();
        for nom in era::ENTETES_REPONSE {
            if let (Ok(n), Some(v)) = (
                axum::http::HeaderName::try_from(*nom),
                upstream.headers().get(*nom),
            ) {
                h.insert(n, v.clone());
            }
        }
        h
    };

    if *method == Method::GET {
        let stream = upstream.bytes_stream();
        let mut builder = Response::builder().status(status);
        for (name, value) in out_headers.iter() {
            builder = builder.header(name, value);
        }
        return builder
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
    }

    let content_type = out_headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = match upstream.bytes().await {
        Ok(b) => b.to_vec(),
        Err(_) => {
            return json_response(
                StatusCode::BAD_GATEWAY,
                error::bad_gateway_body("HttpError"),
            );
        }
    };
    if *method == Method::POST {
        let visibles = state.policy.visibles(scopes);
        let (filtres, lisible) = filtrer_liste(&bytes, &content_type, &visibles);
        let est_liste = methode_rpc.as_deref() == Some("tools/list");
        if est_liste && !lisible {
            tracing::warn!("refus_liste_illisible");
            return erreur_jsonrpc(
                None,
                codes_orch::INTERNE,
                "reponse tools/list upstream illisible (fail-closed)",
                StatusCode::OK,
            );
        }
        let mut builder = Response::builder().status(status);
        for (name, value) in out_headers.iter() {
            builder = builder.header(name, value);
        }
        return builder
            .body(Body::from(filtres))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
    }
    let mut builder = Response::builder().status(status);
    for (name, value) in out_headers.iter() {
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(bytes))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

/// Message JSON-RPC lisible : objet avec `result` ou `error`.
fn message_lisible(v: &serde_json::Value) -> bool {
    v.as_object()
        .is_some_and(|o| o.contains_key("result") || o.contains_key("error"))
}

/// Applique le filtre tools + regle cache 2026-07-28 (`resultType` present :
/// `cacheScope=private`, `ttlMs=0`). Retourne `true` si modifie.
fn appliquer_filtre(
    resultat: &mut serde_json::Map<String, serde_json::Value>,
    visibles: &HashSet<String>,
) -> bool {
    let mut modifie = false;
    if let Some(outils) = resultat.get("tools").and_then(|t| t.as_array()) {
        let conserves: Vec<serde_json::Value> = outils
            .iter()
            .filter(|o| {
                o.as_object()
                    .and_then(|o| o.get("name"))
                    .and_then(|n| n.as_str())
                    .is_some_and(|n| visibles.contains(n))
            })
            .cloned()
            .collect();
        if conserves.len() != outils.len() {
            resultat.insert("tools".to_string(), serde_json::Value::Array(conserves));
            modifie = true;
        }
    }
    if resultat.contains_key("resultType")
        && (resultat.get("cacheScope").and_then(|v| v.as_str()) != Some("private")
            || resultat.get("ttlMs").and_then(|v| v.as_u64()) != Some(0))
    {
        resultat.insert(
            "cacheScope".to_string(),
            serde_json::Value::String("private".to_string()),
        );
        resultat.insert("ttlMs".to_string(), serde_json::Value::Number(0.into()));
        modifie = true;
    }
    modifie
}

fn filtrer_json(corps: &[u8], visibles: &HashSet<String>) -> (Vec<u8>, bool) {
    let v: serde_json::Value = match serde_json::from_slice(corps) {
        Ok(v) => v,
        Err(_) => return (corps.to_vec(), false),
    };
    if !message_lisible(&v) {
        return (corps.to_vec(), false);
    }
    let mut v = v;
    let has_tools = v
        .get("result")
        .and_then(|r| r.as_object())
        .and_then(|r| r.get("tools"))
        .is_some();
    if !has_tools {
        return (corps.to_vec(), true);
    }
    let changed = v
        .get_mut("result")
        .and_then(|r| r.as_object_mut())
        .is_some_and(|r| appliquer_filtre(r, visibles));
    if !changed {
        return (corps.to_vec(), true);
    }
    match serde_json::to_vec(&v) {
        Ok(out) => (out, true),
        Err(_) => (corps.to_vec(), false),
    }
}

/// Filtre SSE evenementiel (lignes vides separatrices, `data:` avec/sans
/// espace, CRLF, plusieurs `data:` concatenes). Retourne (corps, lisible).
fn filtrer_sse(corps: &[u8], visibles: &HashSet<String>) -> (Vec<u8>, bool) {
    let texte = String::from_utf8_lossy(corps);
    let lignes: Vec<&str> = texte.split('\n').collect();
    // Decoupe en evenements (separateur = ligne vide).
    let mut events: Vec<Vec<usize>> = Vec::new();
    let mut courant: Vec<usize> = Vec::new();
    for (i, l) in lignes.iter().enumerate() {
        if l.trim_end_matches('\r').is_empty() {
            if !courant.is_empty() {
                events.push(std::mem::take(&mut courant));
            }
        } else {
            courant.push(i);
        }
    }
    if !courant.is_empty() {
        events.push(courant);
    }
    let mut lignes: Vec<Option<String>> = lignes.iter().map(|s| Some(s.to_string())).collect();
    let mut lisible = false;
    let mut modifie = false;
    for ev in events {
        // `data:` (avec ou sans espace), payloads concatenes par \n (spec SSE).
        let mut payloads: Vec<(usize, String)> = Vec::new();
        for &i in &ev {
            if let Some(l) = lignes[i].as_deref() {
                let t = l.trim_end_matches('\r');
                if let Some(rest) = t.strip_prefix("data:") {
                    let p = rest.strip_prefix(' ').unwrap_or(rest);
                    payloads.push((i, p.to_string()));
                }
            }
        }
        if payloads.is_empty() {
            continue;
        }
        let joint = payloads
            .iter()
            .map(|(_, p)| p.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let mut v: serde_json::Value = match serde_json::from_str(&joint) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !message_lisible(&v) {
            continue;
        }
        lisible = true;
        let has_tools = v
            .get("result")
            .and_then(|r| r.as_object())
            .and_then(|r| r.get("tools"))
            .is_some();
        if !has_tools {
            continue;
        }
        let changed = v
            .get_mut("result")
            .and_then(|r| r.as_object_mut())
            .is_some_and(|r| appliquer_filtre(r, visibles));
        if !changed {
            continue;
        }
        if let Ok(out) = serde_json::to_string(&v) {
            lignes[payloads[0].0] = Some(format!("data: {out}"));
            for (i, _) in payloads.iter().skip(1) {
                lignes[*i] = None;
            }
            modifie = true;
        }
    }
    if !modifie {
        return (corps.to_vec(), lisible);
    }
    let out = lignes.into_iter().flatten().collect::<Vec<_>>().join("\n");
    (out.into_bytes(), lisible)
}

/// Filtre une reponse (JSON nu ou enveloppe SSE). Retourne (corps, lisible).
pub fn filtrer_liste(
    corps: &[u8],
    content_type: &str,
    visibles: &HashSet<String>,
) -> (Vec<u8>, bool) {
    if content_type.to_lowercase().contains("text/event-stream") {
        return filtrer_sse(corps, visibles);
    }
    if content_type.to_lowercase().contains("application/json") {
        return filtrer_json(corps, visibles);
    }
    (corps.to_vec(), false)
}
