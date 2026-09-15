//! Contrat orch-gateway-rs : politique 14R/6W + ere duale + gateway HTTP.
//!
//! Preuves exigibles du lot orch (passerelle) :
//! * 14 lectures autorisees en lecture, 6 mutateurs exigeant l'ecriture,
//!   inconnu refuse meme full scopes ;
//! * `visibles : 14 en lecture, 20 en lecture+ecriture ;
//! * ere : quirk ChatGPT rabaisse, enveloppe moderne jamais rabaissee
//!   (refus sous en-tete historique), methode moderne intacte ;
//! * JSON strict : cle dupliquee refusee ; methode inconnue -32601 ;
//!   en-tete MCP duplique -32020/400 ;
//! * gateway : 401 + challenge, `/health` framework, PRM exacte,
//!   acteur/mode injectes vers l'upstream, refus locaux avant upstream,
//!   tools/list illisible -32603, cache 2026-07-28 force prive.

use std::collections::HashSet;

use mcp_auth::policy::{ToolClass, ToolPolicy};
use mcp_gateway::testkit::compare_tools_list;
use orch_gateway_rs::{
    era, filtrer_liste, policy, OUTILS_ECRITURE, OUTILS_LECTURE, READ_SCOPE, WRITE_SCOPE,
};

fn scopes(s: &[&str]) -> HashSet<String> {
    s.iter().map(|x| x.to_string()).collect()
}

#[test]
fn tailles_politique_14r_6w() {
    assert_eq!(OUTILS_LECTURE.len(), 14, "regression table lecture");
    assert_eq!(OUTILS_ECRITURE.len(), 6, "regression table ecriture");
}

#[test]
fn matrice_autorisation_lecture_ecriture() {
    let p = policy();
    for tool in OUTILS_LECTURE {
        assert_eq!(p.classify(tool), ToolClass::Read, "outil {tool}");
        assert!(p.autoriser_call(tool, &scopes(&[READ_SCOPE])).is_none());
    }
    for tool in OUTILS_ECRITURE {
        assert_eq!(p.classify(tool), ToolClass::Write, "outil {tool}");
        assert!(p.autoriser_call(tool, &scopes(&[READ_SCOPE])).is_some());
        assert!(p
            .autoriser_call(tool, &scopes(&[READ_SCOPE, WRITE_SCOPE]))
            .is_none());
    }
    assert_eq!(p.classify("orch_admin"), ToolClass::Unknown);
    assert!(p
        .autoriser_call("orch_admin", &scopes(&[READ_SCOPE, WRITE_SCOPE]))
        .is_some());
}

#[test]
fn visibles_14_20() {
    let p = policy();
    assert_eq!(p.visibles(&scopes(&[READ_SCOPE])).len(), 14);
    assert_eq!(p.visibles(&scopes(&[READ_SCOPE, WRITE_SCOPE])).len(), 20);
    assert!(p.visibles(&scopes(&[])).is_empty());
}

#[test]
fn testkit_releve_outil_modifie() {
    let ancien = serde_json::json!({
        "jsonrpc": "2.0", "id": 1,
        "result": {"tools": [
            {"name": "agent_job_list", "description": "d", "inputSchema": {"type": "object"}},
        ]}
    });
    let mut nouveau = ancien.clone();
    nouveau["result"]["tools"][0]["description"] = serde_json::json!("x");
    let diffs = compare_tools_list(&ancien, &nouveau);
    assert!(
        diffs.iter().any(|d| d.contains("agent_job_list")),
        "{diffs:?}"
    );
}

#[test]
fn cache_resulttype_force_prive() {
    let corps = br#"{"jsonrpc":"2.0","id":1,"result":{"tools":[],"resultType":"x","cacheScope":"public","ttlMs":60}}"#;
    let (out, lisible) = filtrer_liste(corps, "application/json", &scopes(&[READ_SCOPE]));
    assert!(lisible);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["result"]["cacheScope"], "private");
    assert_eq!(v["result"]["ttlMs"], 0);
}

#[test]
fn reponse_non_liste_relayee_lisible() {
    let corps = br#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#;
    let (out, lisible) = filtrer_liste(corps, "application/json", &scopes(&[READ_SCOPE]));
    assert!(lisible);
    assert_eq!(out, corps);
}

fn oauth_cfg() -> mcp_auth::oauth::OAuthConfig {
    mcp_auth::oauth::OAuthConfig {
        issuer: "https://mymcps.duckdns.org/oauth/orch".to_string(),
        resource_url: orch_gateway_rs::RESOURCE_URL.to_string(),
        resource_name: orch_gateway_rs::RESOURCE_NAME.to_string(),
        default_scope: READ_SCOPE.to_string(),
        valid_scopes: vec![READ_SCOPE.to_string(), WRITE_SCOPE.to_string()],
        extra_submit_scopes: vec![WRITE_SCOPE.to_string()],
        consent_hash: String::new(),
        static_client_id: orch_gateway_rs::STATIC_CLIENT_ID.to_string(),
    }
}

fn gateway_test() -> axum::Router {
    use orch_gateway_rs::{build_router, ServiceConfig};

    build_router(ServiceConfig {
        upstream: "http://127.0.0.1:9".to_string(),
        static_token: "x".repeat(32),
        static_token_scopes: vec![READ_SCOPE.to_string(), WRITE_SCOPE.to_string()],
        oauth: oauth_cfg(),
        max_body_bytes: 1024 * 1024,
    })
    .expect("gateway de test")
}

#[tokio::test]
async fn mcp_sans_bearer_401_avec_prm_orch() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = gateway_test();
    let res = app
        .oneshot(Request::post("/mcp").body(Body::from("{}")).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let challenge = res.headers()["www-authenticate"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        challenge.contains("oauth-protected-resource/orch/mcp"),
        "{challenge}"
    );
}

#[tokio::test]
async fn health_et_prm_orch() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = gateway_test();
    let res = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["status"], "ok");
    assert_eq!(v["service"], "orch-gateway-rs");

    let app = gateway_test();
    let res = app
        .oneshot(
            Request::get(orch_gateway_rs::PRM_ALIAS)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 8192).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["resource"], orch_gateway_rs::RESOURCE_URL);
}

#[tokio::test]
async fn methode_inconnue_32601_avant_upstream() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = gateway_test();
    let body = r#"{"jsonrpc":"2.0","id":7,"method":"drop_database","params":{}}"#;
    let res = app
        .oneshot(
            Request::post("/mcp")
                .header("authorization", format!("Bearer {}", "x".repeat(32)))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], -32601);
}

/// Pont fichier : une session Python existante (synthetique) passe le
/// middleware ; un opaque inconnu reste 401.
#[tokio::test]
async fn pont_fichier_session_existante_passe_middleware() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use orch_gateway_rs::{build_router_with_filestore, ServiceConfig};
    use tower::ServiceExt;

    let dir = std::env::temp_dir().join(format!(
        "orch-pont-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let etat = dir.join("etat.json");
    let tok = "synthetique-orch-pont-01234567890001";
    let doc = serde_json::json!({
        "demandes": {}, "codes": {},
        "acces": { tok: {
            "jeton": tok, "client_id": "client-synth",
            "scopes": ["orch:lecture"], "resource": serde_json::Value::Null,
            "expire_a": 9_999_999_999i64 } },
        "rafraichissements": {},
    });
    std::fs::write(&etat, doc.to_string()).unwrap();
    let app = build_router_with_filestore(
        ServiceConfig {
            upstream: "http://127.0.0.1:9".to_string(),
            static_token: "x".repeat(32),
            static_token_scopes: vec![READ_SCOPE.to_string()],
            oauth: oauth_cfg(),
            max_body_bytes: 1024 * 1024,
        },
        Some(mcp_gateway::router::FileStoreMount {
            etat_path: etat.to_string_lossy().to_string(),
            expected_resource: None,
        }),
    )
    .expect("gateway de test");
    // Methode inconnue : le refus local -32601 prouve l'auth OK (pas 401).
    let body = r#"{"jsonrpc":"2.0","id":7,"method":"drop_database","params":{}}"#;
    let res = app
        .oneshot(
            Request::post("/mcp")
                .header("authorization", format!("Bearer {tok}"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], -32601);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn cle_dupliquee_32600_avant_upstream() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = gateway_test();
    let body = r#"{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{"a":1,"a":2}}"#;
    let res = app
        .oneshot(
            Request::post("/mcp")
                .header("authorization", format!("Bearer {}", "x".repeat(32)))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], -32600);
}

#[tokio::test]
async fn enveloppe_moderne_sous_entete_historique_400() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = gateway_test();
    let body = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"agent_job_get","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}"#;
    let res = app
        .oneshot(
            Request::post("/mcp")
                .header("authorization", format!("Bearer {}", "x".repeat(32)))
                .header("content-type", "application/json")
                .header("mcp-protocol-version", "2025-11-25")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], -32020);
}

#[tokio::test]
async fn quirk_chatgpt_rabaisse_et_acteur_injecte() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let mock = axum::Router::new().route(
        "/mcp",
        axum::routing::post(|req: axum::extract::Request| async move {
            let h = req.headers();
            let version = h
                .get("mcp-protocol-version")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            assert_eq!(version, "2025-11-25", "quirk rabaisse");
            let acteur = h
                .get("x-orch-mcp-acteur")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            assert_eq!(acteur, orch_gateway_rs::STATIC_CLIENT_ID);
            let mode = h
                .get("x-orch-mcp-mode")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            assert_eq!(mode, "cli");
            // Pas d'Authorization client vers l'upstream (aucune).
            assert!(h.get("authorization").is_none());
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#,
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

    use orch_gateway_rs::{build_router, ServiceConfig};
    let app = build_router(ServiceConfig {
        upstream: format!("http://127.0.0.1:{}", addr.port()),
        static_token: "x".repeat(32),
        static_token_scopes: vec![READ_SCOPE.to_string(), WRITE_SCOPE.to_string()],
        oauth: oauth_cfg(),
        max_body_bytes: 1024 * 1024,
    })
    .unwrap();

    let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
    let res = app
        .oneshot(
            Request::post("/mcp")
                .header("authorization", format!("Bearer {}", "x".repeat(32)))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2026-07-28")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn tools_list_illisible_32603() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let mock = axum::Router::new().route(
        "/mcp",
        axum::routing::post(|_req: axum::extract::Request| async move {
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                "ceci n'est pas du json",
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

    use orch_gateway_rs::{build_router, ServiceConfig};
    let app = build_router(ServiceConfig {
        upstream: format!("http://127.0.0.1:{}", addr.port()),
        static_token: "x".repeat(32),
        static_token_scopes: vec![READ_SCOPE.to_string(), WRITE_SCOPE.to_string()],
        oauth: oauth_cfg(),
        max_body_bytes: 1024 * 1024,
    })
    .unwrap();

    let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
    let res = app
        .oneshot(
            Request::post("/mcp")
                .header("authorization", format!("Bearer {}", "x".repeat(32)))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], -32603);
}

#[test]
fn ere_matrice_quirk_et_moderne() {
    let d = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{}});
    let c = era::classer_ere(Some("2026-07-28"), Some(&d));
    assert_eq!(c.relayee.as_deref(), Some("2025-11-25"));
    assert!(c.rabaissee);
    let m = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}});
    let c = era::classer_ere(Some("2026-07-28"), Some(&m));
    assert_eq!(c.relayee.as_deref(), Some("2026-07-28"));
    assert!(!c.rabaissee);
}
