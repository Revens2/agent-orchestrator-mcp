//! Ere protocolaire duale + durcissements du relais tasks.
//!
//! Transcription de `tasks_gateway/upstream.py` + `mcp_types/version`
//! (mcp==2.2.0 fige cote Python) — metier local au depot, jamais dans le
//! framework :
//! * registres de versions : handshake 2024-11-05/2025-03-26/2025-06-18/
//!   2025-11-25 (derniere a handshake : 2025-11-25), moderne 2026-07-28 ;
//! * `classer_ere` : le corps decide ; enveloppe 2026-07-28 jamais rabaissee
//!   (refus -32020 sous en-tete historique), forme historique sous en-tete
//!   moderne rabaissee vers 2025-11-25 (methodes historiques, notifications,
//!   reponses, requetes sans corps) ;
//! * JSON strict : cle dupliquee (n'importe quel niveau) refusee -32600 ;
//! * allowlist des methodes servies par l'upstream SDK 2.2.0 (sinon -32601) ;
//! * en-tetes MCP dupliques refuses -32020 (HTTP 400).

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use std::fmt;

/// Revisions atteignables via le handshake `initialize` (mcp 2.2.0).
pub const HANDSHAKE_VERSIONS: &[&str] = &["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"];
/// Derniere version a handshake (= cible des rabaissements).
pub const LATEST_HANDSHAKE: &str = "2025-11-25";
/// Revisions a enveloppe moderne (jamais rabaissees).
pub const MODERN_VERSIONS: &[&str] = &["2026-07-28"];
/// Cle `_meta` qui porte la version de protocole cote client recent.
pub const META_VERSION_KEY: &str = "io.modelcontextprotocol/protocolVersion";

/// Methodes servies par l'upstream (SDK 2.2.0) via handshake historique.
pub const METHODES_HISTORIQUES: &[&str] = &[
    "initialize",
    "ping",
    "tools/list",
    "tools/call",
    "resources/list",
    "resources/templates/list",
    "resources/read",
    "resources/subscribe",
    "resources/unsubscribe",
    "prompts/list",
    "prompts/get",
    "logging/setLevel",
    "completion/complete",
];
/// Methodes propres a l'ere 2026-07-28 (jamais rabaissees).
pub const METHODES_MODERNES: &[&str] = &["server/discover", "subscriptions/listen"];

/// En-tetes de requete retransmis (+ tout `mcp-param-*`, requis par le 2026-07-28).
pub const ENTETES_REQUETE: &[&str] = &[
    "content-type",
    "accept",
    "mcp-session-id",
    "mcp-protocol-version",
    "mcp-method",
    "mcp-name",
    "last-event-id",
    "user-agent",
];
pub const PREFIXE_PARAM: &str = "mcp-param-";
/// En-tetes MCP dont un doublon rend ambigu ce que voient politique et upstream.
pub const ENTETES_UNIQUES: &[&str] = &[
    "mcp-session-id",
    "mcp-protocol-version",
    "mcp-method",
    "mcp-name",
];
/// En-tetes de reponse retransmis au client.
pub const ENTETES_REPONSE: &[&str] = &["content-type", "mcp-session-id", "cache-control"];

// --- JSON strict (cles dupliquees refusees) -----------------------------------

/// Erreur de parsing strict : JSON illisible OU cle dupliquee.
#[derive(Debug, PartialEq, Eq)]
pub enum StrictError {
    Parse,
    Duplicate(String),
}

struct Checked(Value);

struct CheckedVisitor;

impl<'de> Visitor<'de> for CheckedVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("une valeur JSON sans cle dupliquee")
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Value, E> {
        Ok(Value::Number(
            serde_json::Number::from_f64(v).unwrap_or(0.into()),
        ))
    }
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_string()))
    }
    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        Deserialize::deserialize(d)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = seq.next_element::<Checked>()? {
            out.push(v.0);
        }
        Ok(Value::Array(out))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut o = Map::new();
        while let Some(k) = map.next_key::<String>()? {
            if o.contains_key(&k) {
                return Err(serde::de::Error::custom(format!("cle dupliquee: {k}")));
            }
            let v: Checked = map.next_value()?;
            o.insert(k, v.0);
        }
        Ok(Value::Object(o))
    }
}

impl<'de> Deserialize<'de> for Checked {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(CheckedVisitor).map(Checked)
    }
}

/// `json.loads` strict : toute cle dupliquee (n'importe quel niveau) est refusee.
pub fn parse_strict(body: &[u8]) -> Result<Value, StrictError> {
    let mut de = serde_json::Deserializer::from_slice(body);
    Checked::deserialize(&mut de).map(|c| c.0).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("cle dupliquee:") {
            StrictError::Duplicate(msg)
        } else {
            StrictError::Parse
        }
    })
}

// --- ere ---------------------------------------------------------------------

/// Resultat de `classer_ere` : (version relayee, raison de refus, rabaissee).
pub struct Classement {
    pub relayee: Option<String>,
    pub refus: Option<String>,
    pub rabaissee: bool,
}

/// Decide l'en-tete `MCP-Protocol-Version` relaye a l'upstream.
/// Miroir exact de `classer_ere` (corps d'abord).
pub fn classer_ere(version: Option<&str>, donnees: Option<&Value>) -> Classement {
    let brute = version.map(str::trim);
    let methode = donnees
        .and_then(|d| d.as_object())
        .and_then(|o| o.get("method"))
        .and_then(Value::as_str);
    let meta = donnees
        .and_then(|d| d.as_object())
        .and_then(|o| o.get("params"))
        .and_then(Value::as_object)
        .and_then(|p| p.get("_meta"))
        .and_then(Value::as_object);
    if meta.is_some_and(|m| m.contains_key(META_VERSION_KEY)) {
        if brute.is_none() || brute.is_some_and(|v| HANDSHAKE_VERSIONS.contains(&v)) {
            return Classement {
                relayee: version.map(str::to_string),
                refus: Some(
                    "enveloppe 2026-07-28 (params._meta protocolVersion) sans en-tete \
                     MCP-Protocol-Version moderne"
                        .to_string(),
                ),
                rabaissee: false,
            };
        }
        return Classement {
            relayee: version.map(str::to_string),
            refus: None,
            rabaissee: false,
        };
    }
    if brute.is_some_and(|v| MODERN_VERSIONS.contains(&v))
        && (methode.is_none()
            || methode.is_some_and(|m| {
                METHODES_HISTORIQUES.contains(&m) || m.starts_with("notifications/")
            }))
    {
        return Classement {
            relayee: Some(LATEST_HANDSHAKE.to_string()),
            refus: None,
            rabaissee: true,
        };
    }
    Classement {
        relayee: version.map(str::to_string),
        refus: None,
        rabaissee: false,
    }
}

/// Methode servie par l'upstream (allowlist + `notifications/*`).
pub fn methode_connue(methode: &str) -> bool {
    METHODES_HISTORIQUES.contains(&methode)
        || METHODES_MODERNES.contains(&methode)
        || methode.starts_with("notifications/")
}

/// Nom du premier en-tete MCP present plusieurs fois, sinon `None`.
/// Les paires sont (nom minuscule, valeur), doublons conserves.
pub fn entete_duplique(headers: &[(String, String)]) -> Option<String> {
    let mut vus = std::collections::HashSet::new();
    for (nom, _) in headers {
        if (ENTETES_UNIQUES.contains(&nom.as_str()) || nom.starts_with(PREFIXE_PARAM))
            && !vus.insert(nom.clone())
        {
            return Some(nom.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strict_accepte_json_simple() {
        assert!(parse_strict(br#"{"a":1,"b":[true,null]}"#).is_ok());
        assert!(matches!(
            parse_strict(b"pas du json"),
            Err(StrictError::Parse)
        ));
    }

    #[test]
    fn strict_refuse_cle_dupliquee_imbriquee() {
        match parse_strict(br#"{"params":{"name":"x","name":"y"}}"#) {
            Err(StrictError::Duplicate(_)) => {}
            autre => panic!("attendu Duplicate, recu {autre:?}"),
        }
    }

    #[test]
    fn ere_quirk_chatgpt_rabaisse() {
        let d = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
        let c = classer_ere(Some("2026-07-28"), Some(&d));
        assert_eq!(c.relayee.as_deref(), Some("2025-11-25"));
        assert!(c.refus.is_none() && c.rabaissee);
    }

    #[test]
    fn ere_enveloppe_moderne_jamais_rabaissee() {
        let d = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{META_VERSION_KEY:"2026-07-28"}}});
        let c = classer_ere(Some("2026-07-28"), Some(&d));
        assert_eq!(c.relayee.as_deref(), Some("2026-07-28"));
        assert!(c.refus.is_none() && !c.rabaissee);
    }

    #[test]
    fn ere_enveloppe_moderne_sous_entete_historique_refusee() {
        let d = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"tasks_get","_meta":{META_VERSION_KEY:"2026-07-28"}}});
        for h in [
            None,
            Some("2025-06-18"),
            Some("2025-11-25"),
            Some("2024-11-05"),
        ] {
            let c = classer_ere(h, Some(&d));
            assert!(c.refus.is_some() && !c.rabaissee, "en-tete {h:?}");
        }
    }

    #[test]
    fn ere_methode_moderne_intacte() {
        let d = json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}});
        let c = classer_ere(Some("2026-07-28"), Some(&d));
        assert_eq!(c.relayee.as_deref(), Some("2026-07-28"));
        assert!(!c.rabaissee);
    }

    #[test]
    fn ere_historique_tel_quel() {
        let d = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
        let c = classer_ere(Some("2025-06-18"), Some(&d));
        assert_eq!(c.relayee.as_deref(), Some("2025-06-18"));
        let c = classer_ere(None, Some(&d));
        assert!(c.relayee.is_none());
    }

    #[test]
    fn allowlist_methodes() {
        assert!(methode_connue("initialize"));
        assert!(methode_connue("server/discover"));
        assert!(methode_connue("notifications/initialized"));
        assert!(!methode_connue("drop_database"));
    }

    #[test]
    fn doublon_entete_mcp_detecte() {
        let h = vec![
            ("mcp-session-id".to_string(), "a".to_string()),
            ("mcp-session-id".to_string(), "b".to_string()),
        ];
        assert_eq!(entete_duplique(&h).as_deref(), Some("mcp-session-id"));
        let h2 = vec![("content-type".to_string(), "a".to_string())];
        assert!(entete_duplique(&h2).is_none());
    }
}
