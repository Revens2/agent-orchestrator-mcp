"""Rotation des jetons de rafraichissement, detection de rejeu et revocation
(contrat vault-mcp#11, propage par vault-mcp#13).

Contrat verifie :
- rotation stricte : un refresh ne s'echange qu'une fois ;
- rejeu d'un refresh consomme (hors grace) -> famille entiere revoquee (acces compris) ;
- grace configurable (defaut 60 s) : rejeu tolere dans la fenetre, puis detection ;
- duree glissante bornee par une duree absolue de famille ;
- jetons herites (sans famille, ou 10 ans) : toujours acceptes, ramenes a la borne ;
- revocation RFC 7009 via /revoke, et revocation administrative par client ;
- configuration par environnement fail-safe (valeur invalide -> defaut).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from mcp.server.auth.provider import TokenError
from mcp.server.auth.routes import create_auth_routes
from mcp.server.auth.settings import ClientRegistrationOptions, RevocationOptions
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyHttpUrl
from starlette.applications import Starlette

from orch_gateway import oauth
from orch_gateway.oauth import FournisseurOAuth, MagasinOAuth

# Pas de mode asyncio auto dans ce depot : plugin pytest d anyio (dependance du SDK).
pytestmark = pytest.mark.anyio

SECRET_CLIENT = "s" * 32
JOUR = 24 * 3600


@pytest.fixture(autouse=True)
def _env_propre(monkeypatch: pytest.MonkeyPatch) -> None:
    for nom in (
        "ORCH_GW_OAUTH_REFRESH_TTL_S",
        "ORCH_GW_OAUTH_REFRESH_MAX_LIFETIME_S",
        "ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S",
    ):
        monkeypatch.delenv(nom, raising=False)


@pytest.fixture
def strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rotation stricte (grace 0) : tout rejeu immediat est une detection."""
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "0")


def _client(client_id: str = "client-a") -> OAuthClientInformationFull:
    return OAuthClientInformationFull(
        client_id=client_id,
        client_secret=SECRET_CLIENT,
        redirect_uris=["https://client.example/callback"],
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        token_endpoint_auth_method="client_secret_post",
        scope="orch:lecture",
    )


@pytest.fixture
def fournisseur(tmp_path: Path) -> FournisseurOAuth:
    magasin = MagasinOAuth(repertoire=tmp_path, clients=tmp_path / "clients.json")
    magasin.enregistrer_client(_client("client-a"))
    magasin.enregistrer_client(_client("client-b"))
    return FournisseurOAuth("https://exemple.test", magasin=magasin)


async def _rafraichir(f: FournisseurOAuth, jeton: str, client_id: str = "client-a") -> Any:
    client = _client(client_id)
    charge = await f.load_refresh_token(client, jeton)
    if charge is None:
        return None
    return await f.exchange_refresh_token(client, charge, [])


# --- Rotation et rejeu -------------------------------------------------------------------
async def test_rotation_stricte_le_refresh_ne_sert_qu_une_fois(fournisseur):
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    assert second is not None
    assert second.refresh_token != premier.refresh_token
    assert fournisseur.magasin.lire_rafraichissement(premier.refresh_token) is None
    assert fournisseur.magasin.lire_rafraichissement(second.refresh_token) is not None


async def test_rejeu_revoque_toute_la_famille(fournisseur, strict, caplog):
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    # L'attaquant (ou le client legitime) rejoue l'ancien refresh.
    with caplog.at_level("WARNING", logger="orch_gateway.oauth"):
        assert await _rafraichir(fournisseur, premier.refresh_token) is None
    # Toute la famille tombe : refresh courant ET jeton d'acces courant.
    assert fournisseur.magasin.lire_rafraichissement(second.refresh_token) is None
    assert await fournisseur.load_access_token(second.access_token) is None
    assert await _rafraichir(fournisseur, second.refresh_token) is None
    # Alarme journalisee, sans jamais ecrire un jeton.
    assert "rejeu" in caplog.text
    for jeton in (premier.refresh_token, second.refresh_token, second.access_token):
        assert jeton not in caplog.text


async def test_rejeu_ne_touche_pas_les_autres_familles(fournisseur, strict):
    a = fournisseur._emettre("client-a", ["orch:lecture"], None)
    autre = fournisseur._emettre("client-a", ["orch:lecture"], None)
    await _rafraichir(fournisseur, a.refresh_token)
    await _rafraichir(fournisseur, a.refresh_token)  # rejeu -> famille de `a` revoquee
    assert fournisseur.magasin.lire_rafraichissement(autre.refresh_token) is not None
    assert await fournisseur.load_access_token(autre.access_token) is not None


async def test_un_autre_client_ne_peut_pas_faire_tomber_une_famille(fournisseur):
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    assert await _rafraichir(fournisseur, premier.refresh_token, client_id="client-b") is None
    assert fournisseur.magasin.lire_rafraichissement(second.refresh_token) is not None


async def test_echange_concurrent_du_meme_refresh_ne_produit_qu_une_paire(fournisseur, strict):
    """Les deux requetes passent `load_refresh_token` avant que l'une n'echange :
    la rotation atomique n'en laisse aboutir qu'une."""
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    client = _client()
    charge_1 = await fournisseur.load_refresh_token(client, premier.refresh_token)
    charge_2 = await fournisseur.load_refresh_token(client, premier.refresh_token)
    assert await fournisseur.exchange_refresh_token(client, charge_1, []) is not None
    with pytest.raises(TokenError) as erreur:
        await fournisseur.exchange_refresh_token(client, charge_2, [])
    assert erreur.value.error == "invalid_grant"


async def test_grace_tolere_un_rejeu_immediat(fournisseur, monkeypatch):
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "30")
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    # Reponse perdue : le client rejoue immediatement l'ancien refresh.
    troisieme = await _rafraichir(fournisseur, premier.refresh_token)
    assert troisieme is not None
    # La paire jamais recue est remplacee : un seul refresh vivant par famille.
    assert fournisseur.magasin.lire_rafraichissement(second.refresh_token) is None
    assert await fournisseur.load_access_token(second.access_token) is None
    famille = fournisseur.magasin.lire_rafraichissement(troisieme.refresh_token)["famille"]
    vivants = fournisseur.magasin._etat()["rafraichissements"].values()
    assert sum(1 for d in vivants if d.get("famille") == famille) == 1


async def test_grace_ne_sert_qu_une_fois(fournisseur, monkeypatch):
    """Rejeux successifs dans la fenetre : le second est un rejeu -> famille revoquee."""
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "30")
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    await _rafraichir(fournisseur, premier.refresh_token)
    troisieme = await _rafraichir(fournisseur, premier.refresh_token)
    assert await _rafraichir(fournisseur, premier.refresh_token) is None
    assert fournisseur.magasin.lire_rafraichissement(troisieme.refresh_token) is None
    assert await fournisseur.load_access_token(troisieme.access_token) is None


async def test_successeur_remplace_presente_ensuite_revoque_la_famille(fournisseur, monkeypatch):
    """Attaquant rejoue T1 dans la grace avant le client legitime : quand celui-ci
    presente T2 (remplace), le rejeu est detecte et la famille de l'attaquant tombe."""
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "30")
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    legitime = await _rafraichir(fournisseur, premier.refresh_token)
    attaquant = await _rafraichir(fournisseur, premier.refresh_token)
    assert await _rafraichir(fournisseur, legitime.refresh_token) is None
    assert fournisseur.magasin.lire_rafraichissement(attaquant.refresh_token) is None


async def test_grace_refusee_si_le_successeur_a_deja_tourne(fournisseur, monkeypatch):
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "30")
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    troisieme = await _rafraichir(fournisseur, second.refresh_token)
    with pytest.raises(TokenError) as erreur:
        await _rafraichir(fournisseur, premier.refresh_token)
    assert erreur.value.error == "invalid_grant"
    assert fournisseur.magasin.lire_rafraichissement(troisieme.refresh_token) is not None


async def test_rejeu_apres_la_grace_revoque(fournisseur, monkeypatch):
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "30")
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    reel = time.time
    monkeypatch.setattr(oauth.time, "time", lambda: reel() + 31)
    assert await _rafraichir(fournisseur, premier.refresh_token) is None
    assert fournisseur.magasin.lire_rafraichissement(second.refresh_token) is None


# --- Durees ------------------------------------------------------------------------------
async def test_duree_glissante_bornee_par_la_famille(fournisseur, monkeypatch):
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_TTL_S", str(30 * JOUR))
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_MAX_LIFETIME_S", str(40 * JOUR))
    reel = time.time
    t0 = reel()
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    donnees = fournisseur.magasin.lire_rafraichissement(premier.refresh_token)
    assert donnees["expire_a"] == pytest.approx(t0 + 30 * JOUR, abs=5)
    assert donnees["famille_expire_a"] == pytest.approx(t0 + 40 * JOUR, abs=5)

    # 20 jours plus tard : glissement plafonne a la fin de famille (J40), pas J50.
    monkeypatch.setattr(oauth.time, "time", lambda: t0 + 20 * JOUR)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    suite = fournisseur.magasin.lire_rafraichissement(second.refresh_token)
    assert suite["expire_a"] == donnees["famille_expire_a"]

    # Au-dela de la duree absolue : plus de refresh possible, re-consentement.
    monkeypatch.setattr(oauth.time, "time", lambda: t0 + 41 * JOUR)
    assert await _rafraichir(fournisseur, second.refresh_token) is None


def test_configuration_invalide_retombe_sur_les_defauts(monkeypatch):
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_TTL_S", "dix ans")
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_MAX_LIFETIME_S", "-1")
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "86400")
    assert oauth.duree_rafraichissement_s() == 30 * JOUR
    assert oauth.duree_max_famille_s() == 90 * JOUR
    assert oauth.grace_rejeu_s() == 60


def test_grace_par_defaut_60_s_et_strict_configurable(monkeypatch):
    assert oauth.grace_rejeu_s() == 60
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", "0")
    assert oauth.grace_rejeu_s() == 0


async def test_grace_par_defaut_tolere_le_retry_puis_detecte(fournisseur, monkeypatch):
    """Sans configuration : retry immediat tolere (ChatGPT), rejeu a +61 s revoque."""
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    assert await _rafraichir(fournisseur, premier.refresh_token) is not None
    reel = time.time
    monkeypatch.setattr(oauth.time, "time", lambda: reel() + 61)
    assert await _rafraichir(fournisseur, premier.refresh_token) is None
    assert fournisseur.magasin.lire_rafraichissement(second.refresh_token) is None


def test_duree_glissante_jamais_superieure_a_la_duree_absolue(monkeypatch):
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_TTL_S", str(365 * JOUR))
    monkeypatch.setenv("ORCH_GW_OAUTH_REFRESH_MAX_LIFETIME_S", str(60 * JOUR))
    assert oauth.duree_rafraichissement_s() == 60 * JOUR


# --- Compatibilite : jetons herites --------------------------------------------------------
def _poser_herite(magasin: MagasinOAuth, jeton: str, expire_a: int) -> None:
    """Jeton au format d'avant #11 : ni famille, ni duree absolue."""
    magasin.poser_jetons(
        {"jeton": jeton + "-acces", "client_id": "client-a", "scopes": ["orch:lecture"],
         "expire_a": int(time.time()) + 3600},
        {"jeton": jeton, "client_id": "client-a", "scopes": ["orch:lecture"], "expire_a": expire_a},
    )


async def test_jeton_herite_reste_accepte_et_entre_dans_une_famille(fournisseur, strict):
    _poser_herite(fournisseur.magasin, "herite-30j", int(time.time()) + 10 * JOUR)
    suite = await _rafraichir(fournisseur, "herite-30j")
    assert suite is not None
    donnees = fournisseur.magasin.lire_rafraichissement(suite.refresh_token)
    assert donnees["famille"]
    assert donnees["famille_expire_a"] == pytest.approx(time.time() + 90 * JOUR, abs=5)
    # Et le rejeu du jeton herite est desormais detecte.
    assert await _rafraichir(fournisseur, "herite-30j") is None
    assert fournisseur.magasin.lire_rafraichissement(suite.refresh_token) is None


def test_jeton_herite_de_10_ans_est_ramene_a_la_duree_glissante(fournisseur):
    """Arbres divergents (rag-v2@66927cb) : refresh a 10 ans."""
    _poser_herite(fournisseur.magasin, "herite-10ans", int(time.time()) + 10 * 365 * JOUR)
    donnees = fournisseur.magasin.lire_rafraichissement("herite-10ans")
    assert donnees is not None
    assert donnees["expire_a"] <= time.time() + 30 * JOUR + 5


# --- Revocation ----------------------------------------------------------------------------
async def test_revoke_token_revoque_la_famille(fournisseur):
    paire = fournisseur._emettre("client-a", ["orch:lecture"], None)
    charge = await fournisseur.load_refresh_token(_client(), paire.refresh_token)
    await fournisseur.revoke_token(charge)
    assert await fournisseur.load_access_token(paire.access_token) is None
    assert fournisseur.magasin.lire_rafraichissement(paire.refresh_token) is None


async def test_revoke_ferme_la_grace_des_refresh_consommes(fournisseur):
    """Rotation, /revoke du refresh courant, puis rejeu de l'ancien DANS la grace :
    la famille revoquee ne doit pas ressusciter."""
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    charge = await fournisseur.load_refresh_token(_client(), second.refresh_token)
    await fournisseur.revoke_token(charge)
    assert await _rafraichir(fournisseur, premier.refresh_token) is None


async def test_revoke_d_un_refresh_consomme_revoque_sa_famille(fournisseur):
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    second = await _rafraichir(fournisseur, premier.refresh_token)
    assert fournisseur.magasin.revoquer_famille_du_jeton(premier.refresh_token) > 0
    assert fournisseur.magasin.lire_rafraichissement(second.refresh_token) is None
    assert await fournisseur.load_access_token(second.access_token) is None
    assert await fournisseur.load_access_token(premier.access_token) is None
    assert await _rafraichir(fournisseur, premier.refresh_token) is None


async def test_revocation_admin_ferme_la_grace(fournisseur):
    premier = fournisseur._emettre("client-a", ["orch:lecture"], None)
    await _rafraichir(fournisseur, premier.refresh_token)
    fournisseur.magasin.revoquer_client("client-a")
    assert await _rafraichir(fournisseur, premier.refresh_token) is None


def test_revocation_administrative_par_client(fournisseur):
    a = fournisseur._emettre("client-a", ["orch:lecture"], None)
    b = fournisseur._emettre("client-b", ["orch:lecture"], None)
    assert fournisseur.magasin.resume()["client-a"] == {"acces": 1, "rafraichissements": 1}
    assert fournisseur.magasin.revoquer_client("client-a") == 2
    assert fournisseur.magasin.lire_rafraichissement(a.refresh_token) is None
    assert fournisseur.magasin.lire_rafraichissement(b.refresh_token) is not None
    assert fournisseur.magasin.revoquer_client(None) == 2


async def test_endpoints_token_et_revoke_du_sdk(fournisseur, strict):
    """Bout en bout a travers les handlers reels du SDK : /token puis /revoke."""
    application = Starlette(
        routes=create_auth_routes(
            fournisseur,
            AnyHttpUrl("https://exemple.test"),
            client_registration_options=ClientRegistrationOptions(enabled=True),
            revocation_options=RevocationOptions(enabled=True),
        )
    )
    paire = fournisseur._emettre("client-a", ["orch:lecture"], None)
    auth = {"client_id": "client-a", "client_secret": SECRET_CLIENT}
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="https://exemple.test") as http:
        meta = (await http.get("/.well-known/oauth-authorization-server")).json()
        assert meta["revocation_endpoint"] == "https://exemple.test/revoke"

        r = await http.post(
            "/token",
            data={**auth, "grant_type": "refresh_token", "refresh_token": paire.refresh_token},
        )
        assert r.status_code == 200, r.text
        nouveau = r.json()

        rejeu = await http.post(
            "/token",
            data={**auth, "grant_type": "refresh_token", "refresh_token": paire.refresh_token},
        )
        assert rejeu.status_code == 400
        assert rejeu.json()["error"] == "invalid_grant"
        # Le rejeu a revoque la famille : le refresh courant ne vaut plus rien.
        assert fournisseur.magasin.lire_rafraichissement(nouveau["refresh_token"]) is None

        autre = fournisseur._emettre("client-a", ["orch:lecture"], None)
        rev = await http.post(
            "/revoke",
            data={**auth, "token": autre.refresh_token, "token_type_hint": "refresh_token"},
        )
        assert rev.status_code == 200
        assert await fournisseur.load_access_token(autre.access_token) is None


def test_revocation_annoncee_par_la_passerelle(monkeypatch, tmp_path):
    from starlette.testclient import TestClient

    from orch_gateway.app import construire_application

    monkeypatch.setenv("ORCH_GW_ISSUER", "https://exemple.test/oauth/svc")
    monkeypatch.setenv("ORCH_GW_UPSTREAM", "http://127.0.0.1:9")
    monkeypatch.setenv("ORCH_GW_OAUTH_DIR", str(tmp_path))
    app = construire_application(jeton_statique="", repertoire_oauth=tmp_path)
    meta = TestClient(app).get("/.well-known/oauth-authorization-server").json()
    assert meta["revocation_endpoint"].endswith("/revoke")
