"""Serveur d'autorisation OAuth 2.1 de la passerelle orch, colocalise avec la ressource.

Copie-adaptation du pattern `vault-mcp` (adr/0015) : la specification MCP 2025-11-25 fait
du serveur MCP un *resource server* et autorise un *authorization server* colocalise.
On reprend la meme structure que `vault_mcp/oauth.py` pour que ChatGPT (et tout client
externe) puisse se connecter exactement comme au MCP RAG.

Ce que les connecteurs imposent (claude.ai, ChatGPT), et qui dicte la forme de ce module :

- **`authorization_code` + PKCE S256 uniquement**, jamais de `client_credentials` pur :
  chaque connexion exige un consentement humain (page `/consentement`).
- **`/token` recoit du `x-www-form-urlencoded`**, `client_secret_post`.
- **401 obligatoire** avec `WWW-Authenticate: Bearer resource_metadata="..."`.

Secret client stocke en clair dans le registre (0600) : le SDK compare par
`hmac.compare_digest`, il lui faut la valeur. Meme niveau de protection que l'env 0600.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any

from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    OAuthAuthorizationServerProvider,
    RefreshToken,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

REPERTOIRE_DEFAUT = Path("/srv/orch/data/oauth")
# Le registre vit DANS le repertoire oauth : c'est le seul chemin, avec le code, que
# l'unite systemd laisse en ecriture (ProtectSystem=strict).
FICHIER_CLIENTS = Path("/srv/orch/data/oauth/clients.json")

PORTEE = "orch:lecture"
# Portee d'ECRITURE, distincte et jamais accordee par defaut (a l'inscription). La page
# de consentement la soumet TOUJOURS a l'approbation humaine (voir FournisseurOAuth.authorize).
PORTEE_ECRITURE = "orch:ecriture"
PORTEES = [PORTEE, PORTEE_ECRITURE]
# Client synthetique portant le jeton statique des clients CLI.
CLIENT_STATIQUE = "orch-cli-statique"


def portees_du_jeton_statique() -> list[str]:
    """Portees accordees au Bearer statique des clients CLI.

    Pilotees par `ORCH_GW_TOKEN_SCOPES` (portees separees par des espaces),
    defaut `orch:lecture`. Une portee inconnue est ignoree plutot que propagee.
    """
    brut = os.environ.get("ORCH_GW_TOKEN_SCOPES", PORTEE).split()
    portees = [portee for portee in brut if portee in PORTEES]
    return portees or [PORTEE]


# Un code d'autorisation ne doit vivre que le temps d'un aller-retour de redirection.
DUREE_CODE_S = 90
DUREE_ACCES_S = 3600
DUREE_RAFRAICHISSEMENT_S = 10 * 365 * 24 * 3600  # pas de limite pratique (23/09)
# Au-dela, une demande de consentement laissee ouverte est abandonnee.
DUREE_DEMANDE_S = 600

ITERATIONS_PBKDF2 = 600_000


class ConsentementError(RuntimeError):
    """Phrase de passe refusee, ou demande de consentement inconnue ou expiree."""


def repertoire_oauth() -> Path:
    return Path(os.environ.get("ORCH_GW_OAUTH_DIR", str(REPERTOIRE_DEFAUT)))


def fichier_clients() -> Path:
    """Registre des clients, par defaut sous le repertoire oauth envoye.

    Un seul repertoire pilotable (`ORCH_GW_OAUTH_DIR`) doit suffire en
    exploitation : `ORCH_GW_OAUTH_CLIENTS` reste disponible pour deroger.
    """
    explicite = os.environ.get("ORCH_GW_OAUTH_CLIENTS")
    if explicite:
        return Path(explicite)
    return repertoire_oauth() / "clients.json"


def hacher_phrase(phrase: str, sel: bytes | None = None) -> str:
    """Empreinte PBKDF2 d'une phrase de passe, au format `pbkdf2$iter$sel$empreinte`."""
    sel = sel or secrets.token_bytes(16)
    empreinte = hashlib.pbkdf2_hmac("sha256", phrase.encode(), sel, ITERATIONS_PBKDF2)
    return f"pbkdf2:{ITERATIONS_PBKDF2}:{sel.hex()}:{empreinte.hex()}"


def verifier_phrase(phrase: str, encode: str) -> bool:
    """Compare en temps constant. Un `==` laisserait mesurer la phrase octet par octet."""
    try:
        algo, iterations, sel_hex, attendu_hex = encode.split(":")
    except ValueError:
        return False
    if algo != "pbkdf2":
        return False
    empreinte = hashlib.pbkdf2_hmac(
        "sha256", phrase.encode(), bytes.fromhex(sel_hex), int(iterations)
    )
    return hmac.compare_digest(empreinte.hex(), attendu_hex)


class EtatOAuthCorrompu(RuntimeError):
    """Etat OAuth persistant illisible (JSON invalide ou erreur d'E/S).

    Fail-closed : on ne remplace JAMAIS silencieusement un etat illisible par un
    dictionnaire vide (cela revoquerait de fait toutes les sessions et ecraserait
    le bon etat au prochain write).
    """


@contextlib.contextmanager
def _verrou(cible: Path):
    """Verrou exclusif inter-processus (flock) autour d'un fichier d'etat.

    Le verrou vit dans un fichier `<etat>.lock` a cote de la cible. flock est
    porte par l'open file description : deux threads du meme processus (deux
    `open()` distincts) se bloquent aussi, pas seulement deux processus.
    """
    cible.parent.mkdir(parents=True, exist_ok=True)
    descripteur = open(cible.with_suffix(cible.suffix + ".lock"), "a+b")
    try:
        fcntl.flock(descripteur.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descripteur.fileno(), fcntl.LOCK_UN)
        finally:
            descripteur.close()


def _ecrire_atomique(cible: Path, donnees: Any) -> None:
    """Ecrit un fichier temporaire, fsync, puis renomme (et fsync le repertoire).

    A appeler SOUS `_verrou` (voir `_modifier`) : l'ecriture seule ne protege pas
    contre deux read-modify-write concurrents.
    """
    cible.parent.mkdir(parents=True, exist_ok=True)
    tmp = cible.with_suffix(cible.suffix + ".tmp")
    brut = json.dumps(donnees, ensure_ascii=False, indent=2).encode("utf-8")
    with open(tmp, "wb") as f:
        f.write(brut)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, 0o600)
    tmp.replace(cible)
    # Persiste le rename lui-meme (le nom de fichier vit dans le repertoire).
    try:
        descripteur = os.open(cible.parent, os.O_RDONLY)
        try:
            os.fsync(descripteur)
        finally:
            os.close(descripteur)
    except OSError:  # certains systemes ne permettent pas fsync sur un repertoire
        pass


def _modifier(cible: Path, defaut: Any, modification) -> Any:
    """Read-modify-write sous verrou exclusif, fail-closed sur lecture impossible.

    `modification(donnees)` mute `donnees` sur place et peut retourner une valeur
    (retournee a l'appelant).
    """
    with _verrou(cible):
        donnees = _lire(cible, defaut)
        resultat = modification(donnees)
        _ecrire_atomique(cible, donnees)
        return resultat


def _lire(cible: Path, defaut: Any) -> Any:
    """Lecture fail-closed : ENOENT -> defaut ; JSON invalide ou autre OSError ->
    EtatOAuthCorrompu (jamais un defaut silencieux qui permettrait un ecrasement)."""
    try:
        brut = cible.read_bytes()
    except FileNotFoundError:
        return defaut
    except OSError as exc:
        raise EtatOAuthCorrompu(f"etat OAuth illisible ({cible}): {exc.__class__.__name__}") from exc
    try:
        return json.loads(brut)
    except json.JSONDecodeError as exc:
        raise EtatOAuthCorrompu(f"etat OAuth JSON invalide ({cible})") from exc


class MagasinOAuth:
    """Etat persistant : demandes de consentement, codes, jetons.

    Sur disque plutot qu'en memoire : un redemarrage du service ne doit pas invalider
    les sessions en cours sans que le client puisse comprendre pourquoi.
    """

    def __init__(self, repertoire: Path | None = None, clients: Path | None = None) -> None:
        self._repertoire = repertoire or repertoire_oauth()
        self._fichier_clients = clients or fichier_clients()
        self._fichier_etat = self._repertoire / "etat.json"

    # --- clients ---------------------------------------------------------------------
    def client(self, client_id: str) -> OAuthClientInformationFull | None:
        for brut in _lire(self._fichier_clients, {}).get("clients", []):
            if brut.get("client_id") != client_id:
                continue
            return OAuthClientInformationFull(
                client_id=brut["client_id"],
                client_secret=brut.get("client_secret"),
                redirect_uris=brut.get("redirect_uris") or [],
                grant_types=brut.get("grant_types") or ["authorization_code", "refresh_token"],
                response_types=brut.get("response_types") or ["code"],
                token_endpoint_auth_method=brut.get("token_endpoint_auth_method")
                or "client_secret_post",
                scope=brut.get("scope") or PORTEE,
            )
        return None

    def enregistrer_client(self, client: OAuthClientInformationFull) -> None:
        def _ajouter(donnees: dict[str, Any]) -> None:
            clients = [c for c in donnees.get("clients", []) if c["client_id"] != client.client_id]
            clients.append(
                {
                    "client_id": client.client_id,
                    "client_secret": client.client_secret,
                    "redirect_uris": [str(u) for u in (client.redirect_uris or [])],
                    "grant_types": list(client.grant_types),
                    "response_types": list(client.response_types),
                    "token_endpoint_auth_method": client.token_endpoint_auth_method,
                    "scope": client.scope,
                    "cree_le": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            )
            donnees["clients"] = clients

        _modifier(self._fichier_clients, {"clients": []}, _ajouter)

    # --- etat ------------------------------------------------------------------------
    def _etat(self) -> dict[str, dict[str, Any]]:
        etat: dict[str, dict[str, Any]] = _lire(self._fichier_etat, {})
        for cle in ("demandes", "codes", "acces", "rafraichissements"):
            etat.setdefault(cle, {})
        return etat

    def _modifier_etat(self, modification) -> Any:
        """Read-modify-write de l'etat.json sous verrou, purge des expires incluse."""

        def _corps(etat: dict[str, dict[str, Any]]) -> Any:
            for cle in ("demandes", "codes", "acces", "rafraichissements"):
                etat.setdefault(cle, {})
            resultat = modification(etat)
            self._purger(etat)
            return resultat

        return _modifier(self._fichier_etat, {}, _corps)

    @staticmethod
    def _purger(etat: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Retire ce qui a expire : le fichier ne grossit pas indefiniment."""
        maintenant = int(time.time())
        for cle in ("demandes", "codes", "acces", "rafraichissements"):
            etat[cle] = {
                k: v for k, v in etat.get(cle, {}).items() if v.get("expire_a", 0) > maintenant
            }
        return etat

    def poser_demande(self, donnees: dict[str, Any]) -> str:
        identifiant = secrets.token_urlsafe(24)

        def _poser(etat: dict[str, dict[str, Any]]) -> None:
            etat["demandes"][identifiant] = {**donnees, "expire_a": int(time.time()) + DUREE_DEMANDE_S}

        self._modifier_etat(_poser)
        return identifiant

    def prendre_demande(self, identifiant: str) -> dict[str, Any] | None:
        """Lit **et retire** la demande : un consentement ne se rejoue pas."""
        demande = self._modifier_etat(
            lambda etat: etat["demandes"].pop(identifiant, None)
        )
        if demande is None:
            return None
        return demande if demande.get("expire_a", 0) > int(time.time()) else None

    def poser_code(self, code: str, donnees: dict[str, Any]) -> None:
        def _poser(etat: dict[str, dict[str, Any]]) -> None:
            etat["codes"][code] = {**donnees, "expire_a": int(time.time()) + DUREE_CODE_S}

        self._modifier_etat(_poser)

    def lire_code(self, code: str) -> dict[str, Any] | None:
        return self._etat()["codes"].get(code)

    def retirer_code(self, code: str) -> None:
        self._modifier_etat(lambda etat: etat["codes"].pop(code, None))

    def poser_jetons(self, acces: dict[str, Any], rafraichissement: dict[str, Any]) -> None:
        def _poser(etat: dict[str, dict[str, Any]]) -> None:
            etat["acces"][acces["jeton"]] = acces
            etat["rafraichissements"][rafraichissement["jeton"]] = rafraichissement

        self._modifier_etat(_poser)

    def lire_acces(self, jeton: str) -> dict[str, Any] | None:
        donnees = self._etat()["acces"].get(jeton)
        if donnees and donnees.get("expire_a", 0) <= int(time.time()):
            return None
        return donnees

    def lire_rafraichissement(self, jeton: str) -> dict[str, Any] | None:
        return self._etat()["rafraichissements"].get(jeton)

    def revoquer(self, jeton: str) -> None:
        def _revoquer(etat: dict[str, dict[str, Any]]) -> None:
            etat["acces"].pop(jeton, None)
            etat["rafraichissements"].pop(jeton, None)

        self._modifier_etat(_revoquer)


class FournisseurOAuth(
    OAuthAuthorizationServerProvider[AuthorizationCode, RefreshToken, AccessToken]
):
    """Implemente le contrat du SDK. Le consentement est delegue a la page servie par la
    passerelle elle-meme, d'ou la redirection renvoyee par `authorize`."""

    def __init__(
        self,
        emetteur: str,
        magasin: MagasinOAuth | None = None,
        jeton_statique: str = "",
    ) -> None:
        self._emetteur = emetteur.rstrip("/")
        self._magasin = magasin or MagasinOAuth()
        self._jeton_statique = jeton_statique

    @property
    def magasin(self) -> MagasinOAuth:
        return self._magasin

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return self._magasin.client(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        self._magasin.enregistrer_client(client_info)

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        # Pas de code ici : renvoi vers la page de consentement, qui authentifie
        # l'humain avant d'en creer un. Auto-approuver reviendrait a ce que quiconque
        # connait le client_id obtienne un jeton.
        demandees = list(params.scopes) if params.scopes else [PORTEE]
        if PORTEE_ECRITURE not in demandees:
            # Les connecteurs ne demandent que la portee d'enregistrement (lecture) par
            # defaut. On soumet TOUJOURS lecture+ecriture a l'approbation de la page :
            # jamais silencieux, et l'ecriture reste possible sans reconfigurer le client.
            demandees.append(PORTEE_ECRITURE)
        identifiant = self._magasin.poser_demande(
            {
                "client_id": client.client_id,
                "redirect_uri": str(params.redirect_uri),
                "redirect_uri_fourni": params.redirect_uri_provided_explicitly,
                "state": params.state,
                "code_challenge": params.code_challenge,
                "scopes": demandees,
                "resource": params.resource,
            }
        )
        return f"{self._emetteur}/consentement?demande={identifiant}"

    def creer_code(self, demande: dict[str, Any]) -> str:
        """Appelee par la page de consentement une fois la phrase de passe validee."""
        code = secrets.token_urlsafe(32)
        self._magasin.poser_code(code, demande)
        return code

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        donnees = self._magasin.lire_code(authorization_code)
        if donnees is None or donnees["client_id"] != client.client_id:
            return None
        return AuthorizationCode(
            code=authorization_code,
            scopes=donnees["scopes"],
            expires_at=donnees["expire_a"],
            client_id=donnees["client_id"],
            code_challenge=donnees["code_challenge"],
            redirect_uri=donnees["redirect_uri"],
            redirect_uri_provided_explicitly=donnees["redirect_uri_fourni"],
            resource=donnees.get("resource"),
        )

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        # Un code ne s'echange qu'une fois : le retirer avant d'emettre evite qu'un rejeu
        # produise un second jeton valide.
        self._magasin.retirer_code(authorization_code.code)
        return self._emettre(
            str(client.client_id), authorization_code.scopes, authorization_code.resource
        )

    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        donnees = self._magasin.lire_rafraichissement(refresh_token)
        if donnees is None or donnees["client_id"] != client.client_id:
            return None
        return RefreshToken(
            token=refresh_token,
            client_id=donnees["client_id"],
            scopes=donnees["scopes"],
            expires_at=donnees.get("expire_a"),
        )

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        # Rotation : l'ancien jeton de rafraichissement est revoque a l'echange.
        pass  # rotation stricte retiree (23/09) : un refresh deja utilise reste valide, evite les deconnexions ChatGPT
        return self._emettre(str(client.client_id), scopes or refresh_token.scopes, None)

    async def load_access_token(self, token: str) -> AccessToken | None:
        if self._jeton_statique and hmac.compare_digest(token, self._jeton_statique):
            # Sans expiration : jeton de service, tourne a la main.
            return AccessToken(
                token=token,
                client_id=CLIENT_STATIQUE,
                scopes=portees_du_jeton_statique(),
                expires_at=None,
            )
        donnees = self._magasin.lire_acces(token)
        if donnees is None:
            return None
        return AccessToken(
            token=token,
            client_id=donnees["client_id"],
            scopes=donnees["scopes"],
            expires_at=donnees["expire_a"],
            resource=donnees.get("resource"),
        )

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        self._magasin.revoquer(token.token)

    def _emettre(self, client_id: str, scopes: list[str], resource: str | None) -> OAuthToken:
        maintenant = int(time.time())
        acces = secrets.token_urlsafe(32)
        rafraichissement = secrets.token_urlsafe(32)
        self._magasin.poser_jetons(
            {
                "jeton": acces,
                "client_id": client_id,
                "scopes": scopes,
                "resource": resource,
                "expire_a": maintenant + DUREE_ACCES_S,
            },
            {
                "jeton": rafraichissement,
                "client_id": client_id,
                "scopes": scopes,
                "expire_a": maintenant + DUREE_RAFRAICHISSEMENT_S,
            },
        )
        return OAuthToken(
            access_token=acces,
            token_type="Bearer",  # noqa: S106 - type de jeton OAuth, pas un secret
            expires_in=DUREE_ACCES_S,
            scope=" ".join(scopes),
            refresh_token=rafraichissement,
        )
