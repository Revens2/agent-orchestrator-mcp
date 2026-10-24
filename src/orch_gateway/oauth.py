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
import logging
import os
import secrets
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    OAuthAuthorizationServerProvider,
    RefreshToken,
    TokenError,
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
# Duree GLISSANTE d'un jeton de rafraichissement (vault-mcp#11) : chaque rotation en
# emet un nouveau valable cette duree, sans jamais depasser la duree absolue de sa
# famille. Un client inactif plus longtemps doit re-consentir.
DUREE_RAFRAICHISSEMENT_S = 30 * 24 * 3600
# Duree ABSOLUE d'une famille de jetons (une famille = un consentement humain et
# toutes ses rotations). Au-dela, re-consentement obligatoire, meme pour un client
# actif : une fuite ne vaut jamais persistance illimitee.
DUREE_MAX_FAMILLE_S = 90 * 24 * 3600
# Fenetre pendant laquelle un jeton de rafraichissement DEJA consomme reste
# echangeable (requete rejouee apres une reponse perdue). Defaut 60 s : un client
# (ChatGPT) qui rejoue son refresh apres une reponse perdue ne perd pas sa session.
# 0 = rotation stricte : tout rejeu revoque la famille entiere. Plafonnee : une
# grace longue annulerait la detection de vol.
GRACE_REJEU_DEFAUT_S = 60
GRACE_REJEU_MAX_S = 300
# Au-dela, une demande de consentement laissee ouverte est abandonnee.
DUREE_DEMANDE_S = 600

_journal = logging.getLogger(__name__)


def _duree_env(nom: str, defaut: int, minimum: int, maximum: int | None = None) -> int:
    """Lit une duree en secondes ; valeur invalide ou hors bornes -> defaut (journalise).

    Fail-safe : une faute de frappe ne doit ni desactiver l'expiration ni la rendre
    infinie, elle retombe sur la valeur par defaut.
    """
    brut = os.environ.get(nom, "").strip()
    if not brut:
        return defaut
    try:
        valeur = int(brut)
    except ValueError:
        _journal.warning("%s invalide, defaut %ss applique", nom, defaut)
        return defaut
    if valeur < minimum or (maximum is not None and valeur > maximum):
        _journal.warning("%s hors bornes, defaut %ss applique", nom, defaut)
        return defaut
    return valeur


def duree_max_famille_s() -> int:
    """`ORCH_GW_OAUTH_REFRESH_MAX_LIFETIME_S`, defaut 90 jours, minimum 1 heure."""
    return _duree_env("ORCH_GW_OAUTH_REFRESH_MAX_LIFETIME_S", DUREE_MAX_FAMILLE_S, 3600)


def duree_rafraichissement_s() -> int:
    """`ORCH_GW_OAUTH_REFRESH_TTL_S`, defaut 30 jours, jamais au-dela de la duree
    absolue de famille."""
    glissante = _duree_env("ORCH_GW_OAUTH_REFRESH_TTL_S", DUREE_RAFRAICHISSEMENT_S, 3600)
    return min(glissante, duree_max_famille_s())


def grace_rejeu_s() -> int:
    """`ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S`, defaut 60 s, 0 = strict, maximum 300 s."""
    return _duree_env(
        "ORCH_GW_OAUTH_REFRESH_REUSE_GRACE_S", GRACE_REJEU_DEFAUT_S, 0, GRACE_REJEU_MAX_S
    )


def _empreinte(jeton: str) -> str:
    """Les jetons consommes ne sont conserves que par empreinte : un vol du fichier
    d'etat ne doit pas livrer des jetons rejouables."""
    return hashlib.sha256(jeton.encode()).hexdigest()


def _dans_la_grace(consomme: dict[str, Any], grace_s: int) -> bool:
    """Grace 0 = aucune tolerance, meme dans la seconde du premier echange.

    La grace ne sert qu'UNE fois par jeton (`grace_utilisee`) et ne couvre jamais
    un successeur remplace (`remplace`) : sans cela, des rejeux successifs dans la
    fenetre multiplieraient les refresh valides d'une meme famille."""
    if (
        grace_s <= 0
        or consomme.get("revoque")
        or consomme.get("grace_utilisee")
        or consomme.get("remplace")
    ):
        return False
    return int(time.time()) - int(consomme.get("consomme_a", 0)) <= grace_s

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


# `consommes` : empreinte d'un jeton de rafraichissement deja echange -> sa famille.
# C'est ce qui permet de reconnaitre un rejeu (et donc un vol probable) au lieu de
# le confondre avec un jeton inconnu.
CATEGORIES_ETAT = ("demandes", "codes", "acces", "rafraichissements", "consommes")


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
        for cle in CATEGORIES_ETAT:
            etat.setdefault(cle, {})
        return etat

    def _modifier_etat(self, modification) -> Any:
        """Read-modify-write de l'etat.json sous verrou, purge des expires incluse."""

        def _corps(etat: dict[str, dict[str, Any]]) -> Any:
            for cle in CATEGORIES_ETAT:
                etat.setdefault(cle, {})
            resultat = modification(etat)
            self._purger(etat)
            return resultat

        return _modifier(self._fichier_etat, {}, _corps)

    @staticmethod
    def _purger(etat: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Retire ce qui a expire : le fichier ne grossit pas indefiniment.

        Borne aussi les jetons de rafraichissement herites (vault-mcp#11) : un jeton emis
        avec une duree superieure a la duree glissante courante (10 ans, tweak du 23/09,
        ou une duree reduite par configuration) est ramene a `maintenant + duree
        glissante`. Ramenage idempotent : la borne ne recule plus une fois appliquee.
        """
        maintenant = int(time.time())
        for cle in CATEGORIES_ETAT:
            etat[cle] = {
                k: v for k, v in etat.get(cle, {}).items() if v.get("expire_a", 0) > maintenant
            }
        plafond = maintenant + duree_rafraichissement_s()
        for donnees in etat["rafraichissements"].values():
            borne = min(plafond, donnees.get("famille_expire_a") or plafond)
            if donnees.get("expire_a", 0) > borne:
                donnees["expire_a"] = borne
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

    @staticmethod
    def _retirer_famille(etat: dict[str, dict[str, Any]], famille: str) -> int:
        """Retire tous les jetons vivants (acces + rafraichissement) d'une famille et
        marque ses jetons consommes `revoque` : sans ce marqueur, un refresh consomme
        dans la fenetre de grace ressusciterait la famille revoquee."""
        retires = 0
        for cle in ("acces", "rafraichissements"):
            vivants = etat[cle]
            for jeton in [j for j, d in vivants.items() if d.get("famille") == famille]:
                del vivants[jeton]
                retires += 1
        for marque in etat["consommes"].values():
            if marque.get("famille") == famille:
                marque["revoque"] = True
        return retires

    def tourner(
        self,
        ancien: str,
        client_id: str,
        grace_s: int,
        emettre: Callable[[dict[str, Any], dict[str, dict[str, Any]]], OAuthToken],
    ) -> OAuthToken | None:
        """Rotation ATOMIQUE d'un jeton de rafraichissement (un seul verrou).

        - jeton vivant : il passe en `consommes` (par empreinte) puis
          `emettre(base)` pose la paire suivante dans la meme famille ;
        - jeton consomme depuis <= `grace_s`, UNE seule fois : la paire emise lors
          de la consommation (jamais recue : reponse perdue) est REMPLACEE par une
          nouvelle, famille conservee. Le successeur remplace passe en `consommes`
          marque `remplace` : s'il est presente plus tard, c'est un rejeu et la
          famille tombe. Une famille n'a donc jamais deux refresh vivants ;
        - sinon : None. Deux echanges concurrents du meme jeton ne produisent donc
          jamais deux paires hors grace.

        `emettre(base, etat)` mute `etat` et retourne le resultat a propager.
        """

        def _tourner(etat: dict[str, dict[str, Any]]) -> OAuthToken | None:
            maintenant = int(time.time())
            empreinte = _empreinte(ancien)
            base = etat["rafraichissements"].get(ancien)
            if base is None or base.get("client_id") != client_id:
                consomme = etat["consommes"].get(empreinte)
                if (
                    consomme is None
                    or consomme.get("client_id") != client_id
                    or not _dans_la_grace(consomme, grace_s)
                    or consomme["famille_expire_a"] <= maintenant
                    or not self._remplacer_successeur(etat, consomme, maintenant)
                ):
                    return None
                consomme["grace_utilisee"] = True
                resultat = emettre(consomme, etat)
                consomme["successeur"] = _empreinte(resultat.refresh_token or "")
                consomme["successeur_acces"] = _empreinte(resultat.access_token)
                return resultat
            del etat["rafraichissements"][ancien]
            famille = base.get("famille") or secrets.token_urlsafe(16)
            famille_expire_a = base.get("famille_expire_a") or (
                maintenant + duree_max_famille_s()
            )
            base = {**base, "famille": famille, "famille_expire_a": famille_expire_a}
            etat["consommes"][empreinte] = {
                "client_id": client_id,
                "scopes": base["scopes"],
                "famille": famille,
                "famille_expire_a": famille_expire_a,
                "consomme_a": maintenant,
                # Conserve tant que le jeton aurait ete valide : au-dela, un rejeu
                # serait de toute facon refuse comme expire.
                "expire_a": base.get("expire_a", maintenant),
            }
            if base["famille_expire_a"] <= maintenant:
                return None
            resultat = emettre(base, etat)
            etat["consommes"][empreinte]["successeur"] = _empreinte(resultat.refresh_token or "")
            etat["consommes"][empreinte]["successeur_acces"] = _empreinte(resultat.access_token)
            return resultat

        resultat: OAuthToken | None = self._modifier_etat(_tourner)
        return resultat

    @staticmethod
    def _remplacer_successeur(
        etat: dict[str, dict[str, Any]], consomme: dict[str, Any], maintenant: int
    ) -> bool:
        """Retire la paire emise lors de la consommation de `consomme` et garde son
        refresh en `consommes` marque `remplace` (sa presentation ulterieure = rejeu).

        False si le successeur n'est plus vivant (deja tourne, revoque, ou entree sans
        successeur connu) : la grace n'a alors plus d'objet, le rejeu est refuse."""
        successeur = consomme.get("successeur")
        vivant = next(
            (
                j
                for j in etat["rafraichissements"]
                if successeur and _empreinte(j) == successeur
            ),
            None,
        )
        if vivant is None:
            return False
        donnees = etat["rafraichissements"].pop(vivant)
        etat["consommes"][successeur] = {
            "client_id": donnees["client_id"],
            "scopes": donnees["scopes"],
            "famille": donnees["famille"],
            "famille_expire_a": donnees["famille_expire_a"],
            "consomme_a": maintenant,
            "expire_a": donnees.get("expire_a", maintenant),
            "remplace": True,
        }
        acces = consomme.get("successeur_acces")
        for jeton in [j for j in etat["acces"] if acces and _empreinte(j) == acces]:
            etat["acces"].pop(jeton)
        return True

    def signaler_rejeu(self, jeton: str, client_id: str, grace_s: int) -> str:
        """Classe un jeton de rafraichissement absent des jetons vivants.

        Retourne `"inconnu"`, `"grace"` (rejeu tolere) ou `"rejeu"` ; dans ce dernier
        cas la famille ENTIERE est revoquee (RFC 9700 §4.14.2) : on ne sait pas qui,
        du client legitime ou de l'attaquant, detient le jeton courant.
        """

        def _classer(etat: dict[str, dict[str, Any]]) -> str:
            consomme = etat["consommes"].get(_empreinte(jeton))
            if consomme is None or consomme.get("client_id") != client_id:
                # Un autre client ne doit pas pouvoir faire tomber une famille.
                return "inconnu"
            if consomme.get("revoque"):
                return "rejeu"
            if _dans_la_grace(consomme, grace_s):
                return "grace"
            retires = self._retirer_famille(etat, consomme["famille"])
            _journal.warning(
                "OAuth: rejeu d'un jeton de rafraichissement consomme, famille revoquee "
                "(client=%s famille=%s jetons_retires=%d)",
                client_id,
                consomme["famille"][:8],
                retires,
            )
            return "rejeu"

        return str(self._modifier_etat(_classer))

    def lire_consomme(self, jeton: str) -> dict[str, Any] | None:
        return self._etat()["consommes"].get(_empreinte(jeton))

    def revoquer_famille_du_jeton(self, jeton: str) -> int:
        """Revocation RFC 7009 : revoquer un jeton revoque toute sa famille (acces et
        rafraichissement issus du meme consentement). Jeton herite sans famille :
        seul ce jeton est retire."""

        def _revoquer(etat: dict[str, dict[str, Any]]) -> int:
            donnees = (
                etat["acces"].get(jeton)
                or etat["rafraichissements"].get(jeton)
                # Refresh deja tourne : sa famille doit tomber aussi (sinon la grace
                # le laisserait encore s'echanger).
                or etat["consommes"].get(_empreinte(jeton))
            )
            if donnees is None:
                return 0
            famille = donnees.get("famille")
            if not famille:
                etat["acces"].pop(jeton, None)
                etat["rafraichissements"].pop(jeton, None)
                return 1
            return self._retirer_famille(etat, famille)

        return int(self._modifier_etat(_revoquer))

    def revoquer_client(self, client_id: str | None = None) -> int:
        """Revocation administrative : tous les jetons d'un client (ou de TOUS les
        clients si `client_id` est None). Ne touche ni au registre des clients ni au
        Bearer statique, qui se tourne a la main."""

        def _revoquer(etat: dict[str, dict[str, Any]]) -> int:
            retires = 0
            for cle in ("acces", "rafraichissements"):
                vivants = etat[cle]
                cibles = [
                    j
                    for j, d in vivants.items()
                    if client_id is None or d.get("client_id") == client_id
                ]
                for jeton in cibles:
                    del vivants[jeton]
                    retires += 1
            # Ferme aussi la fenetre de grace des refresh deja consommes.
            for marque in etat["consommes"].values():
                if client_id is None or marque.get("client_id") == client_id:
                    marque["revoque"] = True
            return retires

        return int(self._modifier_etat(_revoquer))

    def resume(self) -> dict[str, dict[str, int]]:
        """Nombre de jetons vivants par client, sans jamais exposer un jeton."""
        etat = self._etat()
        compte: dict[str, dict[str, int]] = {}
        for cle in ("acces", "rafraichissements"):
            for donnees in etat[cle].values():
                ligne = compte.setdefault(
                    donnees.get("client_id", "?"), {"acces": 0, "rafraichissements": 0}
                )
                ligne[cle] += 1
        return compte


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
        if donnees is not None:
            if donnees["client_id"] != client.client_id:
                return None
            if donnees.get("expire_a", 0) <= int(time.time()):
                return None
            return RefreshToken(
                token=refresh_token,
                client_id=donnees["client_id"],
                scopes=donnees["scopes"],
                expires_at=donnees.get("expire_a"),
            )
        # Absent des jetons vivants : inconnu, ou deja consomme. Un jeton consomme
        # presente hors de la fenetre de grace est un rejeu -> famille revoquee.
        verdict = self._magasin.signaler_rejeu(
            refresh_token, str(client.client_id), grace_rejeu_s()
        )
        if verdict != "grace":
            return None
        consomme = self._magasin.lire_consomme(refresh_token)
        if consomme is None:
            return None
        return RefreshToken(
            token=refresh_token,
            client_id=consomme["client_id"],
            scopes=consomme["scopes"],
            expires_at=consomme.get("famille_expire_a"),
        )

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        # Rotation stricte (vault-mcp#11) : l'ancien jeton est consomme et la nouvelle
        # paire posee sous le MEME verrou. Deux echanges concurrents du meme jeton ne
        # peuvent plus produire deux paires valides.
        client_id = str(client.client_id)

        def _emettre_dans_famille(
            base: dict[str, Any], etat: dict[str, dict[str, Any]]
        ) -> OAuthToken:
            acces, rafraichissement, jeton = self._nouvelle_paire(
                client_id,
                scopes or base["scopes"],
                None,
                base["famille"],
                base["famille_expire_a"],
            )
            etat["acces"][acces["jeton"]] = acces
            etat["rafraichissements"][rafraichissement["jeton"]] = rafraichissement
            return jeton

        resultat = self._magasin.tourner(
            refresh_token.token, client_id, grace_rejeu_s(), _emettre_dans_famille
        )
        if resultat is None:
            raise TokenError(
                error="invalid_grant", error_description="refresh token invalide ou deja utilise"
            )
        return resultat

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
        # Le Bearer statique n est pas dans le magasin : il se tourne a la main.
        self._magasin.revoquer_famille_du_jeton(token.token)

    @staticmethod
    def _nouvelle_paire(
        client_id: str,
        scopes: list[str],
        resource: str | None,
        famille: str,
        famille_expire_a: int,
    ) -> tuple[dict[str, Any], dict[str, Any], OAuthToken]:
        """Construit une paire (acces, rafraichissement) d'une famille, sans la poser."""
        maintenant = int(time.time())
        acces = secrets.token_urlsafe(32)
        rafraichissement = secrets.token_urlsafe(32)
        expire_rafraichissement = min(maintenant + duree_rafraichissement_s(), famille_expire_a)
        expire_acces = min(maintenant + DUREE_ACCES_S, famille_expire_a)
        return (
            {
                "jeton": acces,
                "client_id": client_id,
                "scopes": scopes,
                "resource": resource,
                "famille": famille,
                "expire_a": expire_acces,
            },
            {
                "jeton": rafraichissement,
                "client_id": client_id,
                "scopes": scopes,
                "famille": famille,
                "famille_expire_a": famille_expire_a,
                "emis_a": maintenant,
                "expire_a": expire_rafraichissement,
            },
            OAuthToken(
                access_token=acces,
                token_type="Bearer",  # noqa: S106 - type de jeton OAuth, pas un secret
                expires_in=expire_acces - maintenant,
                scope=" ".join(scopes),
                refresh_token=rafraichissement,
            ),
        )

    def _emettre(self, client_id: str, scopes: list[str], resource: str | None) -> OAuthToken:
        """Premiere emission apres consentement : ouvre une nouvelle famille."""
        acces, rafraichissement, jeton = self._nouvelle_paire(
            client_id,
            scopes,
            resource,
            secrets.token_urlsafe(16),
            int(time.time()) + duree_max_famille_s(),
        )
        self._magasin.poser_jetons(acces, rafraichissement)
        return jeton
