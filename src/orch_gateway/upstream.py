"""Proxy vers le serveur MCP upstream (orch-mcp, 127.0.0.1:8802).

La passerelle n'est PAS un serveur MCP de plein exercice : elle valide le jeton
d'acces (OAuth ou Bearer statique) puis relaie le trafic Streamable HTTP vers
l'upstream. Les sessions MCP restent la propriete de l'upstream : l'en-tete
`mcp-session-id` est transmis sans modification dans les deux sens, ce qui rend
le proxy invisible pour le protocole.

Autorisation (politique explicite, voir `politique.py`) appliquee AVANT l'envoi
vers l'upstream, sur la base des portees du jeton deja valide par le gateway :

- `tools/list` : la reponse upstream est filtree pour n'annoncer que les outils
  que le jeton a le droit d'utiliser (jamais les outils d'administration, jamais
  un outil inconnu) ; une reponse `tools/list` illisible n'est jamais relayee ;
- `tools/call` : refuse localement (erreur JSON-RPC) si l'outil demande est en
  ecriture sans portee d'ecriture, est un outil d'administration, ou n'est pas
  classe ; l'upstream n'est alors JAMAIS contacte ;
- autres methodes : relais verbatim si elles figurent dans l'allowlist des
  methodes servies par l'upstream (sinon -32601 local) ;
- un corps illisible, a cles JSON dupliquees, ou une requete par lot (batch
  JSON-RPC) est refuse (fail-closed), le batch n'etant pas utilise par les
  clients MCP ; un en-tete MCP duplique aussi.

Ere protocolaire (voir `classer_ere`) : le SDK Python 2.x route l'ere sur le seul
en-tete `MCP-Protocol-Version`, alors que certains clients historiques (ChatGPT)
annoncent `2026-07-28` avec un corps historique. La passerelle classe d'abord par le
CORPS : seule une forme historique est rabaissee vers la derniere version a
handshake ; une enveloppe 2026-07-28 passe intacte vers le chemin moderne.
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

import httpx
from mcp_types.version import (
    HANDSHAKE_PROTOCOL_VERSIONS,
    LATEST_HANDSHAKE_VERSION,
    MODERN_PROTOCOL_VERSIONS,
)

from orch_gateway.politique import PolitiqueOutils

# Journalisation minimale et structuree des echecs de relais : la classe reelle
# de l'exception httpx (ConnectError, ConnectTimeout, ReadTimeout, PoolTimeout,
# RemoteProtocolError...) n'etait observable nulle part (mission 2026-09-07,
# contention CPU -> 502 Tasks). Aucune donnee sensible ici : methode, destination
# locale et duree seulement.
_journal = logging.getLogger("uvicorn.error")

# Au-dela de cette attente de la reponse upstream, le relais est considere comme
# anormalement lent (session SSE exceptee) et journalise en warning.
_SEUIL_REPONSE_LENTE_S = 30.0

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

# En-tetes de requete retransmis a l'upstream. Pas de host/content-length (httpx les
# gere), pas d'accept-encoding (on evite la compression intermediaire), pas d'origin
# (l'upstream le validerait comme non-localhost et repondrait 403). `mcp-method` /
# `mcp-name` et tout `mcp-param-*` sont requis par le chemin 2026-07-28 et relayes
# sans reecriture : c'est l'upstream qui rejette un desaccord en-tete/corps (-32020).
_ENTETES_REQUETE = {
    "content-type",
    "accept",
    "mcp-session-id",
    "mcp-protocol-version",
    "mcp-method",
    "mcp-name",
    "last-event-id",
    "user-agent",
}
_PREFIXE_PARAM = "mcp-param-"

# En-tetes MCP dont un doublon rendrait ambigu ce que voient politique et upstream.
_ENTETES_UNIQUES = {"mcp-session-id", "mcp-protocol-version", "mcp-method", "mcp-name"}

# En-tetes de reponse retransmis au client. content-length et transfer-encoding sont
# laisses a uvicorn (chunked), les autres (date, server...) n'ont pas lieu d'etre.
_ENTETES_REPONSE = {
    "content-type",
    "mcp-session-id",
    "cache-control",
}

_LIMITE_CORPS = 5 * 1024 * 1024  # 5 Mo : garde-fou contre un corps de requete aberrant.

# Codes d'erreur JSON-RPC utilises pour les refus locaux.
_CODE_ERREUR_AUTORISATION = -32000  # erreur applicative cote serveur
_CODE_PARSE = -32700  # corps JSON-RPC illisible
_CODE_BATCH = -32600  # requete par lot (ou cles dupliquees) non prise en charge
_CODE_METHODE_INCONNUE = -32601
_CODE_INTERNE = -32603
_CODE_ENTETE = -32020  # desaccord / doublon d'en-tete MCP (spec 2026-07-28)

_CLE_VERSION_META = "io.modelcontextprotocol/protocolVersion"

# Methodes servies par l'upstream (SDK 2.2.0, serveur bas niveau) accessibles par
# le handshake historique. Les notifications (`notifications/*`) passent aussi.
_METHODES_HISTORIQUES = frozenset(
    {
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
    }
)
# Methodes propres a l'ere 2026-07-28 (jamais rabaissees).
_METHODES_MODERNES = frozenset({"server/discover", "subscriptions/listen"})
_METHODES_CONNUES = _METHODES_HISTORIQUES | _METHODES_MODERNES

# Compteurs d'exploitation (processus), journalises sans aucune donnee sensible.
COMPTEURS: Counter[str] = Counter()


def compter(nom: str, detail: str = "") -> None:
    COMPTEURS[nom] += 1
    niveau = logging.INFO if nom == "rabaissement_ere" else logging.WARNING
    _journal.log(niveau, "compteur %s=%d %s", nom, COMPTEURS[nom], detail)


class _CleDupliquee(ValueError):
    """Objet JSON portant deux fois la meme cle (interpretation ambigue)."""


def _paires_uniques(paires: list[tuple[str, Any]]) -> dict[str, Any]:
    objet: dict[str, Any] = {}
    for cle, valeur in paires:
        if cle in objet:
            raise _CleDupliquee(cle)
        objet[cle] = valeur
    return objet


def charger_jsonrpc(corps: bytes) -> Any:
    """json.loads strict : toute cle dupliquee (a n'importe quel niveau) est refusee."""
    return json.loads(corps, object_pairs_hook=_paires_uniques)


def classer_ere(version: str | None, donnees: Any) -> tuple[str | None, str | None, bool]:
    """Decide l'en-tete `MCP-Protocol-Version` relaye a l'upstream.

    Retourne ``(version_relayee, raison_refus, rabaissee)``. Regles (corps d'abord) :

    - V = ``params._meta["io.modelcontextprotocol/protocolVersion"]`` presente (meme
      nulle ou invalide) : enveloppe 2026-07-28, JAMAIS rabaissee. Un en-tete absent ou
      historique est alors refuse (-32020) plutot que servi en legacy en silence ;
    - V absente et en-tete moderne : forme historique (quirk ChatGPT : en-tete
      2026-07-28, corps historique) rabaissee vers la derniere version a handshake,
      uniquement pour les methodes historiques, les notifications, les reponses et
      les requetes sans corps (GET flux SSE, DELETE). Toute autre methode
      (`server/discover`...) passe intacte : l'upstream la refuse, la passerelle ne
      devine pas ;
    - sinon : en-tete relaye tel quel.

    Un ``_meta`` quelconque (ex. ``progressToken``) ne rend PAS une requete moderne.
    """
    brute = version.strip() if isinstance(version, str) else None
    methode = donnees.get("method") if isinstance(donnees, dict) else None
    params = donnees.get("params") if isinstance(donnees, dict) else None
    meta = params.get("_meta") if isinstance(params, dict) else None
    if isinstance(meta, dict) and _CLE_VERSION_META in meta:
        if brute is None or brute in HANDSHAKE_PROTOCOL_VERSIONS:
            return (
                version,
                "enveloppe 2026-07-28 (params._meta protocolVersion) sans en-tete "
                "MCP-Protocol-Version moderne",
                False,
            )
        return version, None, False
    if brute in MODERN_PROTOCOL_VERSIONS and (
        not isinstance(methode, str)
        or methode in _METHODES_HISTORIQUES
        or methode.startswith("notifications/")
    ):
        return LATEST_HANDSHAKE_VERSION, None, True
    return version, None, False


def _lire_corps(receive: Receive) -> Awaitable[bytes]:
    """Assemble le corps de la requete ASGI (avec garde-fou de taille)."""

    async def _lire() -> bytes:
        morceaux: list[bytes] = []
        total = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                continue
            morceau = message.get("body", b"")
            morceaux.append(morceau)
            total += len(morceau)
            if total > _LIMITE_CORPS:
                raise ValueError("corps de requete trop volumineux")
            if not message.get("more_body"):
                break
        return b"".join(morceaux)

    return _lire()


def _entetes_bruts(scope: Scope) -> list[tuple[str, str]]:
    """Liste ASGI (bytes) -> paires (nom minuscule, valeur), doublons conserves."""
    return [
        (cle.decode("latin-1").lower(), valeur.decode("latin-1"))
        for cle, valeur in scope.get("headers", [])
    ]


def entete_duplique(bruts: list[tuple[str, str]]) -> str | None:
    """Nom du premier en-tete MCP present plusieurs fois, sinon None."""
    vus: set[str] = set()
    for nom, _valeur in bruts:
        if nom in _ENTETES_UNIQUES or nom.startswith(_PREFIXE_PARAM):
            if nom in vus:
                return nom
            vus.add(nom)
    return None


def _entetes(bruts: list[tuple[str, str]], version: str | None) -> dict[str, str]:
    """En-tetes relayes a l'upstream ; `version` remplace MCP-Protocol-Version."""
    resultat: dict[str, str] = {}
    for nom, valeur in bruts:
        if nom == "mcp-protocol-version":
            if version is not None:
                resultat[nom] = version
        elif nom in _ENTETES_REQUETE or nom.startswith(_PREFIXE_PARAM):
            resultat[nom] = valeur
    return resultat


async def _envoyer_reponse_json(
    send: Send, statut: int, donnees: dict[str, Any]
) -> None:
    corps = json.dumps(donnees, ensure_ascii=False).encode()
    await send(
        {
            "type": "http.response.start",
            "status": statut,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": corps, "more_body": False})


async def _envoyer_corps(
    send: Send, statut: int, entetes: httpx.Headers, corps: bytes
) -> None:
    entetes_asgi = [
        (cle.encode("latin-1"), valeur.encode("latin-1"))
        for cle, valeur in entetes.items()
        if cle.lower() in _ENTETES_REPONSE
    ]
    await send({"type": "http.response.start", "status": statut, "headers": entetes_asgi})
    await send({"type": "http.response.body", "body": corps, "more_body": False})


async def _relayer_flux(send: Send, reponse: httpx.Response, methode: str) -> None:
    """Relai d'un corps eventuellement infini (SSE) morceau par morceau."""
    entetes_asgi = [
        (cle.encode("latin-1"), valeur.encode("latin-1"))
        for cle, valeur in reponse.headers.items()
        if cle.lower() in _ENTETES_REPONSE
    ]
    await send(
        {"type": "http.response.start", "status": reponse.status_code, "headers": entetes_asgi}
    )
    try:
        async for morceau in reponse.aiter_raw():
            await send({"type": "http.response.body", "body": morceau, "more_body": True})
    except (httpx.HTTPError, OSError) as exc:  # flux coupe cote upstream
        _journal.warning(
            "relais %s coupe par l'upstream: %s",
            methode, exc.__class__.__name__,
        )
        await send({"type": "http.response.body", "body": b"", "more_body": False})
        return
    await send({"type": "http.response.body", "body": b"", "more_body": False})


def _message_jsonrpc_lisible(donnees: Any) -> bool:
    return isinstance(donnees, dict) and ("result" in donnees or "error" in donnees)


class ProxyMCP:
    """Endpoint ASGI : /mcp authentifie (par le middleware) puis relaye vers l'upstream."""

    def __init__(self, base_url: str, politique: PolitiqueOutils | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._politique = politique or PolitiqueOutils()
        self._client: httpx.AsyncClient | None = None

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            # Constat du 2026-09-07 (mission contention CPU -> 502 Tasks) : avec
            # max_connections=50, le pool de la passerelle s'est retrouve entierement
            # occupe par des sessions SSE GET /mcp longue duree (50 connexions etablies
            # conservees, fd du processus satures) ; chaque nouveau relais attendait
            # alors un creneau avec pool_timeout=3600 s -> nginx rendait 504 apres 1 h,
            # et les relais en course sur des connexions fermees rendaient 502.
            # Corrections : capacite large (300) pour encaisser les salves de sessions
            # du connecteur, attente de creneau bornee (30 s) pour echouer vite et de
            # facon explicite (PoolTimeout journalise) au lieu de stall silencieusement.
            # Read/connect inchanges : 3600 s pour une session SSE legitime, 5 s au
            # connect. Les echecs sont desormais journalises (classe + duree + methode).
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(3600.0, connect=5.0, pool=30.0),
                limits=httpx.Limits(max_connections=300, max_keepalive_connections=50),
            )
        return self._client

    # --- autorisation ---------------------------------------------------------------
    @staticmethod
    def _portees(scope: Scope) -> frozenset[str]:
        """Portees du jeton valide par le gateway : source UNIQUE d'autorisation.

        Les en-tetes envoyes par le client ne sont jamais consultes ici.
        """
        utilisateur = scope.get("user")
        jeton = getattr(utilisateur, "access_token", None)
        portees = getattr(jeton, "scopes", None)
        if not isinstance(portees, (list, tuple, set, frozenset)):
            return frozenset()
        return frozenset(str(p) for p in portees)

    @staticmethod
    def _identite(scope: Scope) -> str | None:
        """client_id du jeton valide (peuple par AuthenticationMiddleware)."""
        utilisateur = scope.get("user")
        jeton = getattr(utilisateur, "access_token", None)
        client_id = getattr(jeton, "client_id", None)
        if not isinstance(client_id, str) or not client_id:
            return None
        return client_id[:128]

    async def _repondre_erreur_jsonrpc(
        self, send: Send, identifiant: Any, code: int, message: str, statut: int = 200
    ) -> None:
        await _envoyer_reponse_json(
            send,
            statut,
            {
                "jsonrpc": "2.0",
                "id": identifiant,
                "error": {"code": code, "message": message},
            },
        )

    async def _decider(self, send: Send, corps: bytes, portees: set[str]) -> tuple[bool, Any]:
        """Applique la politique au corps JSON-RPC avant tout envoi upstream.

        Retourne ``(refuse, donnees)`` : ``refuse`` vaut ``True`` si une reponse
        locale a deja ete envoyee et que la requete ne doit pas etre relayee.
        """
        try:
            donnees = charger_jsonrpc(corps)
        except _CleDupliquee:
            compter("refus_parse", "cle JSON dupliquee")
            await self._repondre_erreur_jsonrpc(
                send, None, _CODE_BATCH, "cles JSON dupliquees (fail-closed)"
            )
            return True, None
        except (ValueError, UnicodeDecodeError):
            compter("refus_parse", "JSON illisible")
            await self._repondre_erreur_jsonrpc(
                send, None, _CODE_PARSE, "corps JSON-RPC illisible (fail-closed)"
            )
            return True, None
        if isinstance(donnees, list):
            # Le batch n'est pas utilise par les clients MCP ; fail-closed.
            compter("refus_parse", "batch")
            await self._repondre_erreur_jsonrpc(
                send, None, _CODE_BATCH, "requetes par lot non prises en charge"
            )
            return True, None
        if not isinstance(donnees, dict):
            return False, donnees
        if "method" in donnees:
            methode = donnees.get("method")
            if not isinstance(methode, str) or not (
                methode in _METHODES_CONNUES or methode.startswith("notifications/")
            ):
                compter("refus_methode", str(methode)[:64])
                await self._repondre_erreur_jsonrpc(
                    send, donnees.get("id"), _CODE_METHODE_INCONNUE,
                    "methode inconnue ou non autorisee (fail-closed)",
                )
                return True, donnees
        if donnees.get("method") != "tools/call":
            # initialize, ping, notifications, tools/list, ... : relais verbatim
            # (tools/list sera filtre sur la reponse).
            return False, donnees
        params = donnees.get("params")
        nom = params.get("name") if isinstance(params, dict) else None
        if not isinstance(nom, str) or not nom:
            compter("refus_politique", "tools/call sans nom")
            await self._repondre_erreur_jsonrpc(
                send, donnees.get("id"), _CODE_ERREUR_AUTORISATION,
                "tools/call sans nom d'outil (fail-closed)",
            )
            return True, donnees
        raison = self._politique.autoriser_call(nom, portees)
        if raison is None:
            return False, donnees
        compter("refus_politique", nom[:64])
        await self._repondre_erreur_jsonrpc(
            send, donnees.get("id"), _CODE_ERREUR_AUTORISATION, raison
        )
        return True, donnees

    # --- filtrage de tools/list -----------------------------------------------------
    @staticmethod
    def _appliquer(resultat: dict[str, Any], visibles: set[str]) -> bool:
        """Filtre `resultat["tools"]` en place ; True si le resultat a change.

        Un catalogue filtre par portees ne doit pas etre mis en cache partage :
        sur un resultat 2026-07-28 (`resultType` present), `cacheScope` est force a
        "private" et `ttlMs` a 0 ; `resultType` et les autres champs sont conserves.
        """
        outils = resultat["tools"]
        conserves = [
            outil for outil in outils
            if isinstance(outil, dict) and outil.get("name") in visibles
        ]
        modifie = len(conserves) != len(outils)
        if modifie:
            resultat["tools"] = conserves
        if "resultType" in resultat and (
            resultat.get("cacheScope") != "private" or resultat.get("ttlMs") != 0
        ):
            resultat["cacheScope"] = "private"
            resultat["ttlMs"] = 0
            modifie = True
        return modifie

    def _filtrer_json(self, corps: bytes, visibles: set[str]) -> tuple[bytes, bool]:
        """Filtre une reponse JSON-RPC JSON ; retourne (corps, lisible)."""
        try:
            donnees = json.loads(corps)
        except (ValueError, UnicodeDecodeError):
            return corps, False
        if not _message_jsonrpc_lisible(donnees):
            return corps, False
        resultat = donnees.get("result")
        if not isinstance(resultat, dict) or not isinstance(resultat.get("tools"), list):
            return corps, True
        if not self._appliquer(resultat, visibles):
            return corps, True
        return json.dumps(donnees, ensure_ascii=False).encode(), True

    def _filtrer_sse(self, corps: bytes, visibles: set[str]) -> tuple[bytes, bool]:
        """Filtre une reponse SSE ; retourne (corps, lisible).

        Evenements separes par une ligne vide ; un evenement peut porter plusieurs
        lignes `data:` (concatenees par \\n), avec ou sans espace apres `data:`, et des
        fins de ligne CRLF. Un evenement contenant la liste d'outils est reecrit en une
        seule ligne `data: `.
        """
        lignes: list[bytes | None] = list(corps.split(b"\n"))
        evenements: list[list[int]] = []
        courant: list[int] = []
        for i, ligne in enumerate(lignes):
            assert ligne is not None
            if ligne.rstrip(b"\r") == b"":
                if courant:
                    evenements.append(courant)
                    courant = []
                continue
            courant.append(i)
        if courant:
            evenements.append(courant)
        lisible = modifie = False
        for indices in evenements:
            donnees_idx = [i for i in indices if (lignes[i] or b"").startswith(b"data:")]
            if not donnees_idx:
                continue
            morceaux = []
            for i in donnees_idx:
                valeur = (lignes[i] or b"")[5:].rstrip(b"\r")
                morceaux.append(valeur[1:] if valeur.startswith(b" ") else valeur)
            try:
                donnees = json.loads(b"\n".join(morceaux))
            except (ValueError, UnicodeDecodeError):
                continue
            if not _message_jsonrpc_lisible(donnees):
                continue
            lisible = True
            resultat = donnees.get("result")
            if not isinstance(resultat, dict) or not isinstance(resultat.get("tools"), list):
                continue
            if self._appliquer(resultat, visibles):
                lignes[donnees_idx[0]] = b"data: " + json.dumps(donnees, ensure_ascii=False).encode()
                for i in donnees_idx[1:]:
                    lignes[i] = None
                modifie = True
        if not modifie:
            return corps, lisible
        return b"\n".join(ligne for ligne in lignes if ligne is not None), lisible

    def filtrer_liste(
        self, corps: bytes, content_type: str, visibles: set[str]
    ) -> tuple[bytes, bool]:
        """Filtre une reponse (JSON nu ou enveloppe SSE) ; retourne (corps, lisible)."""
        if "text/event-stream" in content_type.lower():
            return self._filtrer_sse(corps, visibles)
        if "application/json" in content_type.lower():
            return self._filtrer_json(corps, visibles)
        return corps, False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return
        methode = scope["method"]
        chemin = scope.get("path", "/mcp")
        query = scope.get("query_string", b"").decode("latin-1")
        url = f"{self._base_url}{chemin}" + (f"?{query}" if query else "")

        portees = self._portees(scope)
        bruts = _entetes_bruts(scope)
        doublon = entete_duplique(bruts)
        if doublon is not None:
            compter("refus_entete", doublon)
            await self._repondre_erreur_jsonrpc(
                send, None, _CODE_ENTETE, f"en-tete {doublon} duplique (fail-closed)", statut=400
            )
            return

        corps = None
        donnees: Any = None
        if methode in ("POST", "PUT", "PATCH"):
            try:
                corps = await _lire_corps(receive)
            except ValueError:
                await _envoyer_reponse_json(
                    send, 413,
                    {"jsonrpc": "2.0", "error": {"code": -32000, "message": "Payload Too Large"}, "id": None},
                )
                return
            if corps:
                refuse_localement, donnees = await self._decider(send, corps, set(portees))
                if refuse_localement:
                    return

        version = next((v for nom, v in bruts if nom == "mcp-protocol-version"), None)
        version_relayee, refus_ere, rabaissee = classer_ere(version, donnees)
        identifiant = donnees.get("id") if isinstance(donnees, dict) else None
        if refus_ere is not None:
            compter("refus_ere", str(version)[:32])
            await self._repondre_erreur_jsonrpc(send, identifiant, _CODE_ENTETE, refus_ere, statut=400)
            return
        if rabaissee:
            methode_rpc = donnees.get("method") if isinstance(donnees, dict) else methode
            compter("rabaissement_ere", f"{version}->{version_relayee} {str(methode_rpc)[:32]}")

        entetes = _entetes(bruts, version_relayee)
        identite = self._identite(scope)
        if identite:
            entetes["x-orch-mcp-acteur"] = identite
            entetes["x-orch-mcp-mode"] = "cli" if identite == "orch-cli-statique" else "oauth"

        debut = time.monotonic()
        try:
            requete = self._http().build_request(
                methode, url, headers=entetes, content=corps
            )
            reponse = await self._http().send(requete, stream=True)
        except httpx.HTTPError as exc:
            _journal.warning(
                "relais %s %s en echec apres %.0f ms: %s (reponse 502)",
                methode, url, (time.monotonic() - debut) * 1000,
                exc.__class__.__name__,
            )
            await _envoyer_reponse_json(
                send,
                502,
                {
                    "jsonrpc": "2.0",
                    "error": {"code": -32000, "message": f"upstream indisponible: {exc.__class__.__name__}"},
                    "id": None,
                },
            )
            return

        attente = time.monotonic() - debut
        if attente > _SEUIL_REPONSE_LENTE_S:
            _journal.warning(
                "relais %s %s : en-tetes upstream apres %.0f s (anormalement lent)",
                methode, url, attente,
            )
        try:
            if methode == "GET":
                await _relayer_flux(send, reponse, methode)
            else:
                corps_reponse = await reponse.aread()
                if methode == "POST":
                    corps_reponse, lisible = self.filtrer_liste(
                        corps_reponse,
                        reponse.headers.get("content-type", "") or "",
                        self._politique.visibles(set(portees)),
                    )
                    est_liste = isinstance(donnees, dict) and donnees.get("method") == "tools/list"
                    if est_liste and not lisible:
                        # Jamais de retransmission brute d'un catalogue non filtrable.
                        compter("refus_liste_illisible", str(reponse.status_code))
                        await self._repondre_erreur_jsonrpc(
                            send, identifiant, _CODE_INTERNE,
                            "reponse tools/list upstream illisible (fail-closed)",
                        )
                        return
                await _envoyer_corps(send, reponse.status_code, reponse.headers, corps_reponse)
        finally:
            await reponse.aclose()
