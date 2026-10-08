#!/usr/bin/env bash
# Restauration du staging depuis une sauvegarde snapshot.sh (root). Rien n'est supprime : l'etat courant est
# d'abord lui-meme sauvegarde (snapshot.sh), puis remplace. /srv/orch, nginx, orch.db : jamais touches.
# Usage : sudo bash restore.sh /srv/autopilot-backups/<dossier> [--with-db]
#   --with-db : restaure aussi autopilot.db (sinon la base courante est conservee : missions en cours intactes)
set -euo pipefail
SRC="${1:?dossier de sauvegarde requis}"; WITH_DB="${2:-}"
RACINE="/srv/autopilot"
(cd "$SRC" && sha256sum -c --quiet SHA256SUMS) || { echo "sauvegarde corrompue : abandon" >&2; exit 1; }
SAFETY="$(bash "$RACINE/deploy/autopilot/snapshot.sh")"
echo "etat courant sauvegarde : $SAFETY"
systemctl stop autopilot.service
TRASH="$SAFETY/replaced"; install -d -m 700 "$TRASH"
mv "$RACINE/src" "$RACINE/deploy" "$TRASH/"
cp -a "$SRC/src" "$SRC/deploy" "$RACINE/"
[ -f "$SRC/VERSION" ] && cp -a "$SRC/VERSION" "$RACINE/VERSION"
cp -a "$SRC/secrets/." "$RACINE/secrets/"
[ -f "$SRC/consent.key" ] && install -m 600 -o orch-app -g orch-app "$SRC/consent.key" "$RACINE/data/consent.key"
if [ "$WITH_DB" = "--with-db" ] && [ -f "$SRC/autopilot.db" ]; then
  for f in autopilot.db autopilot.db-wal autopilot.db-shm; do [ -f "$RACINE/data/$f" ] && mv "$RACINE/data/$f" "$TRASH/"; done
  install -m 600 -o orch-app -g orch-app "$SRC/autopilot.db" "$RACINE/data/autopilot.db"
fi
install -m 644 "$RACINE/deploy/autopilot/autopilot.service" /etc/systemd/system/autopilot.service
systemctl daemon-reload
systemctl start autopilot.service
PORT="$(grep -E '^AUTOPILOT_PORT=' "$RACINE/secrets/autopilot.env" | cut -d= -f2)"
for _ in $(seq 1 20); do curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "http://127.0.0.1:$PORT/health"; echo
echo "OK : restaure depuis $SRC (VERSION $(cat "$RACINE/VERSION" 2>/dev/null)) ; remplace conserve dans $TRASH"
