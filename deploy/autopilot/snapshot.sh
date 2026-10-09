#!/usr/bin/env bash
# Sauvegarde VERIFIEE du staging autopilot (root), avant tout deploiement. Ne touche ni /srv/orch ni nginx.
# Contenu : src/, deploy/, VERSION, secrets/*.env|*.token|consent.key (0600 root), autopilot.db (sauvegarde
# SQLite en ligne, coherente meme service actif) + SHA256SUMS. Verification : sha256sum -c + integrity_check.
# Usage : sudo bash /srv/autopilot/deploy/autopilot/snapshot.sh   -> affiche le dossier cree
set -euo pipefail
RACINE="/srv/autopilot"
VER="$(cat "$RACINE/VERSION" 2>/dev/null || echo inconnu)"
DEST="/srv/autopilot-backups/${VER}-$(date +%Y%m%d%H%M%S)"
PY=/srv/orch/venv/bin/python
install -d -m 700 -o root -g root /srv/autopilot-backups "$DEST"
cp -a "$RACINE/src" "$RACINE/deploy" "$DEST/"
[ -f "$RACINE/VERSION" ] && cp -a "$RACINE/VERSION" "$DEST/"
install -d -m 700 "$DEST/secrets"
cp -a "$RACINE/secrets/." "$DEST/secrets/"
[ -f "$RACINE/data/consent.key" ] && install -m 600 "$RACINE/data/consent.key" "$DEST/consent.key"
if [ -f "$RACINE/data/autopilot.db" ]; then
  "$PY" - "$RACINE/data/autopilot.db" "$DEST/autopilot.db" <<'EOF'
import sqlite3, sys
src = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
dst = sqlite3.connect(sys.argv[2])
src.backup(dst)
ok = dst.execute("PRAGMA integrity_check").fetchone()[0]
dst.close(); src.close()
if ok != "ok":
    raise SystemExit(f"integrity_check: {ok}")
EOF
  chmod 600 "$DEST/autopilot.db"
fi
(cd "$DEST" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS)
(cd "$DEST" && sha256sum -c --quiet SHA256SUMS)
echo "$DEST"
