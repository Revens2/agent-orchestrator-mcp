"""Insere la ligne include orch juste avant le fourre-tout final du server 443 (ancre unique)."""

import sys

path, line = sys.argv[1], sys.argv[2]
with open(path) as fh:
    text = fh.read()
anchor = "    location / { return 444; }"
if text.count(anchor) != 1:
    sys.exit("ancre introuvable ou ambigue : aucune modification")
with open(path, "w") as fh:
    fh.write(text.replace(anchor, line + "\n" + anchor))
