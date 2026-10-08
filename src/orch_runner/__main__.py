"""CLI runner : `python -m orch_runner <run|gen-token|probe|status> [--config PATH]`."""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import sys
from pathlib import Path

from orch_protocol.redact import redact
from orch_runner import winproc
from orch_runner.policy import DEFAULT_HOME, Config, generate_token


class RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = ()
        return True


def setup_logging(home: Path) -> None:
    (home / "logs").mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(home / "logs" / "runner.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(message)s"))
    handler.addFilter(RedactFilter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    if sys.stderr is not None:
        console = logging.StreamHandler()
        console.addFilter(RedactFilter())
        root.addHandler(console)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="orch_runner")
    ap.add_argument("command", choices=["run", "gen-token", "probe", "status", "evidence-keygen", "set-agent-password",
                                        "attest"])
    ap.add_argument("--config", type=Path, default=DEFAULT_HOME / "runner.toml")
    ap.add_argument("--force", action="store_true", help="gen-token : rotation (remplace le jeton existant)")
    args = ap.parse_args(argv)

    if args.command == "gen-token":
        cfg = Config.load(args.config)
        if cfg.token_file.exists() and not args.force:
            print(f"jeton existant ({cfg.token_file}). Rotation : relancer avec --force.", file=sys.stderr)
            return 2
        digest = generate_token(cfg.token_file)
        print("Jeton généré et chiffré DPAPI sur ce PC (il ne quitte jamais la machine).")
        print("Empreinte à déclarer côté VPS dans ORCH_RUNNER_TOKENS :")
        print(f"{cfg.runner_id}:{digest}")
        return 0

    cfg = Config.load(args.config)
    if args.command in ("evidence-keygen", "set-agent-password", "attest"):
        return _isolation_command(args.command, cfg, args.force)
    if args.command == "status":
        path = cfg.home / "status.json"
        print(path.read_text(encoding="utf-8") if path.exists() else '{"error": "aucun statut (runner jamais lancé)"}')
        return 0

    from orch_runner.runner import Runner

    if args.command == "probe":
        info = Runner.__new__(Runner)
        info.config = cfg
        print(json.dumps(Runner.info(info), indent=1, ensure_ascii=False))
        return 0

    setup_logging(cfg.home)
    mutex = winproc.single_instance(cfg.runner_id)
    if mutex is None:
        logging.error("runner déjà actif pour %s : sortie", cfg.runner_id)
        return 3
    Runner(cfg).run()
    return 0


def _isolation_command(command: str, cfg: Config, force: bool) -> int:
    """Commandes du runner isolé, à lancer SOUS le compte runner (A) : les secrets sont chiffrés DPAPI de A."""
    import getpass

    from orch_runner import isolation
    from orch_runner.policy import load_secret, store_secret

    iso = cfg.isolation
    if iso is None:
        print("section [isolation] absente de runner.toml", file=sys.stderr)
        return 2
    if command == "evidence-keygen":
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat

        if iso.signing_key_file.exists() and not force:
            print(f"clé existante ({iso.signing_key_file}). Rotation : --force", file=sys.stderr)
            return 2
        key = Ed25519PrivateKey.generate()
        store_secret(iso.signing_key_file, key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()))
        print("Clé de signature générée (DPAPI du compte runner). Clé PUBLIQUE à déclarer côté VPS :")
        print("AUTOPILOT_RUNNER_PUBKEY=" + key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex())
        return 0
    if command == "set-agent-password":
        secret = getpass.getpass(f"Mot de passe du compte agent {iso.agent_user} (non affiché) : ")
        if not secret:
            return 2
        store_secret(iso.credential_file, secret.encode("utf-8"))
        print(f"Mot de passe stocké chiffré DPAPI : {iso.credential_file}")
        return 0
    # attest : preuve T1 lisible (identités + refus OS), sans lancer d'agent
    probe = isolation.run_probe(iso, load_secret(iso.credential_file).decode("utf-8"))
    att = isolation.attestation(isolation.self_identity(), probe, None)
    print(json.dumps(att, indent=1, ensure_ascii=False))
    print("AUTOPILOT_EXPECTED_AGENT_SID=" + str(att["agent_sid"]))
    return 0 if att["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
