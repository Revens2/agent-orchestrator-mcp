"""Bootstrap reproductible du second runner `pc-fixe` (i5-14600KF / RTX 4070).

Preuves locales, sans réseau ni machine réelle : home temporaire, broker en
mémoire, scripts exercés en --dry-run / --env-file temporaire. Le runner live
du portable (`main-windows-pc`) n'est jamais touché (aucun test ne lit ni
n'écrit %USERPROFILE%\\.orch-runner).
"""

import hashlib
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

import orch_protocol as P
from orch_mcp.runner_api import RunnerAuth, build_routes, parse_tokens
from orch_mcp.store import Store
from orch_runner.policy import Config, Workspace, resolve_workspace

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "deploy" / "windows" / "pc-fixe" / "runner.toml.pc-fixe.example"
ENROLL = REPO / "deploy" / "windows" / "pc-fixe" / "enroll-pc-fixe.ps1"
ADD_TOKEN = REPO / "deploy" / "add-runner-token.sh"

MAIN_TOKEN = "m" * 48
FIXE_TOKEN = "f" * 48


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# ------------------------------------------------------------- identités
def test_pcfixe_runner_id_valid_and_distinct():
    fixe, main = "pc-fixe", "main-windows-pc"
    assert P.valid_id(fixe)
    assert P.valid_id(main)
    assert fixe != main


def test_parse_tokens_multi_runner(tmp_path):
    """Le broker authentifie les deux runners et les isole (pas d'usurpation)."""
    raw = f"main-windows-pc:{_digest(MAIN_TOKEN)},pc-fixe:{_digest(FIXE_TOKEN)}"
    digests = parse_tokens(raw)
    store = Store(tmp_path / "api.db")
    app = Starlette(routes=build_routes(store, RunnerAuth(digests, ["10.200.0.0/16"])))
    client = TestClient(app)

    def post(path, body, token, ip="10.200.9.9"):
        return client.post(
            f"/runner/v1/{path}",
            json={"protocol_version": P.PROTOCOL_VERSION, **body},
            headers={"authorization": f"Bearer {token}", "x-real-ip": ip},
        )

    info = {"runtimes": [{"id": "fake", "available": True}],
            "workspaces": [{"id": "demo", "modes": ["read_only"]}], "max_parallel": 1}
    assert post("hello", {"info": info}, MAIN_TOKEN).json()["runner_id"] == "main-windows-pc"
    assert post("hello", {"info": info}, FIXE_TOKEN).json()["runner_id"] == "pc-fixe"
    # pc-fixe ne peut pas se déclarer main-windows-pc (dérivation depuis le jeton)
    r = post("hello", {"info": info, "runner_id": "main-windows-pc"}, FIXE_TOKEN)
    assert r.status_code == 401
    # job créé par le portable : pc-fixe authentifié ne peut pas le toucher
    job, _ = store.create_job("main-windows-pc", "fake", "demo", "hi", "read_only")
    epoch = post("hello", {"info": info}, FIXE_TOKEN).json()["epoch"]
    r = post("transition", {"epoch": epoch, "job_id": job["job_id"], "fencing": 1,
                            "from": "queued", "to": "claimed"}, FIXE_TOKEN)
    assert r.status_code in (404, 409)
    store.close()


# ------------------------------------------------------- template pc-fixe
def test_pcfixe_example_loads_in_temp_home(tmp_path):
    """Le template se rend en config valide dans un home temporaire isolé."""
    home = tmp_path / ".orch-runner"
    home.mkdir()
    rendered = EXAMPLE.read_text(encoding="utf-8").replace("<user>", "pcfixe-test")
    assert "<user>" not in rendered
    toml_path = home / "runner.toml"
    toml_path.write_text(rendered, encoding="utf-8")
    cfg = Config.load(toml_path)
    assert cfg.runner_id == "pc-fixe"
    assert cfg.broker_url == "http://10.200.114.203:8803"
    assert cfg.max_parallel == 4
    assert cfg.home == home
    assert cfg.permission_policy in P.PERMISSION_POLICIES
    assert set(cfg.workspaces) == {"e2e", "agent-orchestrator-mcp"}
    assert "opencode" in cfg.runtimes
    assert cfg.runtimes["opencode"].extra.get("model") == "opencode/muse-spark-1.3-contributor-free"
    assert cfg.runtimes["claude-desktop"].enabled is False  # Desktop reste sur le portable
    # workspaces temporaires acceptés par la politique (disque fixe, sans lien)
    (tmp_path / "e2e").mkdir()
    assert resolve_workspace(Workspace("e2e", str(tmp_path / "e2e"), ["read_only"])) is not None


def test_no_secret_in_bootstrap_files():
    """Aucune empreinte réelle ni jeton dans Git : placeholders uniquement."""
    hex64 = re.compile(r"[0-9a-f]{64}")
    for path in (EXAMPLE, ENROLL, ADD_TOKEN):
        text = path.read_text(encoding="utf-8")
        assert not hex64.search(text), f"secret apparent dans {path.name}"
    assert "<user>" in EXAMPLE.read_text(encoding="utf-8")
    enroll = ENROLL.read_text(encoding="utf-8")
    for guard in ("main-windows-pc", "active_jobs", "AllowOverwrite", "gen-token", "pc-fixe"):
        assert guard in enroll, f"garde-fou manquant dans enroll-pc-fixe.ps1 : {guard}"


# ---------------------------------------------------------- script VPS
def _bash_exe() -> str | None:
    """Bash comprenant les chemins Windows (Git bash de préférence au lanceur WSL)."""
    for candidate in (r"C:\Program Files\Git\bin\bash.exe", r"C:\Program Files\Git\usr\bin\bash.exe"):
        if Path(candidate).exists():
            return candidate
    return shutil.which("bash")


def test_vps_add_token_script(tmp_path):
    """Ajout idempotent préservant main-windows-pc (backup + unchanged + refus)."""
    bash = _bash_exe()
    if bash is None:
        pytest.skip("bash indisponible")
    env = tmp_path / "orch.env"
    main_fp = "a" * 64
    env.write_text(f"ORCH_DATA_DIR=/srv/orch/data\nORCH_RUNNER_TOKENS=main-windows-pc:{main_fp}\n", encoding="utf-8")
    fixe = f"pc-fixe:{'b' * 64}"

    def run(*args):
        return subprocess.run([bash, str(ADD_TOKEN), *args], capture_output=True, text=True, timeout=60)

    # --dry-run : rien écrit
    r = run(fixe, "--env-file", str(env), "--no-restart", "--dry-run")
    assert r.returncode == 0 and "dry-run" in r.stdout
    assert f"main-windows-pc:{main_fp}" in env.read_text(encoding="utf-8")

    # ajout réel : pc-fixe ajouté, main-windows-pc préservé, backup créé
    r = run(fixe, "--env-file", str(env), "--no-restart")
    assert r.returncode == 0, r.stderr
    line = next(l for l in env.read_text(encoding="utf-8").splitlines() if l.startswith("ORCH_RUNNER_TOKENS="))
    assert line == f"ORCH_RUNNER_TOKENS=main-windows-pc:{main_fp},{fixe}"
    assert list(tmp_path.glob("orch.env.bak-*"))
    # rejouer : inchangé
    r = run(fixe, "--env-file", str(env), "--no-restart")
    assert r.returncode == 0 and "unchanged" in r.stdout
    # rotation : remplace l'entrée pc-fixe, préserve main-windows-pc
    fixe2 = f"pc-fixe:{'c' * 64}"
    r = run(fixe2, "--env-file", str(env), "--no-restart")
    assert r.returncode == 0
    line = next(l for l in env.read_text(encoding="utf-8").splitlines() if l.startswith("ORCH_RUNNER_TOKENS="))
    assert line == f"ORCH_RUNNER_TOKENS=main-windows-pc:{main_fp},{fixe2}"
    # entrée invalide refusée
    assert run("pc-fixe:pas-un-sha", "--env-file", str(env), "--no-restart").returncode == 2
