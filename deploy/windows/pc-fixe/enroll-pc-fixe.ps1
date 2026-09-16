<#
Enrôle le SECOND runner `pc-fixe` (PC fixe i5-14600KF / RTX 4070) — À EXÉCUTER SUR pc-fixe.

Ne touche JAMAIS au runner live du portable (`main-windows-pc`) :
- refuse de s'exécuter si le runner.toml local déclare un autre runner_id
  (ex. main-windows-pc) sauf -AllowOverwrite explicite ;
- refuse si des jobs sont actifs (status.json) sauf -Force explicite ;
- le jeton est généré sur place (DPAPI) et ne quitte jamais la machine :
  seule l'empreinte `pc-fixe:<sha256hex>` s'affiche, à déclarer côté VPS
  via deploy/add-runner-token.sh (aucun secret dans Git).

Usage (sur pc-fixe) :
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\windows\pc-fixe\enroll-pc-fixe.ps1
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\windows\pc-fixe\enroll-pc-fixe.ps1 -Install
  # rotation du jeton pc-fixe uniquement :
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\windows\pc-fixe\enroll-pc-fixe.ps1 -Rotate [-Install]

Prérequis : clone du dépôt, Python 3.11 (voir -Python), NetBird connecté (10.200.x).
#>
param(
    [string]$Python = "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    [string]$RunnerHome = (Join-Path $env:USERPROFILE ".orch-runner"),
    [string]$User = $env:USERNAME,
    [switch]$Install,
    [switch]$Rotate,
    [switch]$AllowOverwrite,
    [switch]$Force
)
$ErrorActionPreference = "Stop"
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$Example = Join-Path $PSScriptRoot "runner.toml.pc-fixe.example"
$TomlPath = Join-Path $RunnerHome "runner.toml"
$StatusPath = Join-Path $RunnerHome "status.json"
$ExpectedRunnerId = "pc-fixe"

if (-not (Test-Path $Example)) { throw "exemple introuvable : $Example" }

# --- garde-fou 1 : ne jamais écraser le runner live d'une autre machine ---
if (Test-Path $TomlPath) {
    $existing = Get-Content $TomlPath -Raw
    if ($existing -notmatch 'runner_id\s*=\s*"pc-fixe"') {
        if (-not $AllowOverwrite) {
            throw "refus : $TomlPath déclare un autre runner (probablement le live main-windows-pc). Relancer avec -AllowOverwrite pour écraser volontairement."
        }
        Write-Warning "écrasement volontaire d'une config non pc-fixe (-AllowOverwrite)."
    }
}

# --- garde-fou 2 : ne jamais couper des jobs actifs ---
if (Test-Path $StatusPath) {
    try { $status = Get-Content $StatusPath -Raw | ConvertFrom-Json } catch { $status = $null }
    $active = @()
    if ($status -and $status.active_jobs) { $active = @($status.active_jobs) }
    if ($active.Count -gt 0 -and -not $Force) {
        throw "refus : $($active.Count) job(s) actif(s) ($($active -join ', ')). Attendre leur fin ou relancer avec -Force."
    }
}

# --- rend le runner.toml pc-fixe (substitution <user>) ---
$tpl = Get-Content $Example -Raw -Encoding utf8
if ($tpl -notmatch "<user>") { throw "l'exemple ne contient aucun placeholder <user> à substituer" }
$rendered = $tpl -replace "<user>", $User
New-Item -ItemType Directory -Force -Path $RunnerHome | Out-Null
if ((Test-Path $TomlPath) -and -not $AllowOverwrite -and -not $Rotate) {
    Write-Output "config conservée : $TomlPath (déjà pc-fixe ; -AllowOverwrite pour régénérer)."
} else {
    Set-Content -Encoding utf8 -Path $TomlPath -Value $rendered
    Write-Output "config écrite : $TomlPath"
}

# --- crée les dossiers de workspaces manquants (rien d'autre) ---
foreach ($line in ($rendered -split "`n")) {
    if ($line -match "^\s*path\s*=\s*'(.*)'") {
        $p = $Matches[1]
        if (-not (Test-Path $p)) {
            New-Item -ItemType Directory -Force -Path $p | Out-Null
            Write-Output "dossier créé : $p"
        }
    }
}

# --- jeton DPAPI sur place (jamais affiché, jamais dans Git) ---
$tokenFile = Join-Path $RunnerHome "runner.token"
$genArgs = @("-m", "orch_runner", "gen-token", "--config", $TomlPath)
if ($Rotate -or -not (Test-Path $tokenFile)) {
    if ((Test-Path $tokenFile) -and -not $Rotate) { throw "logique interne : jeton présent sans -Rotate" }
    if ($Rotate) { $genArgs += "--force" }
    $env:PYTHONPATH = (Join-Path $Repo "src")
    $out = & $Python @genArgs 2>&1
    if ($LASTEXITCODE -ne 0) { throw "gen-token a échoué : $out" }
    $fp = ($out | Select-String -Pattern "^pc-fixe:[0-9a-f]{64}$" | Select-Object -First 1)
    if (-not $fp) { throw "empreinte pc-fixe:<sha256> introuvable dans la sortie gen-token" }
    Write-Output "OK : jeton pc-fixe généré et chiffré DPAPI sur ce PC."
    Write-Output "ÉTAPE VPS (vps-etude) :"
    Write-Output "  sudo bash <repo>/deploy/add-runner-token.sh '$($fp.Line.Trim())'"
} else {
    Write-Output "jeton conservé : $tokenFile (-Rotate pour rotation)."
    Write-Output "Rappel : l'empreinte pc-fixe:<sha256> correspondante doit figurer dans ORCH_RUNNER_TOKENS côté VPS."
}

if ($Install) {
    Write-Output "installation de la tâche planifiée via install-runner.ps1 ..."
    & (Join-Path $Repo "deploy\windows\install-runner.ps1") -Python $Python
} else {
    Write-Output "Pour démarrer le runner : relancer avec -Install (ou deploy\\windows\\install-runner.ps1)."
}
Write-Output "Vérification : $Python -m orch_runner status ; agent_runner_list / agent_runner_inspect(runner_id='pc-fixe')."
