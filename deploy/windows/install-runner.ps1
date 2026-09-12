<#
Installe / met à jour le runner orch pour l'utilisateur courant (sans droits admin).

- copie src\orch_protocol + src\orch_runner vers %USERPROFILE%\.orch-runner\app
- venv dédié %USERPROFILE%\.orch-runner\venv (httpx seulement) + .pth vers app
- tâche planifiée "orch-runner" : à l'ouverture de session de l'utilisateur, pythonw
  (sans console), redémarrage automatique, instance unique, sans limite de durée.
  Les runtimes (Claude/Codex/AGY) utilisent l'authentification du profil : le runner
  tourne donc dans la session de l'utilisateur, pas en service système.

Prérequis : %USERPROFILE%\.orch-runner\runner.toml et runner.token (gen-token).
Ne pas installer depuis AppData : les apps MSIX (Claude Desktop) virtualisent AppData\Local.
Usage : powershell -NoProfile -ExecutionPolicy Bypass -File install-runner.ps1 [-Python C:\...\python.exe] [-Uninstall]
#>
param(
    [string]$Python = "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    [switch]$Uninstall
)
$ErrorActionPreference = "Stop"
# Hors AppData (virtualisé pour les apps MSIX) : la tâche planifiée doit voir les mêmes fichiers.
$Home_ = Join-Path $env:USERPROFILE ".orch-runner"
$App = Join-Path $Home_ "app"
$Venv = Join-Path $Home_ "venv"
$TaskName = "orch-runner"
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

function Stop-Runner {
    $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($t) { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue }
    Get-CimInstance Win32_Process -Filter "Name='pythonw.exe' OR Name='python.exe'" |
        Where-Object { $_.CommandLine -match "orch_runner\s+run" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
}

if ($Uninstall) {
    Stop-Runner
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Output "Tâche $TaskName retirée (config, jeton et logs conservés dans $Home_)."
    exit 0
}

foreach ($f in @("runner.toml", "runner.token")) {
    if (-not (Test-Path (Join-Path $Home_ $f))) { throw "manque $Home_\$f" }
}
Stop-Runner

New-Item -ItemType Directory -Force -Path $App | Out-Null
foreach ($pkg in @("orch_protocol", "orch_runner")) {
    $dst = Join-Path $App $pkg
    if (Test-Path $dst) { Remove-Item -Recurse -Force $dst }
    Copy-Item -Recurse (Join-Path $Repo "src\$pkg") $dst
    Get-ChildItem -Recurse -Directory -Filter __pycache__ $dst | Remove-Item -Recurse -Force
}

if (-not (Test-Path (Join-Path $Venv "Scripts\pythonw.exe"))) { & $Python -m venv $Venv }
& (Join-Path $Venv "Scripts\python.exe") -m pip install -q --disable-pip-version-check "httpx==0.28.1"
$site = & (Join-Path $Venv "Scripts\python.exe") -c "import sysconfig; print(sysconfig.get_paths()['purelib'])"
Set-Content -Encoding ascii -Path (Join-Path $site "orch_runner_app.pth") -Value $App

$pyw = Join-Path $Venv "Scripts\pythonw.exe"
$action = New-ScheduledTaskAction -Execute $pyw -Argument "-m orch_runner run" -WorkingDirectory $Home_
$logon = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
# Filet de sécurité : RestartCount ne couvre que les échecs de lancement, pas la mort du
# processus. Un déclencheur répété chaque minute relance un runner mort ; s'il tourne,
# MultipleInstances=IgnoreNew ignore le déclenchement (et le mutex empêche tout doublon).
$watchdog = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$trigger = @($logon, $watchdog)
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal `
    -Description "Runner de l'orchestrateur d'agents IA (connexion sortante NetBird vers vps-etude)" -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Write-Output "OK : tâche $TaskName installée et démarrée. Statut : $Venv\Scripts\python.exe -m orch_runner status"
