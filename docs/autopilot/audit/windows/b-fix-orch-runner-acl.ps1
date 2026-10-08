<#
.SYNOPSIS
  (b) Retire l'acces CodexSandboxUsers sur C:\Users\Juliann\.orch-runner, avec sauvegarde/restauration icacls.
  PREPARE, NON EXECUTE. A lancer sous Juliann (pas besoin d'elevation : Juliann est proprietaire + FullControl).

.CONSTAT (audit 2026-10-09)
  - ACE EXPLICITE (non heritee) sur le dossier racine :
      DESKTOP-V30KQ19\CodexSandboxUsers:(OI)(CI)(RX)   [SID ...-1003]
  - Tous les fichiers (runner.token, runner.token.bak-rotate-*, runner.toml*, scripts) et sous-dossiers
    (app, venv, logs, jobs, backups...) l'HERITENT ; aucune autre ACE explicite/protegee dans l'arbre (profondeur 2).
  - Donc : retirer la seule ACE racine suffit, l'heritage la fait disparaitre partout.

.USAGE
  .\b-fix-orch-runner-acl.ps1                 # dry-run : affiche ce qui serait fait
  .\b-fix-orch-runner-acl.ps1 -Execute        # sauvegarde + correction + verification
  .\b-fix-orch-runner-acl.ps1 -Restore <fichier.acl>   # rollback
  Option -IncludeOpencodeAuth : casse aussi l'heritage sur auth.json / mcp-auth.json d'OpenCode
  (herites de C:\Users\Juliann\.local : CodexSandboxUsers + SID etranger en Modify). Voir risque ci-dessous.
#>
[CmdletBinding()]
param(
    [switch]$Execute,
    [string]$Restore,
    [switch]$IncludeOpencodeAuth
)
$ErrorActionPreference = 'Stop'
$Profile_  = 'C:\Users\Juliann'
$Target    = Join-Path $Profile_ '.orch-runner'
$BackupDir = Join-Path $Profile_ 'acl-backups'
$Ts        = Get-Date -Format 'yyyyMMdd-HHmmss'
$GroupSid  = (Get-LocalGroup -Name 'CodexSandboxUsers').SID.Value   # attendu S-1-5-21-...-1003
$ForeignSids = @('S-1-5-21-1166338081-286841839-4214772731-2482207823')  # SID non resolu observe sur .local
$OcAuth    = @("$Profile_\.local\share\opencode\auth.json", "$Profile_\.local\share\opencode\mcp-auth.json")

function Show-Acl($p) { & icacls.exe $p | Select-Object -First 8 }

if ($Restore) {
    # icacls /restore s'applique au dossier PARENT de celui sauvegarde (noms relatifs dans le fichier).
    if (-not (Test-Path $Restore)) { throw "fichier de sauvegarde introuvable: $Restore" }
    $parent = if ($Restore -match 'opencode-auth') { "$Profile_\.local\share\opencode" } else { $Profile_ }
    Write-Host "RESTORE: icacls `"$parent`" /restore `"$Restore`""
    if ($Execute) { & icacls.exe $parent /restore $Restore /c; Show-Acl $Target }
    return
}

Write-Host "== Etat actuel"; Show-Acl $Target
$saveFile = Join-Path $BackupDir "orch-runner-$Ts.acl"
Write-Host "PLAN:"
Write-Host "  1. icacls `"$Target`" /save `"$saveFile`" /t /c"
Write-Host "  2. icacls `"$Target`" /remove:g *$GroupSid      (retire l'ACE explicite racine)"
Write-Host "  3. verification : aucune ACE $GroupSid dans l'arbre"
if ($IncludeOpencodeAuth) {
    Write-Host "  4. auth OpenCode : /save puis /inheritance:d puis /remove:g *$GroupSid et SID etrangers sur auth.json, mcp-auth.json"
}
if (-not $Execute) { Write-Host "DRY-RUN (ajouter -Execute)."; return }

New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
& icacls.exe $Target /save $saveFile /t /c | Out-Null
if (-not (Test-Path $saveFile) -or (Get-Item $saveFile).Length -eq 0) { throw "sauvegarde ACL echouee" }
Write-Host "Sauvegarde: $saveFile  (rollback: .\b-fix-orch-runner-acl.ps1 -Restore `"$saveFile`" -Execute)"

& icacls.exe $Target /remove:g "*$GroupSid" /c
if ($LASTEXITCODE -ne 0) { throw "icacls /remove a echoue ($LASTEXITCODE) -> restaurer" }

$left = Get-ChildItem -Force -Recurse $Target -ErrorAction SilentlyContinue | Where-Object {
    (Get-Acl $_.FullName -ErrorAction SilentlyContinue).Access.IdentityReference.Value -match 'CodexSandbox'
}
if ($left) { Write-Warning "ACE residuelles:"; $left.FullName | Select-Object -First 20 } else { Write-Host "OK: plus aucune ACE CodexSandboxUsers sous $Target" }
Show-Acl $Target; Show-Acl "$Target\runner.token"

if ($IncludeOpencodeAuth) {
    # RISQUE : le sandbox Codex peut avoir besoin de .local (Modify) pour ses propres outils ; on ne touche
    # QUE les deux fichiers d'auth OpenCode, pas le dossier. Le sandbox Codex peut re-poser des ACE
    # a sa prochaine initialisation (hypothese a verifier apres un run Codex).
    $ocSave = Join-Path $BackupDir "opencode-auth-$Ts.acl"
    Push-Location "$Profile_\.local\share\opencode"
    & icacls.exe 'auth.json' /save $ocSave /c | Out-Null
    & icacls.exe 'mcp-auth.json' /save "$ocSave.mcp" /c | Out-Null
    Pop-Location
    foreach ($f in $OcAuth) {
        & icacls.exe $f /inheritance:d /c | Out-Null            # copie les ACE heritees en explicites
        & icacls.exe $f /remove:g "*$GroupSid" /c | Out-Null
        foreach ($s in $ForeignSids) { & icacls.exe $f /remove:g "*$s" /c | Out-Null }
        Show-Acl $f
    }
    Write-Host "Sauvegardes auth OpenCode: $ocSave(.mcp) -> restore: icacls `"$Profile_\.local\share\opencode`" /restore <fichier>"
}
