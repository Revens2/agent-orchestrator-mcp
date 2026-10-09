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
  PERIMETRE AUTORISE : C:\Users\Juliann\.orch-runner UNIQUEMENT.
  -IncludeOpencodeAuth est INTERDIT (hors perimetre : .local/share/opencode/auth.json,
  mcp-auth.json) et fait echouer le script en NOT_TOUCHED.
#>
[CmdletBinding()]
param(
    [switch]$Execute,
    [string]$Restore,
    [switch]$IncludeOpencodeAuth
)
$ErrorActionPreference = 'Stop'
if ($IncludeOpencodeAuth) { throw "HORS PERIMETRE : -IncludeOpencodeAuth refuse (auth.json/mcp-auth.json = NOT_TOUCHED). Mission autorise .orch-runner uniquement." }
$Profile_  = 'C:\Users\Juliann'
$Target    = Join-Path $Profile_ '.orch-runner'
$BackupDir = Join-Path $Profile_ 'acl-backups'
$Ts        = Get-Date -Format 'yyyyMMdd-HHmmss'
$GroupSid  = (Get-LocalGroup -Name 'CodexSandboxUsers').SID.Value   # attendu S-1-5-21-...-1003
$OwnerSid  = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value  # Juliann

function Assert-IcaclsOk([string]$step) {
    # Ne masque jamais les erreurs : aucun /c, controle strict du code retour.
    if ($LASTEXITCODE -ne 0) { throw "icacls FAILED [$step] exit=$LASTEXITCODE -> rollback requis" }
}

function Assert-PrivateBackupDir([string]$dir) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $acl = Get-Acl $dir
    $bad = $acl.Access | Where-Object {
        try { $sid = $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { return $true }
        ($sid -in @('S-1-5-11', 'S-1-5-32-545', 'S-1-1-0') -and $_.AccessControlType -eq 'Allow') -or
        ($_.IdentityReference.Value -match 'CodexSandbox') }
    if ($bad) { throw "BackupDir NON PRIVE : $dir contient une ACE large/sandbox. Refus d'y stocker des sauvegardes ACL." }
}

function Test-SandboxAce([string]$p) {
    # Vrai si CodexSandboxUsers apparait en ACE directe OU heritee (icacls + Get-Acl croises, metadonnees seules).
    $viaIcacls = @(icacls.exe $p 2>$null | Select-String -Pattern 'CodexSandbox' -SimpleMatch)
    if ($viaIcacls.Count) { return $true }
    $acl = Get-Acl $p -ErrorAction Stop
    return [bool]($acl.Access | Where-Object { $_.IdentityReference.Value -match 'CodexSandbox' })
}

function Test-RequiredAccessOrch([string]$p) {
    $missing = @()
    $acl = Get-Acl $p -ErrorAction Stop
    $rules = $acl.Access | Where-Object { $_.AccessControlType -eq 'Allow' }
    $has = {
        param($sid, $min)
        [bool]($rules | Where-Object {
            $v = $null
            try { $v = $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { $v = $null }
            ($v -eq $sid) -and ($_.FileSystemRights.ToString() -match $min) })
    }
    if (-not (& $has $OwnerSid 'FullControl|Modify')) { $missing += "Owner($OwnerSid) sans ecriture sur $p" }
    if (-not (& $has 'S-1-5-18' 'FullControl')) { $missing += "SYSTEM sans FullControl sur $p" }
    if (-not (& $has 'S-1-5-32-544' 'FullControl')) { $missing += "Administrateurs sans FullControl sur $p" }
    return $missing
}

function Restore-OrchBackup([string]$saveFile) {
    if (-not (Test-Path $saveFile)) { throw "fichier de sauvegarde introuvable: $saveFile" }
    if ((Get-Item $saveFile).Length -eq 0) { throw "sauvegarde vide/invalide : $saveFile -> refus restore" }
    Write-Warning "ROLLBACK AUTO : icacls `"$Profile_`" /restore `"$saveFile`""
    & icacls.exe $Profile_ /restore $saveFile
    Assert-IcaclsOk "restore .orch-runner"
}

function Show-Acl($p) { & icacls.exe $p | Select-Object -First 8 }

if ($Restore) {
    # icacls /restore s'applique au dossier PARENT de celui sauvegarde (noms relatifs dans le fichier).
    if (-not (Test-Path $Restore)) { throw "fichier de sauvegarde introuvable: $Restore" }
    if ($Restore -match 'opencode-auth') { throw "HORS PERIMETRE : restore opencode-auth refuse (NOT_TOUCHED)." }
    Write-Host "RESTORE: icacls `"$Profile_`" /restore `"$Restore`""
    if ($Execute) {
        Assert-PrivateBackupDir $BackupDir
        Restore-OrchBackup $Restore
        $miss = Test-RequiredAccessOrch $Target
        if ($miss) { throw "APRES RESTORE acces manquant : $($miss -join '; ')" }
        Show-Acl $Target
        Write-Host "RESTORE-OK $Target"
    } else { Write-Host "DRY-RUN restore (ajouter -Execute)." }
    return
}

Write-Host "== Etat actuel (AVANT)"; Show-Acl $Target
Write-Host ("AVANT sandbox sur racine: {0}" -f (Test-SandboxAce $Target))
$missAvant = Test-RequiredAccessOrch $Target
Write-Host ("AVANT acces requis manquants: {0}" -f ($(if ($missAvant.Count) { $missAvant -join '; ' } else { 'aucun' })))
$saveFile = Join-Path $BackupDir "orch-runner-$Ts.acl"
Write-Host "PLAN:"
Write-Host "  1. icacls `"$Target`" /save `"$saveFile`" /t   (SANS /c, verifie + restaurable)"
Write-Host "  2. icacls `"$Target`" /remove:g *$GroupSid      (retire l'ACE explicite racine, SANS /reset global)"
Write-Host "  3. verification : aucune ACE $GroupSid (directe/heritee) dans l'arbre + acces Owner/SYSTEM/Admin conserves"
Write-Host "  INTERDIT : /reset, suppression, toucher auth.json/mcp-auth.json, toucher .local."
if (-not $Execute) { Write-Host "DRY-RUN (ajouter -Execute). Aucune modification effectuee."; return }

Assert-PrivateBackupDir $BackupDir
& icacls.exe $Target /save $saveFile /t
Assert-IcaclsOk "save .orch-runner"
if (-not (Test-Path $saveFile) -or (Get-Item $saveFile).Length -eq 0) { throw "sauvegarde ACL echouee" }
$sc = Get-Content $saveFile -Raw -Encoding Unicode -ErrorAction Stop
if ($sc -notmatch '\.orch-runner') { throw "sauvegarde $saveFile non restaurable (.orch-runner absent) -> arret avant modif" }
Write-Host "Sauvegarde: $saveFile  (rollback: .\b-fix-orch-runner-acl.ps1 -Restore `"$saveFile`" -Execute)"

try {
    & icacls.exe $Target /remove:g "*$GroupSid"
    Assert-IcaclsOk "remove:g CodexSandboxUsers"
} catch {
    try { Restore-OrchBackup $saveFile } catch { Write-Warning "rollback a echoue : $_" }
    throw
}

# TEST APRES explicite (erreurs d'enumeration NON masquees).
$left = @(Get-ChildItem -Force -Recurse $Target -ErrorAction Stop | Where-Object { Test-SandboxAce $_.FullName })
$missApres = @(Test-RequiredAccessOrch $Target)
if (Test-SandboxAce $Target) { $left += $Target }
if ($left.Count) {
    Restore-OrchBackup $saveFile
    throw ("REGRESSION ACE sandbox residuelles ({0}) -> ROLLBACK AUTO effectue (restore {1})" -f ($left -join ', '), $saveFile)
}
if ($missApres.Count) {
    Restore-OrchBackup $saveFile
    throw ("REGRESSION acces requis perdus ({0}) -> ROLLBACK AUTO effectue" -f ($missApres -join '; '))
}
Write-Host "OK: plus aucune ACE CodexSandboxUsers sous $Target (directe/heritee)"
Show-Acl $Target
Write-Host "Note : runner.token / runner.toml verifies par ACL/metadonnees uniquement (jamais ouverts ; octets jamais lus)."
Write-Host "HORS PERIMETRE (NOT_TOUCHED, meme avec -IncludeOpencodeAuth) : .local/share/opencode/auth.json, mcp-auth.json."
