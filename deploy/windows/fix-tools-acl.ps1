<#
.SYNOPSIS
  Prerequis BLOQUANT avant toute creation de compte : retire l'ecriture "Utilisateurs authentifies"
  (heritee de C:\) sur des scripts executes SOUS Juliann. Sinon tout nouveau compte standard pourrait
  modifier ces fichiers et executer du code en tant que Juliann (admin UAC).

  Constat audit 2026-10-09 (docs/autopilot/audit/windows-audit.md) :
    C:\Tools\silent-run.exe, C:\Tools\mcp-hidden-run.pyw, C:\projet\opencode-rotate\Watch-Rotate.ps1
    => Authenticated Users (S-1-5-11) : Modify (herite).

  Effet : coupe l'heritage sur les 2 dossiers, retire S-1-5-11 / Users en ecriture, donne Users en RX.
  Juliann, SYSTEM, Administrateurs gardent leurs droits. Sauvegarde icacls avant, rollback fourni.

.USAGE (PowerShell ELEVE)
  .\fix-tools-acl.ps1                    # dry-run
  .\fix-tools-acl.ps1 -Execute
  .\fix-tools-acl.ps1 -Restore -Execute  # rollback depuis la derniere sauvegarde
#>
[CmdletBinding()]
param([switch]$Execute, [switch]$Restore)
$ErrorActionPreference = 'Stop'
$Targets   = @('C:\Tools', 'C:\projet\opencode-rotate')
$BackupDir = 'C:\Users\Juliann\acl-backups'
$id = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if ($Execute -and -not $id.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw "PowerShell eleve requis" }
$OwnerSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value   # Juliann (session elevee)

function Writable-ByEveryone($p) {
    (Get-Acl $p).Access | Where-Object {
        $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -in @('S-1-5-11', 'S-1-5-32-545', 'S-1-1-0') -and
        $_.AccessControlType -eq 'Allow' -and ($_.FileSystemRights.ToString() -match 'Modify|Write|FullControl|CreateFiles|AppendData') }
}

if ($Restore) {
    foreach ($t in $Targets) {
        $leaf = Split-Path $t -Leaf
        $last = Get-ChildItem $BackupDir -Filter "tools-$leaf-*.acl" | Sort-Object LastWriteTime | Select-Object -Last 1
        if (-not $last) { Write-Warning "aucune sauvegarde pour $t"; continue }
        Write-Host "RESTORE: icacls $(Split-Path $t -Parent) /restore $($last.FullName)"
        if ($Execute) { & icacls.exe (Split-Path $t -Parent) /restore $last.FullName /c }
    }
    return
}

foreach ($t in $Targets) {
    if (-not (Test-Path $t)) { Write-Host "absent : $t"; continue }
    $w = Get-ChildItem $t -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { Writable-ByEveryone $_.FullName }
    Write-Host "$t : $(@($w).Count) element(s) modifiables par tous (racine: $([bool](Writable-ByEveryone $t)))"
}
if (-not $Execute) { Write-Host "DRY-RUN (ajouter -Execute)."; return }

New-Item -ItemType Directory -Force $BackupDir | Out-Null
$ts = Get-Date -Format 'yyyyMMdd-HHmmss'
foreach ($t in $Targets) {
    if (-not (Test-Path $t)) { continue }
    $save = Join-Path $BackupDir "tools-$(Split-Path $t -Leaf)-$ts.acl"
    & icacls.exe $t /save $save /t /c | Out-Null
    if (-not (Test-Path $save) -or (Get-Item $save).Length -eq 0) { throw "sauvegarde ACL echouee pour $t" }
    & icacls.exe $t /inheritance:d /c | Out-Null                       # copie l'herite en explicite
    & icacls.exe $t /remove:g '*S-1-5-11' '*S-1-5-32-545' /t /c | Out-Null
    & icacls.exe $t /grant:r '*S-1-5-32-545:(OI)(CI)RX' "*$($OwnerSid):(OI)(CI)M" /c | Out-Null  # Juliann garde l'edition non elevee
    $left = @(Get-ChildItem $t -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { Writable-ByEveryone $_.FullName })
    if ($left.Count -or (Writable-ByEveryone $t)) { throw "ecriture residuelle sous $t -> rollback : .\fix-tools-acl.ps1 -Restore -Execute" }
    Write-Host "OK $t (sauvegarde $save)"
}
Write-Host "Verifier ensuite que les taches Juliann fonctionnent : schtasks /run /tn opencode-rotate-watch ; Get-ScheduledTaskInfo"
