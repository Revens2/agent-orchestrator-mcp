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
        try { $sid = $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { return $false }
        $sid -in @('S-1-5-11', 'S-1-5-32-545', 'S-1-1-0') -and
        $_.AccessControlType -eq 'Allow' -and ($_.FileSystemRights.ToString() -match 'Modify|Write|FullControl|CreateFiles|AppendData') }
}

function Assert-IcaclsOk([string]$step) {
    # Ne masque jamais les erreurs : aucun /c, controle strict du code retour.
    if ($LASTEXITCODE -ne 0) { throw "icacls FAILED [$step] exit=$LASTEXITCODE -> rollback requis" }
}

function Assert-PrivateBackupDir([string]$dir) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    # Lieu prive : seuls Owner(Juliann)/SYSTEM/Administrateurs. Refuse si inscriptible par S-1-5-11/545/1-0 ou CodexSandboxUsers.
    $acl = Get-Acl $dir
    $bad = $acl.Access | Where-Object {
        try { $sid = $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { return $true }
        ($sid -in @('S-1-5-11', 'S-1-5-32-545', 'S-1-1-0') -and $_.AccessControlType -eq 'Allow') -or
        ($_.IdentityReference.Value -match 'CodexSandbox') }
    if ($bad) { throw "BackupDir NON PRIVE : $dir contient une ACE large/sandbox. Refus d'y stocker des sauvegardes ACL." }
    # Exige Juliann + SYSTEM + Administrateurs en Allow (heritage du profil racine sain).
    foreach ($need in @('S-1-5-32-544', 'S-1-5-18')) {
        $ok = $acl.Access | Where-Object {
            try { $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -eq $need } catch { $false } }
        if (-not $ok) { throw "BackupDir $dir : ACE manquante $need (SYSTEM/Admin requis)." }
    }
}

function Test-RequiredAccess([string]$p, [string]$ownerSid) {
    # Juliann (M min), SYSTEM (F), Administrateurs (F) doivent conserver l'acces. Retourne la liste des manques.
    $missing = @()
    $acl = Get-Acl $p
    $rules = $acl.Access | Where-Object { $_.AccessControlType -eq 'Allow' }
    $has = {
        param($sid, $min)
        [bool]($rules | Where-Object {
            $v = $null
            try { $v = $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { $v = $null }
            ($v -eq $sid) -and ($_.FileSystemRights.ToString() -match $min) })
    }
    if (-not (& $has $ownerSid 'Modify|FullControl|Write')) { $missing += "Owner($ownerSid) sans ecriture sur $p" }
    if (-not (& $has 'S-1-5-18' 'FullControl')) { $missing += "SYSTEM sans FullControl sur $p" }
    if (-not (& $has 'S-1-5-32-544' 'FullControl')) { $missing += "Administrateurs sans FullControl sur $p" }
    return $missing
}

function Restore-Backup([string]$target, [string]$saveFile) {
    # Rollback : icacls <parent> /restore <fichier> (SANS /c pour ne pas masquer les erreurs).
    $parent = Split-Path $target -Parent
    Write-Warning "ROLLBACK AUTO : icacls `"$parent`" /restore `"$saveFile`""
    & icacls.exe $parent /restore $saveFile
    Assert-IcaclsOk "restore $target"
}

if ($Restore) {
    Assert-PrivateBackupDir $BackupDir
    foreach ($t in $Targets) {
        $leaf = Split-Path $t -Leaf
        $cands = @(Get-ChildItem $BackupDir -Filter "tools-$leaf-*.acl" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime)
        if (-not $cands -or $cands.Count -eq 0) { Write-Warning "aucune sauvegarde pour $t"; continue }
        $last = $cands[-1]
        if ($last.Length -eq 0) { throw "sauvegarde vide/invalide : $($last.FullName) -> refus restore" }
        Write-Host "RESTORE: icacls $(Split-Path $t -Parent) /restore $($last.FullName)"
        if ($Execute) {
            & icacls.exe (Split-Path $t -Parent) /restore $last.FullName
            Assert-IcaclsOk "restore $t"
            # TEST APRES restore : acces requis conserves ?
            $miss = Test-RequiredAccess $t $OwnerSid
            if ($miss) { throw "APRES RESTORE acces manquant : $($miss -join '; ')" }
            Write-Host "RESTORE-OK $t"
        }
    }
    if (-not $Execute) { Write-Host "DRY-RUN restore (ajouter -Execute)."; }
    return
}

# TEST AVANT explicite (borne, rapide en dry-run) : racines + 3 fichiers executes sous Juliann.
# L'enumeration recursive exhaustive est reservee a la phase -Execute (elevee) et au TEST APRES.
$ExecFiles = @('C:\Tools\silent-run.exe', 'C:\Tools\mcp-hidden-run.pyw', 'C:\projet\opencode-rotate\Watch-Rotate.ps1')
foreach ($t in $Targets) {
    if (-not (Test-Path $t)) { Write-Host "absent : $t"; continue }
    $miss = @(Test-RequiredAccess $t $OwnerSid)
    Write-Host ("AVANT $t : racine modifiable par S-1-5-11/545/1-0: {0} ; acces requis manquants: {1}" -f [bool](Writable-ByEveryone $t), ($(if ($miss.Count) { $miss -join '; ' } else { 'aucun' })))
}
foreach ($f in $ExecFiles) {
    if (-not (Test-Path $f)) { Write-Host "absent : $f"; continue }
    Write-Host ("AVANT $f : modifiable par S-1-5-11/545/1-0: {0}" -f [bool](Writable-ByEveryone $f))
}
if (-not $Execute) { Write-Host "DRY-RUN (ajouter -Execute). Aucune modification effectuee. (Recurse exhaustif en phase -Execute.)"; return }

Assert-PrivateBackupDir $BackupDir
$ts = Get-Date -Format 'yyyyMMdd-HHmmss'
foreach ($t in $Targets) {
    if (-not (Test-Path $t)) { continue }
    $save = Join-Path $BackupDir "tools-$(Split-Path $t -Leaf)-$ts.acl"
    # Sauvegarde ACL (SANS /c) + verification de restorabilite (fichier non vide + contient le leaf).
    & icacls.exe $t /save $save /t
    Assert-IcaclsOk "save $t"
    if (-not (Test-Path $save) -or (Get-Item $save).Length -eq 0) { throw "sauvegarde ACL echouee pour $t" }
    $content = Get-Content $save -Raw -Encoding Unicode -ErrorAction Stop
    if ($content -notmatch [regex]::Escape((Split-Path $t -Leaf))) { throw "sauvegarde $save non restaurable (leaf absent) -> arret avant toute modif de $t" }
    try {
        # INTERDIT : /reset global, suppression, reset inheritance:r. Seul /inheritance:d (copie herite->explicite) autorise.
        & icacls.exe $t /inheritance:d
        Assert-IcaclsOk "inheritance:d $t"
        & icacls.exe $t /remove:g '*S-1-5-11' '*S-1-5-32-545' /t
        Assert-IcaclsOk "remove:g $t"
        & icacls.exe $t /grant:r '*S-1-5-32-545:(OI)(CI)RX' "*$($OwnerSid):(OI)(CI)M"
        Assert-IcaclsOk "grant $t"  # Juliann garde l'edition non elevee ; Users repasse en RX seul
        # TEST APRES explicite.
        $left = @(Get-ChildItem $t -Recurse -Force -ErrorAction Stop | Where-Object { Writable-ByEveryone $_.FullName })
        $missAfter = @(Test-RequiredAccess $t $OwnerSid)
        if ($left.Count -or (Writable-ByEveryone $t)) {
            Restore-Backup $t $save
            throw "REGRESSION ecriture residuelle sous $t -> ROLLBACK AUTO effectue (restore $save)"
        }
        if ($missAfter.Count) {
            Restore-Backup $t $save
            throw "REGRESSION acces requis perdus sous $t ($($missAfter -join '; ')) -> ROLLBACK AUTO effectue"
        }
        Write-Host "OK $t (sauvegarde $save)"
    } catch {
        # Toute erreur APRES debut de modif => tentative de rollback, puis re-soulever.
        try { Restore-Backup $t $save } catch { Write-Warning "rollback a echoue : $_" }
        throw
    }
}
Write-Host "Verifier ensuite SANS declencher de rotation : Get-ScheduledTaskInfo -TaskName 'opencode-rotate-watch','orch-runner' ; opencode --version (ne pas schtasks /run ici, voir rapport)."
