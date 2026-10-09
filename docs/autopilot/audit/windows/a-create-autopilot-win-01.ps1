<#
.SYNOPSIS
  (a) Cree le compte local STANDARD autopilot-win-01, l'arborescence C:\autopilot avec ACL restrictives,
  un runner orch-runner dedie (runner_id distinct) et sa tache planifiee. PREPARE, NON EXECUTE.
  A lancer dans un PowerShell ELEVE (admin) sous Juliann.

.PREREQUIS BLOQUANTS (constates le 2026-10-09)
  - C:\ donne "Utilisateurs authentifies:(OI)(CI)(IO)(M)" -> tout nouveau compte peut MODIFIER :
      C:\Tools\silent-run.exe           (action de la tache orch-rotate-token, executee sous Juliann)
      C:\Tools\mcp-hidden-run.pyw       (lance les MCP d'OpenCode sous Juliann)
      C:\projet\opencode-rotate\Watch-Rotate.ps1 (tache opencode-rotate-watch toutes les 2 min sous Juliann)
    => creer le compte AVANT correction = escalade vers Juliann (admin UAC). Le script refuse de continuer.
  - Le Python du venv actuel (C:\Users\Juliann\AppData\Local\Programs\Python\Python311) et opencode.exe
    (WinGet, profil Juliann) ne sont PAS accessibles aux autres comptes : il faut des copies/installs dediees.

.USAGE
  .\a-create-autopilot-win-01.ps1                    # dry-run (preflight + plan)
  .\a-create-autopilot-win-01.ps1 -Execute -PythonExe 'C:\Program Files\Python311\python.exe'
#>
[CmdletBinding()]
param(
    [switch]$Execute,
    [string]$UserName   = 'autopilot-win-01',
    [string]$Root       = 'C:\autopilot',
    [string]$RunnerId   = 'autopilot-win-01',
    [string]$PythonExe  = 'C:\Program Files\Python311\python.exe',   # install "for all users" requise
    [string]$OpencodeSrc = 'C:\Users\Juliann\AppData\Local\Microsoft\WinGet\Packages\SST.opencode_Microsoft.Winget.Source_8wekyb3d8bbwe\opencode.exe',
    [string]$RunnerAppSrc = 'C:\Users\Juliann\.orch-runner\app',
    [string]$BrokerUrl  = 'http://10.200.114.203:8803',   # valeur lue dans runner.toml actuel (non secrete)
    [switch]$SkipPreflight
)
$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------- 0. preflight
$id = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $id.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw "PowerShell eleve requis" }

$authUsers = 'S-1-5-11'
$mustNotBeWritable = @('C:\Tools', 'C:\Tools\silent-run.exe', 'C:\Tools\mcp-hidden-run.pyw',
                       'C:\projet\opencode-rotate', 'C:\projet\opencode-rotate\Watch-Rotate.ps1')
$bad = foreach ($p in $mustNotBeWritable) {
    if (Test-Path $p) {
        $w = (Get-Acl $p).Access | Where-Object {
            $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -in @($authUsers, 'S-1-5-32-545') -and
            $_.AccessControlType -eq 'Allow' -and ($_.FileSystemRights.ToString() -match 'Modify|Write|FullControl') }
        if ($w) { $p }
    }
}
if ($bad -and -not $SkipPreflight) {
    Write-Warning "BLOQUANT : ecriture ouverte a Utilisateurs authentifies sur :"; $bad | ForEach-Object { "   $_" }
    Write-Host @"
Correction suggeree (a valider puis executer a part, avec sauvegarde) :
  icacls C:\Tools /save C:\acl-backup-tools.acl /t /c
  icacls C:\Tools /inheritance:d /c
  icacls C:\Tools /remove:g *S-1-5-11 /t /c
  icacls C:\Tools /grant:r "*S-1-5-32-545:(OI)(CI)RX" /c
  (idem pour C:\projet\opencode-rotate ; ou deplacer ces scripts sous le profil Juliann)
"@
    throw "preflight KO"
}
if (Get-LocalUser -Name $UserName -ErrorAction SilentlyContinue) { throw "le compte $UserName existe deja" }
if (-not (Test-Path $PythonExe)) { throw "Python machine introuvable: $PythonExe (installer Python 'for all users')" }

$dirs = [ordered]@{
    "$Root"                 = 'root'
    "$Root\bin"             = 'rx'      # opencode.exe copie, lecture/exec seulement
    "$Root\runner"          = 'rx'      # app + venv runner : code non modifiable par le compte
    "$Root\runner\state"    = 'm'       # runner.toml (lecture), runner.token (DPAPI), status.json, logs
    "$Root\workspaces"      = 'm'       # seuls dossiers ou les agents ecrivent
    "$Root\tmp"             = 'm'
}
Write-Host "PLAN: compte standard $UserName (groupe Utilisateurs uniquement), arbo:"; $dirs.GetEnumerator() | ForEach-Object { "   $($_.Key) [$($_.Value)]" }
if (-not $Execute) { Write-Host "DRY-RUN (ajouter -Execute)."; return }

# ---------------------------------------------------------------- 1. compte standard
$pw = Read-Host "Mot de passe pour $UserName (jamais stocke dans ce script)" -AsSecureString
New-LocalUser -Name $UserName -Password $pw -FullName 'Autopilot runner' `
    -Description 'Compte standard dedie Autopilot V1 (orch-runner)' -PasswordNeverExpires -UserMayNotChangePassword | Out-Null
Add-LocalGroupMember -Group (Get-LocalGroup -SID 'S-1-5-32-545').Name -Member $UserName
foreach ($g in 'S-1-5-32-544', 'S-1-5-32-555', 'S-1-5-32-580') {   # Administrateurs, RDP, Remote Mgmt
    if ((Get-LocalGroupMember -SID $g -ErrorAction SilentlyContinue).Name -match "\\$UserName$") { throw "$UserName ne doit pas etre dans $g" }
}
if ((Get-LocalGroupMember -Group 'CodexSandboxUsers' -ErrorAction SilentlyContinue).Name -match "\\$UserName$") { throw "ne pas ajouter a CodexSandboxUsers" }
$UserSid = (Get-LocalUser -Name $UserName).SID.Value

# ---------------------------------------------------------------- 2. arborescence + ACL (heritage C:\ coupe)
foreach ($d in $dirs.Keys) { New-Item -ItemType Directory -Force -Path $d | Out-Null }
# racine : retire l'heritage de C:\ (Utilisateurs authentifies:M, Utilisateurs:RX)
& icacls.exe $Root /inheritance:r /c | Out-Null
& icacls.exe $Root /grant:r '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' "*${UserSid}:(OI)(CI)RX" /c | Out-Null
foreach ($kv in $dirs.GetEnumerator()) {
    if ($kv.Value -eq 'm') { & icacls.exe $kv.Key /grant "*${UserSid}:(OI)(CI)M" /c | Out-Null }
}
# Juliann : lecture seule des workspaces pour revue (optionnel)
& icacls.exe "$Root\workspaces" /grant '*S-1-5-21-3782191491-1246939613-2125819924-1001:(OI)(CI)RX' /c | Out-Null
& icacls.exe "$Root\runner\state" /setowner "*$UserSid" /c | Out-Null

# ---------------------------------------------------------------- 3. binaires + runner (code, aucun secret)
Copy-Item $OpencodeSrc "$Root\bin\opencode.exe" -Force
Copy-Item $RunnerAppSrc "$Root\runner\app" -Recurse -Force          # orch_runner + orch_protocol
& $PythonExe -m venv "$Root\runner\venv"
& "$Root\runner\venv\Scripts\python.exe" -m pip install --disable-pip-version-check httpx   # deps reelles a confirmer (requirements du repo)
Set-Content "$Root\runner\venv\Lib\site-packages\orch_runner_app.pth" "$Root\runner\app" -Encoding ascii
& icacls.exe "$Root\runner" /reset /t /c | Out-Null                 # re-applique l'heritage RX (pas M) sur le code copie
& icacls.exe "$Root\runner\state" /grant "*${UserSid}:(OI)(CI)M" /c | Out-Null

# runner.toml dedie : home + token_file explicites (pas de dependance a %USERPROFILE%\.orch-runner)
@"
runner_id = "$RunnerId"
broker_url = "$BrokerUrl"
home = '$Root\runner\state'
token_file = '$Root\runner\state\runner.token'
max_parallel = 1
default_permission_policy = "unattended"

[runtimes.opencode]
enabled = true
exe = '$Root\bin\opencode.exe'
model = "opencode/muse-spark-1.3-contributor-free"

[workspaces.autopilot]
path = '$Root\workspaces\autopilot'
modes = ["read_only", "workspace_write"]
description = "workspace Autopilot dedie"
"@ | Set-Content "$Root\runner\state\runner.toml" -Encoding utf8
New-Item -ItemType Directory -Force "$Root\workspaces\autopilot" | Out-Null

# ---------------------------------------------------------------- 4. credentials (a faire SOUS le compte, pas ici)
Write-Host @"
ETAPES MANUELLES sous $UserName (runas /user:$UserName powershell) :
  a) Jeton runner DPAPI lie a CE compte (ne quitte pas le PC, n'affiche que l'empreinte) :
       & '$Root\runner\venv\Scripts\python.exe' -m orch_runner gen-token --config '$Root\runner\state\runner.toml'
     -> ajouter la ligne '${RunnerId}:<sha256>' a ORCH_RUNNER_TOKENS sur le VPS (liste separee par virgules,
        ne PAS remplacer main-windows-pc), puis systemctl restart orch-mcp.
  b) Auth OpenCode propre a ce compte : '$Root\bin\opencode.exe' auth login
     -> stockee dans C:\Users\$UserName\.local\share\opencode\auth.json (profil prive du compte).
     NB : le runner filtre l'env enfant (ENV_ALLOW) : XDG_* / OPENCODE_* ne passent PAS ; c'est USERPROFILE
     du compte dedie qui isole les chemins.
"@

# ---------------------------------------------------------------- 5. tache planifiee sous le compte
$action  = New-ScheduledTaskAction -Execute "$Root\runner\venv\Scripts\pythonw.exe" `
            -Argument "-m orch_runner run --config `"$Root\runner\state\runner.toml`"" -WorkingDirectory "$Root\runner\state"
$trig1   = New-ScheduledTaskTrigger -AtStartup
$trig2   = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(5) -RepetitionInterval (New-TimeSpan -Minutes 1)
$set     = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable
$plain   = [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($pw))
# LogonType Password (pas S4U) : necessaire pour que DPAPI utilisateur fonctionne hors session interactive.
Register-ScheduledTask -TaskName 'autopilot-runner-win-01' -Action $action -Trigger $trig1, $trig2 -Settings $set `
    -User $UserName -Password $plain -RunLevel Limited | Out-Null
$plain = $null
Write-Host "Tache autopilot-runner-win-01 creee (RunLevel Limited). Lancer les tests negatifs (c) avant activation reelle."
