<#
.SYNOPSIS
  Runner Autopilot ISOLE `autopilot-win-01` : deux comptes locaux STANDARD (jamais admin).
    ap-runner (A) : execute orch_runner ; detient jeton broker, cle de signature, mot de passe de B (DPAPI de A).
    ap-agent  (B) : execute OpenCode (CreateProcessWithLogonW) ; aucun jeton, aucune cle ; ecrit SEULEMENT
                    dans C:\autopilot\workspaces ; auth OpenCode dans SON profil.
  Le runner historique main-windows-pc (Juliann) n'est PAS touche.

  PREPARE. Dry-run par defaut. -Execute dans un PowerShell ELEVE sous Juliann, apres autorisation explicite.

.PREREQUIS
  - deploy/windows/fix-tools-acl.ps1 -Execute applique (sinon refus : escalade vers Juliann).
  - Python 3.11+ installe "pour tous les utilisateurs" (C:\Program Files\Python311\python.exe).
  - opencode.exe installe hors profil (copie dans C:\autopilot\bin par ce script).

.ROLLBACK
  .\install-autopilot-runner.ps1 -Uninstall -Execute : supprime la tache, desactive les 2 comptes (pas de
  suppression de profil ni de donnees : a faire manuellement apres verification).
#>
[CmdletBinding()]
param(
    [switch]$Execute,
    [switch]$Uninstall,
    [string]$RunnerUser = 'ap-runner',
    [string]$AgentUser  = 'ap-agent',
    [string]$Root       = 'C:\autopilot',
    [string]$RunnerId   = 'autopilot-win-01',
    [string]$PythonExe  = 'C:\Program Files\Python311\python.exe',
    [string]$OpencodeSrc = 'C:\Users\Juliann\AppData\Local\Microsoft\WinGet\Packages\SST.opencode_Microsoft.Winget.Source_8wekyb3d8bbwe\opencode.exe',
    [string]$RepoSrc    = 'C:\Users\Juliann\orch-workspaces\e2e\autopilot-v1\src',
    [string]$BrokerUrl  = 'http://10.200.114.203:8803',
    [string]$Model      = 'opencode/muse-spark-1.3-contributor-free'
)
$ErrorActionPreference = 'Stop'
$TaskName = 'autopilot-runner-win-01'
$id = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if ($Execute -and -not $id.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw "PowerShell eleve requis" }

if ($Uninstall) {
    Write-Host "PLAN rollback : tache $TaskName supprimee ; comptes $RunnerUser/$AgentUser DESACTIVES (non supprimes)."
    Write-Host "  Cote VPS : retirer '${RunnerId}:<sha256>' de ORCH_RUNNER_TOKENS ; AUTOPILOT_RUNNER_ID=main-windows-pc, kinds read,decision."
    if ($Execute) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
        foreach ($u in $RunnerUser, $AgentUser) { Disable-LocalUser -Name $u -ErrorAction SilentlyContinue }
        Write-Host "OK : tache supprimee, comptes desactives. $Root conserve."
    }
    return
}

# ---------------------------------------------------------------- 0. preflight (lecture seule)
$problems = @()
foreach ($p in 'C:\Tools', 'C:\Tools\silent-run.exe', 'C:\Tools\mcp-hidden-run.pyw',
               'C:\projet\opencode-rotate', 'C:\projet\opencode-rotate\Watch-Rotate.ps1') {
    if (-not (Test-Path $p)) { continue }
    $w = (Get-Acl $p).Access | Where-Object {
        $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -in @('S-1-5-11', 'S-1-5-32-545', 'S-1-1-0') -and
        $_.AccessControlType -eq 'Allow' -and ($_.FileSystemRights.ToString() -match 'Modify|Write|FullControl') }
    if ($w) { $problems += "ecriture ouverte a tous : $p (lancer fix-tools-acl.ps1)" }
}
if (-not (Test-Path $PythonExe)) { $problems += "Python machine introuvable : $PythonExe" }
if (-not (Test-Path $OpencodeSrc)) { $problems += "opencode.exe introuvable : $OpencodeSrc" }
foreach ($u in $RunnerUser, $AgentUser) { if (Get-LocalUser -Name $u -ErrorAction SilentlyContinue) { $problems += "compte deja present : $u" } }
if ($problems) { Write-Warning "PREFLIGHT KO :"; $problems | ForEach-Object { "   - $_" } }

$plan = @"
PLAN
  1. comptes standard $RunnerUser (A) et $AgentUser (B), groupe Utilisateurs seul, masques de l'ecran d'accueil
  2. $Root (heritage C:\ coupe) : SYSTEM/Admins F partout
       bin\          A RX, B RX      (opencode.exe)
       runner\app    A RX            (orch_runner + orch_protocol, NON modifiable par A ni B)
       runner\venv   A RX
       runner\state  A M             (runner.toml, runner.token, signing.key, agent.cred : DPAPI de A) ; B : rien
       workspaces\   B M, A RX       (seule zone d'ecriture de l'agent ; A lit pour hacher)
  3. runner.toml avec section [isolation] (sondes : profil Juliann, .orch-runner, state de A, HKLM, Program Files)
  4. tache planifiee $TaskName sous A (RunLevel Limited)
  5. etapes manuelles sous A et B (jeton, cle, mot de passe B, auth OpenCode de B) : affichees en fin de script
"@
Write-Host $plan
if (-not $Execute) { Write-Host "DRY-RUN (ajouter -Execute apres autorisation)."; return }
if ($problems) { throw "preflight KO : corriger d'abord" }

# ---------------------------------------------------------------- 1. comptes
$pwA = Read-Host "Mot de passe de $RunnerUser (A)" -AsSecureString
$pwB = Read-Host "Mot de passe de $AgentUser (B) - a ressaisir plus tard sous A (set-agent-password)" -AsSecureString
$users = (Get-LocalGroup -SID 'S-1-5-32-545').Name
foreach ($pair in @(@($RunnerUser, $pwA, 'Autopilot runner (A)'), @($AgentUser, $pwB, 'Autopilot agent OpenCode (B)'))) {
    New-LocalUser -Name $pair[0] -Password $pair[1] -FullName $pair[2] -Description 'Autopilot V1 - compte standard dedie' `
        -PasswordNeverExpires -UserMayNotChangePassword | Out-Null
    Add-LocalGroupMember -Group $users -Member $pair[0]
    foreach ($g in 'S-1-5-32-544', 'S-1-5-32-555', 'S-1-5-32-580', 'S-1-5-32-551') {
        if ((Get-LocalGroupMember -SID $g -ErrorAction SilentlyContinue).Name -match "\\$($pair[0])$") { throw "$($pair[0]) ne doit pas etre dans $g" }
    }
}
$hide = 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon\SpecialAccounts\UserList'
New-Item -Path $hide -Force | Out-Null
foreach ($u in $RunnerUser, $AgentUser) { New-ItemProperty -Path $hide -Name $u -Value 0 -PropertyType DWord -Force | Out-Null }
$SidA = (Get-LocalUser -Name $RunnerUser).SID.Value
$SidB = (Get-LocalUser -Name $AgentUser).SID.Value

# ---------------------------------------------------------------- 2. arborescence + ACL
$dirs = "$Root", "$Root\bin", "$Root\runner", "$Root\runner\app", "$Root\runner\state", "$Root\workspaces", "$Root\workspaces\autopilot"
foreach ($d in $dirs) { New-Item -ItemType Directory -Force -Path $d | Out-Null }
& icacls.exe $Root /inheritance:r /grant:r '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' /c | Out-Null
& icacls.exe "$Root\bin" /grant "*${SidA}:(OI)(CI)RX" "*${SidB}:(OI)(CI)RX" /c | Out-Null
& icacls.exe "$Root\runner" /grant "*${SidA}:(OI)(CI)RX" /c | Out-Null
& icacls.exe "$Root\runner\state" /grant "*${SidA}:(OI)(CI)M" /c | Out-Null
& icacls.exe "$Root\workspaces" /grant "*${SidB}:(OI)(CI)M" "*${SidA}:(OI)(CI)RX" /c | Out-Null
# B traverse $Root sans lister (atteindre bin\ et workspaces\)
& icacls.exe $Root /grant "*${SidB}:(NP)(X)" "*${SidA}:(NP)(X)" /c | Out-Null

# ---------------------------------------------------------------- 3. binaires, code, venv
Copy-Item $OpencodeSrc "$Root\bin\opencode.exe" -Force
foreach ($pkg in 'orch_runner', 'orch_protocol') { Copy-Item (Join-Path $RepoSrc $pkg) "$Root\runner\app\$pkg" -Recurse -Force }
& $PythonExe -m venv "$Root\runner\venv"
& "$Root\runner\venv\Scripts\python.exe" -m pip install --disable-pip-version-check 'httpx' 'cryptography'
Set-Content "$Root\runner\venv\Lib\site-packages\orch_runner_app.pth" "$Root\runner\app" -Encoding ascii
& icacls.exe "$Root\runner\app" /reset /t /c | Out-Null
& icacls.exe "$Root\runner\venv" /reset /t /c | Out-Null

$py = $PythonExe -replace "'", "''"
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
model = "$Model"
probe_generation = false   # le compte A n'a pas d'auth OpenCode : la generation tourne sous B, par job

[workspaces.e2e]
path = '$Root\workspaces\autopilot'
modes = ["read_only", "workspace_write"]
description = "workspace Autopilot isole (seule zone d'ecriture du compte agent)"

[isolation]
agent_user = "$AgentUser"
agent_profile = 'C:\Users\$AgentUser'
credential_file = '$Root\runner\state\agent.cred'
signing_key_file = '$Root\runner\state\signing.key'
probe_python = '$py'
# chaque chemin DOIT exister ; un chemin absent compte comme NON refuse (fail-closed)
deny_read = [
  'C:\Users\Juliann\.orch-runner\runner.token',
  'C:\Users\Juliann\.local\share\opencode\auth.json',
  'C:\Users\Juliann\.ssh\config',
  '$Root\runner\state\runner.toml',
]
deny_write = [
  'C:\Users\Juliann',
  '$Root\runner\app\orch_runner',
  '$Root\runner\state',
  'C:\Program Files',
  'C:\Windows\System32',
  'C:\Tools',
]
"@ | Set-Content "$Root\runner\state\runner.toml" -Encoding utf8

# ---------------------------------------------------------------- 4. tache planifiee sous A
$action = New-ScheduledTaskAction -Execute "$Root\runner\venv\Scripts\pythonw.exe" `
          -Argument "-m orch_runner run --config `"$Root\runner\state\runner.toml`"" -WorkingDirectory "$Root\runner\state"
$trig = @((New-ScheduledTaskTrigger -AtStartup),
          (New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(5) -RepetitionInterval (New-TimeSpan -Minutes 1)))
$set = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable
$plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($pwA))
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trig -Settings $set -User $RunnerUser -Password $plain -RunLevel Limited | Out-Null
$plain = $null
Disable-ScheduledTask -TaskName $TaskName | Out-Null   # active seulement apres attestation OK (etape 6)

$rp = "$Root\runner\venv\Scripts\python.exe"; $cfg = "$Root\runner\state\runner.toml"
Write-Host @"

ETAPES MANUELLES (aucun secret ne transite par Claude) :
 a) Auth OpenCode de B :   runas /user:$AgentUser "$Root\bin\opencode.exe auth login"
 b) Sous A :               runas /user:$RunnerUser powershell
      & '$rp' -m orch_runner set-agent-password --config '$cfg'      # saisir le mot de passe de B
      & '$rp' -m orch_runner evidence-keygen   --config '$cfg'      # -> AUTOPILOT_RUNNER_PUBKEY=...
      & '$rp' -m orch_runner gen-token         --config '$cfg'      # -> ${RunnerId}:<sha256>
      & '$rp' -m orch_runner attest            --config '$cfg'      # T1 : tous les refus doivent venir de l'OS ; exit 0
 c) VPS : ajouter '${RunnerId}:<sha256>' a ORCH_RUNNER_TOKENS (sans retirer main-windows-pc) ;
          autopilot.env : AUTOPILOT_RUNNER_ID=$RunnerId, AUTOPILOT_RUNNER_PUBKEY=..., AUTOPILOT_EXPECTED_AGENT_SID=$SidB
 d) Enable-ScheduledTask -TaskName $TaskName
SID A = $SidA ; SID B = $SidB
"@
