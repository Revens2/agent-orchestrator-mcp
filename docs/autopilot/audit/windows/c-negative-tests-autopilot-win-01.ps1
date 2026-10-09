<#
.SYNOPSIS
  (c) Tests negatifs a executer SOUS autopilot-win-01 (jamais sous Juliann). PREPARE, NON EXECUTE.
  Lancement : runas /user:autopilot-win-01 "powershell -NoProfile -ExecutionPolicy Bypass -File <ce fichier>"
  Aucun contenu de secret n'est lu ni affiche : les tests de lecture ouvrent le fichier puis le referment
  immediatement (0 octet lu). Les tests d'ecriture creent un fichier sonde et le suppriment s'il a ete cree.
  Resultat attendu : DENIED partout sauf les controles positifs (ALLOWED).
#>
$ErrorActionPreference = 'Continue'
$J = 'C:\Users\Juliann'
$probe = "ap-probe-$([guid]::NewGuid().ToString('N').Substring(0,8)).txt"
$results = @()

function T([string]$name, [string]$expect, [scriptblock]$sb) {
    $got = try { & $sb; 'ALLOWED' } catch [UnauthorizedAccessException] { 'DENIED' } catch {
        if ($_.Exception.InnerException -is [UnauthorizedAccessException] -or $_.Exception.Message -match 'Access|refus|denied') { 'DENIED' } else { "ERROR: $($_.Exception.GetType().Name)" } }
    $script:results += [pscustomobject]@{ Test = $name; Expected = $expect; Got = $got; Pass = ($got -eq $expect) }
}
function OpenRead($p) { $fs = [IO.File]::Open($p, 'Open', 'Read', 'ReadWrite'); $fs.Close() }   # 0 octet lu
function TryWrite($dir) { $f = Join-Path $dir $probe; [IO.File]::WriteAllText($f, 'x'); Remove-Item $f -Force }

# --- identite
$me = [Security.Principal.WindowsIdentity]::GetCurrent()
$isAdmin = ([Security.Principal.WindowsPrincipal]$me).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$results += [pscustomobject]@{ Test = "identite = $($me.Name), admin=$isAdmin"; Expected = 'autopilot-win-01, admin=False'; Got = "$($me.Name) admin=$isAdmin"; Pass = ($me.Name -match 'autopilot-win-01$' -and -not $isAdmin) }

# --- lecture profil Juliann
T 'list C:\Users\Juliann'                         'DENIED' { [IO.Directory]::GetFileSystemEntries($J) | Out-Null }
T 'list .orch-runner'                             'DENIED' { [IO.Directory]::GetFileSystemEntries("$J\.orch-runner") | Out-Null }
T 'open runner.token (principal)'                 'DENIED' { OpenRead "$J\.orch-runner\runner.token" }
T 'open runner.token.bak-rotate-*'                'DENIED' { OpenRead "$J\.orch-runner\runner.token.bak-rotate-20260915-031351" }
T 'open runner.toml (principal)'                  'DENIED' { OpenRead "$J\.orch-runner\runner.toml" }
T 'open opencode auth.json (Juliann)'             'DENIED' { OpenRead "$J\.local\share\opencode\auth.json" }
T 'open opencode mcp-auth.json (Juliann)'         'DENIED' { OpenRead "$J\.local\share\opencode\mcp-auth.json" }
T 'list .secrets (Juliann)'                       'DENIED' { [IO.Directory]::GetFileSystemEntries("$J\.secrets") | Out-Null }
T 'list .ssh (Juliann)'                           'DENIED' { [IO.Directory]::GetFileSystemEntries("$J\.ssh") | Out-Null }
T 'list DPAPI Protect (Juliann)'                  'DENIED' { [IO.Directory]::GetFileSystemEntries("$J\AppData\Roaming\Microsoft\Protect") | Out-Null }
T 'read workspace e2e (Juliann)'                  'DENIED' { [IO.Directory]::GetFileSystemEntries("$J\orch-workspaces\e2e") | Out-Null }
T 'registre HKU Juliann'                          'DENIED' { Get-ChildItem 'Registry::HKEY_USERS\S-1-5-21-3782191491-1246939613-2125819924-1001\Environment' -ErrorAction Stop | Out-Null }

# --- ecriture hors workspace
T 'write C:\Users\Juliann\orch-workspaces\e2e'   'DENIED' { TryWrite "$J\orch-workspaces\e2e" }
T 'write C:\Tools (action tache Juliann)'         'DENIED' { TryWrite 'C:\Tools' }     # ALLOWED tant que l'ACL C:\ n'est pas corrigee
T 'write C:\projet\opencode-rotate'               'DENIED' { TryWrite 'C:\projet\opencode-rotate' }
T 'write C:\projet'                               'DENIED' { TryWrite 'C:\projet' }
T 'write C:\Windows'                              'DENIED' { TryWrite 'C:\Windows' }
T 'write C:\Program Files'                        'DENIED' { TryWrite 'C:\Program Files' }
T 'write C:\autopilot\runner (code)'              'DENIED' { TryWrite 'C:\autopilot\runner' }
T 'write C:\autopilot\bin (opencode.exe)'         'DENIED' { TryWrite 'C:\autopilot\bin' }
T 'write C:\autopilot (racine)'                   'DENIED' { TryWrite 'C:\autopilot' }
T 'write C:\ (racine disque)'                     'DENIED' { TryWrite 'C:\' }

# --- taches planifiees de Juliann
T 'modifier tache orch-runner'                    'DENIED' { $o = schtasks.exe /change /tn orch-runner /disable 2>&1; if ($LASTEXITCODE -ne 0) { throw [UnauthorizedAccessException]::new("$o") } }
T 'lancer tache opencode-rotate-watch'            'DENIED' { $o = schtasks.exe /run /tn opencode-rotate-watch 2>&1; if ($LASTEXITCODE -ne 0) { throw [UnauthorizedAccessException]::new("$o") } }

# --- controles positifs
T 'write C:\autopilot\workspaces\autopilot'       'ALLOWED' { TryWrite 'C:\autopilot\workspaces\autopilot' }
T 'write C:\autopilot\runner\state'               'ALLOWED' { TryWrite 'C:\autopilot\runner\state' }
T 'exec opencode --version'                       'ALLOWED' { & 'C:\autopilot\bin\opencode.exe' --version | Out-Null; if ($LASTEXITCODE) { throw "exit $LASTEXITCODE" } }

$results | Format-Table -AutoSize | Out-String -Width 200
$fail = @($results | Where-Object { -not $_.Pass })
Write-Host ("RESULTAT: {0}/{1} conformes" -f ($results.Count - $fail.Count), $results.Count)
# Note : C:\ProgramData et C:\Users\Public restent inscriptibles par tout utilisateur (defaut Windows) : non bloquant
# tant qu'aucune tache privilegiee n'y execute de code.
exit $fail.Count
