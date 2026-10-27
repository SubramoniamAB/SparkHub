[CmdletBinding()]
param(
    [ValidateSet('Install', 'Run', 'Start', 'Status', 'Uninstall')]
    [string]$Action = 'Install',

    [ValidateRange(1, 65535)]
    [int]$Port = 8090,

    [ValidateSet('models', 'playbooks', 'recipes', 'ratings', 'forum')]
    [string[]]$Sources = @()
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$TaskName = 'Spark Hub Monitor'
$AppName = 'spark-hub'

if (-not $env:APPDATA) {
    $env:APPDATA = Join-Path $env:USERPROFILE 'AppData\Roaming'
}
if (-not $env:LOCALAPPDATA) {
    $env:LOCALAPPDATA = Join-Path $env:USERPROFILE 'AppData\Local'
}
if (-not $env:XDG_STATE_HOME) {
    $env:XDG_STATE_HOME = $env:LOCALAPPDATA
}

$ConfigDirectory = Join-Path $env:APPDATA $AppName
$StateDirectory = Join-Path $env:XDG_STATE_HOME $AppName
$EnvironmentFile = Join-Path $ConfigDirectory 'env'
$Fetcher = Join-Path $ProjectRoot 'spark_hub_fetch.py'
$Server = Join-Path $ProjectRoot 'spark_hub_server.py'

function Initialize-SparkHubDirectories {
    New-Item -ItemType Directory -Force -Path $ConfigDirectory, $StateDirectory | Out-Null
}

function Write-EnvironmentTemplate {
    if (Test-Path -LiteralPath $EnvironmentFile) {
        return
    }

    @'
# Optional Spark Hub API tokens. Values are loaded before each refresh.
# HF_TOKEN: https://huggingface.co/settings/tokens (read scope)
HF_TOKEN=
# AA_API_KEY: https://artificialanalysis.ai (enables benchmark ratings)
AA_API_KEY=
# GITHUB_TOKEN: only needed if GitHub rate-limits you
GITHUB_TOKEN=
'@ | Set-Content -LiteralPath $EnvironmentFile -Encoding UTF8

    Write-Host "Created optional token file: $EnvironmentFile"
}

function Import-SparkHubEnvironment {
    if (-not (Test-Path -LiteralPath $EnvironmentFile)) {
        return
    }

    foreach ($line in Get-Content -LiteralPath $EnvironmentFile) {
        $entry = $line.Trim()
        if (-not $entry -or $entry.StartsWith('#')) {
            continue
        }

        $parts = $entry -split '=', 2
        if ($parts.Count -ne 2) {
            continue
        }

        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ($name -notin @('HF_TOKEN', 'AA_API_KEY', 'GITHUB_TOKEN')) {
            continue
        }
        if ($value.Length -ge 2 -and
            (($value.StartsWith('"') -and $value.EndsWith('"')) -or
             ($value.StartsWith("'") -and $value.EndsWith("'")))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}

function Get-SparkHubPython {
    $candidates = @()
    $launcher = Get-Command 'py.exe' -ErrorAction SilentlyContinue
    if ($launcher) {
        $candidates += [pscustomobject]@{ Executable = $launcher.Source; Prefix = @('-3') }
    }
    $pythonCommand = Get-Command 'python.exe' -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        $candidates += [pscustomobject]@{ Executable = $pythonCommand.Source; Prefix = @() }
    }

    foreach ($candidate in $candidates) {
        # Avoid quotes inside the -c argument: Windows PowerShell 5.1 can strip
        # them when forwarding native command arguments to py.exe/python.exe.
        $probeArgs = @($candidate.Prefix) + @('-c', 'import sys; print(sys.version_info[0], sys.version_info[1])')
        try {
            $versionText = & $candidate.Executable @probeArgs 2>$null
            $probeExitCode = $LASTEXITCODE
        } catch {
            continue
        }
        if ($probeExitCode -ne 0) {
            continue
        }
        $versionParts = $versionText.Trim() -split '\s+'
        if ($versionParts.Count -ge 2 -and
            [int]$versionParts[0] -ge 3 -and
            ([int]$versionParts[0] -gt 3 -or [int]$versionParts[1] -ge 8)) {
            return $candidate
        }
    }

    throw 'Python 3.8 or newer was not found. Install Python and enable the Python launcher or add python.exe to PATH.'
}

function Invoke-SparkHubFetch {
    param([string[]]$SelectedSources = @())

    if (-not (Test-Path -LiteralPath $Fetcher)) {
        throw "Collector not found: $Fetcher"
    }
    Initialize-SparkHubDirectories
    Write-EnvironmentTemplate
    Import-SparkHubEnvironment
    $env:SPARK_HUB_PROGRESS_FILE = Join-Path $StateDirectory 'progress.json'

    $pythonArgs = @($script:PythonCommand.Prefix) + @($Fetcher, '--out', $ProjectRoot)
    foreach ($source in $SelectedSources) {
        $pythonArgs += @('--only', $source)
    }
    & $script:PythonCommand.Executable @pythonArgs
    return $LASTEXITCODE
}

function Register-SparkHubMonitorTask {
    $powerShell = (Get-Process -Id $PID).Path
    $scriptPath = Join-Path $PSScriptRoot 'setup.ps1'
    $arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$scriptPath`" -Action Start -Port $Port"
    $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

    $taskAction = New-ScheduledTaskAction -Execute $powerShell -Argument $arguments -WorkingDirectory $ProjectRoot
    $taskTrigger = New-ScheduledTaskTrigger -AtLogOn -User $currentUser
    $taskPrincipal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited
    $taskSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit ([TimeSpan]::Zero)
    $task = New-ScheduledTask -Action $taskAction -Trigger $taskTrigger -Principal $taskPrincipal -Settings $taskSettings

    Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "Installed and started the per-user '$TaskName' task. It starts again when this Windows user signs in."
}

switch ($Action) {
    'Install' {
        if (-not (Test-Path -LiteralPath $Server) -or -not (Test-Path -LiteralPath (Join-Path $ProjectRoot 'index.html'))) {
            throw 'Run this script from the SparkHub project folder; required dashboard files are missing.'
        }
        Initialize-SparkHubDirectories
        Write-EnvironmentTemplate
        $script:PythonCommand = Get-SparkHubPython
        Write-Host 'Running the initial Spark Hub data refresh...'
        $fetchExitCode = Invoke-SparkHubFetch
        if ($fetchExitCode -ne 0) {
            Write-Warning "The collector exited with code $fetchExitCode. The dashboard will still start with the data currently in the project folder."
        }
        Register-SparkHubMonitorTask
        Write-Host "Open http://localhost:$Port/ (Data & Refresh: http://localhost:$Port/#/refresh)"
        Write-Host 'For other devices, use the Windows host LAN address and allow this TCP port on the Private firewall profile.'
    }
    'Run' {
        $script:PythonCommand = Get-SparkHubPython
        $exitCode = Invoke-SparkHubFetch -SelectedSources $Sources
        exit $exitCode
    }
    'Start' {
        if (-not (Test-Path -LiteralPath $Server)) {
            throw "Monitor server not found: $Server"
        }
        Initialize-SparkHubDirectories
        Write-EnvironmentTemplate
        Import-SparkHubEnvironment
        $script:PythonCommand = Get-SparkHubPython
        Write-Host "Starting Spark Hub on port $Port"
        $serverArgs = @($script:PythonCommand.Prefix) + @($Server, '--port', "$Port", '--bind', '0.0.0.0')
        & $script:PythonCommand.Executable @serverArgs
        exit $LASTEXITCODE
    }
    'Status' {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        if ($task) {
            Write-Host "Scheduled task: $($task.State)"
        } else {
            Write-Host 'Scheduled task: not installed'
        }
        try {
            $null = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/api/status" -UseBasicParsing -TimeoutSec 3
            Write-Host "Monitor responds at http://localhost:$Port/"
        } catch {
            Write-Host "Monitor is not responding on port $Port."
        }
    }
    'Uninstall' {
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
        Write-Host "Removed the '$TaskName' task. Project files, generated data, and token configuration were kept."
    }
}
