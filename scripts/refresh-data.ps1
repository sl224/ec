[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("dev", "prod")]
    [string]$Target,

    [string]$ConfigPath = (Join-Path $PSScriptRoot "..\\e2ude_config.toml"),

    [string]$StagingRoot,

    [switch]$Preview
)

$schemaByTarget = @{
    dev = "e2ude_core_dev"
    prod = "e2ude_core"
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

if (-not (Test-Path $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
}

$uv = Get-Command uv -ErrorAction SilentlyContinue
if (-not $uv) {
    throw "uv is required to run the refresh wrapper."
}

$resolvedConfigPath = (Resolve-Path $ConfigPath).Path
$schemaName = $schemaByTarget[$Target]

Write-Host "Refresh target : $Target"
Write-Host "Target schema  : $schemaName"
Write-Host "Config path    : $resolvedConfigPath"
if ($StagingRoot) {
    Write-Host "Staging root   : $StagingRoot"
}

if ($Preview) {
    return
}

$previousEnv = @{
    E2UDE_CONFIG_PATH = $env:E2UDE_CONFIG_PATH
    E2UDE_DATABASE__TYPE = $env:E2UDE_DATABASE__TYPE
    E2UDE_DATABASE__SCHEMA_NAME = $env:E2UDE_DATABASE__SCHEMA_NAME
    E2UDE_PATHS__STAGING_ROOT = $env:E2UDE_PATHS__STAGING_ROOT
}

$pushedLocation = $false
$exitCode = 0

try {
    $env:E2UDE_CONFIG_PATH = $resolvedConfigPath
    # $env:E2UDE_DATABASE__TYPE = "mssql"
    $env:E2UDE_DATABASE__TYPE = "sqlite3"
    $env:E2UDE_DATABASE__SCHEMA_NAME = $schemaName

    if ($StagingRoot) {
        $env:E2UDE_PATHS__STAGING_ROOT = $StagingRoot
    }
    else {
        Remove-Item Env:E2UDE_PATHS__STAGING_ROOT -ErrorAction SilentlyContinue
    }

    Push-Location $repoRoot
    $pushedLocation = $true

    & $uv.Source run -m e2ude_core.main
    if ($LASTEXITCODE) {
        $exitCode = $LASTEXITCODE
    }
}
finally {
    if ($pushedLocation) {
        Pop-Location
    }

    foreach ($name in $previousEnv.Keys) {
        $value = $previousEnv[$name]
        if ($null -eq $value) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        else {
            Set-Item "Env:$name" $value
        }
    }
}

if ($exitCode -ne 0) {
    exit $exitCode
}
