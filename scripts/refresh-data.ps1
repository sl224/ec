[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("dev", "prod")]
    [string]$Target,

    [string]$ConfigPath = (Join-Path $PSScriptRoot "..\\e2ude_config.toml"),

    [string]$SchemaName,

    [string]$ConfirmSchema,

    [string]$StagingRoot,

    [switch]$Preview
)

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

if (-not (Test-Path $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
}

$uv = Get-Command uv -ErrorAction SilentlyContinue
if (-not $uv) {
    throw "uv is required to run the refresh wrapper."
}

$cliArgs = @(
    "run",
    "e2ude",
    "refresh",
    "--config",
    (Resolve-Path $ConfigPath).Path
)

if ($SchemaName) {
    $cliArgs += @("--schema", $SchemaName)
}
else {
    $cliArgs += @("--env", $Target)
}
if ($StagingRoot) {
    $cliArgs += @("--staging-root", $StagingRoot)
}
if ($Preview) {
    $cliArgs += "--preview"
}
elseif ($Target -eq "prod" -and -not $SchemaName) {
    if (-not $ConfirmSchema) {
        throw "Production refresh requires -ConfirmSchema e2ude_core."
    }
    $cliArgs += @("--confirm", $ConfirmSchema)
}

Push-Location $repoRoot
try {
    & $uv.Source @cliArgs
    if ($LASTEXITCODE) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
