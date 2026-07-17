$ErrorActionPreference = "Stop"

$pluginRoot = Split-Path -Parent $PSScriptRoot
$version = (Get-Content -LiteralPath (Join-Path $pluginRoot "VERSION") -TotalCount 1).Trim()

if ($env:DELEGATION_BINARY) {
    if (-not (Test-Path -LiteralPath $env:DELEGATION_BINARY -PathType Leaf)) {
        [Console]::Error.WriteLine("delegation: DELEGATION_BINARY does not exist: $env:DELEGATION_BINARY")
        exit 126
    }
    & $env:DELEGATION_BINARY @args
    exit $LASTEXITCODE
}

$architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
switch ($architecture) {
    "X64" { $arch = "amd64" }
    "Arm64" { $arch = "arm64" }
    default {
        [Console]::Error.WriteLine("delegation: unsupported architecture: $architecture")
        exit 126
    }
}

$delegationHome = if ($env:DELEGATION_HOME) {
    $env:DELEGATION_HOME
} else {
    Join-Path $HOME ".delegation"
}
$binary = Join-Path $delegationHome "bin\$version\windows-$arch\delegation.exe"
if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
    [Console]::Error.WriteLine("delegation: runtime $version is not installed for windows-$arch")
    [Console]::Error.WriteLine('delegation: run $delegation-setup in a new Codex task or set DELEGATION_BINARY')
    exit 127
}

& $binary @args
exit $LASTEXITCODE
