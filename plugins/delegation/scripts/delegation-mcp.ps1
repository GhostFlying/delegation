$ErrorActionPreference = "Stop"

$pluginRoot = Split-Path -Parent $PSScriptRoot
$version = (Get-Content -LiteralPath (Join-Path $pluginRoot "VERSION") -TotalCount 1).Trim()

function Assert-DelegationRuntime {
    param(
        [Parameter(Mandatory = $true)] [string] $Path,
        [Parameter(Mandatory = $true)] [string] $Source
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "delegation: $Source is not a regular executable: $Path"
    }
    $item = Get-Item -LiteralPath $Path -Force
    if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "delegation: $Source is not a regular executable: $Path"
    }
    $actualVersion = (& $Path version | Out-String).Trim()
    $versionExitCode = $LASTEXITCODE
    if ($versionExitCode -ne 0) {
        throw "delegation: $Source version command failed with exit code $versionExitCode"
    }
    if ($actualVersion -cne $version) {
        throw "delegation: $Source reports version $actualVersion, expected $version"
    }
}

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
$binaryItem = Get-Item -LiteralPath $binary -Force -ErrorAction SilentlyContinue
if ($null -eq $binaryItem) {
    try {
        & (Join-Path $PSScriptRoot "install-runtime.ps1") | Out-Null
    } catch {
        [Console]::Error.WriteLine($_.Exception.Message)
        [Console]::Error.WriteLine("delegation: automatic runtime installation failed for $version windows-$arch")
        exit 127
    }
}
try {
    Assert-DelegationRuntime -Path $binary -Source "runtime $version for windows-$arch"
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 126
}

& $binary @args
exit $LASTEXITCODE
