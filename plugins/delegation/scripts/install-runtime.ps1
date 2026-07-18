$ErrorActionPreference = "Stop"

function Get-SHA256Hash {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $stream = [System.IO.File]::OpenRead($Path)
    try {
        $sha256 = [System.Security.Cryptography.SHA256]::Create()
        try {
            $digest = $sha256.ComputeHash($stream)
            return ([System.BitConverter]::ToString($digest)).Replace("-", "").ToLowerInvariant()
        } finally {
            $sha256.Dispose()
        }
    } finally {
        $stream.Dispose()
    }
}

$pluginRoot = Split-Path -Parent $PSScriptRoot
$version = (Get-Content -LiteralPath (Join-Path $pluginRoot "VERSION") -TotalCount 1).Trim()
$checksums = Join-Path $pluginRoot "release-artifacts.sha256"
$architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
switch ($architecture) {
    "X64" { $arch = "amd64" }
    "Arm64" { $arch = "arm64" }
    default { throw "delegation: unsupported architecture: $architecture" }
}

$artifact = "delegation_${version}_windows_${arch}.zip"
$checksumLine = Get-Content -LiteralPath $checksums | Where-Object {
    $_ -match "^([0-9a-fA-F]{64})  $([regex]::Escape($artifact))$"
} | Select-Object -First 1
if (-not $checksumLine) {
    throw "delegation: no pinned SHA-256 for $artifact"
}
$expected = $checksumLine.Substring(0, 64).ToLowerInvariant()

$delegationHome = if ($env:DELEGATION_HOME) {
    $env:DELEGATION_HOME
} else {
    Join-Path $HOME ".delegation"
}
$targetParent = Join-Path $delegationHome "bin\$version"
$target = Join-Path $targetParent "windows-$arch"
$binary = Join-Path $target "delegation.exe"
$locks = Join-Path $delegationHome ".locks"
New-Item -ItemType Directory -Force -Path $locks, $targetParent | Out-Null
$lockPath = Join-Path $locks "install-$version-windows-$arch.lock"
$lockStream = $null
$staging = $null
try {
    try {
        # The persistent file is intentional; the exclusive handle is released on process exit.
        $lockStream = [System.IO.File]::Open(
            $lockPath,
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
    } catch [System.IO.IOException] {
        throw "delegation: another runtime installation is in progress for $version windows-$arch"
    }
    if (Test-Path -LiteralPath $target) {
        $targetItem = Get-Item -LiteralPath $target -Force
        if (($targetItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "delegation: runtime target must not be a reparse point: $target"
        }
        if (-not $targetItem.PSIsContainer) {
            throw "delegation: incomplete runtime directory already exists: $target"
        }
        $installedEntries = @(Get-ChildItem -LiteralPath $target -Force)
        if ($installedEntries.Count -ne 1 -or
            $installedEntries[0].Name -ne "delegation.exe" -or
            $installedEntries[0].PSIsContainer) {
            throw "delegation: installed runtime directory contains unexpected files: $target"
        }
        if (($installedEntries[0].Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "delegation: installed runtime must not be a reparse point: $binary"
        }
        $installedVersion = (& $binary version | Out-String).Trim()
        $versionExitCode = $LASTEXITCODE
        if ($versionExitCode -ne 0) {
            throw "delegation: installed runtime version command failed with exit code $versionExitCode"
        }
        if ($installedVersion -ne $version) {
            throw "delegation: installed runtime reports version $installedVersion, expected $version"
        }
        Write-Output $binary
        exit 0
    }

    $staging = Join-Path $targetParent (".install-windows-$arch-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $staging | Out-Null
    $archive = Join-Path $staging $artifact
    $url = "https://github.com/GhostFlying/delegation/releases/download/v$version/$artifact"
    Invoke-WebRequest -Uri $url -OutFile $archive -UseBasicParsing

    $actual = Get-SHA256Hash -Path $archive
    if ($actual -ne $expected) {
        throw "delegation: SHA-256 mismatch for $artifact"
    }

    $expanded = Join-Path $staging "expanded"
    Expand-Archive -LiteralPath $archive -DestinationPath $expanded
    $entries = @(Get-ChildItem -LiteralPath $expanded -Force)
    if ($entries.Count -ne 1 -or
        $entries[0].Name -ne "delegation.exe" -or
        $entries[0].PSIsContainer -or
        ($entries[0].Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "delegation: unexpected files in $artifact"
    }
    $downloadedBinary = $entries[0].FullName
    $installedVersion = (& $downloadedBinary version | Out-String).Trim()
    $versionExitCode = $LASTEXITCODE
    if ($versionExitCode -ne 0) {
        throw "delegation: downloaded runtime version command failed with exit code $versionExitCode"
    }
    if ($installedVersion -ne $version) {
        throw "delegation: downloaded runtime reports version $installedVersion, expected $version"
    }
    try {
        [System.IO.Directory]::Move($expanded, $target)
    } catch [System.IO.IOException] {
        if (Test-Path -LiteralPath $target) {
            throw "delegation: runtime target appeared during installation: $target"
        }
        throw
    }
    $committedTarget = Get-Item -LiteralPath $target -Force
    $committedEntries = @(Get-ChildItem -LiteralPath $target -Force)
    if (-not $committedTarget.PSIsContainer -or
        ($committedTarget.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0 -or
        $committedEntries.Count -ne 1 -or
        $committedEntries[0].Name -ne "delegation.exe" -or
        $committedEntries[0].PSIsContainer -or
        ($committedEntries[0].Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "delegation: committed runtime layout is invalid: $target"
    }
    Write-Output $binary
} finally {
    if ($lockStream) {
        $lockStream.Dispose()
    }
    if ($staging -and (Test-Path -LiteralPath $staging)) {
        Remove-Item -LiteralPath $staging -Recurse -Force
    }
}
