$ErrorActionPreference = "Stop"

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
if (Test-Path -LiteralPath $binary -PathType Leaf) {
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
if (Test-Path -LiteralPath $target) {
    throw "delegation: incomplete runtime directory already exists: $target"
}

$locks = Join-Path $delegationHome ".locks"
New-Item -ItemType Directory -Force -Path $locks, $targetParent | Out-Null
$lockPath = Join-Path $locks "install-$version-windows-$arch.lock"
$lockStream = $null
$lockAcquired = $false
$staging = $null
try {
    $lockStream = [System.IO.File]::Open(
        $lockPath,
        [System.IO.FileMode]::CreateNew,
        [System.IO.FileAccess]::Write,
        [System.IO.FileShare]::None
    )
    $lockAcquired = $true
    $staging = Join-Path $targetParent (".install-windows-$arch-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $staging | Out-Null
    $archive = Join-Path $staging $artifact
    $url = "https://github.com/GhostFlying/delegation/releases/download/v$version/$artifact"
    Invoke-WebRequest -Uri $url -OutFile $archive -UseBasicParsing

    $actual = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -ne $expected) {
        throw "delegation: SHA-256 mismatch for $artifact"
    }

    $expanded = Join-Path $staging "expanded"
    Expand-Archive -LiteralPath $archive -DestinationPath $expanded
    $entries = @(Get-ChildItem -LiteralPath $expanded -Force)
    if ($entries.Count -ne 1 -or $entries[0].Name -ne "delegation.exe" -or $entries[0].PSIsContainer) {
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
    Move-Item -LiteralPath $expanded -Destination $target
    Write-Output $binary
} finally {
    if ($lockStream) {
        $lockStream.Dispose()
    }
    if ($lockAcquired) {
        Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
    }
    if ($staging -and (Test-Path -LiteralPath $staging)) {
        Remove-Item -LiteralPath $staging -Recurse -Force
    }
}
