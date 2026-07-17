$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-True {
    param(
        [Parameter(Mandatory = $true)] [bool] $Condition,
        [Parameter(Mandatory = $true)] [string] $Message
    )
    if (-not $Condition) {
        throw $Message
    }
}

function Invoke-ChildProcess {
    param(
        [Parameter(Mandatory = $true)] [string] $FilePath,
        [Parameter(Mandatory = $true)] [string[]] $Arguments,
        [hashtable] $Environment = @{}
    )
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $FilePath
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        $start.ArgumentList.Add($argument)
    }
    foreach ($entry in $Environment.GetEnumerator()) {
        if ($null -eq $entry.Value) {
            $null = $start.Environment.Remove($entry.Key)
        } else {
            $start.Environment[$entry.Key] = [string] $entry.Value
        }
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    if (-not $process.Start()) {
        throw "failed to start $FilePath"
    }
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    [pscustomobject]@{
        ExitCode = $process.ExitCode
        Stdout = $stdout
        Stderr = $stderr
    }
}

function Write-ArtifactChecksum {
    param(
        [Parameter(Mandatory = $true)] [string] $PluginRoot,
        [Parameter(Mandatory = $true)] [string] $Artifact,
        [Parameter(Mandatory = $true)] [string] $ArtifactName
    )
    $hash = (Get-FileHash -LiteralPath $Artifact -Algorithm SHA256).Hash.ToLowerInvariant()
    Set-Content -LiteralPath (Join-Path $PluginRoot "release-artifacts.sha256") -Value "$hash  $ArtifactName" -Encoding ascii
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$pluginRoot = Join-Path $repoRoot "plugins\delegation"
$launcherPS = Join-Path $pluginRoot "scripts\delegation-mcp.ps1"
$launcherCmd = Join-Path $pluginRoot "scripts\delegation-mcp.cmd"
$pwsh = (Get-Command pwsh.exe).Source
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("delegation-plugin-test-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempRoot | Out-Null

try {
    $runtime = Join-Path $tempRoot "delegation.exe"
    & go -C $repoRoot build -trimpath -buildvcs=false -o $runtime ./cmd/delegation
    if ($LASTEXITCODE -ne 0) {
        throw "go build failed with exit code $LASTEXITCODE"
    }

    $missingEnvironment = @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = (Join-Path $tempRoot "missing")
    }
    $missingPS = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "mcp", "root") $missingEnvironment
    Assert-True ($missingPS.ExitCode -eq 127) "PowerShell launcher missing-runtime exit was $($missingPS.ExitCode)"
    Assert-True ($missingPS.Stderr -match "runtime 0.1.0-alpha.0 is not installed") "PowerShell launcher missing-runtime error was unclear"

    $cmdCommand = "call `"$launcherCmd`" mcp root"
    $missingCmd = Invoke-ChildProcess $env:ComSpec @("/d", "/s", "/c", $cmdCommand) $missingEnvironment
    Assert-True ($missingCmd.ExitCode -eq 127) "cmd launcher missing-runtime exit was $($missingCmd.ExitCode)"

    $overrideEnvironment = @{
        DELEGATION_BINARY = $runtime
        DELEGATION_HOME = (Join-Path $tempRoot "override")
    }
    $override = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "version", "--json") $overrideEnvironment
    Assert-True ($override.ExitCode -eq 0) "PowerShell launcher override failed: $($override.Stderr)"
    Assert-True ($override.Stdout -match '"version":"0.1.0-alpha.0"') "PowerShell launcher did not pass arguments through"
    $overrideUnavailable = Invoke-ChildProcess $env:ComSpec @("/d", "/s", "/c", "call `"$launcherCmd`" mcp root") $overrideEnvironment
    Assert-True ($overrideUnavailable.ExitCode -eq 69) "cmd launcher did not preserve runtime exit code"

    $missingChecksumPlugin = Join-Path $tempRoot "missing-checksum-plugin"
    Copy-Item -LiteralPath $pluginRoot -Destination $missingChecksumPlugin -Recurse
    Set-Content -LiteralPath (Join-Path $missingChecksumPlugin "release-artifacts.sha256") -Value "# intentionally empty for this test" -Encoding ascii
    $missingChecksumInstaller = Join-Path $missingChecksumPlugin "scripts\install-runtime.cmd"
    $missingChecksum = Invoke-ChildProcess $env:ComSpec @("/d", "/s", "/c", "call `"$missingChecksumInstaller`"") @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = (Join-Path $tempRoot "no-checksum")
    }
    Assert-True ($missingChecksum.ExitCode -ne 0) "installer accepted a release without a pinned checksum"
    Assert-True ($missingChecksum.Stderr -match "no pinned SHA-256") "installer checksum error was unclear"

    $testPlugin = Join-Path $tempRoot "plugin"
    Copy-Item -LiteralPath $pluginRoot -Destination $testPlugin -Recurse
    $payload = Join-Path $tempRoot "payload"
    New-Item -ItemType Directory -Path $payload | Out-Null
    Copy-Item -LiteralPath $runtime -Destination (Join-Path $payload "delegation.exe")
    $architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    switch ($architecture) {
        "X64" { $arch = "amd64" }
        "Arm64" { $arch = "arm64" }
        default { throw "unsupported test architecture: $architecture" }
    }
    $artifactName = "delegation_0.1.0-alpha.0_windows_${arch}.zip"
    $artifact = Join-Path $tempRoot $artifactName
    Compress-Archive -LiteralPath (Join-Path $payload "delegation.exe") -DestinationPath $artifact
    Write-ArtifactChecksum $testPlugin $artifact $artifactName

    function global:Invoke-WebRequest {
        param(
            [Parameter(Mandatory = $true)] [string] $Uri,
            [Parameter(Mandatory = $true)] [string] $OutFile,
            [switch] $UseBasicParsing
        )
        if ($Uri -cne $env:DELEGATION_TEST_EXPECTED_URL) {
            throw "unexpected download URL: $Uri"
        }
        $global:DelegationTestDownloadCount++
        Copy-Item -LiteralPath $env:DELEGATION_TEST_ARTIFACT -Destination $OutFile
    }

    $env:DELEGATION_TEST_ARTIFACT = $artifact
    $env:DELEGATION_TEST_EXPECTED_URL = "https://github.com/GhostFlying/delegation/releases/download/v0.1.0-alpha.0/$artifactName"
    $global:DelegationTestDownloadCount = 0
    $env:DELEGATION_HOME = Join-Path $tempRoot "installed-home"
    $installed = & (Join-Path $testPlugin "scripts\install-runtime.ps1")
    $expectedBinary = Join-Path $env:DELEGATION_HOME "bin\0.1.0-alpha.0\windows-$arch\delegation.exe"
    Assert-True ($installed -eq $expectedBinary) "installer returned $installed, expected $expectedBinary"
    Assert-True (Test-Path -LiteralPath $expectedBinary -PathType Leaf) "installer did not atomically install the runtime"
    Assert-True ($global:DelegationTestDownloadCount -eq 1) "installer made $global:DelegationTestDownloadCount download requests"

    $installedEnvironment = @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = $env:DELEGATION_HOME
    }
    $installedPS = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "version", "--json") $installedEnvironment
    Assert-True ($installedPS.ExitCode -eq 0 -and $installedPS.Stdout -match '"version":"0.1.0-alpha.0"') "PowerShell launcher did not find the installed runtime"
    $installedCmd = Invoke-ChildProcess $env:ComSpec @("/d", "/s", "/c", "call `"$launcherCmd`" version --json") $installedEnvironment
    Assert-True ($installedCmd.ExitCode -eq 0 -and $installedCmd.Stdout -match '"version":"0.1.0-alpha.0"') "cmd launcher did not find the installed runtime"

    $badChecksumPlugin = Join-Path $tempRoot "bad-checksum-plugin"
    Copy-Item -LiteralPath $testPlugin -Destination $badChecksumPlugin -Recurse
    Set-Content -LiteralPath (Join-Path $badChecksumPlugin "release-artifacts.sha256") -Value (("0" * 64) + "  " + $artifactName) -Encoding ascii
    $env:DELEGATION_HOME = Join-Path $tempRoot "bad-checksum-home"
    $global:DelegationTestDownloadCount = 0
    $checksumFailed = $false
    try {
        & (Join-Path $badChecksumPlugin "scripts\install-runtime.ps1") | Out-Null
    } catch {
        $checksumFailed = $_.Exception.Message -match "SHA-256 mismatch"
    }
    Assert-True $checksumFailed "installer accepted an artifact with the wrong checksum"
    Assert-True ($global:DelegationTestDownloadCount -eq 1) "checksum test made $global:DelegationTestDownloadCount download requests"

    $extraPayload = Join-Path $tempRoot "extra-payload"
    New-Item -ItemType Directory -Path $extraPayload | Out-Null
    Copy-Item -LiteralPath $runtime -Destination (Join-Path $extraPayload "delegation.exe")
    Set-Content -LiteralPath (Join-Path $extraPayload "unexpected.txt") -Value "unexpected"
    $extraArtifact = Join-Path $tempRoot "extra.zip"
    Compress-Archive -Path (Join-Path $extraPayload "*") -DestinationPath $extraArtifact
    Write-ArtifactChecksum $testPlugin $extraArtifact $artifactName
    $env:DELEGATION_TEST_ARTIFACT = $extraArtifact
    $env:DELEGATION_HOME = Join-Path $tempRoot "extra-home"
    $global:DelegationTestDownloadCount = 0
    $extraFailed = $false
    try {
        & (Join-Path $testPlugin "scripts\install-runtime.ps1") | Out-Null
    } catch {
        $extraFailed = $_.Exception.Message -match "unexpected files"
    }
    Assert-True $extraFailed "installer accepted an archive with extra entries"
    Assert-True ($global:DelegationTestDownloadCount -eq 1) "extra-entry test made $global:DelegationTestDownloadCount download requests"

    $versionPlugin = Join-Path $tempRoot "version-plugin"
    Copy-Item -LiteralPath $testPlugin -Destination $versionPlugin -Recurse
    Set-Content -LiteralPath (Join-Path $versionPlugin "VERSION") -Value "9.9.9-test" -Encoding ascii
    $versionArtifactName = "delegation_9.9.9-test_windows_${arch}.zip"
    Write-ArtifactChecksum $versionPlugin $artifact $versionArtifactName
    $env:DELEGATION_TEST_ARTIFACT = $artifact
    $env:DELEGATION_TEST_EXPECTED_URL = "https://github.com/GhostFlying/delegation/releases/download/v9.9.9-test/$versionArtifactName"
    $env:DELEGATION_HOME = Join-Path $tempRoot "version-home"
    $global:DelegationTestDownloadCount = 0
    $versionFailed = $false
    try {
        & (Join-Path $versionPlugin "scripts\install-runtime.ps1") | Out-Null
    } catch {
        $versionFailed = $_.Exception.Message -match "downloaded runtime reports version"
    }
    Assert-True $versionFailed "installer accepted a runtime with the wrong version"
    Assert-True ($global:DelegationTestDownloadCount -eq 1) "version test made $global:DelegationTestDownloadCount download requests"
} finally {
    Remove-Item Function:\Invoke-WebRequest -ErrorAction SilentlyContinue
    Remove-Item Env:\DELEGATION_TEST_ARTIFACT -ErrorAction SilentlyContinue
    Remove-Item Env:\DELEGATION_TEST_EXPECTED_URL -ErrorAction SilentlyContinue
    Remove-Item Env:\DELEGATION_HOME -ErrorAction SilentlyContinue
    Remove-Variable DelegationTestDownloadCount -Scope Global -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}
