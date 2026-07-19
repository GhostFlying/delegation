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

function New-ProtectedDelegationHome {
    param([Parameter(Mandatory = $true)] [string] $Path)

    New-Item -ItemType Directory -Force -Path $Path | Out-Null
    $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
    $security = New-Object System.Security.AccessControl.DirectorySecurity
    $security.SetOwner($sid)
    $security.SetAccessRuleProtection($true, $false)
    $inheritance = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        $sid,
        [System.Security.AccessControl.FileSystemRights]::FullControl,
        $inheritance,
        [System.Security.AccessControl.PropagationFlags]::None,
        [System.Security.AccessControl.AccessControlType]::Allow
    )
    $security.AddAccessRule($rule) | Out-Null
    Set-Acl -LiteralPath $Path -AclObject $security
}

function Invoke-ChildProcess {
    param(
        [Parameter(Mandatory = $true)] [string] $FilePath,
        [Parameter(Mandatory = $true)] [string[]] $Arguments,
        [hashtable] $Environment = @{},
        [AllowNull()] [string] $StandardInput = $null
    )
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $FilePath
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = $null -ne $StandardInput
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
    if ($null -ne $StandardInput) {
        $process.StandardInput.Write($StandardInput)
		$process.StandardInput.Flush()
		Start-Sleep -Milliseconds 1000
        $process.StandardInput.Close()
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

function Invoke-BatchFile {
    param(
        [Parameter(Mandatory = $true)] [string] $Path,
        [string[]] $ScriptArguments = @(),
        [hashtable] $Environment = @{},
        [AllowNull()] [string] $StandardInput = $null
    )
    $arguments = @("/d", "/s", "/c", "call", $Path) + $ScriptArguments
    Invoke-ChildProcess $env:ComSpec $arguments $Environment $StandardInput
}

function Invoke-WindowsPowerShellInstall {
    param(
        [Parameter(Mandatory = $true)] [string] $PowerShell,
        [Parameter(Mandatory = $true)] [string] $Installer,
        [Parameter(Mandatory = $true)] [string] $Artifact,
        [Parameter(Mandatory = $true)] [string] $ExpectedUrl,
        [Parameter(Mandatory = $true)] [string] $DelegationHome
    )
    $command = @'
$ErrorActionPreference = "Stop"
function Get-FileHash {
    throw "test: installer must not call Get-FileHash"
}
function Get-Acl {
    throw "test: installer must not call Get-Acl"
}
function Invoke-WebRequest {
    param(
        [Parameter(Mandatory = $true)] [string] $Uri,
        [Parameter(Mandatory = $true)] [string] $OutFile,
        [switch] $UseBasicParsing
    )
    if ($Uri -cne $env:DELEGATION_TEST_EXPECTED_URL) {
        throw "unexpected download URL: $Uri"
    }
    Copy-Item -LiteralPath $env:DELEGATION_TEST_ARTIFACT -Destination $OutFile
}
& $env:DELEGATION_TEST_INSTALLER_PS1
'@
    Invoke-ChildProcess $PowerShell @("-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command) @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = $DelegationHome
        DELEGATION_TEST_ARTIFACT = $Artifact
        DELEGATION_TEST_EXPECTED_URL = $ExpectedUrl
        DELEGATION_TEST_INSTALLER_PS1 = $Installer
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
$windowsPowerShell = (Get-Command powershell.exe).Source
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
    Assert-True ($missingPS.Stderr -match "runtime 0.1.0-alpha.0.m1.1 is not installed") "PowerShell launcher missing-runtime error was unclear"

    $missingCmd = Invoke-BatchFile -Path $launcherCmd -ScriptArguments @("mcp", "root") -Environment $missingEnvironment
    Assert-True ($missingCmd.ExitCode -eq 127) "cmd launcher missing-runtime exit was $($missingCmd.ExitCode); stdout: $($missingCmd.Stdout); stderr: $($missingCmd.Stderr)"

    $overrideEnvironment = @{
        DELEGATION_BINARY = $runtime
        DELEGATION_HOME = (Join-Path $tempRoot "override")
    }
    $override = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "version", "--json") $overrideEnvironment
    Assert-True ($override.ExitCode -eq 0) "PowerShell launcher override failed: $($override.Stderr)"
    Assert-True ($override.Stdout -match '"version":"0.1.0-alpha.0.m1.1"') "PowerShell launcher did not pass arguments through"
    $overrideConfig = Join-Path $tempRoot "override\peer.json"
    $overrideSetup = Invoke-ChildProcess $runtime @(
        "setup", "peer",
        "--config", $overrideConfig,
        "--controller-id", "11111111-1111-4111-8111-111111111111",
        "--device-id", "22222222-2222-4222-8222-222222222222",
        "--device-name", "acceptance-device",
        "--broker-url", "ws://127.0.0.1:8787",
        "--auth-mode", "none",
        "--json"
    ) $overrideEnvironment
    Assert-True ($overrideSetup.ExitCode -eq 0) "peer setup for MCP launcher failed: $($overrideSetup.Stderr)"
    $overrideEnvironment.DELEGATION_CONFIG = $overrideConfig
    $mcpInput = @(
        '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"launcher-test","version":"1"}}}',
        '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}',
        '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
    ) -join "`n"
    $mcpInput += "`n"
    $overrideMCP = Invoke-BatchFile -Path $launcherCmd -ScriptArguments @("mcp", "root") -Environment $overrideEnvironment -StandardInput $mcpInput
    Assert-True ($overrideMCP.ExitCode -eq 0) "cmd launcher root MCP failed: $($overrideMCP.Stderr)"
    Assert-True ($overrideMCP.Stdout -match '"name":"list_devices"') "cmd launcher root MCP did not expose list_devices"
    Assert-True ($overrideMCP.Stdout -match '"name":"describe_device"') "cmd launcher root MCP did not expose describe_device"

    $missingChecksumPlugin = Join-Path $tempRoot "missing-checksum-plugin"
    Copy-Item -LiteralPath $pluginRoot -Destination $missingChecksumPlugin -Recurse
    Set-Content -LiteralPath (Join-Path $missingChecksumPlugin "release-artifacts.sha256") -Value "# intentionally empty for this test" -Encoding ascii
    $missingChecksumInstaller = Join-Path $missingChecksumPlugin "scripts\install-runtime.cmd"
    $missingChecksum = Invoke-BatchFile -Path $missingChecksumInstaller -Environment @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = (Join-Path $tempRoot "no-checksum")
    }
    Assert-True ($missingChecksum.ExitCode -ne 0) "installer accepted a release without a pinned checksum"
    Assert-True ($missingChecksum.Stderr -match "no pinned SHA-256") "installer checksum error was unclear"

    $testPlugin = Join-Path $tempRoot "plugin with spaces"
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
    $artifactName = "delegation_0.1.0-alpha.0.m1.1_windows_${arch}.zip"
    $artifact = Join-Path $tempRoot $artifactName
    Compress-Archive -LiteralPath (Join-Path $payload "delegation.exe") -DestinationPath $artifact
    Write-ArtifactChecksum $testPlugin $artifact $artifactName

    $expectedUrl = "https://github.com/GhostFlying/delegation/releases/download/v0.1.0-alpha.0.m1.1/$artifactName"
    $windowsPowerShellHome = Join-Path $tempRoot "windows-powershell-home"
    $windowsPowerShellInstall = Invoke-WindowsPowerShellInstall $windowsPowerShell (Join-Path $testPlugin "scripts\install-runtime.ps1") $artifact $expectedUrl $windowsPowerShellHome
    $windowsPowerShellBinary = Join-Path $windowsPowerShellHome "bin\0.1.0-alpha.0.m1.1\windows-$arch\delegation.exe"
    Assert-True ($windowsPowerShellInstall.ExitCode -eq 0) "Windows PowerShell installation failed: $($windowsPowerShellInstall.Stderr)"
    Assert-True (($windowsPowerShellInstall.Stdout | Out-String).Trim() -eq $windowsPowerShellBinary) "Windows PowerShell installer returned an unexpected path"
    Assert-True (Test-Path -LiteralPath $windowsPowerShellBinary -PathType Leaf) "Windows PowerShell did not commit the runtime"

	$resolvedAncestorTarget = Join-Path $tempRoot "resolved-ancestor-target"
	$resolvedAncestorAlias = Join-Path $tempRoot "resolved-ancestor-alias"
	New-Item -ItemType Directory -Path $resolvedAncestorTarget | Out-Null
	New-Item -ItemType Junction -Path $resolvedAncestorAlias -Target $resolvedAncestorTarget | Out-Null
	$resolvedAncestorHome = Join-Path $resolvedAncestorAlias "delegation-home"
	$resolvedAncestorInstall = Invoke-WindowsPowerShellInstall $windowsPowerShell (Join-Path $testPlugin "scripts\install-runtime.ps1") $artifact $expectedUrl $resolvedAncestorHome
	$resolvedAncestorBinary = Join-Path $resolvedAncestorHome "bin\0.1.0-alpha.0.m1.1\windows-$arch\delegation.exe"
	Assert-True ($resolvedAncestorInstall.ExitCode -eq 0) "Windows installer rejected a junction that resolves to a local volume: $($resolvedAncestorInstall.Stderr)"
	Assert-True (Test-Path -LiteralPath $resolvedAncestorBinary -PathType Leaf) "Windows installer did not commit through a resolved local ancestor"

	$networkShareName = "DelegationTest" + [guid]::NewGuid().ToString("N")
	$networkSharePath = Join-Path $tempRoot "network-share-target"
	$networkAlias = Join-Path $tempRoot "network-share-alias"
	$networkFixtureReady = $false
	$networkShareCreated = $false
	New-Item -ItemType Directory -Path $networkSharePath | Out-Null
	try {
		$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
		New-SmbShare -Name $networkShareName -Path $networkSharePath -FullAccess $currentUser -Temporary | Out-Null
		$networkShareCreated = $true
		$networkTarget = "\\localhost\$networkShareName"
		New-Item -ItemType SymbolicLink -Path $networkAlias -Target $networkTarget -ErrorAction Stop | Out-Null
		$networkFixtureReady = $true
		$networkHome = Join-Path $networkAlias "delegation-home"
		$networkInstall = Invoke-WindowsPowerShellInstall $windowsPowerShell (Join-Path $testPlugin "scripts\install-runtime.ps1") $artifact $expectedUrl $networkHome
		Assert-True ($networkInstall.ExitCode -ne 0) "Windows installer accepted a local-path ancestor that resolves to SMB"
		Assert-True ($networkInstall.Stderr -match "network path|local-volume validation|could not resolve delegation home") "Windows installer returned an unclear resolved-network error: $($networkInstall.Stderr)"
		Assert-True (-not (Test-Path -LiteralPath (Join-Path $networkSharePath "delegation-home"))) "Windows installer wrote through the SMB ancestor before rejecting it"
	} catch {
		if ($networkFixtureReady -or $env:CI -eq "true") {
			throw
		}
		Write-Verbose "resolved SMB ancestor test unavailable: $($_.Exception.Message)"
	} finally {
		if ($networkFixtureReady -and (Test-Path -LiteralPath $networkAlias)) {
			Remove-Item -LiteralPath $networkAlias -Force
		}
		if ($networkShareCreated) {
			Remove-SmbShare -Name $networkShareName -Force -Confirm:$false
		}
	}

    $installerCmd = Join-Path $testPlugin "scripts\install-runtime.cmd"
    $windowsPowerShellEnvironment = @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = $windowsPowerShellHome
    }
    $windowsPowerShellConfig = Join-Path $windowsPowerShellHome "peer.json"
    $windowsPowerShellSetup = Invoke-ChildProcess $windowsPowerShellBinary @(
        "setup", "peer",
        "--controller-id", "33333333-3333-4333-8333-333333333333",
        "--device-id", "44444444-4444-4444-8444-444444444444",
        "--device-name", "installed-runtime-device",
        "--broker-url", "ws://127.0.0.1:8787",
        "--auth-mode", "none",
        "--json"
    ) $windowsPowerShellEnvironment
    Assert-True ($windowsPowerShellSetup.ExitCode -eq 0) "installed runtime could not initialize its default home: $($windowsPowerShellSetup.Stderr)"
    Assert-True ($windowsPowerShellSetup.Stdout -match '"role":"peer"') "installed runtime setup returned an unexpected result"
    Assert-True (Test-Path -LiteralPath $windowsPowerShellConfig -PathType Leaf) "installed runtime setup did not use the default config path"
    $windowsPowerShellDoctor = Invoke-ChildProcess $windowsPowerShellBinary @("doctor", "--config", $windowsPowerShellConfig, "--json") $windowsPowerShellEnvironment
    Assert-True ($windowsPowerShellDoctor.ExitCode -eq 0 -and $windowsPowerShellDoctor.Stdout -match '"ok":true') "installed runtime was not ready after default setup: $($windowsPowerShellDoctor.Stderr)"

    $unsafeHome = Join-Path $tempRoot "unsafe-existing-home"
    New-Item -ItemType Directory -Path $unsafeHome | Out-Null
    $unsafeAclBefore = Get-Acl -LiteralPath $unsafeHome
    Assert-True (-not $unsafeAclBefore.AreAccessRulesProtected) "unsafe-home fixture unexpectedly has a protected DACL"
    $unsafeInstall = Invoke-WindowsPowerShellInstall $windowsPowerShell (Join-Path $testPlugin "scripts\install-runtime.ps1") $artifact $expectedUrl $unsafeHome
    Assert-True ($unsafeInstall.ExitCode -ne 0) "installer accepted an unsafe existing delegation home"
    $unsafeErrorIsClear = $unsafeInstall.Stderr -match "delegation home must be owned by the current user" -and
        $unsafeInstall.Stderr -match "protected current-user-only DACL" -and
        $unsafeInstall.Stderr -match "refusing to\s+modify existing permissions"
    Assert-True $unsafeErrorIsClear "unsafe delegation-home error was unclear: $($unsafeInstall.Stderr)"
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $unsafeHome "bin"))) "installer wrote into an unsafe existing delegation home"
    $unsafeAclAfter = Get-Acl -LiteralPath $unsafeHome
    Assert-True (-not $unsafeAclAfter.AreAccessRulesProtected) "installer silently changed the unsafe delegation-home DACL"

    $installerCmdRepeat = Invoke-BatchFile -Path $installerCmd -Environment $windowsPowerShellEnvironment
    Assert-True ($installerCmdRepeat.ExitCode -eq 0) "cmd installer repeat failed: $($installerCmdRepeat.Stderr)"
    Assert-True (($installerCmdRepeat.Stdout | Out-String).Trim() -eq $windowsPowerShellBinary) "cmd installer did not reuse the existing runtime"

    $windowsPowerShellLock = Join-Path $windowsPowerShellHome ".locks\install-0.1.0-alpha.0.m1.1-windows-$arch.lock"
    $heldWindowsPowerShellLock = [System.IO.File]::Open(
        $windowsPowerShellLock,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
    try {
        $installerCmdLocked = Invoke-BatchFile -Path $installerCmd -Environment $windowsPowerShellEnvironment
    } finally {
        $heldWindowsPowerShellLock.Dispose()
    }
    Assert-True ($installerCmdLocked.ExitCode -ne 0 -and $installerCmdLocked.Stderr -match "another runtime installation is in progress") "cmd installer ignored an active Windows PowerShell lock"
    $installerCmdRecovered = Invoke-BatchFile -Path $installerCmd -Environment $windowsPowerShellEnvironment
    Assert-True ($installerCmdRecovered.ExitCode -eq 0) "cmd installer did not recover after the process-held lock was released"

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
        if ($env:DELEGATION_TEST_CREATE_TARGET) {
            New-Item -ItemType Directory -Force -Path $env:DELEGATION_TEST_CREATE_TARGET | Out-Null
        }
    }

    $env:DELEGATION_TEST_ARTIFACT = $artifact
    $env:DELEGATION_TEST_EXPECTED_URL = $expectedUrl
    $global:DelegationTestDownloadCount = 0
    $env:DELEGATION_HOME = Join-Path $tempRoot "installed-home"
    $staleLock = Join-Path $env:DELEGATION_HOME ".locks\install-0.1.0-alpha.0.m1.1-windows-$arch.lock"
    $installed = & (Join-Path $testPlugin "scripts\install-runtime.ps1")
    $expectedBinary = Join-Path $env:DELEGATION_HOME "bin\0.1.0-alpha.0.m1.1\windows-$arch\delegation.exe"
    Assert-True ($installed -eq $expectedBinary) "installer returned $installed, expected $expectedBinary"
    Assert-True (Test-Path -LiteralPath $expectedBinary -PathType Leaf) "installer did not atomically install the runtime"
    Assert-True ($global:DelegationTestDownloadCount -eq 1) "installer made $global:DelegationTestDownloadCount download requests"

    $installedEnvironment = @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = $env:DELEGATION_HOME
    }
    $installedPS = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "version", "--json") $installedEnvironment
    Assert-True ($installedPS.ExitCode -eq 0 -and $installedPS.Stdout -match '"version":"0.1.0-alpha.0.m1.1"') "PowerShell launcher did not find the installed runtime"
    $installedCmd = Invoke-BatchFile -Path $launcherCmd -ScriptArguments @("version", "--json") -Environment $installedEnvironment
    Assert-True ($installedCmd.ExitCode -eq 0 -and $installedCmd.Stdout -match '"version":"0.1.0-alpha.0.m1.1"') "cmd launcher did not find the installed runtime"
    Assert-True (Test-Path -LiteralPath $staleLock -PathType Leaf) "installer removed its persistent lock file"

    $installedExtra = Join-Path (Split-Path -Parent $expectedBinary) "unexpected.txt"
    Set-Content -LiteralPath $installedExtra -Value "unexpected"
    $existingExtraFailed = $false
    try {
        & (Join-Path $testPlugin "scripts\install-runtime.ps1") | Out-Null
    } catch {
        $existingExtraFailed = $_.Exception.Message -match "installed runtime directory contains unexpected files"
    } finally {
        Remove-Item -LiteralPath $installedExtra -Force
    }
    Assert-True $existingExtraFailed "installer accepted an installed runtime directory with extra files"

    $reparseHome = Join-Path $tempRoot "reparse-home"
    $reparseTarget = Join-Path $reparseHome "bin\0.1.0-alpha.0.m1.1\windows-$arch"
    New-ProtectedDelegationHome -Path $reparseHome
    New-Item -ItemType Directory -Force -Path $reparseTarget | Out-Null
    $reparseBinary = Join-Path $reparseTarget "delegation.exe"
    $reparseCreated = $false
    try {
        New-Item -ItemType SymbolicLink -Path $reparseBinary -Target $runtime -ErrorAction Stop | Out-Null
        $reparseCreated = $true
    } catch {
        Write-Verbose "file symlink test unavailable: $($_.Exception.Message)"
    }
    if ($reparseCreated) {
        $reparseResult = Invoke-BatchFile -Path $installerCmd -Environment @{
            DELEGATION_BINARY = $null
            DELEGATION_HOME = $reparseHome
        }
        Assert-True ($reparseResult.ExitCode -ne 0 -and $reparseResult.Stderr -match "installed runtime must not be a reparse point") "Windows PowerShell installer accepted a reparse-point runtime binary"
    }

    $junctionHome = Join-Path $tempRoot "junction-home"
    $junctionParent = Join-Path $junctionHome "bin\0.1.0-alpha.0.m1.1"
    $junctionOutside = Join-Path $tempRoot "junction-outside"
    New-ProtectedDelegationHome -Path $junctionHome
    New-Item -ItemType Directory -Force -Path $junctionParent, $junctionOutside | Out-Null
    $junctionTarget = Join-Path $junctionParent "windows-$arch"
    New-Item -ItemType Junction -Path $junctionTarget -Target $junctionOutside | Out-Null
    $junctionResult = Invoke-BatchFile -Path $installerCmd -Environment @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = $junctionHome
    }
    Assert-True ($junctionResult.ExitCode -ne 0 -and $junctionResult.Stderr -match "runtime target must not be a reparse point") "Windows PowerShell installer accepted a reparse-point target directory"

    $raceHome = Join-Path $tempRoot "race-home"
    $raceTarget = Join-Path $raceHome "bin\0.1.0-alpha.0.m1.1\windows-$arch"
    $env:DELEGATION_HOME = $raceHome
    $env:DELEGATION_TEST_CREATE_TARGET = $raceTarget
    $raceFailed = $false
    try {
        & (Join-Path $testPlugin "scripts\install-runtime.ps1") | Out-Null
    } catch {
        $raceFailed = $_.Exception.Message -match "runtime target appeared during installation"
    } finally {
        Remove-Item Env:\DELEGATION_TEST_CREATE_TARGET -ErrorAction SilentlyContinue
    }
    Assert-True $raceFailed "installer reported success after a racing target appeared"
    Assert-True (@(Get-ChildItem -LiteralPath $raceTarget -Force).Count -eq 0) "installer nested staging output under a racing target"

    $activeHome = Join-Path $tempRoot "active-lock-home"
    $activeLockDirectory = Join-Path $activeHome ".locks"
    New-ProtectedDelegationHome -Path $activeHome
    New-Item -ItemType Directory -Force -Path $activeLockDirectory | Out-Null
    $activeLock = Join-Path $activeLockDirectory "install-0.1.0-alpha.0.m1.1-windows-$arch.lock"
    $heldLock = [System.IO.File]::Open(
        $activeLock,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
    $env:DELEGATION_HOME = $activeHome
    $activeLockFailed = $false
    try {
        & (Join-Path $testPlugin "scripts\install-runtime.ps1") | Out-Null
    } catch {
        $activeLockFailed = $_.Exception.Message -match "another runtime installation is in progress"
    } finally {
        $heldLock.Dispose()
    }
    Assert-True $activeLockFailed "installer ignored an active process-held lock"
    $recovered = & (Join-Path $testPlugin "scripts\install-runtime.ps1")
    $expectedRecovered = Join-Path $activeHome "bin\0.1.0-alpha.0.m1.1\windows-$arch\delegation.exe"
    Assert-True ($recovered -eq $expectedRecovered -and (Test-Path -LiteralPath $expectedRecovered -PathType Leaf)) "installer did not recover after the process-held lock was released"

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
    Remove-Item Env:\DELEGATION_TEST_CREATE_TARGET -ErrorAction SilentlyContinue
    Remove-Item Env:\DELEGATION_HOME -ErrorAction SilentlyContinue
    Remove-Variable DelegationTestDownloadCount -Scope Global -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}
