param(
    [switch] $NativeProcessOnly
)

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
        [AllowNull()] [string] $StandardInput = $null,
        [AllowNull()] [object] $CloseStandardInputAfterJSONRPCResponseID = $null,
        [ValidateRange(1, 2147483)] [int] $JSONRPCResponseTimeoutSeconds = 30,
        [ValidateRange(1, 2147483)] [int] $ProcessExitTimeoutSeconds = 30
    )
    $start = New-DelegationNativeProcessStartInfo `
        -FilePath $FilePath `
        -ArgumentList $Arguments `
        -Environment $Environment
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = $null -ne $StandardInput
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    if (-not $process.Start()) {
        $process.Dispose()
        throw "failed to start $FilePath"
    }
    try {
        $observedJSONRPCResponse = $null -eq $CloseStandardInputAfterJSONRPCResponseID
        $responseTimedOut = $false
        $processExitTimedOut = $false
        $stdout = $null
        $stderr = $null
        if ($null -ne $StandardInput) {
            $process.StandardInput.Write($StandardInput)
            $process.StandardInput.Flush()
            if ($null -eq $CloseStandardInputAfterJSONRPCResponseID) {
                Start-Sleep -Milliseconds 1000
                $process.StandardInput.Close()
            } else {
                $stdoutBuilder = [System.Text.StringBuilder]::new()
                $stderrTask = $process.StandardError.ReadToEndAsync()
                $responseDeadline = [DateTime]::UtcNow.AddSeconds($JSONRPCResponseTimeoutSeconds)
                $pendingRead = $null
                while (-not $observedJSONRPCResponse) {
                    $remaining = $responseDeadline - [DateTime]::UtcNow
                    if ($remaining.TotalMilliseconds -le 0) {
                        $responseTimedOut = $true
                        break
                    }
                    $pendingRead = $process.StandardOutput.ReadLineAsync()
                    if (-not $pendingRead.Wait([int] [Math]::Ceiling($remaining.TotalMilliseconds))) {
                        $responseTimedOut = $true
                        break
                    }
                    $line = $pendingRead.GetAwaiter().GetResult()
                    $pendingRead = $null
                    if ($null -eq $line) {
                        break
                    }
                    $null = $stdoutBuilder.AppendLine($line)
                    try {
                        $message = $line | ConvertFrom-Json
                        $idProperty = $message.PSObject.Properties["id"]
                        if ($null -ne $idProperty -and
                            [string] $idProperty.Value -ceq [string] $CloseStandardInputAfterJSONRPCResponseID) {
                            $observedJSONRPCResponse = $true
                        }
                    } catch {
                        $null = $_
                    }
                }
                $process.StandardInput.Close()
                $stdoutRemainderTask = $null
                if ($null -eq $pendingRead) {
                    $stdoutRemainderTask = $process.StandardOutput.ReadToEndAsync()
                }
                if (-not $process.WaitForExit($ProcessExitTimeoutSeconds * 1000)) {
                    $processExitTimedOut = $true
                    Stop-DelegationNativeProcessTree -Process $process
                } else {
                    $process.WaitForExit()
                }
                if ($null -ne $pendingRead) {
                    $line = $pendingRead.GetAwaiter().GetResult()
                    if ($null -ne $line) {
                        $null = $stdoutBuilder.AppendLine($line)
                    }
                }
                if ($null -ne $stdoutRemainderTask) {
                    $null = $stdoutBuilder.Append($stdoutRemainderTask.GetAwaiter().GetResult())
                } else {
                    $null = $stdoutBuilder.Append($process.StandardOutput.ReadToEnd())
                }
                $stdout = $stdoutBuilder.ToString()
                $stderr = $stderrTask.GetAwaiter().GetResult()
            }
        }
        if ($null -eq $stdout) {
            $stdout = $process.StandardOutput.ReadToEnd()
        }
        if ($null -eq $stderr) {
            $stderr = $process.StandardError.ReadToEnd()
        }
        $process.WaitForExit()
        $result = [pscustomobject]@{
            ExitCode = $process.ExitCode
            Stdout = $stdout
            Stderr = $stderr
            ObservedJSONRPCResponse = $observedJSONRPCResponse
            JSONRPCResponseTimedOut = $responseTimedOut
            ProcessExitTimedOut = $processExitTimedOut
        }
        return $result
    } finally {
        $process.Dispose()
    }
}

function Invoke-BatchFile {
    param(
        [Parameter(Mandatory = $true)] [string] $Path,
        [string[]] $ScriptArguments = @(),
        [hashtable] $Environment = @{},
        [AllowNull()] [string] $StandardInput = $null,
        [AllowNull()] [object] $CloseStandardInputAfterJSONRPCResponseID = $null,
        [ValidateRange(1, 2147483)] [int] $JSONRPCResponseTimeoutSeconds = 30,
        [ValidateRange(1, 2147483)] [int] $ProcessExitTimeoutSeconds = 30
    )
    $arguments = @("/d", "/s", "/c", "call", $Path) + $ScriptArguments
    Invoke-ChildProcess `
        -FilePath $env:ComSpec `
        -Arguments $arguments `
        -Environment $Environment `
        -StandardInput $StandardInput `
        -CloseStandardInputAfterJSONRPCResponseID $CloseStandardInputAfterJSONRPCResponseID `
        -JSONRPCResponseTimeoutSeconds $JSONRPCResponseTimeoutSeconds `
        -ProcessExitTimeoutSeconds $ProcessExitTimeoutSeconds
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

function Assert-ProcessExited {
    param(
        [Parameter(Mandatory = $true)] [int] $Id,
        [Parameter(Mandatory = $true)] [string] $Message
    )

    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        if ($null -eq (Get-Process -Id $Id -ErrorAction SilentlyContinue)) {
            return
        }
        Start-Sleep -Milliseconds 100
    }
    throw $Message
}

function Test-DelegationNativeProcessHelper {
    param(
        [Parameter(Mandatory = $true)] [string] $RepoRoot,
        [Parameter(Mandatory = $true)] [string] $TempRoot
    )

    $originalInvokeDelegationNativeTaskkill = (Get-Command Invoke-DelegationNativeTaskkill -CommandType Function).ScriptBlock
    $originalStartDelegationNativeStreamCopy = (Get-Command Start-DelegationNativeStreamCopy -CommandType Function).ScriptBlock
    $originalStartDelegationNativeReadToEnd = (Get-Command Start-DelegationNativeReadToEnd -CommandType Function).ScriptBlock
    $originalWriteDelegationNativeStandardInput = (Get-Command Write-DelegationNativeStandardInput -CommandType Function).ScriptBlock
    $probeRoot = Join-Path $TempRoot "native probe with spaces"
    New-Item -ItemType Directory -Path $probeRoot | Out-Null
    $probeSource = Join-Path $probeRoot "probe.go"
    $probeBinary = Join-Path $probeRoot "native probe.exe"
    @'
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type ioResult struct {
	PID            int      `json:"pid"`
	Arguments      []string `json:"arguments"`
	EnvironmentAdd string   `json:"environmentAdd"`
	EnvironmentEmpty string `json:"environmentEmpty"`
	RemovePresent  bool     `json:"removePresent"`
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	switch os.Args[1] {
	case "argv":
		_ = json.NewEncoder(os.Stdout).Encode(os.Args[2:])
		fmt.Fprint(os.Stderr, "\u53c2\u6570\u9519\u8bef-\u96ea\U0001f642")
	case "io":
		added := os.Getenv("DELEGATION_TEST_ADD")
		empty := os.Getenv("DELEGATION_TEST_EMPTY")
		_, removePresent := os.LookupEnv("DELEGATION_TEST_REMOVE")
		_ = json.NewEncoder(os.Stdout).Encode(ioResult{
			PID:            os.Getpid(),
			Arguments:      os.Args[2:],
			EnvironmentAdd: added,
			EnvironmentEmpty: empty,
			RemovePresent:  removePresent,
		})
		fmt.Fprintln(os.Stderr, "\u539f\u751f\u63a2\u9488\u9519\u8bef-\u96ea\U0001f642")
	case "tree":
		if len(os.Args) != 3 {
			os.Exit(2)
		}
		child := exec.Command(os.Args[0], "child")
		if err := child.Start(); err != nil {
			panic(err)
		}
		pids := strconv.Itoa(os.Getpid()) + "\n" + strconv.Itoa(child.Process.Pid)
		if err := os.WriteFile(os.Args[2], []byte(pids), 0600); err != nil {
			panic(err)
		}
		fmt.Println(os.Getpid())
		for {
			time.Sleep(time.Second)
		}
	case "child", "sleep":
		for {
			time.Sleep(time.Second)
		}
	case "short":
		fmt.Println(os.Getpid())
		time.Sleep(2 * time.Second)
	default:
		os.Exit(2)
	}
}
'@ | Set-Content -LiteralPath $probeSource -Encoding utf8
    & go build -tags=ts_omit_logtail -trimpath -buildvcs=false -o $probeBinary $probeSource
    if ($LASTEXITCODE -ne 0) {
        throw "native process probe build failed with exit code $LASTEXITCODE"
    }

    $callerErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    Set-StrictMode -Off
    $helperPath = Join-Path $RepoRoot "plugins\delegation\scripts\windows-process.ps1"
    . $helperPath
    Assert-True ($ErrorActionPreference -ceq "Continue") "dot-sourcing helper changed ErrorActionPreference"
    $undefinedValue = $delegationUndefinedCallerVariable
    Assert-True ($null -eq $undefinedValue) "dot-sourcing helper enabled strict mode in the caller"
    $ErrorActionPreference = $callerErrorActionPreference
    Set-StrictMode -Version Latest

    $backslashesBeforeQuote = "before" + ("\" * 3) + '"' + "after"
    $trailingBackslashes = "path with space" + ("\" * 3)
    $smile = [char]::ConvertFromUtf32(0x1f642)
    $unicodeArgument = "Unicode-" + [char] 0x96ea + $smile
    $argvStderr = -join @(
        [char] 0x53c2,
        [char] 0x6570,
        [char] 0x9519,
        [char] 0x8bef,
        "-",
        [char] 0x96ea,
        $smile
    )
    $environmentAdd = -join @(
        [char] 0x65b0,
        [char] 0x589e,
        "-",
        [char] 0x96ea,
        $smile
    )
    $ioStderr = -join @(
        [char] 0x539f,
        [char] 0x751f,
        [char] 0x63a2,
        [char] 0x9488,
        [char] 0x9519,
        [char] 0x8bef,
        "-",
        [char] 0x96ea,
        $smile
    )
    $expectedArguments = @(
        "",
        "plain",
        "contains space",
        "contains`t tab",
        'embedded"quote',
        $unicodeArgument,
        $backslashesBeforeQuote,
        $trailingBackslashes
    )
    $captured = Invoke-DelegationNativeProcessCapture `
        -FilePath $probeBinary `
        -ArgumentList (@("argv") + $expectedArguments) `
        -TimeoutSeconds 30
    try {
        Assert-True (-not $captured.TimedOut) "argv probe timed out"
        Assert-True ($captured.ExitCode -eq 0) "argv probe exited with $($captured.ExitCode): $($captured.Stderr)"
        $actualArguments = $captured.Stdout | ConvertFrom-Json
        Assert-True ($actualArguments.Count -eq $expectedArguments.Count) "argv probe returned $($actualArguments.Count) arguments, expected $($expectedArguments.Count)"
        for ($index = 0; $index -lt $expectedArguments.Count; $index++) {
            Assert-True ($actualArguments[$index] -ceq $expectedArguments[$index]) "argv $index changed during native launch"
        }
        Assert-True ($captured.Stderr -ceq $argvStderr) "strict UTF-8 stderr changed during native capture"
    } finally {
        $captured.Process.Dispose()
    }

    $configPath = Join-Path $probeRoot "config path with spaces.json"
    $environmentPath = Join-Path $probeRoot "environment path with spaces.env"
    $stdoutPath = Join-Path $probeRoot "stdout path with spaces.log"
    $stderrPath = Join-Path $probeRoot "stderr path with spaces.log"
    $strictUTF8 = [System.Text.UTF8Encoding]::new($false, $true)
    Set-Content -LiteralPath $configPath -Value "{}" -Encoding ascii
    Set-Content -LiteralPath $environmentPath -Value "NAME=value" -Encoding ascii
    $originalAdd = [System.Environment]::GetEnvironmentVariable("DELEGATION_TEST_ADD")
    $originalEmpty = [System.Environment]::GetEnvironmentVariable("DELEGATION_TEST_EMPTY")
    $originalRemove = [System.Environment]::GetEnvironmentVariable("DELEGATION_TEST_REMOVE")
    $env:DELEGATION_TEST_ADD = "parent add"
    $env:DELEGATION_TEST_EMPTY = "parent nonempty"
    $env:DELEGATION_TEST_REMOVE = "parent remove"
    try {
        $process = Start-DelegationNativeProcess `
            -FilePath $probeBinary `
            -ArgumentList @("io", $configPath, $environmentPath) `
            -Environment @{
                DELEGATION_TEST_ADD = $environmentAdd
                DELEGATION_TEST_EMPTY = ""
                DELEGATION_TEST_REMOVE = $null
            } `
            -StandardOutputPath $stdoutPath `
            -StandardErrorPath $stderrPath
        try {
            $waitResult = @(Wait-DelegationNativeProcess -Process $process -TimeoutSeconds 30)
            Assert-True ($waitResult.Count -eq 1) "wait returned $($waitResult.Count) pipeline values instead of one"
            Assert-True ($waitResult[0] -is [bool]) "wait result was not Boolean"
            Assert-True ([bool] $waitResult[0]) "redirected native process timed out"
            Assert-True ($process.ExitCode -eq 0) "redirected native process exited with $($process.ExitCode)"
            $ioResult = [System.IO.File]::ReadAllText($stdoutPath, $strictUTF8) | ConvertFrom-Json
            Assert-True ([int] $ioResult.pid -eq $process.Id) "returned PID was not the native executable PID"
            Assert-True ($ioResult.arguments[0] -ceq $configPath) "config path with spaces changed"
            Assert-True ($ioResult.arguments[1] -ceq $environmentPath) "environment-file path with spaces changed"
            Assert-True ($ioResult.environmentAdd -ceq $environmentAdd) "Unicode environment addition was not applied"
            Assert-True ($ioResult.environmentEmpty -ceq "") "empty environment override was not applied"
            Assert-True (-not [bool] $ioResult.removePresent) "environment removal was not applied"
            Assert-True ([System.IO.File]::ReadAllText($stderrPath, $strictUTF8).Contains($ioStderr)) "stderr path did not receive native Unicode output"
            Stop-DelegationNativeProcessTree -Process $process
        } finally {
            $process.Dispose()
        }
        Assert-True ($env:DELEGATION_TEST_ADD -ceq "parent add") "environment addition changed the parent environment"
        Assert-True ($env:DELEGATION_TEST_EMPTY -ceq "parent nonempty") "empty environment override changed the parent environment"
        Assert-True ($env:DELEGATION_TEST_REMOVE -ceq "parent remove") "environment removal changed the parent environment"
    } finally {
        [System.Environment]::SetEnvironmentVariable("DELEGATION_TEST_ADD", $originalAdd)
        [System.Environment]::SetEnvironmentVariable("DELEGATION_TEST_EMPTY", $originalEmpty)
        [System.Environment]::SetEnvironmentVariable("DELEGATION_TEST_REMOVE", $originalRemove)
    }

    $childPIDPath = Join-Path $probeRoot "child pid with spaces.txt"
    $treeOutputPath = Join-Path $probeRoot "tree stdout with spaces.log"
    $treeErrorPath = Join-Path $probeRoot "tree stderr with spaces.log"
    $tree = Start-DelegationNativeProcess `
        -FilePath $probeBinary `
        -ArgumentList @("tree", $childPIDPath) `
        -StandardOutputPath $treeOutputPath `
        -StandardErrorPath $treeErrorPath
    $treePID = 0
    $childPID = 0
    try {
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            if (Test-Path -LiteralPath $childPIDPath -PathType Leaf) {
                $treePIDs = @(Get-Content -LiteralPath $childPIDPath)
                if ($treePIDs.Count -eq 2) {
                    $treePID = [int] $treePIDs[0]
                    $childPID = [int] $treePIDs[1]
                    break
                }
            }
            if ($tree.HasExited) {
                throw "tree probe exited before creating its child"
            }
            Start-Sleep -Milliseconds 100
        }
        Assert-True ($treePID -eq $tree.Id) "tree probe did not report the tracked native PID"
        Assert-True ($childPID -gt 0) "tree probe did not report a descendant PID"
        Assert-True ($null -ne (Get-Process -Id $childPID -ErrorAction SilentlyContinue)) "tree probe descendant was not running"
        Stop-DelegationNativeProcessTree -Process $tree
        Assert-ProcessExited -Id $tree.Id -Message "exact native parent PID survived process-tree cleanup"
        Assert-ProcessExited -Id $childPID -Message "native descendant survived exact-PID process-tree cleanup"
        $reportedPID = [int] [System.IO.File]::ReadAllText($treeOutputPath, $strictUTF8)
        Assert-True ($reportedPID -eq $tree.Id) "tracked process object did not own the native executable PID"
    } finally {
        if (-not $tree.HasExited) {
            Stop-DelegationNativeProcessTree -Process $tree
        }
        $tree.Dispose()
    }

    $stalledPIDPath = Join-Path $probeRoot "stalled jsonrpc pid.txt"
    $stalledTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $stalled = Invoke-ChildProcess `
        -FilePath $probeBinary `
        -Arguments @("tree", $stalledPIDPath) `
        -StandardInput "{} `n" `
        -CloseStandardInputAfterJSONRPCResponseID 2 `
        -JSONRPCResponseTimeoutSeconds 1 `
        -ProcessExitTimeoutSeconds 1
    $stalledTimer.Stop()
    Assert-True $stalled.JSONRPCResponseTimedOut "missing JSON-RPC response did not report its deadline"
    Assert-True (-not $stalled.ObservedJSONRPCResponse) "stalled JSON-RPC fixture reported a response"
    Assert-True $stalled.ProcessExitTimedOut "stalled JSON-RPC fixture exited without exact-PID cleanup"
    Assert-True ($stalledTimer.Elapsed.TotalSeconds -lt 15) "stalled JSON-RPC fixture was not bounded"
    Assert-True (Test-Path -LiteralPath $stalledPIDPath -PathType Leaf) "stalled JSON-RPC fixture did not report its process tree"
    $stalledPIDs = @(Get-Content -LiteralPath $stalledPIDPath)
    Assert-True ($stalledPIDs.Count -eq 2) "stalled JSON-RPC fixture returned an incomplete process tree"
    foreach ($stalledPID in $stalledPIDs) {
        Assert-ProcessExited -Id ([int] $stalledPID) -Message "stalled JSON-RPC cleanup leaked PID $stalledPID"
    }

    $naturalExit = Start-DelegationNativeProcess `
        -FilePath $probeBinary `
        -ArgumentList @("short")
    $taskkillRaceState = [pscustomobject] @{ Invoked = $false }
    try {
        function Invoke-DelegationNativeTaskkill {
            param([int] $ProcessId)

            $null = $ProcessId
            $null = ($taskkillRaceState.Invoked = $true)
            Start-Sleep -Milliseconds 2500
            return [pscustomobject] @{
                ExitCode = 128
                Stdout = ""
                Stderr = "process already exited"
            }
        }
        Stop-DelegationNativeProcessTree -Process $naturalExit
        Assert-True $taskkillRaceState.Invoked "natural-exit taskkill race did not reach the post-check window"
        Assert-True ($naturalExit.HasExited) "natural-exit taskkill race did not wait for the tracked process"
    } finally {
        $null = Set-Item -Path Function:\Invoke-DelegationNativeTaskkill -Value $originalInvokeDelegationNativeTaskkill -Force
        $naturalExit.Dispose()
    }

    $taskkillFailure = Start-DelegationNativeProcess `
        -FilePath $probeBinary `
        -ArgumentList @("sleep")
    try {
        $taskkillFailedClosed = $false
        function Invoke-DelegationNativeTaskkill {
            param([int] $ProcessId)

            $null = $ProcessId
            return [pscustomobject] @{
                ExitCode = 5
                Stdout = ""
                Stderr = "access denied"
            }
        }
        try {
            Stop-DelegationNativeProcessTree -Process $taskkillFailure
        } catch {
            $taskkillFailedClosed = $_.Exception.Message -match "taskkill.exe failed with exit code 5"
        }
        Assert-True $taskkillFailedClosed "taskkill error was tolerated while the tracked process was still running"
        Assert-True (-not $taskkillFailure.HasExited) "taskkill failure fixture exited unexpectedly"
    } finally {
        $null = Set-Item -Path Function:\Invoke-DelegationNativeTaskkill -Value $originalInvokeDelegationNativeTaskkill -Force
        Stop-DelegationNativeProcessTree -Process $taskkillFailure
        $taskkillFailure.Dispose()
    }

    $copyFailurePIDPath = Join-Path $probeRoot "copy failure pid.txt"
    $copyInitializationFailed = $false
    try {
        function Start-DelegationNativeStreamCopy {
            param(
                [System.IO.Stream] $Source,
                [System.IO.Stream] $Destination
            )

            $null = $Source
            $null = $Destination
            for ($attempt = 0; $attempt -lt 50; $attempt++) {
                if (Test-Path -LiteralPath $copyFailurePIDPath -PathType Leaf) {
                    break
                }
                Start-Sleep -Milliseconds 100
            }
            throw "injected stream-copy initialization failure"
        }
        try {
            $null = Start-DelegationNativeProcess `
                -FilePath $probeBinary `
                -ArgumentList @("tree", $copyFailurePIDPath) `
                -StandardOutputPath (Join-Path $probeRoot "copy failure stdout.log")
        } catch {
            $copyInitializationFailed = $_.Exception.Message -match "injected stream-copy initialization failure"
        }
    } finally {
        $null = Set-Item -Path Function:\Start-DelegationNativeStreamCopy -Value $originalStartDelegationNativeStreamCopy -Force
    }
    Assert-True $copyInitializationFailed "stream-copy initialization failure was not returned"
    Assert-True (Test-Path -LiteralPath $copyFailurePIDPath -PathType Leaf) "copy failure fixture did not report its process tree"
    $copyFailurePIDs = @(Get-Content -LiteralPath $copyFailurePIDPath)
    Assert-True ($copyFailurePIDs.Count -eq 2) "copy failure fixture returned an incomplete process tree"
    foreach ($copyFailurePID in $copyFailurePIDs) {
        Assert-ProcessExited -Id ([int] $copyFailurePID) -Message "stream-copy initialization failure leaked PID $copyFailurePID"
    }

    $partialOpenOutputPath = Join-Path $probeRoot "partial open stdout.log"
    $partialOpenErrorPath = Join-Path $probeRoot "missing error directory\stderr.log"
    $partialOpenFailed = $false
    try {
        $null = Start-DelegationNativeProcess `
            -FilePath $probeBinary `
            -ArgumentList @("sleep") `
            -StandardOutputPath $partialOpenOutputPath `
            -StandardErrorPath $partialOpenErrorPath
    } catch {
        $partialOpenFailed = $true
    }
    Assert-True $partialOpenFailed "invalid stderr path did not fail after stdout was opened"
    $reopenedOutput = [System.IO.File]::Open(
        $partialOpenOutputPath,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
    $reopenedOutput.Dispose()

    foreach ($captureFailureKind in @("read", "input")) {
        $captureFailurePIDPath = Join-Path $probeRoot "$captureFailureKind failure pid.txt"
        $captureFailed = $false
        try {
            if ($captureFailureKind -eq "read") {
                function Start-DelegationNativeReadToEnd {
                    param([System.IO.TextReader] $Reader)

                    $null = $Reader
                    for ($attempt = 0; $attempt -lt 50; $attempt++) {
                        if (Test-Path -LiteralPath $captureFailurePIDPath -PathType Leaf) {
                            break
                        }
                        Start-Sleep -Milliseconds 100
                    }
                    throw "injected capture read initialization failure"
                }
            } else {
                function Write-DelegationNativeStandardInput {
                    param(
                        [System.IO.TextWriter] $Writer,
                        [string] $Value
                    )

                    $null = $Writer
                    $null = $Value
                    for ($attempt = 0; $attempt -lt 50; $attempt++) {
                        if (Test-Path -LiteralPath $captureFailurePIDPath -PathType Leaf) {
                            break
                        }
                        Start-Sleep -Milliseconds 100
                    }
                    throw "injected capture input failure"
                }
            }
            try {
                $null = Invoke-DelegationNativeProcessCapture `
                    -FilePath $probeBinary `
                    -ArgumentList @("tree", $captureFailurePIDPath) `
                    -StandardInput $(if ($captureFailureKind -eq "input") { "input" } else { $null }) `
                    -TimeoutSeconds 30
            } catch {
                $captureFailed = $_.Exception.Message -match "injected capture $captureFailureKind"
            }
        } finally {
            $null = Set-Item -Path Function:\Start-DelegationNativeReadToEnd -Value $originalStartDelegationNativeReadToEnd -Force
            $null = Set-Item -Path Function:\Write-DelegationNativeStandardInput -Value $originalWriteDelegationNativeStandardInput -Force
        }
        Assert-True $captureFailed "capture $captureFailureKind failure was not returned"
        Assert-True (Test-Path -LiteralPath $captureFailurePIDPath -PathType Leaf) "capture $captureFailureKind fixture did not report its process tree"
        $captureFailurePIDs = @(Get-Content -LiteralPath $captureFailurePIDPath)
        Assert-True ($captureFailurePIDs.Count -eq 2) "capture $captureFailureKind fixture returned an incomplete process tree"
        foreach ($captureFailurePID in $captureFailurePIDs) {
            Assert-ProcessExited -Id ([int] $captureFailurePID) -Message "capture $captureFailureKind failure leaked PID $captureFailurePID"
        }
    }

    $timeoutCleanupState = [pscustomobject] @{
        Calls = 0
        ProcessId = 0
    }
    $timeoutCleanupError = $null
    try {
        function Invoke-DelegationNativeTaskkill {
            param([int] $ProcessId)

            $null = ($timeoutCleanupState.Calls++)
            $null = ($timeoutCleanupState.ProcessId = $ProcessId)
            if ($timeoutCleanupState.Calls -eq 1) {
                return [pscustomobject] @{
                    ExitCode = 5
                    Stdout = ""
                    Stderr = "injected timeout cleanup failure"
                }
            }
            return & $originalInvokeDelegationNativeTaskkill -ProcessId $ProcessId
        }
        try {
            $null = Invoke-DelegationNativeProcessCapture `
                -FilePath $probeBinary `
                -ArgumentList @("sleep") `
                -TimeoutSeconds 1
        } catch {
            $timeoutCleanupError = $_.Exception.Message
        }
    } finally {
        $null = Set-Item -Path Function:\Invoke-DelegationNativeTaskkill -Value $originalInvokeDelegationNativeTaskkill -Force
    }
    Assert-True ($timeoutCleanupState.Calls -eq 2) "capture timeout did not retry exact-PID cleanup after its first failure"
    Assert-True ($timeoutCleanupError -match "taskkill.exe failed with exit code 5") "capture timeout did not preserve its original cleanup error"
    Assert-ProcessExited -Id $timeoutCleanupState.ProcessId -Message "capture timeout error path leaked its native PID"

    $timed = Invoke-DelegationNativeProcessCapture `
        -FilePath $probeBinary `
        -ArgumentList @("sleep") `
        -TimeoutSeconds 1
    try {
        Assert-True $timed.TimedOut "bounded captured execution did not report its timeout"
        Assert-True ($timed.Process.HasExited) "timed-out captured process did not exit"
        Assert-True ($timed.ExitCode -ne 0) "timed-out captured process returned a successful exit code"
        Assert-True ($null -ne $timed.Stdout) "timed-out captured process did not complete stdout"
        Assert-True ($null -ne $timed.Stderr) "timed-out captured process did not complete stderr"
        Assert-ProcessExited -Id $timed.Id -Message "timed-out captured process survived cleanup"
    } finally {
        $timed.Process.Dispose()
    }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$pluginRoot = Join-Path $repoRoot "plugins\delegation"
. (Join-Path $pluginRoot "scripts\windows-process.ps1")
$version = (Get-Content -LiteralPath (Join-Path $pluginRoot "VERSION") -Raw).Trim()
$versionJSON = '"version":"' + $version + '"'
$launcherPS = Join-Path $pluginRoot "scripts\delegation-mcp.ps1"
$launcherCmd = Join-Path $pluginRoot "scripts\delegation-mcp.cmd"
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("delegation-plugin-test-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempRoot | Out-Null

try {
    Test-DelegationNativeProcessHelper -RepoRoot $repoRoot -TempRoot $tempRoot
    if ($NativeProcessOnly) {
        return
    }

    $pwsh = (Get-Command pwsh.exe).Source
    $windowsPowerShell = (Get-Command powershell.exe).Source
    $runtime = Join-Path $tempRoot "delegation.exe"
    & go -C $repoRoot build -tags=ts_omit_logtail -trimpath -buildvcs=false -o $runtime ./cmd/delegation
    if ($LASTEXITCODE -ne 0) {
        throw "go build failed with exit code $LASTEXITCODE"
    }

    $missingEnvironment = @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = (Join-Path $tempRoot "missing")
    }
    $missingPS = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "mcp", "root") $missingEnvironment
    Assert-True ($missingPS.ExitCode -eq 127) "PowerShell launcher missing-runtime exit was $($missingPS.ExitCode)"
    Assert-True ($missingPS.Stderr.Contains("runtime $version is not installed")) "PowerShell launcher missing-runtime error was unclear"
    Assert-True ($missingPS.Stderr.Contains('run $delegation-setup in a new Codex or TraeX task')) "PowerShell launcher setup hint was host-specific"

    $missingCmd = Invoke-BatchFile -Path $launcherCmd -ScriptArguments @("mcp", "root") -Environment $missingEnvironment
    Assert-True ($missingCmd.ExitCode -eq 127) "cmd launcher missing-runtime exit was $($missingCmd.ExitCode); stdout: $($missingCmd.Stdout); stderr: $($missingCmd.Stderr)"
    Assert-True ($missingCmd.Stderr.Contains('run $delegation-setup in a new Codex or TraeX task')) "cmd launcher setup hint was host-specific"

    $overrideEnvironment = @{
        DELEGATION_BINARY = $runtime
        DELEGATION_HOME = (Join-Path $tempRoot "override")
    }
    $override = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "version", "--json") $overrideEnvironment
    Assert-True ($override.ExitCode -eq 0) "PowerShell launcher override failed: $($override.Stderr)"
    Assert-True ($override.Stdout.Contains($versionJSON)) "PowerShell launcher did not pass arguments through"
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
    $overrideMCP = Invoke-BatchFile `
        -Path $launcherCmd `
        -ScriptArguments @("mcp", "root") `
        -Environment $overrideEnvironment `
        -StandardInput $mcpInput `
        -CloseStandardInputAfterJSONRPCResponseID 2
    Assert-True $overrideMCP.ObservedJSONRPCResponse "cmd launcher root MCP did not return the tools/list response before stdin closed; stdout: $($overrideMCP.Stdout); stderr: $($overrideMCP.Stderr)"
    Assert-True (-not $overrideMCP.JSONRPCResponseTimedOut) "cmd launcher root MCP tools/list response timed out; stdout: $($overrideMCP.Stdout); stderr: $($overrideMCP.Stderr)"
    Assert-True (-not $overrideMCP.ProcessExitTimedOut) "cmd launcher root MCP did not exit after stdin closed; stdout: $($overrideMCP.Stdout); stderr: $($overrideMCP.Stderr)"
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
    $architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    switch ($architecture) {
        "X64" { $arch = "amd64" }
        "Arm64" { $arch = "arm64" }
        default { throw "unsupported test architecture: $architecture" }
    }
    $artifactName = "delegation_${version}_windows_${arch}.zip"
    $packageRoot = Join-Path $tempRoot "release-part"
    & go -C $repoRoot run ./cmd/releasepack package-target `
        --target "windows-$arch" --binary $runtime --out $packageRoot
    if ($LASTEXITCODE -ne 0) {
        throw "releasepack package-target failed with exit code $LASTEXITCODE"
    }
    $artifact = Join-Path $packageRoot $artifactName
    Write-ArtifactChecksum $testPlugin $artifact $artifactName

    $expectedUrl = "https://github.com/GhostFlying/delegation/releases/download/v$version/$artifactName"
    $windowsPowerShellHome = Join-Path $tempRoot "windows-powershell-home"
    $windowsPowerShellInstall = Invoke-WindowsPowerShellInstall $windowsPowerShell (Join-Path $testPlugin "scripts\install-runtime.ps1") $artifact $expectedUrl $windowsPowerShellHome
    $windowsPowerShellBinary = Join-Path $windowsPowerShellHome "bin\$version\windows-$arch\delegation.exe"
    Assert-True ($windowsPowerShellInstall.ExitCode -eq 0) "Windows PowerShell installation failed: $($windowsPowerShellInstall.Stderr)"
    Assert-True (($windowsPowerShellInstall.Stdout | Out-String).Trim() -eq $windowsPowerShellBinary) "Windows PowerShell installer returned an unexpected path"
    Assert-True (Test-Path -LiteralPath $windowsPowerShellBinary -PathType Leaf) "Windows PowerShell did not commit the runtime"
    Assert-True (Test-Path -LiteralPath (Join-Path (Split-Path -Parent $windowsPowerShellBinary) "THIRD_PARTY_NOTICES.txt") -PathType Leaf) "Windows PowerShell did not retain the release notice"

	$resolvedAncestorTarget = Join-Path $tempRoot "resolved-ancestor-target"
	$resolvedAncestorAlias = Join-Path $tempRoot "resolved-ancestor-alias"
	New-Item -ItemType Directory -Path $resolvedAncestorTarget | Out-Null
	New-Item -ItemType Junction -Path $resolvedAncestorAlias -Target $resolvedAncestorTarget | Out-Null
	$resolvedAncestorHome = Join-Path $resolvedAncestorAlias "delegation-home"
	$resolvedAncestorInstall = Invoke-WindowsPowerShellInstall $windowsPowerShell (Join-Path $testPlugin "scripts\install-runtime.ps1") $artifact $expectedUrl $resolvedAncestorHome
	$resolvedAncestorBinary = Join-Path $resolvedAncestorHome "bin\$version\windows-$arch\delegation.exe"
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

    $windowsPowerShellLock = Join-Path $windowsPowerShellHome ".locks\install-${version}-windows-$arch.lock"
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
    $staleLock = Join-Path $env:DELEGATION_HOME ".locks\install-${version}-windows-$arch.lock"
    $installed = & (Join-Path $testPlugin "scripts\install-runtime.ps1")
    $expectedBinary = Join-Path $env:DELEGATION_HOME "bin\$version\windows-$arch\delegation.exe"
    Assert-True ($installed -eq $expectedBinary) "installer returned $installed, expected $expectedBinary"
    Assert-True (Test-Path -LiteralPath $expectedBinary -PathType Leaf) "installer did not atomically install the runtime"
    Assert-True (Test-Path -LiteralPath (Join-Path (Split-Path -Parent $expectedBinary) "THIRD_PARTY_NOTICES.txt") -PathType Leaf) "installer did not retain the release notice"
    Assert-True ($global:DelegationTestDownloadCount -eq 1) "installer made $global:DelegationTestDownloadCount download requests"

    $installedEnvironment = @{
        DELEGATION_BINARY = $null
        DELEGATION_HOME = $env:DELEGATION_HOME
    }
    $installedPS = Invoke-ChildProcess $pwsh @("-NoLogo", "-NoProfile", "-File", $launcherPS, "version", "--json") $installedEnvironment
    Assert-True ($installedPS.ExitCode -eq 0 -and $installedPS.Stdout.Contains($versionJSON)) "PowerShell launcher did not find the installed runtime"
    $installedCmd = Invoke-BatchFile -Path $launcherCmd -ScriptArguments @("version", "--json") -Environment $installedEnvironment
    Assert-True ($installedCmd.ExitCode -eq 0 -and $installedCmd.Stdout.Contains($versionJSON)) "cmd launcher did not find the installed runtime"
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
    $reparseTarget = Join-Path $reparseHome "bin\$version\windows-$arch"
    New-ProtectedDelegationHome -Path $reparseHome
    New-Item -ItemType Directory -Force -Path $reparseTarget | Out-Null
    $reparseBinary = Join-Path $reparseTarget "delegation.exe"
    Set-Content -LiteralPath (Join-Path $reparseTarget "THIRD_PARTY_NOTICES.txt") -Value "fixture" -Encoding ascii
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
    $junctionParent = Join-Path $junctionHome "bin\$version"
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
    $raceTarget = Join-Path $raceHome "bin\$version\windows-$arch"
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
    $activeLock = Join-Path $activeLockDirectory "install-${version}-windows-$arch.lock"
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
    $expectedRecovered = Join-Path $activeHome "bin\$version\windows-$arch\delegation.exe"
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
