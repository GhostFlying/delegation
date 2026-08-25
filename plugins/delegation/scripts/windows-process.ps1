function ConvertTo-DelegationWindowsCommandLineArgument {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $Argument
    )

    if ($Argument.Length -gt 0 -and $Argument -notmatch '[\s"]') {
        return $Argument
    }

    $builder = [System.Text.StringBuilder]::new()
    $null = $builder.Append([char] 34)
    $backslashCount = 0
    foreach ($character in $Argument.ToCharArray()) {
        if ($character -eq [char] 92) {
            $backslashCount++
            continue
        }
        if ($character -eq [char] 34) {
            $null = $builder.Append([char] 92, (2 * $backslashCount) + 1)
            $null = $builder.Append($character)
            $backslashCount = 0
            continue
        }
        if ($backslashCount -gt 0) {
            $null = $builder.Append([char] 92, $backslashCount)
            $backslashCount = 0
        }
        $null = $builder.Append($character)
    }
    if ($backslashCount -gt 0) {
        $null = $builder.Append([char] 92, 2 * $backslashCount)
    }
    $null = $builder.Append([char] 34)
    return $builder.ToString()
}

function ConvertTo-DelegationWindowsCommandLine {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $ArgumentList
    )

    $encoded = foreach ($argument in $ArgumentList) {
        if ($null -eq $argument) {
            throw "native process arguments must not be null"
        }
        ConvertTo-DelegationWindowsCommandLineArgument -Argument $argument
    }
    return $encoded -join " "
}

function New-DelegationNativeProcessStartInfo {
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $ArgumentList,
        [hashtable] $Environment = @{}
    )

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.UseShellExecute = $false

    if ($null -ne $startInfo.PSObject.Properties["ArgumentList"]) {
        foreach ($argument in $ArgumentList) {
            if ($null -eq $argument) {
                throw "native process arguments must not be null"
            }
            $startInfo.ArgumentList.Add($argument)
        }
    } else {
        $startInfo.Arguments = ConvertTo-DelegationWindowsCommandLine -ArgumentList $ArgumentList
    }

    if ($null -ne $startInfo.PSObject.Properties["Environment"]) {
        $environmentTarget = $startInfo.Environment
    } else {
        $environmentTarget = $startInfo.EnvironmentVariables
    }
    foreach ($entry in $Environment.GetEnumerator()) {
        if ($null -eq $entry.Value) {
            $null = $environmentTarget.Remove([string] $entry.Key)
        } else {
            $environmentTarget[[string] $entry.Key] = [string] $entry.Value
        }
    }
    return $startInfo
}

function Complete-DelegationNativeProcessOutput {
    param(
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process] $Process
    )

    $failure = $null
    foreach ($streamName in @("DelegationStandardOutput", "DelegationStandardError")) {
        $taskProperty = $Process.PSObject.Properties["${streamName}Task"]
        $streamProperty = $Process.PSObject.Properties["${streamName}Stream"]
        if ($null -eq $taskProperty -or $null -eq $streamProperty) {
            continue
        }
        try {
            $null = $taskProperty.Value.GetAwaiter().GetResult()
        } catch {
            if ($null -eq $failure) {
                $failure = $_
            }
        } finally {
            $streamProperty.Value.Dispose()
            $Process.PSObject.Properties.Remove("${streamName}Task")
            $Process.PSObject.Properties.Remove("${streamName}Stream")
        }
    }
    if ($null -ne $failure) {
        throw $failure
    }
}

function Start-DelegationNativeStreamCopy {
    param(
        [Parameter(Mandatory = $true)]
        [System.IO.Stream] $Source,
        [Parameter(Mandatory = $true)]
        [System.IO.Stream] $Destination
    )

    return $Source.CopyToAsync($Destination)
}

function Start-DelegationNativeProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $ArgumentList,
        [hashtable] $Environment = @{},
        [string] $StandardOutputPath,
        [string] $StandardErrorPath
    )

    $startInfo = New-DelegationNativeProcessStartInfo `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -Environment $Environment
    $outputStream = $null
    $errorStream = $null
    $outputTask = $null
    $errorTask = $null
    $started = $false
    $process = $null
    try {
        if ($PSBoundParameters.ContainsKey("StandardOutputPath")) {
            $startInfo.RedirectStandardOutput = $true
            $outputStream = [System.IO.File]::Open(
                $StandardOutputPath,
                [System.IO.FileMode]::Create,
                [System.IO.FileAccess]::Write,
                [System.IO.FileShare]::Read
            )
        }
        if ($PSBoundParameters.ContainsKey("StandardErrorPath")) {
            $startInfo.RedirectStandardError = $true
            $errorStream = [System.IO.File]::Open(
                $StandardErrorPath,
                [System.IO.FileMode]::Create,
                [System.IO.FileAccess]::Write,
                [System.IO.FileShare]::Read
            )
        }

        $process = [System.Diagnostics.Process]::new()
        $process.StartInfo = $startInfo
        if (-not $process.Start()) {
            throw "failed to start native process: $FilePath"
        }
        $started = $true
        if ($null -ne $outputStream) {
            $outputTask = Start-DelegationNativeStreamCopy `
                -Source $process.StandardOutput.BaseStream `
                -Destination $outputStream
        }
        if ($null -ne $errorStream) {
            $errorTask = Start-DelegationNativeStreamCopy `
                -Source $process.StandardError.BaseStream `
                -Destination $errorStream
        }
        if ($null -ne $outputTask) {
            $process | Add-Member -NotePropertyName DelegationStandardOutputStream -NotePropertyValue $outputStream
            $process | Add-Member -NotePropertyName DelegationStandardOutputTask -NotePropertyValue $outputTask
            $outputStream = $null
            $outputTask = $null
        }
        if ($null -ne $errorTask) {
            $process | Add-Member -NotePropertyName DelegationStandardErrorStream -NotePropertyValue $errorStream
            $process | Add-Member -NotePropertyName DelegationStandardErrorTask -NotePropertyValue $errorTask
            $errorStream = $null
            $errorTask = $null
        }
        return $process
    } catch {
        $startFailure = $_
        $cleanupFailure = $null
        if ($started) {
            try {
                Stop-DelegationNativeProcessTree -Process $process
            } catch {
                $cleanupFailure = $_
            }
        }
        foreach ($copyTask in @($outputTask, $errorTask)) {
            if ($null -ne $copyTask) {
                try {
                    $null = $copyTask.GetAwaiter().GetResult()
                } catch {
                    if ($null -eq $cleanupFailure) {
                        $cleanupFailure = $_
                    }
                }
            }
        }
        if ($null -ne $outputStream) {
            $outputStream.Dispose()
        }
        if ($null -ne $errorStream) {
            $errorStream.Dispose()
        }
        if ($null -ne $process) {
            $process.Dispose()
        }
        if ($null -ne $cleanupFailure) {
            throw "native process start failed and cleanup also failed: $($startFailure.Exception.Message); $($cleanupFailure.Exception.Message)"
        }
        throw $startFailure
    }
}

function Wait-DelegationNativeProcess {
    param(
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process] $Process,
        [ValidateRange(1, 2147483)]
        [int] $TimeoutSeconds
    )

    $exited = $true
    if ($PSBoundParameters.ContainsKey("TimeoutSeconds")) {
        $exited = $Process.WaitForExit($TimeoutSeconds * 1000)
    } else {
        $Process.WaitForExit()
    }
    if (-not $exited) {
        return $false
    }
    $Process.WaitForExit()
    Complete-DelegationNativeProcessOutput -Process $Process
    return $true
}

function Start-DelegationNativeReadToEnd {
    param(
        [Parameter(Mandatory = $true)]
        [System.IO.TextReader] $Reader
    )

    return $Reader.ReadToEndAsync()
}

function Write-DelegationNativeStandardInput {
    param(
        [Parameter(Mandatory = $true)]
        [System.IO.TextWriter] $Writer,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $Value
    )

    $Writer.Write($Value)
    $Writer.Close()
}

function Invoke-DelegationNativeProcessCapture {
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $ArgumentList,
        [hashtable] $Environment = @{},
        [AllowNull()]
        [string] $StandardInput = $null,
        [ValidateRange(1, 2147483)]
        [int] $TimeoutSeconds = 30
    )

    $startInfo = New-DelegationNativeProcessStartInfo `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -Environment $Environment
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.RedirectStandardInput = $null -ne $StandardInput
    $strictUTF8 = [System.Text.UTF8Encoding]::new($false, $true)
    $startInfo.StandardOutputEncoding = $strictUTF8
    $startInfo.StandardErrorEncoding = $strictUTF8

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $started = $false
    $stdoutTask = $null
    $stderrTask = $null
    try {
        if (-not $process.Start()) {
            throw "failed to start native process: $FilePath"
        }
        $started = $true
        $stdoutTask = Start-DelegationNativeReadToEnd -Reader $process.StandardOutput
        $stderrTask = Start-DelegationNativeReadToEnd -Reader $process.StandardError
        if ($null -ne $StandardInput) {
            Write-DelegationNativeStandardInput `
                -Writer $process.StandardInput `
                -Value $StandardInput
        }

        $timedOut = -not $process.WaitForExit($TimeoutSeconds * 1000)
        if ($timedOut) {
            Stop-DelegationNativeProcessTree -Process $process
        } else {
            $process.WaitForExit()
        }

        return [pscustomobject] @{
            Process = $process
            Id = $process.Id
            ExitCode = $process.ExitCode
            Stdout = $stdoutTask.GetAwaiter().GetResult()
            Stderr = $stderrTask.GetAwaiter().GetResult()
            TimedOut = $timedOut
        }
    } catch {
        $captureFailure = $_
        $cleanupFailure = $null
        if ($started) {
            try {
                Stop-DelegationNativeProcessTree -Process $process
            } catch {
                $cleanupFailure = $_
            }
        }
        if (-not $started -or $process.HasExited) {
            foreach ($readTask in @($stdoutTask, $stderrTask)) {
                if ($null -ne $readTask) {
                    try {
                        $null = $readTask.GetAwaiter().GetResult()
                    } catch {
                        if ($null -eq $cleanupFailure) {
                            $cleanupFailure = $_
                        }
                    }
                }
            }
        }
        $process.Dispose()
        if ($null -ne $cleanupFailure) {
            throw "native process capture failed and cleanup also failed: $($captureFailure.Exception.Message); $($cleanupFailure.Exception.Message)"
        }
        throw $captureFailure
    }
}

function Invoke-DelegationNativeTaskkill {
    param(
        [Parameter(Mandatory = $true)]
        [int] $ProcessId
    )

    $taskkill = Join-Path $env:SystemRoot "System32\taskkill.exe"
    $startInfo = New-DelegationNativeProcessStartInfo `
        -FilePath $taskkill `
        -ArgumentList @("/PID", [string] $ProcessId, "/T", "/F")
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $killer = [System.Diagnostics.Process]::new()
    $killer.StartInfo = $startInfo
    if (-not $killer.Start()) {
        $killer.Dispose()
        throw "failed to start taskkill.exe for native PID $ProcessId"
    }
    $stdoutTask = $killer.StandardOutput.ReadToEndAsync()
    $stderrTask = $killer.StandardError.ReadToEndAsync()
    if (-not $killer.WaitForExit(30000)) {
        $killer.Kill()
        $killer.WaitForExit()
        $null = $stdoutTask.GetAwaiter().GetResult()
        $null = $stderrTask.GetAwaiter().GetResult()
        $killer.Dispose()
        throw "taskkill.exe timed out for native PID $ProcessId"
    }
    $killer.WaitForExit()
    $result = [pscustomobject] @{
        ExitCode = $killer.ExitCode
        Stdout = $stdoutTask.GetAwaiter().GetResult()
        Stderr = $stderrTask.GetAwaiter().GetResult()
    }
    $killer.Dispose()
    return $result
}

function Stop-DelegationNativeProcessTree {
    param(
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process] $Process
    )

    $Process.Refresh()
    if ($Process.HasExited) {
        $null = Wait-DelegationNativeProcess -Process $Process
        return
    }

    $taskkillResult = Invoke-DelegationNativeTaskkill -ProcessId $Process.Id
    $Process.Refresh()
    if ($taskkillResult.ExitCode -ne 0 -and -not $Process.HasExited) {
        throw "taskkill.exe failed with exit code $($taskkillResult.ExitCode) for native PID $($Process.Id)"
    }
    $null = Wait-DelegationNativeProcess -Process $Process
}
