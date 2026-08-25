$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

Add-Type -AssemblyName System.IO.Compression | Out-Null

function Assert-True {
    param(
        [Parameter(Mandatory = $true)] [bool] $Condition,
        [Parameter(Mandatory = $true)] [string] $Message
    )

    if (-not $Condition) {
        throw $Message
    }
}

function ConvertTo-UTF8Bytes {
    param([Parameter(Mandatory = $true)] [string] $Text)

    $encoding = New-Object System.Text.UTF8Encoding($false, $true)
    return , $encoding.GetBytes($Text)
}

function ConvertTo-LowerHex {
    param([Parameter(Mandatory = $true)] [byte[]] $Bytes)

    return ([System.BitConverter]::ToString($Bytes)).Replace("-", "").ToLowerInvariant()
}

function Get-BytesSha256 {
    param([Parameter(Mandatory = $true)] [byte[]] $Bytes)

    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ConvertTo-LowerHex -Bytes $algorithm.ComputeHash($Bytes)
    } finally {
        $algorithm.Dispose()
    }
}

function Get-PathSha256 {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $stream = [System.IO.File]::OpenRead($Path)
    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ConvertTo-LowerHex -Bytes $algorithm.ComputeHash($stream)
    } finally {
        $algorithm.Dispose()
        $stream.Dispose()
    }
}

function Get-BaselineFixture {
    $payload = @(
        [pscustomobject] @{
            Name = "payload/file one.txt"
            Bytes = (ConvertTo-UTF8Bytes -Text "first payload`n")
            ExternalAttributes = 0
        },
        [pscustomobject] @{
            Name = "payload/file-two.bin"
            Bytes = [byte[]] @(0, 1, 2, 3, 254, 255)
            ExternalAttributes = 0
        }
    )
    $payloadLines = @()
    foreach ($entry in $payload) {
        $payloadLines += "$(Get-BytesSha256 -Bytes $entry.Bytes)  $($entry.Name)"
    }
    $payloadManifestBytes = ConvertTo-UTF8Bytes -Text (($payloadLines -join "`n") + "`n")
    $payloadManifestSha256 = Get-BytesSha256 -Bytes $payloadManifestBytes
    $manifest = [ordered] @{
        schemaVersion = 1
        kind = "delegation-test-windows-handoff"
        os = "windows"
        arch = "amd64"
        secretFree = $true
        payloadManifest = "payload.sha256"
        payloadManifestSha256 = $payloadManifestSha256
        releaseCommit = "1111111111111111111111111111111111111111"
        releaseTree = "2222222222222222222222222222222222222222"
        pluginTree = "3333333333333333333333333333333333333333"
        runtimeVersion = "0.1.0-test"
    }
    $manifestBytes = ConvertTo-UTF8Bytes -Text (
        ($manifest | ConvertTo-Json -Depth 8) + "`n"
    )
    return [pscustomobject] @{
        Payload = $payload
        PayloadManifestBytes = $payloadManifestBytes
        PayloadManifestSha256 = $payloadManifestSha256
        ManifestBytes = $manifestBytes
        ManifestSha256 = (Get-BytesSha256 -Bytes $manifestBytes)
        Kind = $manifest.kind
        ReleaseCommit = $manifest.releaseCommit
        ReleaseTree = $manifest.releaseTree
        PluginTree = $manifest.pluginTree
        RuntimeVersion = $manifest.runtimeVersion
    }
}

function New-TestArchive {
    param(
        [Parameter(Mandatory = $true)] [string] $Path,
        [Parameter(Mandatory = $true)] [object] $Baseline,
        [Parameter(Mandatory = $true)]
        [ValidateSet(
            "Valid",
            "ManifestMismatch",
            "PayloadManifestMismatch",
            "PayloadMismatch",
            "Extra",
            "Missing",
            "DuplicateCase",
            "Traversal",
            "Absolute",
            "Backslash",
            "AlternateDataStream",
            "ReservedDevice",
            "TrailingDot",
            "Directory",
            "FileDirectoryCollision",
            "Link"
        )]
        [string] $Scenario
    )

    $entries = @(
        [pscustomobject] @{
            Name = "handoff-manifest.json"
            Bytes = $Baseline.ManifestBytes
            ExternalAttributes = 0
        },
        [pscustomobject] @{
            Name = "payload.sha256"
            Bytes = $Baseline.PayloadManifestBytes
            ExternalAttributes = 0
        }
    )
    foreach ($entry in $Baseline.Payload) {
        $entries += [pscustomobject] @{
            Name = $entry.Name
            Bytes = $entry.Bytes
            ExternalAttributes = $entry.ExternalAttributes
        }
    }

    switch ($Scenario) {
        "ManifestMismatch" {
            $entries[0].Bytes = ConvertTo-UTF8Bytes -Text (
                ([System.Text.Encoding]::UTF8.GetString($entries[0].Bytes)).TrimEnd() + " `n"
            )
        }
        "PayloadManifestMismatch" {
            $entries[1].Bytes = ConvertTo-UTF8Bytes -Text (
                [System.Text.Encoding]::UTF8.GetString($entries[1].Bytes) + "`n"
            )
        }
        "PayloadMismatch" {
            $entries[2].Bytes = ConvertTo-UTF8Bytes -Text "changed payload`n"
        }
        "Extra" {
            $entries += [pscustomobject] @{
                Name = "payload/extra.txt"
                Bytes = (ConvertTo-UTF8Bytes -Text "extra`n")
                ExternalAttributes = 0
            }
        }
        "Missing" {
            $entries = @($entries[0..2])
        }
        "DuplicateCase" {
            $entries += [pscustomobject] @{
                Name = "PAYLOAD/file one.txt"
                Bytes = (ConvertTo-UTF8Bytes -Text "duplicate`n")
                ExternalAttributes = 0
            }
        }
        "Traversal" {
            $entries += [pscustomobject] @{
                Name = "../escape.txt"
                Bytes = (ConvertTo-UTF8Bytes -Text "escape`n")
                ExternalAttributes = 0
            }
        }
        "Absolute" {
            $entries += [pscustomobject] @{
                Name = "/absolute.txt"
                Bytes = (ConvertTo-UTF8Bytes -Text "absolute`n")
                ExternalAttributes = 0
            }
        }
        "Backslash" {
            $entries += [pscustomobject] @{
                Name = "payload\backslash.txt"
                Bytes = (ConvertTo-UTF8Bytes -Text "backslash`n")
                ExternalAttributes = 0
            }
        }
        "AlternateDataStream" {
            $entries += [pscustomobject] @{
                Name = "payload/file.txt:stream"
                Bytes = (ConvertTo-UTF8Bytes -Text "stream`n")
                ExternalAttributes = 0
            }
        }
        "ReservedDevice" {
            $entries += [pscustomobject] @{
                Name = "payload/CON.txt"
                Bytes = (ConvertTo-UTF8Bytes -Text "device`n")
                ExternalAttributes = 0
            }
        }
        "TrailingDot" {
            $entries += [pscustomobject] @{
                Name = "payload/trailing."
                Bytes = (ConvertTo-UTF8Bytes -Text "dot`n")
                ExternalAttributes = 0
            }
        }
        "Directory" {
            $entries += [pscustomobject] @{
                Name = "payload/directory/"
                Bytes = [byte[]] @()
                ExternalAttributes = 0
            }
        }
        "FileDirectoryCollision" {
            $entries += [pscustomobject] @{
                Name = "payload"
                Bytes = (ConvertTo-UTF8Bytes -Text "collision`n")
                ExternalAttributes = 0
            }
        }
        "Link" {
            $entries += [pscustomobject] @{
                Name = "payload/link"
                Bytes = (ConvertTo-UTF8Bytes -Text "file one.txt")
                ExternalAttributes = -1610612736
            }
        }
    }

    $fileStream = [System.IO.File]::Open(
        $Path,
        [System.IO.FileMode]::CreateNew,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
    $zip = $null
    try {
        $zip = New-Object System.IO.Compression.ZipArchive(
            $fileStream,
            [System.IO.Compression.ZipArchiveMode]::Create,
            $false
        )
        foreach ($entry in $entries) {
            $zipEntry = $zip.CreateEntry(
                $entry.Name,
                [System.IO.Compression.CompressionLevel]::Optimal
            )
            $zipEntry.LastWriteTime = New-Object System.DateTimeOffset(
                2026,
                1,
                1,
                0,
                0,
                0,
                [System.TimeSpan]::Zero
            )
            $zipEntry.ExternalAttributes = $entry.ExternalAttributes
            $target = $zipEntry.Open()
            try {
                $target.Write($entry.Bytes, 0, $entry.Bytes.Length)
            } finally {
                $target.Dispose()
            }
        }
    } finally {
        if ($null -ne $zip) {
            $zip.Dispose()
        } else {
            $fileStream.Dispose()
        }
    }
    $item = Get-Item -LiteralPath $Path
    return [pscustomobject] @{
        Path = $item.FullName
        Size = $item.Length
        Sha256 = (Get-PathSha256 -Path $item.FullName)
    }
}

function Get-HarnessArguments {
    param(
        [Parameter(Mandatory = $true)] [object] $Archive,
        [Parameter(Mandatory = $true)] [object] $Baseline,
        [Parameter(Mandatory = $true)] [string] $Mode,
        [AllowNull()] [string] $DestinationPath = $null
    )

    $arguments = @{
        Mode = $Mode
        ArchivePath = $Archive.Path
        ExpectedArchiveSha256 = $Archive.Sha256
        ExpectedArchiveSize = $Archive.Size
        ExpectedManifestSha256 = $Baseline.ManifestSha256
        ExpectedPayloadManifestSha256 = $Baseline.PayloadManifestSha256
        ExpectedKind = $Baseline.Kind
        ExpectedReleaseCommit = $Baseline.ReleaseCommit
        ExpectedReleaseTree = $Baseline.ReleaseTree
        ExpectedPluginTree = $Baseline.PluginTree
        ExpectedRuntimeVersion = $Baseline.RuntimeVersion
    }
    if ($null -ne $DestinationPath) {
        $arguments.DestinationPath = $DestinationPath
    }
    return $arguments
}

function Invoke-ExpectedFailure {
    param(
        [Parameter(Mandatory = $true)] [string] $Harness,
        [Parameter(Mandatory = $true)] [hashtable] $Arguments,
        [Parameter(Mandatory = $true)] [string] $MessagePattern
    )

    $message = $null
    try {
        & $Harness @Arguments | Out-Null
    } catch {
        $message = $_.Exception.Message
    }
    Assert-True ($null -ne $message) "harness unexpectedly accepted an invalid input"
    Assert-True ($message -match $MessagePattern) (
        "harness failure '$message' did not match '$MessagePattern'"
    )
}

$harness = Join-Path $PSScriptRoot "windows_qualification_handoff.ps1"
$harnessSource = Get-Content -LiteralPath $harness -Raw
Assert-True (
    $harnessSource -notmatch '(?im)Split-Path[^\r\n]*-LiteralPath[^\r\n]*-Parent' -and
    $harnessSource -notmatch '(?im)Split-Path[^\r\n]*-Parent[^\r\n]*-LiteralPath'
) "harness contains the Windows PowerShell 5.1-incompatible Split-Path parameter combination"

$tempRoot = Join-Path (
    [System.IO.Path]::GetTempPath()
) ("delegation qualification harness " + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempRoot | Out-Null

try {
    $baseline = Get-BaselineFixture
    $valid = New-TestArchive `
        -Path (Join-Path $tempRoot "valid handoff.zip") `
        -Baseline $baseline `
        -Scenario Valid

    $beforeValidation = @(
        Get-ChildItem -LiteralPath $tempRoot -Force -Recurse |
            ForEach-Object { $_.FullName }
    )
    $validateArguments = Get-HarnessArguments `
        -Archive $valid `
        -Baseline $baseline `
        -Mode Validate
    $validation = (& $harness @validateArguments | ConvertFrom-Json)
    $validationRepeat = (& $harness @validateArguments | ConvertFrom-Json)
    $afterValidation = @(
        Get-ChildItem -LiteralPath $tempRoot -Force -Recurse |
            ForEach-Object { $_.FullName }
    )
    Assert-True (
        (@($beforeValidation) -join "`n") -ceq (@($afterValidation) -join "`n")
    ) "Validate mode wrote to the fixture tree"
    Assert-True ($validation.mode -ceq "Validate") "Validate result reported the wrong mode"
    Assert-True ($validation.entryCount -eq 4) "Validate result reported the wrong entry count"
    Assert-True (
        $validation.payloadEntryCount -eq 2
    ) "Validate result reported the wrong payload count"
    Assert-True (
        $validation.archiveSha256 -ceq $valid.Sha256
    ) "Validate result reported the wrong outer hash"
    Assert-True (
        ($validation | ConvertTo-Json -Compress) -ceq
        ($validationRepeat | ConvertTo-Json -Compress)
    ) "repeated validation did not return equivalent JSON"

    $destination = Join-Path $tempRoot "extracted files"
    New-Item -ItemType Directory -Path $destination | Out-Null
    $extractArguments = Get-HarnessArguments `
        -Archive $valid `
        -Baseline $baseline `
        -Mode Extract `
        -DestinationPath $destination
    $extraction = (& $harness @extractArguments | ConvertFrom-Json)
    Assert-True ($extraction.mode -ceq "Extract") "Extract result reported the wrong mode"
    $extractedFiles = @(
        Get-ChildItem -LiteralPath $destination -File -Force -Recurse |
            ForEach-Object {
                $_.FullName.Substring($destination.Length + 1).Replace(
                    [System.IO.Path]::DirectorySeparatorChar,
                    [char] "/"
                )
            } |
            Sort-Object
    )
    $expectedFiles = @(
        "handoff-manifest.json",
        "payload.sha256",
        "payload/file one.txt",
        "payload/file-two.bin"
    ) | Sort-Object
    Assert-True (
        ($extractedFiles -join "`n") -ceq ($expectedFiles -join "`n")
    ) "Extract mode did not create the exact validated file set"
    foreach ($entry in $baseline.Payload) {
        $path = Join-Path $destination $entry.Name.Replace(
            [char] "/",
            [System.IO.Path]::DirectorySeparatorChar
        )
        Assert-True (
            (Get-PathSha256 -Path $path) -ceq (Get-BytesSha256 -Bytes $entry.Bytes)
        ) "Extract mode changed payload bytes for $($entry.Name)"
    }

    $wrongHashArguments = Get-HarnessArguments `
        -Archive $valid `
        -Baseline $baseline `
        -Mode Validate
    $wrongHashArguments.ExpectedArchiveSha256 = ("0" * 64)
    Invoke-ExpectedFailure `
        -Harness $harness `
        -Arguments $wrongHashArguments `
        -MessagePattern "archive SHA-256"

    $wrongSizeArguments = Get-HarnessArguments `
        -Archive $valid `
        -Baseline $baseline `
        -Mode Validate
    $wrongSizeArguments.ExpectedArchiveSize = $valid.Size + 1
    Invoke-ExpectedFailure `
        -Harness $harness `
        -Arguments $wrongSizeArguments `
        -MessagePattern "archive size"

    $failureScenarios = @(
        [pscustomobject] @{
            Scenario = "ManifestMismatch"
            Pattern = "handoff-manifest.json SHA-256"
        },
        [pscustomobject] @{
            Scenario = "PayloadManifestMismatch"
            Pattern = "payload.sha256 SHA-256"
        },
        [pscustomobject] @{
            Scenario = "PayloadMismatch"
            Pattern = "payload entry SHA-256 mismatch"
        },
        [pscustomobject] @{
            Scenario = "Extra"
            Pattern = "complete ZIP payload|unmanifested"
        },
        [pscustomobject] @{
            Scenario = "Missing"
            Pattern = "complete ZIP payload|missing ZIP entry"
        },
        [pscustomobject] @{
            Scenario = "DuplicateCase"
            Pattern = "duplicate or case-colliding"
        },
        [pscustomobject] @{
            Scenario = "Traversal"
            Pattern = "non-canonical segment"
        },
        [pscustomobject] @{
            Scenario = "Absolute"
            Pattern = "canonical relative slash path"
        },
        [pscustomobject] @{
            Scenario = "Backslash"
            Pattern = "canonical relative slash path|Windows-invalid character"
        },
        [pscustomobject] @{
            Scenario = "AlternateDataStream"
            Pattern = "Windows-invalid character"
        },
        [pscustomobject] @{
            Scenario = "ReservedDevice"
            Pattern = "reserved Windows device name"
        },
        [pscustomobject] @{
            Scenario = "TrailingDot"
            Pattern = "Windows-ambiguous segment"
        },
        [pscustomobject] @{
            Scenario = "Directory"
            Pattern = "non-canonical segment|directory entries are not permitted"
        },
        [pscustomobject] @{
            Scenario = "FileDirectoryCollision"
            Pattern = "file/directory path collision"
        },
        [pscustomobject] @{
            Scenario = "Link"
            Pattern = "link or non-regular|link or reparse"
        }
    )
    foreach ($failure in $failureScenarios) {
        $archive = New-TestArchive `
            -Path (Join-Path $tempRoot "$($failure.Scenario).zip") `
            -Baseline $baseline `
            -Scenario $failure.Scenario
        $arguments = Get-HarnessArguments `
            -Archive $archive `
            -Baseline $baseline `
            -Mode Validate
        Invoke-ExpectedFailure `
            -Harness $harness `
            -Arguments $arguments `
            -MessagePattern $failure.Pattern
    }

    $nonEmptyDestination = Join-Path $tempRoot "non-empty destination"
    New-Item -ItemType Directory -Path $nonEmptyDestination | Out-Null
    Set-Content -LiteralPath (Join-Path $nonEmptyDestination "sentinel.txt") -Value "keep"
    $nonEmptyArguments = Get-HarnessArguments `
        -Archive $valid `
        -Baseline $baseline `
        -Mode Extract `
        -DestinationPath $nonEmptyDestination
    Invoke-ExpectedFailure `
        -Harness $harness `
        -Arguments $nonEmptyArguments `
        -MessagePattern "destination must be empty"
    Assert-True (
        (Get-Content -LiteralPath (Join-Path $nonEmptyDestination "sentinel.txt") -Raw).Trim() -ceq
        "keep"
    ) "non-empty destination failure changed its existing file"

    $missingDestination = Join-Path $tempRoot "missing destination"
    $missingDestinationArguments = Get-HarnessArguments `
        -Archive $valid `
        -Baseline $baseline `
        -Mode Extract `
        -DestinationPath $missingDestination
    Invoke-ExpectedFailure `
        -Harness $harness `
        -Arguments $missingDestinationArguments `
        -MessagePattern "destination does not exist"
    Assert-True (
        -not (Test-Path -LiteralPath $missingDestination)
    ) "missing destination failure created the destination"

    if ([System.Environment]::OSVersion.Platform -eq [System.PlatformID]::Win32NT) {
        $junctionTarget = Join-Path $tempRoot "junction target"
        $junctionPath = Join-Path $tempRoot "junction destination"
        New-Item -ItemType Directory -Path $junctionTarget | Out-Null
        New-Item -ItemType Junction -Path $junctionPath -Target $junctionTarget | Out-Null
        $junctionArguments = Get-HarnessArguments `
            -Archive $valid `
            -Baseline $baseline `
            -Mode Extract `
            -DestinationPath $junctionPath
        Invoke-ExpectedFailure `
            -Harness $harness `
            -Arguments $junctionArguments `
            -MessagePattern "reparse point"
        Assert-True (
            @(Get-ChildItem -LiteralPath $junctionTarget -Force).Count -eq 0
        ) "reparse destination failure wrote through the junction"

        $ancestorJunctionTarget = Join-Path $tempRoot "ancestor junction target"
        $ancestorJunctionPath = Join-Path $tempRoot "ancestor junction"
        New-Item -ItemType Directory -Path $ancestorJunctionTarget | Out-Null
        New-Item -ItemType Junction `
            -Path $ancestorJunctionPath `
            -Target $ancestorJunctionTarget | Out-Null
        $nestedJunctionDestination = Join-Path $ancestorJunctionPath "nested destination"
        New-Item -ItemType Directory -Path $nestedJunctionDestination | Out-Null
        $nestedJunctionArguments = Get-HarnessArguments `
            -Archive $valid `
            -Baseline $baseline `
            -Mode Extract `
            -DestinationPath $nestedJunctionDestination
        Invoke-ExpectedFailure `
            -Harness $harness `
            -Arguments $nestedJunctionArguments `
            -MessagePattern "reparse point"
        Assert-True (
            @(Get-ChildItem -LiteralPath $nestedJunctionDestination -Force).Count -eq 0
        ) "reparse ancestor failure wrote through the junction"
    }

    Write-Output "windows qualification handoff tests passed on PowerShell $($PSVersionTable.PSVersion)"
} finally {
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Force -Recurse -ErrorAction SilentlyContinue
    }
}
