param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("Validate", "Extract")]
    [string] $Mode,

    [Parameter(Mandatory = $true)]
    [string] $ArchivePath,

    [string] $DestinationPath,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{64}$")]
    [string] $ExpectedArchiveSha256,

    [Parameter(Mandatory = $true)]
    [long] $ExpectedArchiveSize,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{64}$")]
    [string] $ExpectedManifestSha256,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{64}$")]
    [string] $ExpectedPayloadManifestSha256,

    [Parameter(Mandatory = $true)]
    [string] $ExpectedKind,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{40}$")]
    [string] $ExpectedReleaseCommit,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{40}$")]
    [string] $ExpectedReleaseTree,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{40}$")]
    [string] $ExpectedPluginTree,

    [Parameter(Mandatory = $true)]
    [string] $ExpectedRuntimeVersion
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

Add-Type -AssemblyName System.IO.Compression | Out-Null

function ConvertTo-LowerHex {
    param([Parameter(Mandatory = $true)] [byte[]] $Bytes)

    return ([System.BitConverter]::ToString($Bytes)).Replace("-", "").ToLowerInvariant()
}

function Get-StreamSha256 {
    param([Parameter(Mandatory = $true)] [System.IO.Stream] $Stream)

    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ConvertTo-LowerHex -Bytes $algorithm.ComputeHash($Stream)
    } finally {
        $algorithm.Dispose()
    }
}

function Get-BytesSha256 {
    param([Parameter(Mandatory = $true)] [byte[]] $Bytes)

    $stream = New-Object System.IO.MemoryStream(, $Bytes)
    try {
        return Get-StreamSha256 -Stream $stream
    } finally {
        $stream.Dispose()
    }
}

function Get-PathSha256 {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $stream = [System.IO.File]::Open(
        $Path,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )
    try {
        return Get-StreamSha256 -Stream $stream
    } finally {
        $stream.Dispose()
    }
}

function Read-ZipEntryBytes {
    param(
        [Parameter(Mandatory = $true)]
        [System.IO.Compression.ZipArchiveEntry] $Entry,

        [Parameter(Mandatory = $true)]
        [long] $MaximumLength
    )

    if ($Entry.Length -gt $MaximumLength) {
        throw "ZIP entry $($Entry.FullName) exceeds the bounded metadata size"
    }
    $source = $Entry.Open()
    $memory = New-Object System.IO.MemoryStream
    try {
        $source.CopyTo($memory)
        if ($memory.Length -ne $Entry.Length) {
            throw "ZIP entry $($Entry.FullName) returned an unexpected byte count"
        }
        return , $memory.ToArray()
    } finally {
        $memory.Dispose()
        $source.Dispose()
    }
}

function Get-RequiredProperty {
    param(
        [Parameter(Mandatory = $true)] [object] $InputObject,
        [Parameter(Mandatory = $true)] [string] $Name
    )

    $property = $InputObject.PSObject.Properties[$Name]
    if ($null -eq $property) {
        throw "handoff manifest is missing required property $Name"
    }
    return $property.Value
}

function Assert-CanonicalWindowsArchivePath {
    param([Parameter(Mandatory = $true)] [string] $Path)

    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw "ZIP entry path is empty"
    }
    if ($Path.StartsWith("/") -or $Path.Contains("\")) {
        throw "ZIP entry path is not a canonical relative slash path: $Path"
    }
    if ($Path -match '[<>:"\\|?*\x00-\x1F]') {
        throw "ZIP entry path contains a Windows-invalid character: $Path"
    }

    $segments = $Path.Split([char] "/")
    foreach ($segment in $segments) {
        if ([string]::IsNullOrEmpty($segment) -or $segment -eq "." -or $segment -eq "..") {
            throw "ZIP entry path contains a non-canonical segment: $Path"
        }
        if ($segment.EndsWith(".") -or $segment.EndsWith(" ")) {
            throw "ZIP entry path contains a Windows-ambiguous segment: $Path"
        }
        if ($segment -match '^(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)') {
            throw "ZIP entry path contains a reserved Windows device name: $Path"
        }
    }
}

function Assert-RegularZipEntry {
    param([Parameter(Mandatory = $true)] [System.IO.Compression.ZipArchiveEntry] $Entry)

    if ([string]::IsNullOrEmpty($Entry.Name) -or
        $Entry.FullName.EndsWith("/") -or
        $Entry.FullName.EndsWith("\")) {
        throw "ZIP directory entries are not permitted: $($Entry.FullName)"
    }

    $attributes = ([int64] $Entry.ExternalAttributes) -band 0xFFFFFFFFL
    $dosAttributes = $attributes -band 0xFFFFL
    $unixType = ($attributes -shr 16) -band 0xF000L
    if (($dosAttributes -band 0x400L) -ne 0) {
        throw "ZIP link or reparse entries are not permitted: $($Entry.FullName)"
    }
    if ($unixType -ne 0 -and $unixType -ne 0x8000L) {
        throw "ZIP link or non-regular entries are not permitted: $($Entry.FullName)"
    }
}

function Convert-PayloadManifest {
    param([Parameter(Mandatory = $true)] [byte[]] $Bytes)

    $encoding = New-Object System.Text.UTF8Encoding($false, $true)
    $text = $encoding.GetString($Bytes)
    $lines = @($text -split "\r?\n")
    if ($lines.Count -gt 0 -and $lines[$lines.Count - 1] -eq "") {
        if ($lines.Count -eq 1) {
            $lines = @()
        } else {
            $lines = @($lines[0..($lines.Count - 2)])
        }
    }
    if ($lines.Count -eq 0) {
        throw "payload manifest is empty"
    }

    $records = @()
    $paths = New-Object 'System.Collections.Generic.HashSet[string]' (
        [System.StringComparer]::OrdinalIgnoreCase
    )
    $previousPath = $null
    foreach ($line in $lines) {
        if ($line -cnotmatch '^([0-9a-f]{64})  (.+)$') {
            throw "payload manifest contains a malformed line"
        }
        $hash = $Matches[1]
        $path = $Matches[2]
        Assert-CanonicalWindowsArchivePath -Path $path
        if ($path -ceq "handoff-manifest.json" -or $path -ceq "payload.sha256") {
            throw "payload manifest must not cover its trust metadata"
        }
        if (-not $paths.Add($path)) {
            throw "payload manifest contains a duplicate or case-colliding path: $path"
        }
        if ($null -ne $previousPath -and
            [System.String]::CompareOrdinal($previousPath, $path) -ge 0) {
            throw "payload manifest paths are not strictly ordinal-sorted"
        }
        $previousPath = $path
        $records += [pscustomobject] @{
            Path = $path
            Sha256 = $hash
        }
    }
    return $records
}

function Assert-ManifestIdentity {
    param(
        [Parameter(Mandatory = $true)] [object] $Manifest,
        [Parameter(Mandatory = $true)] [string] $PayloadManifestSha256
    )

    if ((Get-RequiredProperty -InputObject $Manifest -Name "schemaVersion") -ne 1) {
        throw "handoff manifest schemaVersion is not 1"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "kind") -cne $ExpectedKind) {
        throw "handoff manifest kind does not match the frozen anchor"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "os") -cne "windows") {
        throw "handoff manifest OS is not windows"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "arch") -cne "amd64") {
        throw "handoff manifest architecture is not amd64"
    }
    $secretFree = Get-RequiredProperty -InputObject $Manifest -Name "secretFree"
    if (($secretFree -isnot [bool]) -or -not $secretFree) {
        throw "handoff manifest is not explicitly secret-free"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "payloadManifest") -cne
        "payload.sha256") {
        throw "handoff manifest names an unexpected payload manifest"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "payloadManifestSha256") -cne
        $PayloadManifestSha256) {
        throw "handoff manifest payload SHA-256 does not match the validated entry"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "releaseCommit") -cne
        $ExpectedReleaseCommit.ToLowerInvariant()) {
        throw "handoff manifest release commit does not match the frozen anchor"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "releaseTree") -cne
        $ExpectedReleaseTree.ToLowerInvariant()) {
        throw "handoff manifest release tree does not match the frozen anchor"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "pluginTree") -cne
        $ExpectedPluginTree.ToLowerInvariant()) {
        throw "handoff manifest plugin tree does not match the frozen anchor"
    }
    if ((Get-RequiredProperty -InputObject $Manifest -Name "runtimeVersion") -cne
        $ExpectedRuntimeVersion) {
        throw "handoff manifest runtime version does not match the frozen anchor"
    }
}

function Assert-ContainedPath {
    param(
        [Parameter(Mandatory = $true)] [string] $Candidate,
        [Parameter(Mandatory = $true)] [string] $Root,
        [Parameter(Mandatory = $true)] [string] $RootPrefix
    )

    if (-not [System.String]::Equals(
            $Candidate,
            $Root,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -and
        -not $Candidate.StartsWith(
            $RootPrefix,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "ZIP entry destination escapes the extraction root"
    }
}

function Assert-LocalWindowsDirectory {
    param([Parameter(Mandatory = $true)] [string] $Path)

    if ([System.Environment]::OSVersion.Platform -ne [System.PlatformID]::Win32NT) {
        return
    }
    $root = [System.IO.Path]::GetPathRoot($Path)
    if ([string]::IsNullOrEmpty($root) -or
        $root.StartsWith("\\", [System.StringComparison]::Ordinal)) {
        throw "extraction destination must use a local Windows volume"
    }
    $drive = New-Object System.IO.DriveInfo($root)
    switch ($drive.DriveType) {
        ([System.IO.DriveType]::Fixed) { return }
        ([System.IO.DriveType]::Removable) { return }
        ([System.IO.DriveType]::Ram) { return }
        default { throw "extraction destination must use a local Windows volume" }
    }
}

function Assert-NonReparseItem {
    param(
        [Parameter(Mandatory = $true)] [System.IO.FileSystemInfo] $Item,
        [Parameter(Mandatory = $true)] [string] $Description
    )

    if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "$Description must not be a reparse point"
    }
}

function Assert-NonReparseDirectoryAncestry {
    param([Parameter(Mandatory = $true)] [System.IO.DirectoryInfo] $Directory)

    $current = $Directory
    while ($null -ne $current) {
        Assert-NonReparseItem `
            -Item $current `
            -Description "extraction destination or ancestor"
        $current = $current.Parent
    }
}

$archiveItem = Get-Item -LiteralPath $ArchivePath -Force
if ($archiveItem.PSIsContainer) {
    throw "handoff archive path is not a regular file"
}
Assert-NonReparseItem -Item $archiveItem -Description "handoff archive"
if ($archiveItem.Length -ne $ExpectedArchiveSize) {
    throw "handoff archive size does not match the frozen anchor"
}
$archiveSha256 = Get-PathSha256 -Path $archiveItem.FullName
if ($archiveSha256 -cne $ExpectedArchiveSha256.ToLowerInvariant()) {
    throw "handoff archive SHA-256 does not match the frozen anchor"
}

if ($Mode -eq "Validate" -and -not [string]::IsNullOrEmpty($DestinationPath)) {
    throw "Validate mode does not accept an extraction destination"
}
if ($Mode -eq "Extract" -and [string]::IsNullOrWhiteSpace($DestinationPath)) {
    throw "Extract mode requires an extraction destination"
}

$destinationRoot = $null
$destinationPrefix = $null
if ($Mode -eq "Extract") {
    if (-not (Test-Path -LiteralPath $DestinationPath)) {
        throw "extraction destination does not exist"
    }
    $destinationItem = Get-Item -LiteralPath $DestinationPath -Force
    if (-not $destinationItem.PSIsContainer) {
        throw "extraction destination is not a directory"
    }
    Assert-NonReparseDirectoryAncestry -Directory $destinationItem
    if (@(Get-ChildItem -LiteralPath $destinationItem.FullName -Force).Count -ne 0) {
        throw "extraction destination must be empty"
    }
    $destinationRoot = [System.IO.Path]::GetFullPath($destinationItem.FullName)
    Assert-LocalWindowsDirectory -Path $destinationRoot
    $destinationPrefix = $destinationRoot.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    ) + [System.IO.Path]::DirectorySeparatorChar
}

$fileStream = [System.IO.File]::Open(
    $archiveItem.FullName,
    [System.IO.FileMode]::Open,
    [System.IO.FileAccess]::Read,
    [System.IO.FileShare]::Read
)
$zip = $null
try {
    $zip = New-Object System.IO.Compression.ZipArchive(
        $fileStream,
        [System.IO.Compression.ZipArchiveMode]::Read,
        $false
    )
    if ($zip.Entries.Count -lt 3) {
        throw "handoff archive does not contain trust metadata and payload"
    }

    $entryPaths = New-Object 'System.Collections.Generic.HashSet[string]' (
        [System.StringComparer]::OrdinalIgnoreCase
    )
    $entryByPath = New-Object System.Collections.Hashtable (
        [System.StringComparer]::OrdinalIgnoreCase
    )
    $entryHashByPath = New-Object System.Collections.Hashtable (
        [System.StringComparer]::OrdinalIgnoreCase
    )
    foreach ($entry in $zip.Entries) {
        Assert-CanonicalWindowsArchivePath -Path $entry.FullName
        Assert-RegularZipEntry -Entry $entry
        if (-not $entryPaths.Add($entry.FullName)) {
            throw "ZIP contains a duplicate or case-colliding path: $($entry.FullName)"
        }
        $source = $entry.Open()
        try {
            $entryHash = Get-StreamSha256 -Stream $source
        } finally {
            $source.Dispose()
        }
        $entryByPath[$entry.FullName] = $entry
        $entryHashByPath[$entry.FullName] = $entryHash
    }
    foreach ($entryPath in $entryByPath.Keys) {
        $segments = $entryPath.Split([char] "/")
        if ($segments.Count -gt 1) {
            for ($index = 1; $index -lt $segments.Count; $index++) {
                $ancestorPath = [System.String]::Join(
                    "/",
                    [string[]] $segments[0..($index - 1)]
                )
                if ($entryByPath.ContainsKey($ancestorPath)) {
                    throw "ZIP contains a file/directory path collision: $ancestorPath"
                }
            }
        }
    }

    if (-not $entryByPath.ContainsKey("handoff-manifest.json")) {
        throw "ZIP is missing handoff-manifest.json"
    }
    if (-not $entryByPath.ContainsKey("payload.sha256")) {
        throw "ZIP is missing payload.sha256"
    }
    $manifestHash = $entryHashByPath["handoff-manifest.json"]
    $payloadManifestHash = $entryHashByPath["payload.sha256"]
    if ($manifestHash -cne $ExpectedManifestSha256.ToLowerInvariant()) {
        throw "handoff-manifest.json SHA-256 does not match the frozen anchor"
    }
    if ($payloadManifestHash -cne $ExpectedPayloadManifestSha256.ToLowerInvariant()) {
        throw "payload.sha256 SHA-256 does not match the frozen anchor"
    }

    $manifestBytes = Read-ZipEntryBytes `
        -Entry $entryByPath["handoff-manifest.json"] `
        -MaximumLength 1048576
    $payloadManifestBytes = Read-ZipEntryBytes `
        -Entry $entryByPath["payload.sha256"] `
        -MaximumLength 16777216
    $utf8 = New-Object System.Text.UTF8Encoding($false, $true)
    $manifestText = $utf8.GetString($manifestBytes)
    try {
        $manifest = $manifestText | ConvertFrom-Json
    } catch {
        throw "handoff manifest is not valid UTF-8 JSON: $($_.Exception.Message)"
    }
    Assert-ManifestIdentity `
        -Manifest $manifest `
        -PayloadManifestSha256 $payloadManifestHash

    $payloadRecords = @(Convert-PayloadManifest -Bytes $payloadManifestBytes)
    if ($payloadRecords.Count -ne ($zip.Entries.Count - 2)) {
        throw "payload manifest does not cover the complete ZIP payload"
    }
    $payloadPaths = New-Object 'System.Collections.Generic.HashSet[string]' (
        [System.StringComparer]::OrdinalIgnoreCase
    )
    foreach ($record in $payloadRecords) {
        if (-not $entryByPath.ContainsKey($record.Path)) {
            throw "payload manifest names a missing ZIP entry: $($record.Path)"
        }
        if ($entryHashByPath[$record.Path] -cne $record.Sha256) {
            throw "payload entry SHA-256 mismatch: $($record.Path)"
        }
        $null = $payloadPaths.Add($record.Path)
    }
    foreach ($entryPath in $entryByPath.Keys) {
        if ($entryPath -cne "handoff-manifest.json" -and
            $entryPath -cne "payload.sha256" -and
            -not $payloadPaths.Contains($entryPath)) {
            throw "ZIP contains an unmanifested payload entry: $entryPath"
        }
    }

    if ($Mode -eq "Extract") {
        foreach ($entryPath in $entryByPath.Keys) {
            $relativePath = $entryPath.Replace(
                [char] "/",
                [System.IO.Path]::DirectorySeparatorChar
            )
            $targetPath = [System.IO.Path]::GetFullPath(
                [System.IO.Path]::Combine($destinationRoot, $relativePath)
            )
            Assert-ContainedPath `
                -Candidate $targetPath `
                -Root $destinationRoot `
                -RootPrefix $destinationPrefix
            $parentPath = [System.IO.Path]::GetDirectoryName($targetPath)
            $parent = [System.IO.Directory]::CreateDirectory($parentPath)
            Assert-NonReparseItem -Item $parent -Description "extraction parent"

            $source = $entryByPath[$entryPath].Open()
            $target = [System.IO.File]::Open(
                $targetPath,
                [System.IO.FileMode]::CreateNew,
                [System.IO.FileAccess]::Write,
                [System.IO.FileShare]::None
            )
            try {
                $source.CopyTo($target)
            } finally {
                $target.Dispose()
                $source.Dispose()
            }
            $targetItem = Get-Item -LiteralPath $targetPath -Force
            Assert-NonReparseItem -Item $targetItem -Description "extracted file"
            if ($targetItem.Length -ne $entryByPath[$entryPath].Length) {
                throw "extracted file size mismatch: $entryPath"
            }
            if ((Get-PathSha256 -Path $targetItem.FullName) -cne
                $entryHashByPath[$entryPath]) {
                throw "extracted file SHA-256 mismatch: $entryPath"
            }
        }

        $extractedPaths = New-Object 'System.Collections.Generic.HashSet[string]' (
            [System.StringComparer]::OrdinalIgnoreCase
        )
        foreach ($item in @(Get-ChildItem -LiteralPath $destinationRoot -Force -Recurse)) {
            Assert-NonReparseItem -Item $item -Description "extracted tree item"
            if (-not $item.PSIsContainer) {
                $relativePath = $item.FullName.Substring($destinationPrefix.Length).Replace(
                    [System.IO.Path]::DirectorySeparatorChar,
                    [char] "/"
                )
                if (-not $entryByPath.ContainsKey($relativePath)) {
                    throw "extracted tree contains an unexpected file: $relativePath"
                }
                if (-not $extractedPaths.Add($relativePath)) {
                    throw "extracted tree contains a duplicate or case-colliding file"
                }
            }
        }
        if ($extractedPaths.Count -ne $entryByPath.Count) {
            throw "extracted tree does not contain the complete validated entry set"
        }
    }

    [ordered] @{
        schemaVersion = 1
        mode = $Mode
        archiveSha256 = $archiveSha256
        archiveSize = $archiveItem.Length
        manifestSha256 = $manifestHash
        payloadManifestSha256 = $payloadManifestHash
        entryCount = $zip.Entries.Count
        payloadEntryCount = $payloadRecords.Count
        kind = $ExpectedKind
        releaseCommit = $ExpectedReleaseCommit.ToLowerInvariant()
        releaseTree = $ExpectedReleaseTree.ToLowerInvariant()
        pluginTree = $ExpectedPluginTree.ToLowerInvariant()
        runtimeVersion = $ExpectedRuntimeVersion
    } | ConvertTo-Json -Compress
} finally {
    if ($null -ne $zip) {
        $zip.Dispose()
    } else {
        $fileStream.Dispose()
    }
}
