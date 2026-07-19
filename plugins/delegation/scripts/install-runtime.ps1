$ErrorActionPreference = "Stop"

if (-not ("DelegationNativeMethods" -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

public static class DelegationNativeMethods {
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern SafeFileHandle CreateFileW(
        string fileName,
        uint desiredAccess,
        uint shareMode,
        IntPtr securityAttributes,
        uint creationDisposition,
        uint flagsAndAttributes,
        IntPtr templateFile
    );

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern uint GetFinalPathNameByHandleW(
        SafeFileHandle file,
        StringBuilder filePath,
        uint filePathSize,
        uint flags
    );

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
    public static extern uint GetDriveTypeW(string rootPathName);
}
'@
}

function Assert-LocalDelegationDirectoryHandle {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $fileReadAttributes = [uint32] 0x80
    $shareAll = [uint32] (0x1 -bor 0x2 -bor 0x4)
    $openExisting = [uint32] 3
    $backupSemantics = [uint32] 0x02000000
    $handle = [DelegationNativeMethods]::CreateFileW(
        $Path,
        $fileReadAttributes,
        $shareAll,
        [IntPtr]::Zero,
        $openExisting,
        $backupSemantics,
        [IntPtr]::Zero
    )
    if ($handle.IsInvalid) {
        $errorCode = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        $handle.Dispose()
        throw "delegation: could not open delegation home path for local-volume validation (Win32 $errorCode): $Path"
    }
    try {
        $capacity = 256
        while ($true) {
            $resolved = New-Object System.Text.StringBuilder($capacity)
            $length = [DelegationNativeMethods]::GetFinalPathNameByHandleW(
                $handle,
                $resolved,
                [uint32] $resolved.Capacity,
                [uint32] 0
            )
            if ($length -eq 0) {
                $errorCode = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
                throw "delegation: could not resolve delegation home path (Win32 $errorCode): $Path"
            }
            if ($length -lt $resolved.Capacity) {
                $finalPath = $resolved.ToString()
                break
            }
            if ($length -gt 32768) {
                throw "delegation: resolved delegation home path exceeds the Windows path limit: $Path"
            }
            $capacity = [int] $length + 1
        }
    } finally {
        $handle.Dispose()
    }

    if ($finalPath.StartsWith("\\?\UNC\", [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "delegation: delegation home must not resolve to a Windows network path: $Path"
    }
    if ($finalPath.Length -lt 7 -or
        -not $finalPath.StartsWith("\\?\", [System.StringComparison]::OrdinalIgnoreCase) -or
        $finalPath[5] -ne ':' -or
        $finalPath[6] -ne [char] 92 -or
        -not ([char]::IsLetter($finalPath[4]))) {
        throw "delegation: delegation home resolved to an unsupported Windows volume path: $Path"
    }
    $resolvedRoot = $finalPath.Substring(4, 3)
    $driveType = [DelegationNativeMethods]::GetDriveTypeW($resolvedRoot)
    switch ($driveType) {
        2 { return }
        3 { return }
        6 { return }
        4 { throw "delegation: delegation home must not resolve to a mapped Windows network drive: $Path" }
        default { throw "delegation: delegation home must resolve to a writable local Windows volume: $Path" }
    }
}

function Assert-LocalDelegationPathAncestor {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $current = $Path
    while (-not [System.IO.Directory]::Exists($current)) {
        $parent = [System.IO.Directory]::GetParent($current)
        if ($null -eq $parent) {
            throw "delegation: delegation home has no existing Windows ancestor: $Path"
        }
        $current = $parent.FullName
    }
    Assert-LocalDelegationDirectoryHandle -Path $current
}

function New-DelegationHomeSecurity {
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
    return $security
}

function Resolve-LocalDelegationHome {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $root = [System.IO.Path]::GetPathRoot($fullPath)
    if ([string]::IsNullOrEmpty($root) -or $root.StartsWith("\\", [System.StringComparison]::Ordinal)) {
        throw "delegation: delegation home must use a local Windows volume: $Path"
    }
    $drive = New-Object System.IO.DriveInfo($root)
    switch ($drive.DriveType) {
        ([System.IO.DriveType]::Fixed) { break }
        ([System.IO.DriveType]::Removable) { break }
        ([System.IO.DriveType]::Ram) { break }
        default { throw "delegation: delegation home must use a writable local Windows volume: $Path" }
    }
    Assert-LocalDelegationPathAncestor -Path $fullPath
    return $fullPath
}

function New-PrivateDelegationHome {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $directory = New-Object System.IO.DirectoryInfo($Path)
    if ($null -eq $directory.Parent) {
        throw "delegation: delegation home must have a parent directory: $Path"
    }
    if (-not $directory.Parent.Exists) {
        [System.IO.Directory]::CreateDirectory($directory.Parent.FullName) | Out-Null
    }
    $security = New-DelegationHomeSecurity
    try {
        $directory.Create($security)
    } catch [System.Management.Automation.MethodException] {
        [System.IO.FileSystemAclExtensions]::Create($directory, $security)
    }
}

function Get-DelegationHomeSecurity {
    param([Parameter(Mandatory = $true)] [System.IO.DirectoryInfo] $Directory)

    $sections = [System.Security.AccessControl.AccessControlSections]::Owner -bor
        [System.Security.AccessControl.AccessControlSections]::Access
    $instanceMethod = $Directory.GetType().GetMethods() | Where-Object {
        $_.Name -eq "GetAccessControl" -and $_.GetParameters().Count -eq 1
    } | Select-Object -First 1
    if ($null -ne $instanceMethod) {
        return $Directory.GetAccessControl($sections)
    }
    return [System.IO.FileSystemAclExtensions]::GetAccessControl($Directory, $sections)
}

function Assert-PrivateDelegationHome {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $item = Get-Item -LiteralPath $Path -Force
    if (-not $item.PSIsContainer -or
        ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "delegation: delegation home must be a non-reparse-point directory: $Path"
    }
    $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
    $security = Get-DelegationHomeSecurity -Directory $item
    $owner = $security.GetOwner([System.Security.Principal.SecurityIdentifier])
    $rules = @($security.GetAccessRules(
        $true,
        $true,
        [System.Security.Principal.SecurityIdentifier]
    ))
    $requiredInheritance = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    $valid = $security.AreAccessRulesProtected -and
        $owner.Value -eq $sid.Value -and
        $rules.Count -eq 1 -and
        -not $rules[0].IsInherited -and
        $rules[0].IdentityReference.Value -eq $sid.Value -and
        $rules[0].AccessControlType -eq [System.Security.AccessControl.AccessControlType]::Allow -and
        ($rules[0].InheritanceFlags -band $requiredInheritance) -eq $requiredInheritance -and
        $rules[0].PropagationFlags -eq [System.Security.AccessControl.PropagationFlags]::None -and
        ($rules[0].FileSystemRights -band [System.Security.AccessControl.FileSystemRights]::FullControl) -eq
            [System.Security.AccessControl.FileSystemRights]::FullControl
    if (-not $valid) {
        throw "delegation: delegation home must be owned by the current user with a protected current-user-only DACL; refusing to modify existing permissions: $Path"
    }
}

function Initialize-DelegationHome {
    param([Parameter(Mandatory = $true)] [string] $Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        New-PrivateDelegationHome -Path $Path
    }
    Assert-PrivateDelegationHome -Path $Path
    Assert-LocalDelegationDirectoryHandle -Path $Path
}

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
$delegationHome = Resolve-LocalDelegationHome -Path $delegationHome
$targetParent = Join-Path $delegationHome "bin\$version"
$target = Join-Path $targetParent "windows-$arch"
$binary = Join-Path $target "delegation.exe"
$locks = Join-Path $delegationHome ".locks"
Initialize-DelegationHome -Path $delegationHome
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
