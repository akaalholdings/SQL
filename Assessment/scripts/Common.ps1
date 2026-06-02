Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-RepoRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

function Resolve-PathFromRepo {
    param(
        [Parameter(Mandatory = $true)][string]$RelativeOrAbsolutePath
    )

    if ([System.IO.Path]::IsPathRooted($RelativeOrAbsolutePath)) {
        return (Resolve-Path $RelativeOrAbsolutePath).Path
    }

    $candidate = Join-Path (Get-RepoRoot) $RelativeOrAbsolutePath
    return (Resolve-Path $candidate).Path
}

function Ensure-Directory {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function To-Bool {
    param([object]$Value)
    if ($null -eq $Value) { return $false }

    $text = ($Value.ToString()).Trim().ToLowerInvariant()
    return @('1', 'true', 'yes', 'y') -contains $text
}

function To-Int {
    param([object]$Value, [int]$Default = 0)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace($Value.ToString())) { return $Default }

    $result = 0
    if ([int]::TryParse($Value.ToString(), [ref]$result)) { return $result }

    return $Default
}

function To-Double {
    param([object]$Value, [double]$Default = 0)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace($Value.ToString())) { return $Default }

    $result = 0.0
    if ([double]::TryParse($Value.ToString(), [ref]$result)) { return $result }

    return $Default
}

function Get-Text {
    param([object]$Value)
    if ($null -eq $Value) { return '' }
    return $Value.ToString().Trim()
}

function Is-EmptyText {
    param([object]$Value)
    return [string]::IsNullOrWhiteSpace((Get-Text $Value))
}

function Read-JsonFile {
    param([Parameter(Mandatory = $true)][string]$Path)
    $content = Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    return $content | ConvertFrom-Json
}

function Export-CanonicalCsv {
    param(
        [Parameter(Mandatory = $true)]$Rows,
        [Parameter(Mandatory = $true)][string]$OutputPath
    )

    $dir = Split-Path -Parent $OutputPath
    Ensure-Directory -Path $dir
    $Rows | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8
}
