[CmdletBinding(DefaultParameterSetName = 'SingleInstance')]
param(
    [Parameter(Mandatory = $true, ParameterSetName = 'SingleInstance')]
    [string]$SqlInstance,

    [Parameter(Mandatory = $true, ParameterSetName = 'InstanceList')]
    [string]$InstanceListCsv,

    [string]$OutputRoot = './outputs',

    [string]$DatabaseName = '',

    [bool]$UseIntegratedSecurity = $true,

    [string]$SqlUsername = '',

    [securestring]$SqlPassword,

    [int]$ConnectionTimeoutSeconds = 15,

    [bool]$Encrypt = $true,

    [bool]$TrustServerCertificate = $true,

    [switch]$EnableWorkloadSampling,

    [int]$SampleIntervalSeconds = 60,

    [int]$SampleDurationSeconds = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'SqlServerAzureRawInventory.Common.ps1')

$invokeParams = @{
    OutputRoot                 = $OutputRoot
    DatabaseName               = $DatabaseName
    UseIntegratedSecurity      = $UseIntegratedSecurity
    SqlUsername                = $SqlUsername
    SqlPassword                = $SqlPassword
    ConnectionTimeoutSeconds   = $ConnectionTimeoutSeconds
    Encrypt                    = $Encrypt
    TrustServerCertificate     = $TrustServerCertificate
    EnableWorkloadSampling     = $EnableWorkloadSampling
    SampleIntervalSeconds      = $SampleIntervalSeconds
    SampleDurationSeconds      = $SampleDurationSeconds
}

if ($PSCmdlet.ParameterSetName -eq 'SingleInstance') {
    $invokeParams.SqlInstance = $SqlInstance
}
else {
    $invokeParams.InstanceListCsv = $InstanceListCsv
}

Invoke-SqlServerAzureRawInventory @invokeParams
