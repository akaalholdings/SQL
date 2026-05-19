param(
    [Parameter(Mandatory = $true)]
    [string]$SqlInstance,

    [string]$DatabaseName = '',

    [string]$OutputRoot = './outputs/sqlserver-azure-migration-assessment',

    [bool]$UseIntegratedSecurity = $true,

    [string]$SqlUsername = '',

    [securestring]$SqlPassword,

    [int]$ConnectionTimeoutSeconds = 15,

    [bool]$Encrypt = $true,

    [bool]$TrustServerCertificate = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'SqlServerAzureMigrationAssessment.Common.ps1')

$repoRoot = Get-RepoRoot
$resolvedOutputRoot = if ([System.IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot
}
else {
    Join-Path $repoRoot $OutputRoot
}

$connectionString = New-AssessmentConnectionString `
    -SqlInstance $SqlInstance `
    -UseIntegratedSecurity $UseIntegratedSecurity `
    -SqlUsername $SqlUsername `
    -SqlPassword $SqlPassword `
    -ConnectionTimeoutSeconds $ConnectionTimeoutSeconds `
    -Encrypt $Encrypt `
    -TrustServerCertificate $TrustServerCertificate

Write-Host "Collecting server design evidence from $SqlInstance..."
$serverDesignRows = Get-SqlServerDesignEvidence `
    -ConnectionString $connectionString `
    -SqlInstance $SqlInstance

Write-Host "Collecting cluster and HA/DR evidence from $SqlInstance..."
$clusterDesignRows = Get-ClusterDesignEvidence `
    -ConnectionString $connectionString `
    -SqlInstance $SqlInstance

Write-Host "Collecting database design evidence from $SqlInstance..."
$databaseDesignRows = Get-DatabaseDesignEvidence `
    -ConnectionString $connectionString `
    -SqlInstance $SqlInstance `
    -DatabaseName $DatabaseName

Write-Host 'Building feature usage matrix...'
$featureUsageRows = New-FeatureUsageRows `
    -ServerDesignRows $serverDesignRows `
    -ClusterDesignRows $clusterDesignRows `
    -DatabaseDesignRows $databaseDesignRows

Write-Host 'Building Azure target recommendations...'
$recommendationRows = New-AzureMigrationRecommendation `
    -ServerDesignRows $serverDesignRows `
    -ClusterDesignRows $clusterDesignRows `
    -DatabaseDesignRows $databaseDesignRows `
    -FeatureUsageRows $featureUsageRows

$remediationRows = New-RemediationPlanRows -RecommendationRows $recommendationRows

Export-AzureMigrationAssessmentOutputs `
    -OutputRoot $resolvedOutputRoot `
    -ServerDesignRows $serverDesignRows `
    -ClusterDesignRows $clusterDesignRows `
    -DatabaseDesignRows $databaseDesignRows `
    -FeatureUsageRows $featureUsageRows `
    -RecommendationRows $recommendationRows `
    -RemediationRows $remediationRows

Write-Host "Azure SQL migration assessment complete. Output root: $resolvedOutputRoot"
