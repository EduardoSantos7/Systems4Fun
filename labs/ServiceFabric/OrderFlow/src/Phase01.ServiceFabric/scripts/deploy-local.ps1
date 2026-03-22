#Requires -Version 7.0

param(
    [int]$InstanceCount = -1,
    [string]$ApplicationVersion = "1.0.0"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Config ─────────────────────────────────────────────────────────────────
$ClusterEndpoint = "http://localhost:19080"
$ApiVersion      = "6.0"
$AppTypeName     = "OrderFlowAppType"
$AppInstanceUri  = "fabric:/OrderFlowApp"
$ImageStorePath  = "OrderFlowApp"
$ImageStoreRoot  = "C:\SfDevCluster\Data\ImageStoreShare"

# ── Paths ──────────────────────────────────────────────────────────────────
$ScriptDir      = $PSScriptRoot
$Phase01Dir     = Split-Path $ScriptDir -Parent
$ServiceProject = Join-Path $Phase01Dir "OrderFlow.OrderApi"
$AppManifestDir = Join-Path $Phase01Dir "OrderFlowApp\ApplicationPackageRoot"
$PublishOutput  = Join-Path $Phase01Dir "publish\OrderFlow.OrderApi"
$AppPackage     = Join-Path $Phase01Dir "pkg\OrderFlowApp"
$CodePackageDst = Join-Path $AppPackage "OrderApiPkg\Code"

# ── Helper: call the SF REST API ───────────────────────────────────────────
function Invoke-SF {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body          = $null,
        [hashtable]$ExtraQuery = @{}
    )

    $qs = "api-version=$ApiVersion"
    foreach ($kv in $ExtraQuery.GetEnumerator()) {
        $qs += "&$($kv.Key)=$($kv.Value)"
    }

    $uri    = "$ClusterEndpoint$Path`?$qs"
    $params = @{ Method = $Method; Uri = $uri; ContentType = "application/json" }
    if ($null -ne $Body) { $params.Body = ($Body | ConvertTo-Json -Depth 10) }

    try {
        return Invoke-RestMethod @params
    } catch {
        $status = $_.Exception.Response.StatusCode.value__
        $msg    = $_.ErrorDetails.Message
        throw "SF API $Method $Path -> HTTP $status : $msg"
    }
}

# ── Helper: check if application instance exists ───────────────────────────
function Get-SFApplication {
    try {
        $result = Invoke-SF -Method GET -Path "/Applications/OrderFlowApp"
        # Name property confirms this is a real application object, not an empty response
        if ($result -and $result.Name -eq $AppInstanceUri) {
            return $result
        }
        return $null
    } catch {
        return $null
    }
}

# ── Helper: check if application type version exists ──────────────────────
# Returns ALL registered versions of this app type (not just the target version)
function Get-SFApplicationType {
    try {
        $result = Invoke-SF -Method GET -Path "/ApplicationTypes"
        if (-not $result -or -not $result.Items) { return $null }
        $matches = $result.Items | Where-Object { $_.Name -eq $AppTypeName }
        if (-not $matches) { return $null }
        return $matches
    } catch {
        return $null
    }
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1 - Build & publish
# ═══════════════════════════════════════════════════════════════════════════
Write-Host "`n[1/6] Publishing OrderFlow.OrderApi..." -ForegroundColor Cyan

dotnet publish $ServiceProject `
    --configuration Release `
    --output $PublishOutput `
    --self-contained false

if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2 - Assemble application package
# ═══════════════════════════════════════════════════════════════════════════
Write-Host "`n[2/6] Assembling application package..." -ForegroundColor Cyan

if (Test-Path $AppPackage) { Remove-Item $AppPackage -Recurse -Force }

Copy-Item $AppManifestDir $AppPackage -Recurse

Copy-Item (Join-Path $ServiceProject "PackageRoot\ServiceManifest.xml") `
          (Join-Path $AppPackage "OrderApiPkg\ServiceManifest.xml") -Force

$ConfigDst = Join-Path $AppPackage "OrderApiPkg\Config"
if (Test-Path $ConfigDst) { Remove-Item $ConfigDst -Recurse -Force }
Copy-Item (Join-Path $ServiceProject "PackageRoot\Config") $ConfigDst -Recurse

New-Item -ItemType Directory -Path $CodePackageDst -Force | Out-Null
Copy-Item "$PublishOutput\*" $CodePackageDst -Recurse -Force

Write-Host "    Package assembled at: $AppPackage" -ForegroundColor Gray

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3 - Verify cluster is reachable
# ═══════════════════════════════════════════════════════════════════════════
Write-Host "`n[3/6] Connecting to cluster..." -ForegroundColor Cyan

if (-not (Test-Path $ImageStoreRoot)) {
    throw "Image store not found at: $ImageStoreRoot`nIs the local SF cluster running?"
}

$health = Invoke-SF -Method GET -Path "/`$/GetClusterHealth"
Write-Host "    Cluster health : $($health.AggregatedHealthState)" -ForegroundColor Gray
Write-Host "    Image store    : $ImageStoreRoot" -ForegroundColor Gray

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4 - Clean up any existing deployment
# ═══════════════════════════════════════════════════════════════════════════
Write-Host "`n[4/6] Cleaning up existing deployment..." -ForegroundColor Cyan

if ($null -ne (Get-SFApplication)) {
    Write-Host "    Deleting application instance (force)..." -ForegroundColor Yellow
    try {
        Invoke-SF -Method POST `
            -Path "/Applications/OrderFlowApp/`$/Delete" `
            -ExtraQuery @{ ForceRemove = "true" } | Out-Null
    } catch {
        Write-Host "    Delete call failed (may already be gone): $_" -ForegroundColor DarkGray
    }

    Write-Host "    Waiting for deletion..." -ForegroundColor Yellow
    $waited = 0
    while ($null -ne (Get-SFApplication) -and $waited -lt 60) {
        Start-Sleep -Seconds 3
        $waited += 3
        Write-Host "    Still deleting... ($waited s)" -ForegroundColor DarkGray
    }

    if ($null -ne (Get-SFApplication)) {
        throw "Application deletion timed out after 60s. Run DevClusterSetup.ps1 -ResetCluster to recover."
    }
    Write-Host "    Application deleted." -ForegroundColor Gray
    Start-Sleep -Seconds 2
} else {
    Write-Host "    No existing application instance." -ForegroundColor DarkGray
}

$existingTypes = Get-SFApplicationType
if ($null -ne $existingTypes) {
    foreach ($type in @($existingTypes)) {
        Write-Host "    Unprovisioning $($type.Name) v$($type.Version)..." -ForegroundColor Yellow
        try {
            Invoke-SF -Method POST `
                -Path "/ApplicationTypes/$($type.Name)/`$/Unprovision" `
                -Body @{ ApplicationTypeVersion = $type.Version } | Out-Null
        } catch {
            Write-Host "    Unprovision call failed: $_" -ForegroundColor DarkGray
        }
    }

    # Wait until all versions are gone
    $waited = 0
    while ($null -ne (Get-SFApplicationType) -and $waited -lt 30) {
        Start-Sleep -Seconds 2
        $waited += 2
        Write-Host "    Still unprovisioning... ($waited s)" -ForegroundColor DarkGray
    }
    Write-Host "    All versions unprovisioned." -ForegroundColor Gray
} else {
    Write-Host "    No existing application type." -ForegroundColor DarkGray
}

$ImageStoreDest = Join-Path $ImageStoreRoot $ImageStorePath
if (Test-Path $ImageStoreDest) {
    Remove-Item $ImageStoreDest -Recurse -Force
    Write-Host "    Image store entry deleted." -ForegroundColor DarkGray
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5 - Copy package to image store
# ═══════════════════════════════════════════════════════════════════════════
Write-Host "`n[5/6] Copying package to image store..." -ForegroundColor Cyan

Copy-Item $AppPackage $ImageStoreDest -Recurse -Force

$fileCount = (Get-ChildItem $ImageStoreDest -Recurse -File).Count
Write-Host "    Copied $fileCount files to image store." -ForegroundColor Gray

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6 - Provision and create
# ═══════════════════════════════════════════════════════════════════════════
Write-Host "`n[6/6] Provisioning and creating application..." -ForegroundColor Cyan

Invoke-SF -Method POST -Path "/ApplicationTypes/`$/Provision" `
    -Body @{ ApplicationTypeBuildPath = $ImageStorePath } | Out-Null
Write-Host "    Application type provisioned." -ForegroundColor Gray

# Poll until the type is registered before creating the instance
$waited = 0
while ($null -eq (Get-SFApplicationType) -and $waited -lt 30) {
    Start-Sleep -Seconds 2
    $waited += 2
    Write-Host "    Waiting for type registration... ($waited s)" -ForegroundColor DarkGray
}

if ($null -eq (Get-SFApplicationType)) {
    throw "Application type did not become available after 30s."
}

Invoke-SF -Method POST -Path "/Applications/`$/Create" -Body @{
    Name          = $AppInstanceUri
    TypeName      = $AppTypeName
    TypeVersion   = $ApplicationVersion
    ParameterList = @(
        @{ Key = "OrderApi_InstanceCount"; Value = "$InstanceCount" }
    )
} | Out-Null
Write-Host "    Application instance created." -ForegroundColor Gray

# ═══════════════════════════════════════════════════════════════════════════
Write-Host "`n✓ Deployed successfully!" -ForegroundColor Green
Write-Host "  Application : $AppInstanceUri" -ForegroundColor White
Write-Host "  Version     : $ApplicationVersion" -ForegroundColor White
Write-Host "  Instances   : $InstanceCount  (-1 = one per node)" -ForegroundColor White
Write-Host ""
Write-Host "  Explorer    : http://localhost:19080/Explorer" -ForegroundColor White
Write-Host ""
Write-Host "  Find your dynamic port in Explorer:" -ForegroundColor Gray
Write-Host "    Applications -> OrderFlowAppType -> fabric:/OrderFlowApp" -ForegroundColor Gray
Write-Host "    -> OrderApi -> Partition -> any Instance -> Details" -ForegroundColor Gray
Write-Host ""
Write-Host "  Quick verify (replace PORT with the port from Explorer):" -ForegroundColor Gray
Write-Host "    Invoke-RestMethod http://localhost:PORT/api/inventory" -ForegroundColor Gray
Write-Host "    Invoke-RestMethod http://localhost:PORT/api/orders" -ForegroundColor Gray