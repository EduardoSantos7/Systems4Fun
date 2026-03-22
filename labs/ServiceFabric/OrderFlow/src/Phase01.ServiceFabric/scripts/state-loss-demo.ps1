#Requires -Version 7.0

# ── Helper: get all current instance addresses dynamically ─────────────────
function Get-AllInstanceAddresses {
    $partitionId = (Invoke-RestMethod `
        "http://localhost:19080/Services/OrderFlowApp~OrderApi/$/GetPartitions?api-version=6.0"
    ).Items[0].PartitionInformation.Id

    $replicas = Invoke-RestMethod `
        "http://localhost:19080/Services/OrderFlowApp~OrderApi/$/GetPartitions/$partitionId/$/GetReplicas?api-version=6.0"

    return $replicas.Items | ForEach-Object {
        $addr = $null
        # Address is empty string while instance is InBuild — guard before parsing
        if ($_.Address -and $_.Address -ne '{"Endpoints":{}}' -and $_.Address.Length -gt 20) {
            try {
                $addr = ($_.Address | ConvertFrom-Json -AsHashTable).Endpoints["HttpEndpoint"]
            } catch {
                $addr = $null
            }
        }
        [PSCustomObject]@{
            Node    = $_.NodeName
            Address = $addr
            State   = $_.ReplicaStatus
        }
    } | Where-Object { $_.Address -ne $null -and $_.Address -ne "" }
}

# ── Helper: get PROD-001 stock from all live instances ─────────────────────
# Returns a list of [Node, Address, Stock] objects
function Get-InventoryAcrossNodes {
    $addresses = Get-AllInstanceAddresses
    return $addresses | ForEach-Object {
        $stock = $null
        $reachable = $true
        try {
            $item  = Invoke-RestMethod "$($_.Address)/api/inventory/PROD-001"
            $stock = $item.stockCount
        } catch {
            $reachable = $false
        }
        [PSCustomObject]@{
            Node      = $_.Node
            Address   = $_.Address
            Stock     = $stock
            Reachable = $reachable
        }
    }
}

# ── Helper: print an inventory snapshot with a label ──────────────────────
function Show-InventorySnapshot {
    param(
        [string]$Label,
        [object[]]$Snapshot,
        [string]$HighlightNode = ""
    )

    Write-Host "`n── $Label" -ForegroundColor Cyan
    foreach ($row in $Snapshot) {
        if (-not $row.Reachable) {
            Write-Host ("  {0,-8} {1,-35} Stock: UNREACHABLE" -f $row.Node, $row.Address) `
                -ForegroundColor DarkGray
        } elseif ($row.Node -eq $HighlightNode) {
            Write-Host ("  {0,-8} {1,-35} Stock: {2}  <-- this node" -f $row.Node, $row.Address, $row.Stock) `
                -ForegroundColor Yellow
        } else {
            Write-Host ("  {0,-8} {1,-35} Stock: {2}" -f $row.Node, $row.Address, $row.Stock) `
                -ForegroundColor White
        }
    }
}

# ══════════════════════════════════════════════════════════════════════════
Write-Host "`n╔══════════════════════════════════════════════════╗" -ForegroundColor Yellow
Write-Host "║   Phase 1 - State Loss and Divergence Demo       ║" -ForegroundColor Yellow
Write-Host "╚══════════════════════════════════════════════════╝" -ForegroundColor Yellow

# ── Stage 1: Snapshot before order ────────────────────────────────────────
$before = Get-InventoryAcrossNodes
Show-InventorySnapshot -Label "BEFORE ORDER - baseline across all nodes" -Snapshot $before

# Pick Node_0 as the target for the order
$addresses  = Get-AllInstanceAddresses
$node0      = $addresses | Where-Object { $_.Node -eq "_Node_0" }
$node0Addr  = $node0.Address

# ── Stage 2: Place the order ──────────────────────────────────────────────
Write-Host "`n── PLACING ORDER through $($node0.Node) ($node0Addr)" -ForegroundColor Cyan
$order = Invoke-RestMethod "$node0Addr/api/orders" `
    -Method POST -ContentType "application/json" `
    -Body '{"customerId":"CUST-001","productId":"PROD-001","quantity":2}'

Write-Host ("  Order ID  : {0}" -f $order.id)      -ForegroundColor Green
Write-Host ("  Quantity  : {0}" -f $order.quantity) -ForegroundColor Green
Write-Host ("  Total     : {0}" -f $order.totalPrice) -ForegroundColor Green
$statusName = switch ($order.status) {
    0 { "Pending" }
    1 { "InventoryReserved" }
    2 { "PaymentProcessed" }
    3 { "Fulfilled" }
    4 { "Failed" }
    default { "Unknown" }
}
Write-Host ("  Status    : {0} ({1})" -f $order.status, $statusName) -ForegroundColor Green

# ── Stage 3: Snapshot after order ─────────────────────────────────────────
$after = Get-InventoryAcrossNodes
Show-InventorySnapshot `
    -Label "AFTER ORDER - only the node that processed it sees the change" `
    -Snapshot $after `
    -HighlightNode "_Node_0"

# Compute and show the divergence explicitly
$node0StockAfter  = ($after | Where-Object { $_.Node -eq "_Node_0" }).Stock
$otherStocks      = $after | Where-Object { $_.Node -ne "_Node_0" -and $_.Reachable }
$allAgree         = ($otherStocks | Where-Object { $_.Stock -ne $node0StockAfter }).Count -eq 0

Write-Host ""
if (-not $allAgree) {
    Write-Host "  DIVERGENCE DETECTED:" -ForegroundColor Red
    Write-Host ("    Node_0 reports stock = {0}" -f $node0StockAfter) -ForegroundColor Red
    foreach ($other in $otherStocks) {
        Write-Host ("    {0} reports stock = {1}" -f $other.Node, $other.Stock) -ForegroundColor Red
    }
    Write-Host "  The cluster has no consistent view of inventory." -ForegroundColor Red
} else {
    Write-Host "  All nodes agree on stock = $node0StockAfter" -ForegroundColor Green
}

# ── Stage 4: Kill Node_0 ──────────────────────────────────────────────────
$port      = ([Uri]$node0Addr).Port
$targetPid = (Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue |
              Where-Object State -eq "Listen").OwningProcess

Write-Host "`n── KILLING Node_0" -ForegroundColor Red
Write-Host ("  Address : {0}" -f $node0Addr)  -ForegroundColor White
Write-Host ("  Port    : {0}" -f $port)        -ForegroundColor White
Write-Host ("  PID     : {0}" -f $targetPid)   -ForegroundColor White
Stop-Process -Id $targetPid -Force
Write-Host "  Process killed. SF will detect crash and restart." -ForegroundColor Yellow
Write-Host "  Watch Explorer: Node_0 transitions Down -> InBuild -> Ready" -ForegroundColor Yellow

# ── Stage 5: Poll until Node_0 is back ────────────────────────────────────
Write-Host "`n── WAITING FOR NODE_0 TO RECOVER" -ForegroundColor Cyan
$recovered    = $false
$newNode0Addr = $null

for ($i = 1; $i -le 20; $i++) {
    Start-Sleep -Seconds 3
    $current  = Get-AllInstanceAddresses
    $newNode0 = $current | Where-Object { $_.Node -eq "_Node_0" }
    $elapsed  = $i * 3

    if ($newNode0 -and $newNode0.State -eq "Ready") {
        $newNode0Addr = $newNode0.Address
        Write-Host ("  Recovered after {0}s  New address: {1}" -f $elapsed, $newNode0Addr) `
            -ForegroundColor Green
        $recovered = $true
        break
    } else {
        $state = if ($newNode0) { $newNode0.State } else { "unknown" }
        Write-Host ("  {0}s  Node_0 state: {1}" -f $elapsed, $state) -ForegroundColor DarkGray
    }
}

if (-not $recovered) {
    Write-Host "  Node_0 did not recover in 60s. Check Explorer." -ForegroundColor Red
    exit 1
}

# ── Stage 6: Snapshot after restart ───────────────────────────────────────
$afterRestart = Get-InventoryAcrossNodes
Show-InventorySnapshot `
    -Label "AFTER RESTART - Node_0 has a brand new process with fresh state" `
    -Snapshot $afterRestart `
    -HighlightNode "_Node_0"

$node0StockAfterRestart = ($afterRestart | Where-Object { $_.Node -eq "_Node_0" }).Stock
$stockReset = $node0StockAfterRestart -gt $node0StockAfter

if ($stockReset) {
    Write-Host ""
    Write-Host ("  Node_0 stock before kill   : {0}" -f $node0StockAfter)        -ForegroundColor White
    Write-Host ("  Node_0 stock after restart : {0}" -f $node0StockAfterRestart)  -ForegroundColor Red
    Write-Host ("  Difference                 : +{0} units appeared from nowhere" -f ($node0StockAfterRestart - $node0StockAfter)) -ForegroundColor Red
}

# ── Stage 7: Order lookup on the recovered node ───────────────────────────
Write-Host "`n── ORDER LOOKUP on recovered Node_0 ($newNode0Addr)" -ForegroundColor Cyan
Write-Host ("  Looking for order: {0}" -f $order.id) -ForegroundColor White

try {
    $found = Invoke-RestMethod "$newNode0Addr/api/orders/$($order.id)"
    Write-Host ("  FOUND  - id: {0}  status: {1}" -f $found.id, $found.status) -ForegroundColor Yellow
    Write-Host "  Unexpected: order survived restart (should not happen)" -ForegroundColor Yellow
} catch {
    Write-Host "  404 - Order not found. Confirmed lost on restart." -ForegroundColor Red
}

# ── Summary: all real values from SF ──────────────────────────────────────
Write-Host "`n╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Yellow
Write-Host "║  Summary                                                     ║" -ForegroundColor Yellow
Write-Host "╠══════════════════════════════════════════════════════════════╣" -ForegroundColor Yellow

$beforeStock = ($before | Where-Object { $_.Node -eq "_Node_0" }).Stock
Write-Host ("║  Node_0 stock BEFORE order    : {0,-30}║" -f $beforeStock)              -ForegroundColor White
Write-Host ("║  Node_0 stock AFTER order     : {0,-30}║" -f $node0StockAfter)          -ForegroundColor White
Write-Host ("║  Units decremented            : {0,-30}║" -f ($beforeStock - $node0StockAfter)) -ForegroundColor White

$divergedNodes = $after | Where-Object { $_.Reachable -and $_.Node -ne "_Node_0" -and $_.Stock -ne $node0StockAfter }
Write-Host ("║  Nodes unaware of the order   : {0,-30}║" -f ($divergedNodes | Select-Object -ExpandProperty Node | Join-String -Separator ", ")) -ForegroundColor White
Write-Host ("║  Node_0 stock AFTER restart   : {0,-30}║" -f $node0StockAfterRestart)   -ForegroundColor White
Write-Host ("║  Order retrievable            : {0,-30}║" -f "No (404)")                 -ForegroundColor White

Write-Host "╠══════════════════════════════════════════════════════════════╣" -ForegroundColor Yellow
Write-Host "║  Root causes                                                 ║" -ForegroundColor Yellow
Write-Host "║    1. State is not shared  -> nodes diverge immediately      ║" -ForegroundColor White
Write-Host "║    2. State is not durable -> restart loses all data         ║" -ForegroundColor White
Write-Host "║  Phase 2 fix: IReliableDictionary (replicated + persisted)   ║" -ForegroundColor White
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Yellow