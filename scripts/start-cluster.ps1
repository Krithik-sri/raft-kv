$nodes = @(
    @{ Id = "node1"; Port = 5001 },
    @{ Id = "node2"; Port = 5002 },
    @{ Id = "node3"; Port = 5003 },
    @{ Id = "node4"; Port = 5004 },
    @{ Id = "node5"; Port = 5005 }
)

foreach ($node in $nodes) {
    $id = $node.Id
    $port = $node.Port

    $peers = $nodes |
        Where-Object { $_.Id -ne $id } |
        ForEach-Object { "$($_.Id)=localhost:$($_.Port)" }

    $peerString = $peers -join ","

    Write-Host "Starting $id on localhost:$port"
    Write-Host "Peers: $peerString"

    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-Command",
        "go run ./cmd/server --id $id --addr localhost:$port --peers `"$peerString`""
    )
}