param(
    [switch]$SkipImages = $false
)

Write-Host "========================================="
Write-Host "Multigres Windows Launcher"
Write-Host "========================================="

Set-Location -Path $PSScriptRoot

if (-not $SkipImages) {
    Write-Host "`nWaiting for Docker builds to complete..."
    # The agent is already building these in the background
}

Write-Host "`nCreating data directory..."
New-Item -ItemType Directory -Force -Path .\data | Out-Null

Write-Host "`nInitializing Kind cluster..."
kind create cluster --config=kind.yaml --name=multidemo

Write-Host "`nModifying sysctl limits on nodes..."
$nodes = kind get nodes --name=multidemo
foreach ($node in $nodes) {
    docker exec $node sh -c "sysctl -w fs.file-max=2097152"
    docker exec $node sh -c "sysctl -w fs.nr_open=2097152"
    docker exec $node sh -c "sysctl -w net.core.somaxconn=65535"
    docker exec $node sh -c "sysctl -w net.ipv4.ip_local_port_range='1024 65535'"
}

Write-Host "`nLoading Docker images into Kind..."
kind load docker-image multigres/multigres multigres/pgctld-postgres multigres/multiadmin-web --name=multidemo

Write-Host "`nDeploying etcd..."
kubectl apply -f k8s-etcd.yaml
kubectl rollout status statefulset/etcd --timeout=120s

Write-Host "`nDeploying Core Multigres Components..."
kubectl apply -f k8s-multipooler-statefulset.yaml
kubectl apply -f k8s-multiorch.yaml
kubectl apply -f k8s-multigateway.yaml

kubectl rollout status statefulset/multipooler-zone1 --timeout=180s
kubectl rollout status deployment/multiorch --timeout=120s
kubectl rollout status deployment/multigateway --timeout=120s

Write-Host "`n========================================="
Write-Host "Multigres Cluster Ready on Windows!"
Write-Host "========================================="
Write-Host "To access postgres, open another terminal and run:"
Write-Host "kubectl port-forward svc/multigateway 15432:5432"
Write-Host "Then connect using: psql -h localhost -p 15432 -U postgres"
