param(
    [string]$Namespace = "stc-hy-airflow",
    [string]$RegistrySecret = "registry-credentials",
    [string]$Image = "ghcr.io/kl3init/kdlstc-airflow:3.2.2-dlt1.21.0-r1"
)

$ErrorActionPreference = "Stop"
$platformRoot = Split-Path -Parent $PSScriptRoot
$repoRoot = Split-Path -Parent $platformRoot
$registry = $Image.Split('/')[0]
$dockerConfigDir = Join-Path ([IO.Path]::GetTempPath()) (
    "kdlstc-docker-" + [guid]::NewGuid().ToString("N")
)

New-Item -ItemType Directory -Path $dockerConfigDir | Out-Null
$previousDockerConfig = $env:DOCKER_CONFIG
$env:DOCKER_CONFIG = $dockerConfigDir

try {
    $encoded = kubectl -n $Namespace get secret $RegistrySecret `
        -o 'jsonpath={.data.\.dockerconfigjson}'
    $config = [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($encoded)
    ) | ConvertFrom-Json
    $entry = $config.auths.$registry
    if (-not $entry) {
        throw "Secret $Namespace/$RegistrySecret has no credential for $registry"
    }

    $userPassword = [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($entry.auth)
    ).Split(':', 2)
    $userPassword[1] | docker login $registry `
        --username $userPassword[0] --password-stdin
    if ($LASTEXITCODE -ne 0) { throw "Docker login failed" }

    docker build --pull `
        --file (Join-Path $platformRoot 'Dockerfile.airflow') `
        --tag $Image `
        $repoRoot
    if ($LASTEXITCODE -ne 0) { throw "Docker build failed" }

    docker push $Image
    if ($LASTEXITCODE -ne 0) { throw "Docker push failed" }

    Write-Output "Pushed $Image"
}
finally {
    docker logout $registry 2>$null | Out-Null
    $env:DOCKER_CONFIG = $previousDockerConfig
    $resolvedTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    $resolvedConfig = [IO.Path]::GetFullPath($dockerConfigDir)
    if ($resolvedConfig.StartsWith($resolvedTemp, [StringComparison]::OrdinalIgnoreCase)) {
        Remove-Item -LiteralPath $resolvedConfig -Recurse -Force -ErrorAction SilentlyContinue
    }
}
