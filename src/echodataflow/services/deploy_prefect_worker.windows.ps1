$EnvFile = "$HOME\.config\echodataflow\services.env"

if (-not (Test-Path $EnvFile)) {
    Write-Error "Missing services env file: $EnvFile"
    exit 1
}

Get-Content $EnvFile | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        $Name = $matches[1].Trim()
        $Value = $matches[2].Trim()
        Set-Item -Path "Env:$Name" -Value $Value
    }
}

$RequiredVars = @(
    "ECHODATAFLOW_ENV",
    "ECHODATAFLOW_WORKDIR",
    "ECHODATAFLOW_LOG_DIR",
    "MAMBA_BIN",
    "PREFECT_POOL"
)

foreach ($Var in $RequiredVars) {
    if (-not (Get-Item -Path "Env:$Var" -ErrorAction SilentlyContinue)) {
        Write-Error "Missing required environment variable: $Var"
        exit 1
    }
}

New-Item -ItemType Directory -Force -Path $env:ECHODATAFLOW_LOG_DIR | Out-Null

Set-Location $env:ECHODATAFLOW_WORKDIR

& $env:MAMBA_BIN run -n $env:ECHODATAFLOW_ENV `
    prefect worker start `
    --pool $env:PREFECT_POOL