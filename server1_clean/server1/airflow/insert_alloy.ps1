$p = "docker-compose.yml"
$c = Get-Content -Raw $p
if ($c -match "(?m)^  alloy:\s*$") { Write-Host "alloy already exists"; exit 0 }
$svc = @"
  alloy:
    image: grafana/alloy:latest
    container_name: airflow-alloy
    command:
      - run
      - /etc/alloy/config.alloy
    volumes:
      - ./alloy/config.alloy:/etc/alloy/config.alloy:ro
    ports:
      - "12345:12345"
    restart: unless-stopped
"@
$new = [regex]::Replace($c, "(?m)^volumes:\s*$", $svc + "`r`nvolumes:", 1)
Set-Content -Path $p -Value $new
Write-Host "Alloy service inserted successfully"
