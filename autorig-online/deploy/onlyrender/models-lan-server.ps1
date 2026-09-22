# AutoRig farm model server: read-only HTTP of the ComfyUI models dir on the farm LAN only.
# Other farm boxes pull large model files from here (sha256-checked) instead of the internet.
$ErrorActionPreference = 'Stop'
$root = 'D:\ComfyUI_windows_portable'
$models = Join-Path $root 'ComfyUI\models'
$py = Join-Path $root 'python_embeded\python.exe'
$log = 'C:\ProgramData\AutoRig\models-lan-server.log'
# Bind only to this box's 192.168.0.x address, never 0.0.0.0.
for ($i = 0; $i -lt 30; $i++) {
    $ip = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object { $_.IPAddress -like '192.168.0.*' } | Select-Object -First 1 -ExpandProperty IPAddress
    if ($ip) { break }
    Start-Sleep -Seconds 10
}
if (-not $ip) { Add-Content $log "$(Get-Date -Format s) no 192.168.0.x address; not serving"; exit 1 }
Add-Content $log "$(Get-Date -Format s) serving $models on http://${ip}:18998"
& $py -m http.server 18998 --bind $ip --directory $models *>> $log
