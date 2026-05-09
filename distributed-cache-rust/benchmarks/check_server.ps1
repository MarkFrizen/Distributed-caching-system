$ErrorActionPreference = "Stop"
$tcp = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 8080)
$stream = $tcp.GetStream()
$bytes = [System.Text.Encoding]::ASCII.GetBytes("*1`r`n`$4`r`nPING`r`n")
$stream.Write($bytes, 0, $bytes.Length)
Start-Sleep -Milliseconds 200
$buffer = New-Object byte[] 1024
$read = $stream.Read($buffer, 0, $buffer.Length)
$resp = [System.Text.Encoding]::ASCII.GetString($buffer, 0, $read)
$tcp.Close()
Write-Host "Response: $resp"
if ($resp -match '\+PONG') { exit 0 } else { exit 1 }
