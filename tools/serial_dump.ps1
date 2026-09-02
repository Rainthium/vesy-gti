<#
Дамп сырого потока COM-порта весового индикатора — для объектов БЕЗ UniServer
(на объектах с UniServer дамп снимается его модулем SERIALTCPIP1 → приёмник на ВМ).

Ничего не устанавливает: только штатный PowerShell Windows. Пишет байты как есть
в файл и показывает hex-превью первых пакетов, чтобы сразу видеть, идёт ли поток.

Запуск (от администратора не нужен, но порт должен быть СВОБОДЕН — закрыть
программу весов или читать виртуальный порт hub4com):

  powershell -ExecutionPolicy Bypass -File serial_dump.ps1 -Port COM3 -Baud 9600 -Seconds 120 -Out C:\dump.bin

Параметры порта по умолчанию 8-N-1, DTR/RTS не поднимаем (правило №6 — CH340).
#>
param(
    [string]$Port = "COM3",
    [int]$Baud = 9600,
    [int]$Seconds = 120,
    [string]$Out = "dump.bin",
    [string]$Parity = "None",   # None | Even | Odd
    [int]$DataBits = 8,
    [string]$StopBits = "One"   # One | Two
)

$sp = New-Object System.IO.Ports.SerialPort $Port, $Baud, $Parity, $DataBits, $StopBits
$sp.ReadTimeout = 500
$sp.DtrEnable = $false
$sp.RtsEnable = $false
$sp.Handshake = "None"

try {
    $sp.Open()
} catch {
    Write-Host "не удалось открыть $Port : $($_.Exception.Message)"
    Write-Host "порт занят другой программой? список портов: $([System.IO.Ports.SerialPort]::GetPortNames() -join ', ')"
    exit 1
}

$fs = [System.IO.File]::Open($Out, [System.IO.FileMode]::Create)
$buf = New-Object byte[] 4096
$total = 0
$shown = 0
$deadline = (Get-Date).AddSeconds($Seconds)
Write-Host "читаю $Port ($Baud $DataBits-$Parity-$StopBits) $Seconds с → $Out"

try {
    while ((Get-Date) -lt $deadline) {
        try {
            $n = $sp.Read($buf, 0, $buf.Length)
        } catch [System.TimeoutException] {
            continue
        }
        if ($n -le 0) { continue }
        $fs.Write($buf, 0, $n)
        $total += $n
        if ($shown -lt 8) {
            # первые порции — hex-строкой, чтобы глазами увидеть кадры (STX/ETX, '=', цифры)
            $hex = ($buf[0..($n - 1)] | ForEach-Object { $_.ToString("X2") }) -join " "
            Write-Host ("{0:HH:mm:ss.fff} +{1,4} байт: {2}" -f (Get-Date), $n, $hex)
            $shown++
        }
    }
} finally {
    $fs.Close()
    $sp.Close()
}

if ($total -eq 0) {
    Write-Host "за $Seconds с не пришло ни байта — проверьте порт, скорость и что индикатор в режиме непрерывной выдачи (tF=0 у XK3190)"
    exit 2
}
Write-Host "готово: $total байт в $Out (~$([math]::Round($total / $Seconds, 1)) байт/с)"
