$files = @(
    "client\main.go",
    "gateway\main.go",
    "device\main.go",
    "beacon\main.go"
)
foreach ($f in $files) {
    $c = Get-Content $f -Raw
    $c = $c -replace '\*quic\.Stream', 'quic.Stream'
    $c = $c -replace '\*quic\.Conn', 'quic.Connection'
    Set-Content $f -Value $c
}
