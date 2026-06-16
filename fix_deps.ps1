$files = Get-ChildItem -Recurse go.mod
foreach ($f in $files) {
    $c = Get-Content $f.FullName -Raw
    $c = $c -replace 'go 1\.25\.0', 'go 1.22'
    Set-Content $f.FullName -Value $c
}

cd client
go get github.com/quic-go/quic-go@v0.42.0
go mod tidy
cd ..

cd gateway
go get github.com/quic-go/quic-go@v0.42.0
go mod tidy
cd ..

cd device
go get github.com/quic-go/quic-go@v0.42.0
go mod tidy
cd ..

cd beacon
go get github.com/quic-go/quic-go@v0.42.0
go mod tidy
cd ..
