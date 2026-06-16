$files = Get-ChildItem -Recurse Dockerfile
foreach ($f in $files) {
    $c = Get-Content $f.FullName -Raw
    $c = $c -replace 'FROM golang:1\.22-alpine', 'FROM golang:1.25-alpine'
    Set-Content $f.FullName -Value $c
}
