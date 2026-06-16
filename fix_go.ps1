$files = Get-ChildItem -Recurse go.mod
foreach ($f in $files) {
    $c = Get-Content $f.FullName -Raw
    $c = $c -replace 'go 1\.25\.0', 'go 1.22'
    Set-Content $f.FullName -Value $c
}
