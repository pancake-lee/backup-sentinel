go env -w GOOS=windows

mkdir -p ./bin
go build -o ./bin/backupsentinel.exe ./cmd/backupsentinel

go env -w GOOS=linux
