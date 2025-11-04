go env -w GOOS=windows

mkdir -p ./bin
go build -o ./bin/backupSentinel.exe ./cmd/backupsentinel
go build -ldflags "-H=windowsgui" -o ./bin/backupSentinelQuiet.exe ./cmd/backupsentinel

go env -w GOOS=linux
