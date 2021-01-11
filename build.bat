@echo off
set output=%~dp0output
SET GOARCH=amd64

echo output dir: %output%
echo.
if not exist %output% (
		md %output%
    )

echo Compile for windows ...
del /f /s /q %output%\*.*
pushd %~dp0
go build -ldflags="-w -s" -o "./output/sharding.exe" -i ./cmd/gaea/main.go

echo.
set /p linux=compile for linux? [y/n]:
if "%linux%"=="y" (
SET CGO_ENABLED=0
SET GOOS=linux
echo Compile for linux amd64 ...
go build -ldflags="-w -s" -o "./output/sharding" -i ./cmd/gaea/main.go
)

echo.
set /p linux=Compile succeed ! press any key to exit .
