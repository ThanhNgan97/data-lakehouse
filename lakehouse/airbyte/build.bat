@echo off
echo ====================================================================
echo   Building Airbyte DB Provisioner (Standalone Executable)
echo ====================================================================
set CGO_ENABLED=0
go build -ldflags="-s -w" -o db-provisioner.exe ./cmd/db-provisioner

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [SUCCESS] Built successfully: db-provisioner.exe
    echo You can now double-click db-provisioner.exe to launch the Wizard!
) else (
    echo.
    echo [ERROR] Build failed. Please verify that Go is installed and on PATH.
)
pause
