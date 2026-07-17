@echo off
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0delegation-mcp.ps1" %*
exit /b %ERRORLEVEL%
