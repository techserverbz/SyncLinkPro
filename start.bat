@echo off
cd /d "%~dp0"

REM Read port from .env (default 7878)
set PORT=7878
if exist .env (
    for /f "tokens=1,2 delims==" %%a in (.env) do (
        if "%%a"=="PORT" set PORT=%%b
    )
)

echo.
echo   SyncLinkPro  -^>  http://localhost:%PORT%
echo   Press Ctrl+C or close this window to stop.
echo.

python app.py

REM When python exits (terminal closed or Ctrl+C), port is freed automatically
echo.
echo   SyncLinkPro stopped. Port %PORT% released.
pause
