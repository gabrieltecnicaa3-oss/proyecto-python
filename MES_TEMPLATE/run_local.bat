@echo off
chcp 65001 > nul
cd /d "%~dp0"
set "DB_ENGINE=sqlite"

echo.
echo ====================================
echo  Modo LOCAL - SQLite
echo ====================================
echo.
.venv\Scripts\python.exe app2.py
pause
