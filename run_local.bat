@echo off
chcp 65001 > nul
cd /d "c:\Users\usuar\OneDrive\Desktop\python"
set "DB_ENGINE=sqlite"

echo.
echo ====================================
echo  Modo LOCAL - SQLite
echo ====================================
echo.
.venv\Scripts\python.exe app2.py
pause
