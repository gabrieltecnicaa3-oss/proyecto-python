@echo off
chcp 65001 > nul
cd /d "c:\Users\usuar\OneDrive\Desktop\python"
set "DB_ENGINE=mysql"
set "MYSQL_HOST=127.0.0.1"
set "MYSQL_PORT=3306"
set "MYSQL_USER=appuser"
set "MYSQL_PASSWORD=App1234!"
set "MYSQL_DB=gestion_produccion"

echo.
echo ====================================
echo Validando sintaxis Python...
echo ====================================
echo.
.venv\Scripts\python.exe -m py_compile app2.py
if %errorlevel% equ 0 (
    echo.
    echo ✅ Sintaxis válida - iniciando servidor...
    echo.
    echo ====================================
    echo Iniciando Servidor Flask
    echo ====================================
    echo.
    REM Activar venv e iniciar Flask en nueva ventana (sin modo debug)
    start "" cmd /k "cd /d "c:\Users\usuar\OneDrive\Desktop\python" && .venv\Scripts\activate.bat && set FLASK_ENV=production && set DB_ENGINE=mysql && set MYSQL_HOST=127.0.0.1 && set MYSQL_PORT=3306 && set MYSQL_USER=appuser && set MYSQL_PASSWORD=App1234! && set MYSQL_DB=gestion_produccion && python app2.py"
    echo.
    echo ✅ Servidor iniciando en http://127.0.0.1:5000
    echo.
    echo 💡 El servidor está corriendo en una ventana separada
    echo 💡 Cierra esa ventana para detener el servidor
    echo.
    timeout /t 3 /nobreak
) else (
    echo.
    echo ❌ Error de sintaxis encontrado
    echo Contacta con soporte
    echo.
    pause
)
