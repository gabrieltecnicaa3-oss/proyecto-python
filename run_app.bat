@echo off
cd /d "c:\Users\usuar\OneDrive\Desktop\python"
echo.
echo ====================================
echo Validando sintaxis Python...
echo ====================================
echo.
python -m py_compile app2.py
if %errorlevel% equ 0 (
    echo.
    echo ✅ Sintaxis válida - iniciando servidor...
    echo.
    echo ====================================
    echo Iniciando Servidor Flask
    echo ====================================
    echo.
    python app2.py
) else (
    echo.
    echo ❌ Error de sintaxis encontrado
    echo Contacta con soporte
    echo.
)
pause
