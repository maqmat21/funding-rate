@echo off
:loop
cls
echo ====================================================
echo EJECUTANDO ESCANER DE FUNDING RATE (Cada 10 min)
echo Ultima ejecucion: %date% %time%
echo ====================================================

:: 1. Ejecutar el script de Python
python get_funding.py

:: 2. Comandos de Git para subir al repo
git add high_funding.json
git commit -m "Update funding: %date% %time%"
git push origin main

echo.
echo Esperando 10 minutos para la siguiente revision...
echo No cierres esta ventana.
echo.

:: 3. Esperar 600 segundos (10 minutos)
timeout /t 600 /nobreak

:: 4. Volver al inicio del bucle
goto loop