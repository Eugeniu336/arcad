@echo off
title Tabla Indoita - server
cd /d "%~dp0"
node server.js
if errorlevel 1 (
  echo.
  echo Nu am putut porni serverul. Ai instalat Node.js? Descarca-l de pe https://nodejs.org
)
pause
