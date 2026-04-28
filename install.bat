@echo off
setlocal EnableExtensions
cd /d "%~dp0"

title UAM - Installer

if not exist "ba2_manager.py" (
    echo [ERROR] ba2_manager.py is missing from this folder.
    echo Put ba2_manager.py next to install.bat, then run install again.
    echo.
    pause
    exit /b 1
)

set "BOOT="
where py >nul 2>&1
if %errorlevel% equ 0 (
    py -3 -c "import sys; assert sys.version_info >= (3, 8)" 2>nul
    if %errorlevel% equ 0 set "BOOT=py -3"
)
if not defined BOOT (
    where python >nul 2>&1
    if %errorlevel% equ 0 (
        python -c "import sys; assert sys.version_info >= (3, 8)" 2>nul
        if %errorlevel% equ 0 set "BOOT=python"
    )
)
if not defined BOOT (
    echo [ERROR] Python 3.8 or newer not found on PATH.
    echo Install from https://www.python.org/downloads/ ^(enable Add to PATH^).
    echo.
    pause
    exit /b 1
)

if not exist "venv\Scripts\python.exe" (
    echo Creating virtual environment...
    %BOOT% -m venv venv
    if %errorlevel% neq 0 (
        echo [ERROR] Could not create venv. Try: %BOOT% -m pip install --user virtualenv
        echo.
        pause
        exit /b 1
    )
)

echo Installing dependencies into venv...
"venv\Scripts\python.exe" -m pip install --upgrade pip
"venv\Scripts\python.exe" -m pip install --upgrade PyQt5
if %errorlevel% neq 0 (
    echo [ERROR] pip install failed. See messages above.
    echo.
    pause
    exit /b 1
)

echo.
echo ========================================
echo  Done. Use launcher.bat to start.
echo ========================================
echo.
pause
exit /b 0
