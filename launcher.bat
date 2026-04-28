@echo off
setlocal EnableExtensions
cd /d "%~dp0"

title BA2 Manager

if not exist "ba2_manager.py" (
    echo [ERROR] ba2_manager.py not found in this folder.
    echo.
    pause
    exit /b 1
)

if not exist "venv\Scripts\activate.bat" (
    echo [ERROR] No venv here. Run install.bat first.
    echo.
    pause
    exit /b 1
)

call "venv\Scripts\activate.bat"
python "ba2_manager.py" %*
set "APP_EXIT=%errorlevel%"
if %APP_EXIT% neq 0 (
    echo.
    echo The program exited with an error.
    pause
)
exit /b %APP_EXIT%
