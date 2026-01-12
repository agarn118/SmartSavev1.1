@echo off
REM run_all_scrapers.bat
REM Double-click to launch all grocery scrapers in separate windows
REM Default query is "milk" - edit QUERY variable below to change
REM Set FRESH=--fresh to start with clean data files (recommended for new runs)

SET QUERY=milk
SET MAX_PAGES=10
SET FRESH=--fresh
SET PROJECT_ROOT=%~dp0
SET VENV_ACTIVATE=C:\Users\ashto\Desktop\First_claude\.venv\Scripts\activate.bat

echo ========================================
echo   Grocery Scraper Launcher
echo ========================================
echo Query: %QUERY%
echo Max Pages: %MAX_PAGES%
echo Fresh Start: %FRESH%
echo.

echo Launching Safeway scraper...
start "Safeway Scraper" cmd /k "cd /d %PROJECT_ROOT% && call %VENV_ACTIVATE% && python -m scrapers.run --site safeway --query "%QUERY%" --max-pages %MAX_PAGES% %FRESH% && echo. && echo Done! Press any key to close... && pause >nul"

timeout /t 1 >nul

echo Launching Sobeys scraper...
start "Sobeys Scraper" cmd /k "cd /d %PROJECT_ROOT% && call %VENV_ACTIVATE% && python -m scrapers.run --site sobeys --query "%QUERY%" --max-pages %MAX_PAGES% %FRESH% && echo. && echo Done! Press any key to close... && pause >nul"

timeout /t 1 >nul

echo Launching NoFrills scraper...
start "NoFrills Scraper" cmd /k "cd /d %PROJECT_ROOT% && call %VENV_ACTIVATE% && python -m scrapers.run --site nofrills --query "%QUERY%" --max-pages %MAX_PAGES% %FRESH% && echo. && echo Done! Press any key to close... && pause >nul"

timeout /t 1 >nul

echo Launching Real Canadian Superstore scraper...
start "Superstore Scraper" cmd /k "cd /d %PROJECT_ROOT% && call %VENV_ACTIVATE% && python -m scrapers.run --site realcanadiansuperstore --query "%QUERY%" --max-pages %MAX_PAGES% %FRESH% && echo. && echo Done! Press any key to close... && pause >nul"

echo.
echo All scrapers launched in separate windows!
echo Check each window for progress.
echo.
pause
