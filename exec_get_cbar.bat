@ECHO OFF
TITLE Cbar currencies
color a
ECHO.
ECHO ===================================================
ECHO.
CALL %LOCALAPPDATA%\Continuum\anaconda3\Scripts\activate.bat %LOCALAPPDATA%\Continuum\anaconda3
python get_cbar_currencies.py
ECHO.
ECHO ===================================================
ECHO.
pause