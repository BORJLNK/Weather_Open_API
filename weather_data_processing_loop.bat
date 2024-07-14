@echo off
:: Set the overall console text and background color
color 0A

echo Starting the batch script...
:loop
call conda activate data_eng

:: Set the color for the date and time output
echo.
echo [\033[32mCurrent date and time: %DATE% %TIME%\033[0m]

python "E:\Data Engineering\Spark\weather_api_to_csv.py"

:: Set the color for script execution messages
echo [\033[36mScript executed. Waiting for 10 minutes...\033[0m]

:: Use ping to introduce a delay
ping -n 601 127.0.0.1 > nul  || goto :error

echo [\033[33mWait over. Looping again...\033[0m]
goto loop

:error
:: Set the color for error messages
echo [\033[31mThere was an error during the ping command.\033[0m]
pause
