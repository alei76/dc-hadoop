@echo off
setlocal
cd /D %0\..\..
call mvn clean 
call mvn deploy
pause
