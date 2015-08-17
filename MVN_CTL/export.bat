@echo off
setlocal
cd /D %0\..\.. 
call mvn dependency:copy-dependencies
pause
