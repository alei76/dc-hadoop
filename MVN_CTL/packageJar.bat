@echo off
setlocal
cd /D %0\..\..
call mvn clean compile
call mvn compile package
pause