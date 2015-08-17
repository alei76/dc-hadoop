@echo off
setlocal
cd /D %0\..\..
call mvn clean
call mvn -Declipse.workspace=. eclipse:add-maven-repo
call mvn eclipse:eclipse
pause
