@ECHO OFF
SETLOCAL EnableDelayedExpansion
SET "WRAPPER_DIR=%USERPROFILE%\.m2\wrapper\dists\apache-maven-3.9.16-bin\5grr65jo27hi51sujmtcldfovl\apache-maven-3.9.16"
IF EXIST "%WRAPPER_DIR%\bin\mvn.cmd" (
  CALL "%WRAPPER_DIR%\bin\mvn.cmd" "-Dmaven.repo.local=%~dp0.m2\repository" %*
  EXIT /B !ERRORLEVEL!
)
WHERE mvn >NUL 2>NUL
IF %ERRORLEVEL% EQU 0 (
  CALL mvn "-Dmaven.repo.local=%~dp0.m2\repository" %*
  EXIT /B !ERRORLEVEL!
)
ECHO Maven 3.9.16 is not cached and mvn is not on PATH.
EXIT /B 1
