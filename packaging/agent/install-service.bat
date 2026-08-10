@echo off
chcp 65001 >nul
rem ============================================================
rem  Установка агента службой Windows через nssm.
rem  Запускать ОТ АДМИНИСТРАТОРА из каталога агента (C:\vesy-agent),
rem  где уже лежат:
rem    app\ves-agent.exe   — распакованная сборка (папка app из архива)
rem    config.toml         — боевой конфиг (по образцу config.example.toml)
rem    nssm.exe            — https://nssm.cc (из архива win64)
rem  Подробно: УСТАНОВКА.md
rem ============================================================
setlocal
set "BASE=%~dp0"
set "BASE=%BASE:~0,-1%"

if not exist "%BASE%\app\ves-agent.exe" (
    echo ОШИБКА: не найден %BASE%\app\ves-agent.exe
    exit /b 1
)
if not exist "%BASE%\config.toml" (
    echo ОШИБКА: не найден %BASE%\config.toml — создайте по config.example.toml
    exit /b 1
)
if not exist "%BASE%\nssm.exe" (
    echo ОШИБКА: не найден %BASE%\nssm.exe — скачайте с https://nssm.cc
    exit /b 1
)

mkdir "%BASE%\logs" 2>nul

"%BASE%\nssm.exe" install ves-agent "%BASE%\app\ves-agent.exe" --config "%BASE%\config.toml"
if errorlevel 1 (
    echo ОШИБКА: nssm install не выполнился. Если служба уже существует —
    echo удалите её: nssm.exe remove ves-agent confirm — и запустите скрипт снова.
    exit /b 1
)
"%BASE%\nssm.exe" set ves-agent DisplayName "Весовой агент ГТИ"
"%BASE%\nssm.exe" set ves-agent Description "Единая весовая система ОАО «ГТИ»: агент весового ПК"
"%BASE%\nssm.exe" set ves-agent AppDirectory "%BASE%"
"%BASE%\nssm.exe" set ves-agent Start SERVICE_AUTO_START
"%BASE%\nssm.exe" set ves-agent AppStdout "%BASE%\logs\agent.log"
"%BASE%\nssm.exe" set ves-agent AppStderr "%BASE%\logs\agent.log"
"%BASE%\nssm.exe" set ves-agent AppRotateFiles 1
"%BASE%\nssm.exe" set ves-agent AppRotateOnline 1
"%BASE%\nssm.exe" set ves-agent AppRotateBytes 10485760
"%BASE%\nssm.exe" set ves-agent AppEnvironmentExtra PYTHONIOENCODING=utf-8
"%BASE%\nssm.exe" set ves-agent AppExit Default Restart
"%BASE%\nssm.exe" set ves-agent AppRestartDelay 5000

"%BASE%\nssm.exe" start ves-agent
if errorlevel 1 (
    echo Служба установлена, но не запустилась — смотрите logs\agent.log
    exit /b 1
)
echo.
echo Служба ves-agent установлена и запущена.
echo Интерфейс оператора: http://IP-этого-ПК:8090  (порт из config.toml)
echo Логи: %BASE%\logs\agent.log
endlocal
