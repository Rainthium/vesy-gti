@echo off
chcp 65001 >nul
rem ============================================================
rem  Установка агента службой Windows через nssm.
rem  Запускать ОТ АДМИНИСТРАТОРА из каталога агента (C:\vesy-agent),
rem  где уже лежат:
rem    app\ves-agent.exe   — распакованная сборка (папка app из архива)
rem    config.toml         — боевой конфиг (по образцу config.example.toml)
rem    nssm.exe            — https://nssm.cc (из архива win64)
rem
rem  Имя службы можно задать первым аргументом — это нужно, когда на
rem  одном ПК работают агенты РАЗНЫХ весов (объект с двумя весами):
rem    install-service.bat            → служба ves-agent   (по умолчанию)
rem    install-service.bat ves-agent-2 → служба ves-agent-2
rem  Имя записывается в service.txt: по нему агент останавливает при
rem  автообновлении СВОЮ службу, а не соседнюю. Пишите имя без кавычек,
rem  пробелов и знаков & | > — только латиница, цифры, . _ - (до 64
rem  символов); иначе скрипт откажется ставить службу.
rem  Подробно: УСТАНОВКА.md
rem ============================================================
setlocal
set "BASE=%~dp0"
set "BASE=%BASE:~0,-1%"

set "SERVICE=%~1"
if "%SERVICE%"=="" set "SERVICE=ves-agent"
echo %SERVICE%|findstr /r /c:"^[A-Za-z0-9._-][A-Za-z0-9._-]*$" >nul
if errorlevel 1 (
    echo ОШИБКА: имя службы "%SERVICE%" недопустимо.
    echo Разрешены латинские буквы, цифры, точка, дефис и подчёркивание — без пробелов.
    exit /b 1
)
rem длину проверяем отдельно: агент принимает имя не длиннее 64 символов
if not "%SERVICE:~64%"=="" (
    echo ОШИБКА: имя службы длиннее 64 символов.
    exit /b 1
)

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

"%BASE%\nssm.exe" install %SERVICE% "%BASE%\app\ves-agent.exe" --config "%BASE%\config.toml"
if errorlevel 1 (
    echo ОШИБКА: nssm install не выполнился. Если служба уже существует —
    echo удалите её: nssm.exe remove %SERVICE% confirm — и запустите скрипт снова.
    exit /b 1
)
"%BASE%\nssm.exe" set %SERVICE% DisplayName "Весовой агент ГТИ (%SERVICE%)"
"%BASE%\nssm.exe" set %SERVICE% Description "Единая весовая система ОАО «ГТИ»: агент весового ПК"
"%BASE%\nssm.exe" set %SERVICE% AppDirectory "%BASE%"
"%BASE%\nssm.exe" set %SERVICE% Start SERVICE_AUTO_START
"%BASE%\nssm.exe" set %SERVICE% AppStdout "%BASE%\logs\agent.log"
"%BASE%\nssm.exe" set %SERVICE% AppStderr "%BASE%\logs\agent.log"
"%BASE%\nssm.exe" set %SERVICE% AppRotateFiles 1
"%BASE%\nssm.exe" set %SERVICE% AppRotateOnline 1
"%BASE%\nssm.exe" set %SERVICE% AppRotateBytes 10485760
"%BASE%\nssm.exe" set %SERVICE% AppEnvironmentExtra PYTHONIOENCODING=utf-8
"%BASE%\nssm.exe" set %SERVICE% AppExit Default Restart
"%BASE%\nssm.exe" set %SERVICE% AppRestartDelay 5000

rem имя службы для автообновления (agent/updater.py читает этот файл)
>"%BASE%\service.txt" echo %SERVICE%

"%BASE%\nssm.exe" start %SERVICE%
if errorlevel 1 (
    echo Служба установлена, но не запустилась — смотрите logs\agent.log
    exit /b 1
)
echo.
echo Служба %SERVICE% установлена и запущена.
echo Интерфейс оператора: http://IP-этого-ПК:8090  (порт из config.toml —
echo второму агенту на этом же ПК задайте в его config.toml другой порт)
echo Логи: %BASE%\logs\agent.log
endlocal
