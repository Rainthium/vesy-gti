@echo off
chcp 65001 >nul
rem ============================================================
rem  Обновление агента на новую версию. Запускать ОТ АДМИНИСТРАТОРА.
rem  Перед запуском: распакуйте НОВЫЙ архив и положите его папку app
rem  сюда под именем app_new (рядом с текущей app).
rem  Данные не трогаются: config.toml, agent.sqlite3, photos\ остаются.
rem
rem  Имя службы берётся из service.txt (его пишет install-service.bat),
rem  иначе ves-agent. Можно задать первым аргументом:
rem    update.bat ves-agent-2
rem  Обычный путь обновления — кнопка в панели центра; этот скрипт
rem  остаётся ручным запасным вариантом.
rem ============================================================
setlocal
set "BASE=%~dp0"
set "BASE=%BASE:~0,-1%"

set "SERVICE=%~1"
rem чтение через for/f, а не set /p: перенаправление внутри if cmd готовит
rem заранее и на установке без service.txt печатало бы ошибку
if "%SERVICE%"=="" if exist "%BASE%\service.txt" (
    rem "if not defined" — берём ПЕРВУЮ строку, как и агент (иначе на файле
    rem с дописанным хвостом остановили бы соседнюю службу)
    for /f "usebackq delims=" %%s in ("%BASE%\service.txt") do if not defined SERVICE set "SERVICE=%%s"
)
if "%SERVICE%"=="" set "SERVICE=ves-agent"
echo %SERVICE%|findstr /r /c:"^[A-Za-z0-9._-][A-Za-z0-9._-]*$" >nul
if errorlevel 1 (
    echo ОШИБКА: имя службы "%SERVICE%" недопустимо — почините service.txt
    echo или укажите имя аргументом: update.bat ves-agent-2
    exit /b 1
)

if not exist "%BASE%\app_new\ves-agent.exe" (
    echo ОШИБКА: не найден %BASE%\app_new\ves-agent.exe
    echo Сначала распакуйте новую версию: папку app из архива положите как app_new
    exit /b 1
)

"%BASE%\nssm.exe" stop %SERVICE%
rem даём процессу закрыть SQLite и COM-порт
timeout /t 3 /nobreak >nul

rmdir /s /q "%BASE%\app_old" 2>nul
move "%BASE%\app" "%BASE%\app_old" >nul
if errorlevel 1 (
    echo ОШИБКА: не удалось убрать текущую app — служба ещё не остановилась?
    echo Подождите и запустите update.bat снова.
    exit /b 1
)
move "%BASE%\app_new" "%BASE%\app" >nul
if not exist "%BASE%\app\ves-agent.exe" (
    echo ОШИБКА: новая версия не встала на место. Верните app_old как app вручную.
    exit /b 1
)

"%BASE%\nssm.exe" start %SERVICE%
if errorlevel 1 (
    echo Служба не запустилась — смотрите logs\agent.log.
    echo Откат: nssm stop %SERVICE%, вернуть app_old на место app, nssm start %SERVICE%
    exit /b 1
)
echo.
echo Обновление выполнено. Старая версия сохранена в app_old
echo (удалите её после проверки, что всё работает).
endlocal
