@echo off
chcp 65001 >nul
rem ============================================================
rem  Обновление агента на новую версию. Запускать ОТ АДМИНИСТРАТОРА.
rem  Перед запуском: распакуйте НОВЫЙ архив и положите его папку app
rem  сюда под именем app_new (рядом с текущей app).
rem  Данные не трогаются: config.toml, agent.sqlite3, photos\ остаются.
rem ============================================================
setlocal
set "BASE=%~dp0"
set "BASE=%BASE:~0,-1%"

if not exist "%BASE%\app_new\ves-agent.exe" (
    echo ОШИБКА: не найден %BASE%\app_new\ves-agent.exe
    echo Сначала распакуйте новую версию: папку app из архива положите как app_new
    exit /b 1
)

"%BASE%\nssm.exe" stop ves-agent
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

"%BASE%\nssm.exe" start ves-agent
if errorlevel 1 (
    echo Служба не запустилась — смотрите logs\agent.log.
    echo Откат: nssm stop ves-agent, вернуть app_old на место app, nssm start ves-agent
    exit /b 1
)
echo.
echo Обновление выполнено. Старая версия сохранена в app_old
echo (удалите её после проверки, что всё работает).
endlocal
