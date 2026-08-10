@echo off
chcp 65001 >nul
rem ============================================================
rem  Запасная сборка ves-agent.exe на любой Windows-машине.
rem  Основной путь — GitHub Actions (workflow «Сборка агента»),
rem  архив прикрепляется к релизу по тегу agent-v*.
rem
rem  Требуется установленный uv (https://docs.astral.sh/uv/):
rem    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
rem  Python 3.12 uv поставит сам.
rem ============================================================
setlocal
cd /d "%~dp0..\.."

uv sync || goto :err
uv run --with pyinstaller pyinstaller packaging\agent\ves-agent.spec --noconfirm || goto :err

echo.
echo Готово: dist\ves-agent\  (эта папка кладётся на весовой ПК как app)
exit /b 0

:err
echo.
echo СБОРКА НЕ УДАЛАСЬ — смотрите вывод выше.
exit /b 1
