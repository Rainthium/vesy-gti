"""Точка входа PyInstaller-сборки ves-agent.exe.

CLI полностью совпадает с ``python -m agent.main``:

    ves-agent.exe --config C:/vesy-agent/config.toml
    ves-agent.exe --config ... add-operator --login a.osmonov --full-name "А. Осмонов"
"""

from agent.main import main

if __name__ == "__main__":
    main()
