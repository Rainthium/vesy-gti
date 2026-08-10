"""Runtime-хук PyInstaller: stdout/stderr всегда UTF-8.

Замороженный Python работает в изолированном режиме и игнорирует
PYTHONUTF8/PYTHONIOENCODING, поэтому русские сообщения (help, логи)
в консоли cp1252/cp866 роняли exe (UnicodeEncodeError). errors="replace" —
кодировка вывода никогда не валит службу.
"""

import sys

for _stream in (sys.stdout, sys.stderr):
    if _stream is not None and hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")
