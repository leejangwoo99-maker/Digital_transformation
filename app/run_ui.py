# app/run_ui.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
from pathlib import Path

from streamlit.web import cli as stcli


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _root_dir() -> Path:
    # dev: .../PythonProject
    # exe: .../launcher.dist
    if _is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def main() -> None:
    root = _root_dir()

    # Streamlit entry: dist/app/streamlit_app/app.py (실파일이어야 함)
    app_path = root / "app" / "streamlit_app" / "app.py"
    if not app_path.exists():
        raise FileNotFoundError(
            f"Streamlit entry not found: {app_path}\n"
            f"TIP) Nuitka standalone에서는 다음을 include-data-dir로 포함해야 합니다:\n"
            f"    --include-data-dir=app/streamlit_app=app/streamlit_app"
        )

    host = os.getenv("UI_HOST", "127.0.0.1")
    port = os.getenv("UI_PORT", "8501")

    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        str(host),
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]

    stcli.main()


if __name__ == "__main__":
    main()