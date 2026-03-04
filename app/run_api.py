# app/run_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
from pathlib import Path
import multiprocessing as mp

import uvicorn


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _root_dir() -> Path:
    if _is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def main() -> None:
    try:
        mp.freeze_support()
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    root = _root_dir()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(float(os.getenv("API_PORT", "8000")))

    # exe에서 workers>1은 불안정할 수 있어 기본 1
    workers = int(float(os.getenv("API_WORKERS", "1")))
    if workers < 1:
        workers = 1

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        workers=workers,
        log_level=os.getenv("API_LOG_LEVEL", "info"),
        access_log=True,
        reload=False,
    )


if __name__ == "__main__":
    main()