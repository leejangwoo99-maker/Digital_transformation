# app/run_mailer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import time
from pathlib import Path
import multiprocessing as mp


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

    from app.job.snapshot_mailer import start_scheduler  # lazy import

    start_scheduler()

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()