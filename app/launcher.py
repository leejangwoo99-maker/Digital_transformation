# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import traceback
import subprocess
from pathlib import Path
from datetime import datetime


# =============================================================================
# 0) Utils / Logging
# =============================================================================
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def _is_nuitka_frozen() -> bool:
    """
    Nuitka/pyinstaller 모두 대응되는 frozen 판정.
    - pyinstaller: sys.frozen = True
    - nuitka: __compiled__ global이 존재(대부분 True로 동작)
    """
    try:
        if bool(getattr(sys, "frozen", False)):
            return True
    except Exception:
        pass
    # Nuitka가 만들어주는 전역 심볼(대부분 존재)
    try:
        if "__compiled__" in globals():
            return True
    except Exception:
        pass
    return False


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8", errors="replace")


def _append_text(p: Path, s: str) -> None:
    with p.open("a", encoding="utf-8", errors="replace") as f:
        f.write(s)


def _log(msg: str) -> None:
    line = f"[{_now()}] [INFO] {msg}\n"
    print(line, end="", flush=True)
    try:
        _append_text(LOG_FILE, line)
    except Exception:
        pass


def _log_err(msg: str) -> None:
    line = f"[{_now()}] [ERROR] {msg}\n"
    print(line, end="", flush=True)
    try:
        _append_text(LOG_FILE, line)
    except Exception:
        pass


def _pause_if_needed(reason: str = "") -> None:
    """
    더블클릭 실행 시 콘솔이 바로 닫히는 문제 방지.
    - 기본: 에러가 나면 무조건 pause
    - 필요시: LAUNCHER_PAUSE_ALWAYS=1로 항상 pause
    """
    always = os.getenv("LAUNCHER_PAUSE_ALWAYS", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
    if always or reason:
        try:
            print("")
            if reason:
                print(f"[PAUSE] {reason}")
            input("[PAUSE] Enter를 누르면 종료합니다...")
        except Exception:
            time.sleep(10)


# =============================================================================
# 1) Path Resolution
# =============================================================================
FROZEN = _is_nuitka_frozen()

# exe 기준 BASE_DIR / ROOT_DIR 잡기
# - exe 실행: ...\launcher.dist\launcher.exe  -> BASE_DIR = ...\launcher.dist
# - dev 실행: ...\app\launcher.py             -> BASE_DIR = ...\app
if FROZEN:
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

# ROOT_DIR는 프로젝트 루트(배포 루트)
# - dist 구조가: <ROOT>\launcher.dist\launcher.exe 라면 ROOT_DIR = <ROOT>
ROOT_DIR = BASE_DIR.parent if BASE_DIR.name.endswith(".dist") else BASE_DIR.parent

LOG_DIR = BASE_DIR / "logs"
_ensure_dir(LOG_DIR)
LOG_FILE = LOG_DIR / f"launcher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# =============================================================================
# 2) EXE / Module Targets
# =============================================================================
# launcher가 띄울 대상 (가능하면 exe로 실행, 없으면 python -m fallback)
TARGETS = {
    "api": {
        "exe": "run_api.exe",
        "module": "app.run_api",
        "name": "API",
    },
    "ui": {
        "exe": "run_ui.exe",
        "module": "app.run_ui",
        "name": "UI",
    },
    "mailer": {
        "exe": "run_mailer.exe",
        "module": "app.run_mailer",
        "name": "MAILER",
    },
}


def _env_for_child() -> dict:
    """
    자식 프로세스가 app/ 패키지를 import 할 수 있도록 PYTHONPATH 보정.
    (dev/python 모드에서 특히 중요)
    """
    env = dict(os.environ)
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    # dev/python fallback일 때를 대비해 ROOT_DIR를 PYTHONPATH로 밀어줌
    pp = env.get("PYTHONPATH", "")
    parts = [p for p in pp.split(os.pathsep) if p.strip()]
    if str(ROOT_DIR) not in parts:
        parts.insert(0, str(ROOT_DIR))
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def _spawn_target(key: str, extra_args: list[str] | None = None) -> subprocess.Popen | None:
    """
    1) dist에 <name>.exe가 있으면 exe 실행
    2) 없으면 python -m app.<...> 실행
    """
    extra_args = extra_args or []
    t = TARGETS[key]
    exe_path = BASE_DIR / t["exe"]

    env = _env_for_child()

    # 1) exe 우선
    if exe_path.exists():
        cmd = [str(exe_path), *extra_args]
        _log(f"spawn {t['name']} via EXE: {cmd}")
        return subprocess.Popen(
            cmd,
            cwd=str(ROOT_DIR),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        )

    # 2) python -m fallback (dev 또는 exe만 따로 빌드한 경우 대비)
    py = sys.executable  # dev일 땐 venv python, exe일 땐 launcher.exe이므로 fallback은 의미 없음
    if FROZEN:
        _log_err(f"{t['name']} exe가 없어서 실행 불가: {exe_path}")
        return None

    cmd = [py, "-m", t["module"], *extra_args]
    _log(f"spawn {t['name']} via python -m: {cmd}")
    return subprocess.Popen(
        cmd,
        cwd=str(ROOT_DIR),
        env=env,
    )


def _parse_args(argv: list[str]) -> dict:
    """
    옵션:
      launcher.exe            -> api+ui+mailer 모두 실행
      launcher.exe --api      -> api만
      launcher.exe --ui       -> ui만
      launcher.exe --mailer   -> mailer만
    """
    flags = {"api": False, "ui": False, "mailer": False}
    for a in argv[1:]:
        a = (a or "").strip().lower()
        if a == "--api":
            flags["api"] = True
        elif a == "--ui":
            flags["ui"] = True
        elif a == "--mailer":
            flags["mailer"] = True

    if not any(flags.values()):
        # 아무 옵션 없으면 전부 실행
        flags = {"api": True, "ui": True, "mailer": True}
    return flags


def main() -> int:
    _write_text(LOG_FILE, "")  # 새 로그 시작

    _log("BOOT launcher start")
    _log(f"BASE_DIR={BASE_DIR}")
    _log(f"ROOT_DIR={ROOT_DIR}")
    _log(f"FROZEN={FROZEN}")
    _log(f"LOG={LOG_FILE}")

    flags = _parse_args(sys.argv)
    _log(f"FLAGS={flags}")

    procs: list[tuple[str, subprocess.Popen]] = []

    # 실행 순서 권장: api -> ui -> mailer
    if flags["api"]:
        p = _spawn_target("api")
        if p is None:
            _log_err("API start failed")
            return 2
        procs.append(("api", p))
        time.sleep(1.0)

    if flags["ui"]:
        p = _spawn_target("ui")
        if p is None:
            _log_err("UI start failed")
            return 3
        procs.append(("ui", p))
        time.sleep(1.0)

    if flags["mailer"]:
        p = _spawn_target("mailer")
        if p is None:
            _log_err("MAILER start failed")
            return 4
        procs.append(("mailer", p))

    _log(f"spawned: {[k for (k, _) in procs]}")

    # launcher는 “프로세스 감시/유지” 역할
    # - 창이 바로 꺼지면 사용자가 아무 것도 못 보니까,
    #   최소한 3초는 살려두고, 이후에는 백그라운드로 두고 싶으면 종료 가능.
    # 여기서는 "실행 확인"까지 찍고 종료하는 방식으로 간다.
    time.sleep(2.0)
    _log("launcher done (children running in background)")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
        sys.exit(rc)
    except Exception as e:
        tb = traceback.format_exc()
        _log_err(f"UNCAUGHT: {type(e).__name__}: {e}")
        _log_err("TRACEBACK:\n" + tb)
        # 에러 시 콘솔 유지
        _pause_if_needed("예외 발생. 위 로그와 logs 폴더의 launcher_*.log 확인하세요.")
        sys.exit(99)