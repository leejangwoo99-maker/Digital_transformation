# -*- coding: utf-8 -*-
"""
c1_fct_detail_factory.py
- advisory lock 제거
- cursor/watchdog DB 저장
- startup catch-up 이후에 stall watchdog 시작
- cursor 컬럼: run_id, end_day, end_time, file_path
- realtime 500 / backfill 1000

[보강]
- insert/cursor, watchdog/heartbeat, health log 엔진 분리
- 엔진별 lock 추가
- None engine 방어
- faulthandler / signal / threading.excepthook 추가
- heartbeat ping이 progress를 갱신하지 않도록 분리
- 급사 추적용 로컬 덤프 파일 추가
"""

from __future__ import annotations

import os
import re
import sys
import time as time_mod
import queue
import socket
import atexit
import signal
import threading
import traceback
import urllib.parse
import faulthandler
from pathlib import Path
from datetime import datetime, timedelta, date, time as dt_time
from typing import Dict, Tuple, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from zoneinfo import ZoneInfo

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


KST = ZoneInfo("Asia/Seoul")

os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

BASE_DIR = Path(os.getenv("C1_BASE_DIR", r"\\192.168.108.155\FCT LogFile\Machine Log\FCT"))

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SCHEMA_NAME = "c1_fct_detail"
TABLE_NAME = "fct_detail"
CURSOR_TABLE = "fct_detail_cursor"
WATCHDOG_TABLE = "fct_detail_watchdog"
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "c1_log"

DAEMON_NAME = "c1_fct_detail_factory"

CANDIDATE_WINDOW_SEC = int(os.getenv("C1_CANDIDATE_WINDOW_SEC", "3600"))
STABLE_REQUIRED = int(os.getenv("C1_STABLE_REQUIRED", "3"))
LOOP_SLEEP_SEC = int(os.getenv("C1_LOOP_SLEEP_SEC", "5"))
DB_RETRY_INTERVAL_SEC = int(os.getenv("DB_RETRY_INTERVAL_SEC", "5"))

REALTIME_INSERT_BATCH_SIZE = int(os.getenv("C1_INSERT_BATCH_SIZE", "500"))
BACKFILL_INSERT_BATCH_SIZE = int(os.getenv("C1_BACKFILL_INSERT_BATCH_SIZE", "1000"))

WORK_MEM = os.getenv("PG_WORK_MEM", "16MB")
STMT_TIMEOUT_MS = int(os.getenv("C1_STMT_TIMEOUT_MS", "30000"))
LOCK_TIMEOUT_MS = int(os.getenv("C1_LOCK_TIMEOUT_MS", "3000"))
IDLE_TX_TIMEOUT_MS = int(os.getenv("C1_IDLE_TX_TIMEOUT_MS", "5000"))

PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

DB_LOG_QUEUE_MAX = int(os.getenv("DB_LOG_QUEUE_MAX", "10000"))
DB_LOG_BATCH_SIZE = int(os.getenv("DB_LOG_BATCH_SIZE", "300"))
DB_LOG_FLUSH_INTERVAL_SEC = float(os.getenv("DB_LOG_FLUSH_INTERVAL_SEC", "2.0"))
DB_LOG_CONTENTS_MAXLEN = int(os.getenv("DB_LOG_CONTENTS_MAXLEN", "2000"))

HEARTBEAT_INTERVAL_SEC = int(os.getenv("C1_HEARTBEAT_INTERVAL_SEC", "30"))
WATCHDOG_SELF_EXIT_SEC = int(os.getenv("C1_WATCHDOG_SELF_EXIT_SEC", "120"))
ACTIVE_BATCH_GRACE_SEC = int(os.getenv("C1_ACTIVE_BATCH_GRACE_SEC", "300"))
SLOW_BATCH_WARN_SEC = int(os.getenv("C1_SLOW_BATCH_WARN_SEC", "20"))
CURSOR_LOOKBACK_MIN = int(os.getenv("C1_CURSOR_LOOKBACK_MIN", "10"))

LOCAL_LOG_DIR = Path(os.getenv("C1_LOCAL_LOG_DIR", r"C:\AptivAgent\logs"))
LOCAL_LOG_FILE = LOCAL_LOG_DIR / os.getenv("C1_LOCAL_LOG_FILE", "c1_fct_detail_factory.log")
CRASH_DUMP_FILE = LOCAL_LOG_DIR / os.getenv("C1_CRASH_DUMP_FILE", "c1_fct_detail_factory_crash_dump.log")

INSERT_APP_NAME = "c1_fct_detail_loader_factory_insert"
WATCHDOG_APP_NAME = "c1_fct_detail_loader_factory_watchdog"
HEALTH_APP_NAME = "c1_fct_detail_loader_factory_health"

LINE_RE = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{1,3})\]\s(.+)$")
FNAME_RE = re.compile(r"^(.*)_(\d{8})_(\d{6}(?:\.\d{1,3})?)\.txt$", re.IGNORECASE)

_ENGINE_INSERT = None
_ENGINE_WATCHDOG = None
_ENGINE_HEALTH = None

ENGINE_INSERT_LOCK = threading.RLock()
ENGINE_WATCHDOG_LOCK = threading.RLock()
ENGINE_HEALTH_LOCK = threading.RLock()

_FAULT_FH = None

FILE_STATE: Dict[str, Tuple[int, int]] = {}
db_log_queue: queue.Queue = queue.Queue(maxsize=max(1000, DB_LOG_QUEUE_MAX))

HOSTNAME = socket.gethostname()
PID = os.getpid()

STATE_LOCK = threading.Lock()
RUNTIME_STATE = {
    "stage": "boot",
    "last_progress_ts": time_mod.time(),
    "last_batch_started_ts": 0.0,
    "last_batch_finished_ts": 0.0,
    "last_batch_idx": 0,
    "last_batch_total": 0,
    "last_batch_source": "",
    "last_batch_rows": 0,
    "last_loop": 0,
    "last_message": "starting",
    "total_attempted_rows": 0,
    "last_run_id": "",
    "last_file_path": "",
    "last_end_day": "",
    "last_end_time": "",
}

STOP_EVENT = threading.Event()


def _now_kst() -> datetime:
    return datetime.now(tz=KST)


def _ts() -> str:
    return _now_kst().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_local_log_dir():
    try:
        LOCAL_LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _write_local_log(line: str):
    try:
        _ensure_local_log_dir()
        with open(LOCAL_LOG_FILE, "a", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")
            f.flush()
    except Exception:
        pass


def _write_crash_dump_line(line: str):
    try:
        _ensure_local_log_dir()
        with open(CRASH_DUMP_FILE, "a", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")
            f.flush()
    except Exception:
        pass


def _masked_db_info(cfg=DB_CONFIG) -> str:
    return f"postgresql://{cfg['user']}:***@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"


def _normalize_info(info: str) -> str:
    if not info:
        return "info"
    s = re.sub(r"[^a-z0-9_]+", "_", info.strip().lower())
    s = s.strip("_")
    return s or "info"


def _infer_info_from_msg(msg: str) -> str:
    m = (msg or "").lower()
    if "[error]" in m or "trace" in m or "fatal" in m or "[unhandled]" in m:
        return "error"
    if "[retry]" in m or "failed" in m or "conn error" in m or "down" in m:
        return "down"
    if "[boot]" in m or "[ok]" in m:
        return "boot"
    if "[stop]" in m or "[exit]" in m:
        return "stop"
    if "sleep" in m:
        return "sleep"
    if "[warn]" in m:
        return "warn"
    if "[perf]" in m:
        return "perf"
    return "info"


def _set_stage(stage: str, message: str = ""):
    with STATE_LOCK:
        RUNTIME_STATE["stage"] = stage
        if message:
            RUNTIME_STATE["last_message"] = message


def _mark_progress(message: str = ""):
    with STATE_LOCK:
        RUNTIME_STATE["last_progress_ts"] = time_mod.time()
        if message:
            RUNTIME_STATE["last_message"] = message


def _set_batch_state(source: str, idx: int, total: int, rows: int):
    with STATE_LOCK:
        RUNTIME_STATE["last_batch_source"] = source
        RUNTIME_STATE["last_batch_idx"] = idx
        RUNTIME_STATE["last_batch_total"] = total
        RUNTIME_STATE["last_batch_rows"] = rows
        RUNTIME_STATE["last_batch_started_ts"] = time_mod.time()


def _finish_batch_state(total_attempted_rows: int):
    with STATE_LOCK:
        RUNTIME_STATE["last_batch_finished_ts"] = time_mod.time()
        RUNTIME_STATE["last_progress_ts"] = time_mod.time()
        RUNTIME_STATE["total_attempted_rows"] = total_attempted_rows


def _runtime_snapshot_dict() -> dict:
    with STATE_LOCK:
        stall_sec = int(time_mod.time() - float(RUNTIME_STATE["last_progress_ts"]))
        return {
            "ts": _ts(),
            "host": HOSTNAME,
            "pid": PID,
            "daemon_name": DAEMON_NAME,
            "stage": RUNTIME_STATE["stage"],
            "loop": RUNTIME_STATE["last_loop"],
            "last_batch_idx": RUNTIME_STATE["last_batch_idx"],
            "last_batch_total": RUNTIME_STATE["last_batch_total"],
            "last_batch_source": RUNTIME_STATE["last_batch_source"],
            "last_batch_rows": RUNTIME_STATE["last_batch_rows"],
            "last_batch_started_ts": RUNTIME_STATE["last_batch_started_ts"],
            "last_batch_finished_ts": RUNTIME_STATE["last_batch_finished_ts"],
            "total_attempted_rows": RUNTIME_STATE["total_attempted_rows"],
            "stall_sec": stall_sec,
            "last_message": RUNTIME_STATE["last_message"],
            "run_id": RUNTIME_STATE["last_run_id"],
            "end_day": RUNTIME_STATE["last_end_day"] or None,
            "end_time": RUNTIME_STATE["last_end_time"] or None,
            "file_path": RUNTIME_STATE["last_file_path"] or None,
        }


def _runtime_snapshot_text() -> str:
    s = _runtime_snapshot_dict()
    return (
        f"stage={s['stage']} loop={s['loop']} "
        f"last_batch={s['last_batch_idx']}/{s['last_batch_total']} "
        f"source={s['last_batch_source']} batch_rows={s['last_batch_rows']:,} "
        f"total_attempted_rows={s['total_attempted_rows']:,} stall_sec={s['stall_sec']} "
        f"msg={s['last_message']}"
    )


def _watchdog_should_skip_self_exit(stage: str, message: str) -> bool:
    stage_l = (stage or "").lower()
    message_l = (message or "").lower()
    return (
        stage_l.startswith("db_reconnect")
        or "db reconnect" in message_l
        or "engine create/connect attempt" in message_l
    )


def _watchdog_should_skip_for_active_batch(snap: dict) -> bool:
    stage_l = str(snap.get("stage", "")).lower()
    started_ts = float(snap.get("last_batch_started_ts") or 0.0)
    finished_ts = float(snap.get("last_batch_finished_ts") or 0.0)
    if not stage_l.startswith("insert_batch"):
        return False
    if started_ts <= 0 or finished_ts >= started_ts:
        return False
    running_sec = time_mod.time() - started_ts
    return running_sec < ACTIVE_BATCH_GRACE_SEC


def _dump_all_threads(reason: str):
    try:
        _write_crash_dump_line(f"\n===== {_ts()} DUMP START reason={reason} pid={PID} host={HOSTNAME} =====")
        _write_crash_dump_line(f"snapshot={_runtime_snapshot_text()}")
        frames = sys._current_frames()
        for th in threading.enumerate():
            _write_crash_dump_line(f"\n--- thread name={th.name} ident={th.ident} daemon={th.daemon} ---")
            frame = frames.get(th.ident)
            if frame is None:
                _write_crash_dump_line("(no frame)")
                continue
            _write_crash_dump_line("".join(traceback.format_stack(frame)).rstrip())
        _write_crash_dump_line(f"===== {_ts()} DUMP END reason={reason} =====\n")
    except Exception:
        pass


def _install_fault_handlers():
    global _FAULT_FH

    _ensure_local_log_dir()
    try:
        _FAULT_FH = open(CRASH_DUMP_FILE, "a", encoding="utf-8")
        _FAULT_FH.write(f"\n===== {_ts()} faulthandler enabled pid={PID} host={HOSTNAME} =====\n")
        _FAULT_FH.flush()
        faulthandler.enable(file=_FAULT_FH, all_threads=True)

        for sig_name in ("SIGABRT", "SIGSEGV", "SIGFPE", "SIGILL"):
            sig_obj = getattr(signal, sig_name, None)
            if sig_obj is not None:
                try:
                    faulthandler.register(sig_obj, file=_FAULT_FH, all_threads=True, chain=True)
                except Exception:
                    pass
    except Exception as e:
        _write_local_log(f"[{_ts()}] [WARN] faulthandler enable failed: {type(e).__name__}: {e}")

    def _threading_excepthook(args):
        try:
            msg = (
                f"[THREAD][UNHANDLED] name={getattr(args.thread, 'name', '?')} "
                f"type={getattr(args.exc_type, '__name__', str(args.exc_type))} "
                f"value={repr(args.exc_value)}"
            )
            _write_local_log(f"[{_ts()}] {msg}")
            _write_crash_dump_line(f"[{_ts()}] {msg}")
            tb = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
            _write_crash_dump_line(tb.rstrip())
            try:
                _enqueue_db_log("error", msg[:DB_LOG_CONTENTS_MAXLEN])
            except Exception:
                pass
        except Exception:
            pass

    try:
        threading.excepthook = _threading_excepthook  # type: ignore
    except Exception:
        pass

    def _signal_handler(signum, frame):
        sig_name = None
        for name in dir(signal):
            if name.startswith("SIG") and not name.startswith("SIG_"):
                if getattr(signal, name, None) == signum:
                    sig_name = name
                    break
        sig_label = sig_name or str(signum)
        msg = f"[SIGNAL] received signal={sig_label}({signum}) snapshot={_runtime_snapshot_text()}"
        _write_local_log(f"[{_ts()}] {msg}")
        _write_crash_dump_line(f"[{_ts()}] {msg}")
        try:
            stack_text = "".join(traceback.format_stack(frame)) if frame is not None else "(no frame)"
            _write_crash_dump_line(stack_text.rstrip())
        except Exception:
            pass
        _dump_all_threads(f"signal_{sig_label}")
        STOP_EVENT.set()
        raise SystemExit(128 + int(signum))

    for sig_name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        sig_obj = getattr(signal, sig_name, None)
        if sig_obj is not None:
            try:
                signal.signal(sig_obj, _signal_handler)
            except Exception:
                pass


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, OperationalError):
        return True
    if isinstance(e, DBAPIError) and getattr(e, "connection_invalidated", False):
        return True
    if psycopg2 is not None and isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True

    msg = (str(e) or "").lower()
    keywords = [
        "server closed the connection",
        "connection not open",
        "terminating connection",
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "ssl connection has been closed",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
        "could not receive data from server",
        "could not send data to server",
        "admin shutdown",
        "queuepool limit",
        "'nonetype' object has no attribute 'connect'",
        "none.connect",
        "insert engine is none",
    ]
    return any(k in msg for k in keywords)


def _parse_time_to_seconds(t_str: str) -> float:
    hh = int(t_str[0:2])
    mm = int(t_str[3:5])
    ss = float(t_str[6:])
    return hh * 3600.0 + mm * 60.0 + ss


def _round_to_hms(t_str: str) -> dt_time:
    sec = _parse_time_to_seconds(t_str)
    sec_rounded = int(sec + 0.5) % (24 * 3600)
    hh = sec_rounded // 3600
    mm = (sec_rounded % 3600) // 60
    ss = sec_rounded % 60
    return dt_time(hour=hh, minute=mm, second=ss)


def _file_info_from_filename(fp: Path) -> Optional[Tuple[str, date, str, datetime]]:
    m = FNAME_RE.match(fp.name)
    if not m:
        return None

    barcode = m.group(1).strip()
    yyyymmdd = m.group(2).strip()
    hhmmss_raw = m.group(3).strip()
    hhmmss = hhmmss_raw.split(".")[0]

    try:
        base_day = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        start_dt = datetime.strptime(yyyymmdd + hhmmss, "%Y%m%d%H%M%S")
    except Exception:
        return None

    run_id = f"{barcode}_{yyyymmdd}_{hhmmss}"
    return run_id, base_day, hhmmss, start_dt


def _infer_remark_strict(file_path: Path) -> Optional[str]:
    parts_upper = [p.upper() for p in file_path.parts]
    if any("PD NONE" in p for p in parts_upper):
        return "Non-PD"
    if any("PD" in p for p in parts_upper):
        return "PD"
    return None


def _safe_read_lines(path: Path) -> List[str]:
    encodings = ["cp949", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return f.read().splitlines()
        except Exception:
            pass
    with open(path, "r", encoding="cp949", errors="replace") as f:
        return f.read().splitlines()


def _day_dir(day: date) -> Path:
    return BASE_DIR / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}"


def _chunks(seq: List[tuple], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _enqueue_db_log(info: str, contents: str):
    now = _now_kst()
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": _normalize_info(info),
        "contents": (contents or "")[:DB_LOG_CONTENTS_MAXLEN],
    }
    try:
        db_log_queue.put_nowait(row)
    except queue.Full:
        line = f"[{_ts()}] [WARN] db_log_queue full. health log dropped."
        print(line, flush=True)
        _write_local_log(line)
        _write_crash_dump_line(line)


def log(msg: str, info: str | None = None):
    line = f"[{_ts()}] [PID:{PID}] [HOST:{HOSTNAME}] {msg}"
    print(line, flush=True)
    _write_local_log(line)
    try:
        tag = _normalize_info(info) if info else _infer_info_from_msg(msg)
        _enqueue_db_log(tag, msg)
    except Exception:
        pass


def log_exc(prefix: str, e: Exception):
    log(f"{prefix}: {type(e).__name__}: {repr(e)}", info="error")
    _write_crash_dump_line(f"[{_ts()}] {prefix}: {type(e).__name__}: {repr(e)}")
    tb = traceback.format_exc()
    for line in tb.rstrip().splitlines()[:200]:
        log(f"{prefix} TRACE: {line}", info="error")
        _write_crash_dump_line(line)


def _build_engine(application_name: str, cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?connect_timeout=5"
    options = (
        f"-c work_mem={WORK_MEM} "
        f"-c statement_timeout={STMT_TIMEOUT_MS} "
        f"-c lock_timeout={LOCK_TIMEOUT_MS} "
        f"-c idle_in_transaction_session_timeout={IDLE_TX_TIMEOUT_MS}"
    )

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        isolation_level="AUTOCOMMIT",
        connect_args={
            "connect_timeout": 5,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": application_name,
            "options": options,
        },
    )


def _dispose_engine_insert():
    global _ENGINE_INSERT
    with ENGINE_INSERT_LOCK:
        try:
            if _ENGINE_INSERT is not None:
                _ENGINE_INSERT.dispose()
        except Exception:
            pass
        _ENGINE_INSERT = None


def _dispose_engine_watchdog():
    global _ENGINE_WATCHDOG
    with ENGINE_WATCHDOG_LOCK:
        try:
            if _ENGINE_WATCHDOG is not None:
                _ENGINE_WATCHDOG.dispose()
        except Exception:
            pass
        _ENGINE_WATCHDOG = None


def _dispose_engine_health():
    global _ENGINE_HEALTH
    with ENGINE_HEALTH_LOCK:
        try:
            if _ENGINE_HEALTH is not None:
                _ENGINE_HEALTH.dispose()
        except Exception:
            pass
        _ENGINE_HEALTH = None


def _get_engine_ref(engine_name: str):
    if engine_name == "insert":
        return _ENGINE_INSERT
    if engine_name == "watchdog":
        return _ENGINE_WATCHDOG
    if engine_name == "health":
        return _ENGINE_HEALTH
    raise ValueError(f"unknown engine_name={engine_name}")


def _set_engine_ref(engine_name: str, engine):
    global _ENGINE_INSERT, _ENGINE_WATCHDOG, _ENGINE_HEALTH
    if engine_name == "insert":
        _ENGINE_INSERT = engine
        return
    if engine_name == "watchdog":
        _ENGINE_WATCHDOG = engine
        return
    if engine_name == "health":
        _ENGINE_HEALTH = engine
        return
    raise ValueError(f"unknown engine_name={engine_name}")


def _get_engine_blocking(engine_name: str, app_name: str, lock: threading.RLock, touch_progress: bool = False):
    attempt = 0

    while True:
        with lock:
            engine = _get_engine_ref(engine_name)

            if engine is not None:
                if touch_progress:
                    _mark_progress(f"{engine_name} engine reused")
                return engine

            attempt += 1
            new_engine = None
            try:
                if engine_name == "insert":
                    _set_stage("db_reconnect_insert", f"{engine_name} engine create/connect attempt={attempt}")
                    _mark_progress(f"db reconnect {engine_name} attempt={attempt}")
                log(f"[DB][TRY] {engine_name} engine create/connect attempt={attempt}", info="info")
                new_engine = _build_engine(app_name)
                with new_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                _set_engine_ref(engine_name, new_engine)
                if touch_progress:
                    _mark_progress(f"{engine_name} engine connected")
                log(
                    f"[DB][OK] {engine_name} engine ready app={app_name} "
                    f"pool_size=1 max_overflow=0 work_mem={WORK_MEM} "
                    f"stmt_timeout_ms={STMT_TIMEOUT_MS} lock_timeout_ms={LOCK_TIMEOUT_MS} "
                    f"idle_tx_timeout_ms={IDLE_TX_TIMEOUT_MS}",
                    info="boot",
                )
                return new_engine
            except Exception as e:
                if engine_name == "insert":
                    _set_stage("db_reconnect_insert", f"{engine_name} engine create/connect failed attempt={attempt}")
                    _mark_progress(f"db reconnect {engine_name} failed attempt={attempt}")
                log(f"[DB][RETRY] {engine_name} engine create/connect failed", info="down")
                log_exc(f"[DB][RETRY] {engine_name} connect error", e)
                try:
                    if new_engine is not None:
                        new_engine.dispose()
                except Exception:
                    pass
                _set_engine_ref(engine_name, None)

        time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def get_engine_insert_blocking(touch_progress: bool = True):
    return _get_engine_blocking("insert", INSERT_APP_NAME, ENGINE_INSERT_LOCK, touch_progress=touch_progress)


def get_engine_watchdog_blocking():
    return _get_engine_blocking("watchdog", WATCHDOG_APP_NAME, ENGINE_WATCHDOG_LOCK, touch_progress=False)


def get_engine_health_blocking():
    return _get_engine_blocking("health", HEALTH_APP_NAME, ENGINE_HEALTH_LOCK, touch_progress=False)


def _save_cursor_db(engine, run_id: str, end_day: date, end_time_obj: dt_time, file_path: str):
    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{CURSOR_TABLE}
        (daemon_name, run_id, end_day, end_time, file_path, updated_at)
        VALUES (:daemon_name, :run_id, :end_day, :end_time, :file_path, now())
        ON CONFLICT (daemon_name) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            end_day = EXCLUDED.end_day,
            end_time = EXCLUDED.end_time,
            file_path = EXCLUDED.file_path,
            updated_at = now()
        """
    )
    params = {
        "daemon_name": DAEMON_NAME,
        "run_id": run_id,
        "end_day": end_day,
        "end_time": end_time_obj,
        "file_path": file_path,
    }

    while True:
        try:
            if engine is None:
                engine = get_engine_insert_blocking()
            with engine.connect() as conn:
                conn.execute(sql, params)
            with STATE_LOCK:
                RUNTIME_STATE["last_run_id"] = run_id
                RUNTIME_STATE["last_file_path"] = file_path
                RUNTIME_STATE["last_end_day"] = end_day.strftime("%Y-%m-%d")
                RUNTIME_STATE["last_end_time"] = end_time_obj.strftime("%H:%M:%S")
            log(f"[INFO] cursor saved run_id={run_id} end_day={end_day} end_time={end_time_obj} file={file_path}", info="info")
            _mark_progress("cursor saved")
            return
        except Exception as e:
            if _is_connection_error(e):
                _dispose_engine_insert()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_insert_blocking()
                continue
            log_exc("save_cursor_db", e)
            raise


def _load_cursor_db(engine) -> dict:
    sql = text(
        f"""
        SELECT run_id, end_day, end_time, file_path, updated_at
        FROM {SCHEMA_NAME}.{CURSOR_TABLE}
        WHERE daemon_name = :daemon_name
        """
    )
    while True:
        try:
            if engine is None:
                engine = get_engine_insert_blocking()
            with engine.connect() as conn:
                row = conn.execute(sql, {"daemon_name": DAEMON_NAME}).fetchone()
            if not row:
                return {}
            return {
                "run_id": row[0],
                "end_day": row[1],
                "end_time": row[2],
                "file_path": row[3],
                "updated_at": row[4],
            }
        except Exception as e:
            if _is_connection_error(e):
                _dispose_engine_insert()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_insert_blocking()
                continue
            log_exc("load_cursor_db", e)
            raise


def _save_watchdog_db(status: str):
    snap = _runtime_snapshot_dict()
    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{WATCHDOG_TABLE}
        (daemon_name, status, stage, last_message, stall_sec, total_attempted_rows, run_id, end_day, end_time, file_path, updated_at)
        VALUES
        (:daemon_name, :status, :stage, :last_message, :stall_sec, :total_attempted_rows, :run_id, :end_day, :end_time, :file_path, now())
        ON CONFLICT (daemon_name) DO UPDATE SET
            status = EXCLUDED.status,
            stage = EXCLUDED.stage,
            last_message = EXCLUDED.last_message,
            stall_sec = EXCLUDED.stall_sec,
            total_attempted_rows = EXCLUDED.total_attempted_rows,
            run_id = EXCLUDED.run_id,
            end_day = EXCLUDED.end_day,
            end_time = EXCLUDED.end_time,
            file_path = EXCLUDED.file_path,
            updated_at = now()
        """
    )
    params = {
        "daemon_name": DAEMON_NAME,
        "status": status,
        "stage": snap["stage"],
        "last_message": snap["last_message"],
        "stall_sec": snap["stall_sec"],
        "total_attempted_rows": snap["total_attempted_rows"],
        "run_id": snap["run_id"],
        "end_day": snap["end_day"],
        "end_time": snap["end_time"],
        "file_path": snap["file_path"],
    }

    while True:
        try:
            engine = get_engine_watchdog_blocking()
            with engine.connect() as conn:
                conn.execute(sql, params)
            return
        except Exception as e:
            if _is_connection_error(e):
                _dispose_engine_watchdog()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            _write_local_log(f"[{_ts()}] [WARN] save_watchdog_db failed: {type(e).__name__}: {e}")
            _write_crash_dump_line(f"[{_ts()}] [WARN] save_watchdog_db failed: {type(e).__name__}: {e}")
            return


def _cursor_resume_from(engine, now_dt: datetime) -> datetime:
    cur = _load_cursor_db(engine)
    if not cur:
        return datetime(now_dt.year, now_dt.month, now_dt.day, 0, 0, 0)

    raw_day = cur.get("end_day")
    raw_time = cur.get("end_time")
    if not raw_day or not raw_time:
        return datetime(now_dt.year, now_dt.month, now_dt.day, 0, 0, 0)

    try:
        if isinstance(raw_day, date) and isinstance(raw_time, dt_time):
            last_dt = datetime.combine(raw_day, raw_time)
        else:
            last_dt = datetime.strptime(f"{raw_day} {raw_time}", "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime(now_dt.year, now_dt.month, now_dt.day, 0, 0, 0)

    resume_from = last_dt - timedelta(minutes=CURSOR_LOOKBACK_MIN)
    today_start = datetime(now_dt.year, now_dt.month, now_dt.day, 0, 0, 0)
    return resume_from if resume_from >= today_start else today_start


class HealthLogWorker:
    def __init__(self):
        self.stop_event = STOP_EVENT
        self.thread = threading.Thread(target=self._run, name="HealthLogWorker", daemon=True)
        self._local_buffer: list[dict] = []

    def start(self):
        self.thread.start()

    def join(self, timeout: float | None = None):
        self.thread.join(timeout=timeout)

    def _flush_batch(self, batch: list[dict]):
        if not batch:
            return

        while True:
            try:
                engine = get_engine_health_blocking()
                with engine.connect() as conn:
                    dbapi_conn = getattr(conn.connection, "driver_connection", None)
                    if dbapi_conn is not None and psycopg2 is not None:
                        cur = dbapi_conn.cursor()
                        sql = f"""
                            INSERT INTO {LOG_SCHEMA}.{LOG_TABLE}
                            (end_day, end_time, info, contents)
                            VALUES %s
                        """
                        values = [(r["end_day"], r["end_time"], r["info"], r["contents"]) for r in batch]
                        psycopg2.extras.execute_values(cur, sql, values, page_size=min(1000, len(values)))  # type: ignore
                    else:
                        conn.execute(
                            text(
                                f"""
                                INSERT INTO {LOG_SCHEMA}.{LOG_TABLE}
                                (end_day, end_time, info, contents)
                                VALUES (:end_day, :end_time, :info, :contents)
                                """
                            ),
                            batch,
                        )
                return
            except Exception as e:
                _write_local_log(f"[{_ts()}] [WARN] HealthLogWorker flush failed: {type(e).__name__}: {e}")
                _write_crash_dump_line(f"[{_ts()}] [WARN] HealthLogWorker flush failed: {type(e).__name__}: {e}")
                if _is_connection_error(e):
                    _dispose_engine_health()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    def _run(self):
        last_flush_ts = time_mod.time()
        while not self.stop_event.is_set():
            try:
                timeout = max(0.2, DB_LOG_FLUSH_INTERVAL_SEC / 2.0)
                try:
                    item = db_log_queue.get(timeout=timeout)
                    self._local_buffer.append(item)
                except queue.Empty:
                    pass

                while len(self._local_buffer) < DB_LOG_BATCH_SIZE:
                    try:
                        self._local_buffer.append(db_log_queue.get_nowait())
                    except queue.Empty:
                        break

                now_ts = time_mod.time()
                need_flush = (
                    len(self._local_buffer) >= DB_LOG_BATCH_SIZE
                    or (self._local_buffer and (now_ts - last_flush_ts) >= DB_LOG_FLUSH_INTERVAL_SEC)
                )

                if need_flush:
                    batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                    del self._local_buffer[:len(batch)]
                    self._flush_batch(batch)
                    last_flush_ts = now_ts
            except Exception as e:
                _write_local_log(f"[{_ts()}] [ERROR] HealthLogWorker run loop failed: {type(e).__name__}: {e}")
                _write_crash_dump_line(traceback.format_exc().rstrip())
                time_mod.sleep(1.0)

        try:
            while True:
                try:
                    self._local_buffer.append(db_log_queue.get_nowait())
                except queue.Empty:
                    break
            while self._local_buffer:
                batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                del self._local_buffer[:len(batch)]
                self._flush_batch(batch)
        except Exception as e:
            _write_local_log(f"[{_ts()}] [WARN] HealthLogWorker final flush failed: {type(e).__name__}: {e}")
            _write_crash_dump_line(traceback.format_exc().rstrip())


class MainHeartbeatWorker:
    def __init__(self):
        self.stop_event = STOP_EVENT
        self.thread = threading.Thread(target=self._run, name="MainHeartbeatWorker", daemon=True)

    def start(self):
        self.thread.start()

    def join(self, timeout: float | None = None):
        self.thread.join(timeout=timeout)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                log(f"[HEARTBEAT] {_runtime_snapshot_text()}", info="info")
                _save_watchdog_db("alive")
            except Exception as e:
                _write_local_log(f"[{_ts()}] [WARN] MainHeartbeatWorker failed: {type(e).__name__}: {e}")
                _write_crash_dump_line(traceback.format_exc().rstrip())

            for _ in range(max(1, HEARTBEAT_INTERVAL_SEC)):
                if self.stop_event.is_set():
                    break
                time_mod.sleep(1)


class StallWatchdogWorker:
    def __init__(self):
        self.stop_event = STOP_EVENT
        self.thread = threading.Thread(target=self._run, name="StallWatchdogWorker", daemon=True)

    def start(self):
        self.thread.start()

    def join(self, timeout: float | None = None):
        self.thread.join(timeout=timeout)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                snap = _runtime_snapshot_dict()
                stall_sec = int(snap["stall_sec"])
                if WATCHDOG_SELF_EXIT_SEC > 0 and stall_sec >= WATCHDOG_SELF_EXIT_SEC:
                    if _watchdog_should_skip_self_exit(str(snap.get("stage", "")), str(snap.get("last_message", ""))):
                        try:
                            _save_watchdog_db("db_reconnect_wait")
                        except Exception:
                            pass
                        _write_local_log(
                            f"[{_ts()}] [WATCHDOG][SKIP] db reconnect in progress. "
                            f"stall_sec={stall_sec} snapshot={_runtime_snapshot_text()}"
                        )
                        _write_crash_dump_line(
                            f"[{_ts()}] [WATCHDOG][SKIP] db reconnect in progress. "
                            f"stall_sec={stall_sec} snapshot={_runtime_snapshot_text()}"
                        )
                        _mark_progress("watchdog skipped self-exit during db reconnect")
                        continue
                    if _watchdog_should_skip_for_active_batch(snap):
                        batch_running_sec = int(time_mod.time() - float(snap.get("last_batch_started_ts") or 0.0))
                        try:
                            _save_watchdog_db("active_batch_wait")
                        except Exception:
                            pass
                        _write_local_log(
                            f"[{_ts()}] [WATCHDOG][SKIP] active insert batch still running. "
                            f"batch_running_sec={batch_running_sec} snapshot={_runtime_snapshot_text()}"
                        )
                        _write_crash_dump_line(
                            f"[{_ts()}] [WATCHDOG][SKIP] active insert batch still running. "
                            f"batch_running_sec={batch_running_sec} snapshot={_runtime_snapshot_text()}"
                        )
                        continue
                    confirm_snap = _runtime_snapshot_dict()
                    confirm_stall_sec = int(confirm_snap["stall_sec"])
                    if confirm_stall_sec < WATCHDOG_SELF_EXIT_SEC:
                        _write_local_log(
                            f"[{_ts()}] [WATCHDOG][CANCEL] progress recovered before self-exit. "
                            f"stall_sec={confirm_stall_sec} snapshot={_runtime_snapshot_text()}"
                        )
                        _write_crash_dump_line(
                            f"[{_ts()}] [WATCHDOG][CANCEL] progress recovered before self-exit. "
                            f"stall_sec={confirm_stall_sec} snapshot={_runtime_snapshot_text()}"
                        )
                        continue
                    if _watchdog_should_skip_self_exit(str(confirm_snap.get("stage", "")), str(confirm_snap.get("last_message", ""))):
                        _write_local_log(
                            f"[{_ts()}] [WATCHDOG][CANCEL] db reconnect state confirmed on recheck. "
                            f"snapshot={_runtime_snapshot_text()}"
                        )
                        _write_crash_dump_line(
                            f"[{_ts()}] [WATCHDOG][CANCEL] db reconnect state confirmed on recheck. "
                            f"snapshot={_runtime_snapshot_text()}"
                        )
                        continue
                    if _watchdog_should_skip_for_active_batch(confirm_snap):
                        _write_local_log(
                            f"[{_ts()}] [WATCHDOG][CANCEL] active insert batch confirmed on recheck. "
                            f"snapshot={_runtime_snapshot_text()}"
                        )
                        _write_crash_dump_line(
                            f"[{_ts()}] [WATCHDOG][CANCEL] active insert batch confirmed on recheck. "
                            f"snapshot={_runtime_snapshot_text()}"
                        )
                        continue
                    try:
                        _save_watchdog_db("stalled_exit")
                    except Exception:
                        pass
                    _dump_all_threads("stall_watchdog_exit")
                    log(f"[EXIT] watchdog self-exit triggered no_work_progress_sec={stall_sec} snapshot={_runtime_snapshot_text()}", info="error")
                    try:
                        _dispose_engine_insert()
                        _dispose_engine_watchdog()
                        _dispose_engine_health()
                    except Exception:
                        pass
                    os._exit(3)
            except Exception as e:
                _write_local_log(f"[{_ts()}] [WARN] StallWatchdogWorker failed: {type(e).__name__}: {e}")
                _write_crash_dump_line(traceback.format_exc().rstrip())

            for _ in range(5):
                if self.stop_event.is_set():
                    break
                time_mod.sleep(1)


def _fail_fast_if_base_dir_missing():
    try:
        exists = BASE_DIR.exists()
    except OSError as e:
        log(f"[ERROR] BASE_DIR access error. stop. path={BASE_DIR} err={type(e).__name__}: {e}", info="error")
        raise SystemExit(1)

    if not exists:
        log(f"[ERROR] BASE_DIR not found. stop. path={BASE_DIR}", info="error")
        raise SystemExit(1)

    log(f"[INFO] BASE_DIR exists: {BASE_DIR}", info="info")


def _load_processed_run_sizes(_engine) -> Dict[str, int]:
    log("[INFO] skip load_processed_run_sizes. start with empty processed_run_size map", info="info")
    _mark_progress("skip load_processed_run_sizes")
    return {}


def _update_stable_state(path_str: str) -> Tuple[Optional[int], int]:
    p = Path(path_str)
    try:
        size = int(p.stat().st_size)
    except Exception:
        return None, 0

    last_size, stable = FILE_STATE.get(path_str, (None, 0))
    if last_size is None:
        FILE_STATE[path_str] = (size, 0)
        return size, 0

    stable = stable + 1 if size == last_size else 0
    FILE_STATE[path_str] = (size, stable)
    return size, stable


def _collect_candidates_between(start_dt: datetime, end_dt: datetime) -> Dict[str, List[str]]:
    day_set = set()
    cur_day = start_dt.date()
    while cur_day <= end_dt.date():
        day_set.add(cur_day)
        cur_day += timedelta(days=1)

    out: Dict[str, List[str]] = {}

    for day in day_set:
        day_dir = _day_dir(day)
        try:
            if not day_dir.exists():
                continue
            for idx, fp in enumerate(day_dir.rglob("*.txt"), start=1):
                if idx % 1000 == 0:
                    _mark_progress(f"scanning day_dir={day_dir} scanned={idx}")
                if not fp.is_file():
                    continue
                info = _file_info_from_filename(fp)
                if info is None:
                    continue
                run_id, _, _, file_start_dt = info
                if start_dt <= file_start_dt <= end_dt:
                    out.setdefault(run_id, []).append(str(fp))
        except OSError as e:
            log(f"[ERROR] rglob access error: {day_dir} | {type(e).__name__}: {e}", info="error")
            raise

    return out


def _collect_realtime_candidates(now_dt: datetime, window_sec: int) -> Dict[str, List[str]]:
    return _collect_candidates_between(now_dt - timedelta(seconds=window_sec), now_dt)


def _choose_best_path_for_run(paths: List[str], allow_unstable: bool = False) -> Optional[str]:
    best_path = None
    best_tuple = None
    for p in paths:
        size, stable = _update_stable_state(p)
        if size is None:
            continue
        stable_flag = 1 if (allow_unstable or stable >= STABLE_REQUIRED) else 0
        cand = (stable_flag, size)
        if best_tuple is None or cand > best_tuple:
            best_tuple = cand
            best_path = p
    if best_tuple is None or best_tuple[0] <= 0:
        return None
    return best_path


def _parse_one_file(path_str: str, run_id: str, base_day: date, start_dt: datetime):
    p = Path(path_str)
    remark = _infer_remark_strict(p)
    if remark is None:
        return path_str, run_id, base_day, start_dt, [], None, None, "SKIP_REMARK"

    m = FNAME_RE.match(p.name)
    if not m:
        return path_str, run_id, base_day, start_dt, [], None, None, "SKIP_BADNAME"

    barcode = m.group(1).strip()
    try:
        lines = _safe_read_lines(p)
    except Exception:
        return path_str, run_id, base_day, start_dt, [], None, None, "ERROR"

    parsed_times: List[str] = []
    parsed_contents: List[str] = []
    for line in lines:
        mm2 = LINE_RE.match(line)
        if not mm2:
            continue
        t_str = mm2.group(1).strip()
        content = mm2.group(2).strip()
        if not content:
            continue
        parsed_times.append(t_str[:12])
        parsed_contents.append(content[:80])

    if not parsed_times:
        return path_str, run_id, base_day, start_dt, [], None, None, "SKIP_EMPTY"

    end_time_obj = _round_to_hms(parsed_times[-1])
    first_sec = _parse_time_to_seconds(parsed_times[0])
    last_sec = _parse_time_to_seconds(parsed_times[-1])
    end_day = base_day + timedelta(days=1) if last_sec < first_sec else base_day

    rows = []
    prev_sec = None
    for t_str, content in zip(parsed_times, parsed_contents):
        cur_sec = _parse_time_to_seconds(t_str)
        test_ct = None
        if prev_sec is not None:
            diff = cur_sec - prev_sec
            if diff < 0:
                diff += 86400.0
            test_ct = diff
        prev_sec = cur_sec
        rows.append((barcode, remark, end_day, end_time_obj, content, test_ct, t_str, str(p), run_id))

    return path_str, run_id, base_day, start_dt, rows, end_day, end_time_obj, "OK"


def _prepare_tasks(cand_map: Dict[str, List[str]], processed_run_size: Dict[str, int], allow_unstable: bool = False):
    ready_tasks: List[Tuple[str, str, date, datetime]] = []
    ready_runs = 0
    skipped_not_stable = 0
    skipped_already_done = 0

    for idx, (run_id, paths) in enumerate(cand_map.items(), start=1):
        if idx % 500 == 0:
            _mark_progress(f"prepare_tasks processed_runs={idx}")

        best_path = _choose_best_path_for_run(paths, allow_unstable=allow_unstable)
        if best_path is None:
            skipped_not_stable += 1
            continue

        size, stable = _update_stable_state(best_path)
        if size is None:
            skipped_not_stable += 1
            continue
        if (not allow_unstable) and stable < STABLE_REQUIRED:
            skipped_not_stable += 1
            continue

        prev_size = processed_run_size.get(run_id, -1)
        if prev_size >= 0 and size <= prev_size:
            skipped_already_done += 1
            continue

        info = _file_info_from_filename(Path(best_path))
        if info is None:
            continue

        _, base_day, _, start_dt = info
        ready_tasks.append((best_path, run_id, base_day, start_dt))
        ready_runs += 1

    ready_tasks.sort(key=lambda x: (x[3], x[1], x[0]))
    return ready_tasks, ready_runs, skipped_not_stable, skipped_already_done


def _bulk_insert_execute_values(engine, rows: List[tuple]):
    sql = f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
        (barcode_information, remark, end_day, end_time, contents, test_ct, test_time, file_path, run_id)
        VALUES %s
        ON CONFLICT (run_id, test_time, contents) DO NOTHING
    """

    while True:
        try:
            if engine is None:
                raise OperationalError("insert engine is None", None, None)

            with engine.connect() as conn:
                dbapi_conn = getattr(conn.connection, "driver_connection", None)
                if dbapi_conn is None or psycopg2 is None:
                    raise RuntimeError("driver_connection unavailable for execute_values")
                cur = dbapi_conn.cursor()
                psycopg2.extras.execute_values(cur, sql, rows, page_size=min(len(rows), 1000))  # type: ignore
            return

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] bulk insert conn error -> reconnect insert engine", info="down")
                log_exc("[DB][RETRY] bulk insert", e)
                _dispose_engine_insert()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_insert_blocking()
                continue
            raise


def _insert_rows(engine, rows: List[tuple], source_label: str, total_attempted_before: int, batch_size: int) -> int:
    if not rows:
        return 0

    total = len(rows)
    batches = (total + batch_size - 1) // batch_size
    inserted_total = 0

    log(f"[INFO] insert_rows start source={source_label} total_rows={total:,} batch_size={batch_size} batches={batches}", info="info")

    for idx, batch in enumerate(_chunks(rows, batch_size), start=1):
        while True:
            try:
                if engine is None:
                    engine = get_engine_insert_blocking()

                _set_stage("insert_batch", f"source={source_label} batch={idx}/{batches}")
                _set_batch_state(source_label, idx, batches, len(batch))
                log(f"[INFO] insert_rows batch_start {idx}/{batches} source={source_label} attempted_rows={len(batch):,}", info="info")

                batch_t0 = time_mod.perf_counter()
                _bulk_insert_execute_values(engine, list(batch))
                batch_elapsed = time_mod.perf_counter() - batch_t0

                inserted_total += len(batch)
                total_now = total_attempted_before + inserted_total
                _finish_batch_state(total_now)
                _mark_progress(f"insert batch done source={source_label} batch={idx}/{batches}")

                log(f"[INFO] insert_rows batch {idx}/{batches} source={source_label} attempted_rows={len(batch):,} cumulative={inserted_total:,} batch_sec={batch_elapsed:.3f}", info="info")

                if batch_elapsed >= SLOW_BATCH_WARN_SEC:
                    log(f"[WARN] slow batch source={source_label} batch={idx}/{batches} batch_sec={batch_elapsed:.3f} rows={len(batch):,}", info="warn")
                break
            except Exception as e:
                if _is_connection_error(e):
                    log(f"insert conn error -> reconnect source={source_label} batch={idx}/{batches}", info="down")
                    log_exc("insert", e)
                    _dispose_engine_insert()
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine = get_engine_insert_blocking()
                    continue
                log_exc("insert", e)
                raise

    log(f"[INFO] insert_rows done source={source_label} total_attempted={inserted_total:,}", info="info")
    return inserted_total


def _flush_file_buffer(engine, buffer_rows: List[tuple], buffer_last_marker: Optional[Tuple[str, date, dt_time, str]],
                       source_label: str, total_attempted_before: int, batch_size: int) -> int:
    if not buffer_rows:
        return 0

    if engine is None:
        engine = get_engine_insert_blocking()

    attempted = _insert_rows(engine, buffer_rows, source_label, total_attempted_before, batch_size)

    if buffer_last_marker is not None:
        run_id, end_day, end_time_obj, file_path = buffer_last_marker
        _save_cursor_db(engine, run_id, end_day, end_time_obj, file_path)

    return attempted


def _run_task_batch(engine, ready_tasks: List[Tuple[str, str, date, datetime]], processed_run_size: Dict[str, int],
                    source_label: str, total_attempted_before: int, batch_size: int) -> int:
    ok_files = 0
    skip_remark = 0
    skip_badname = 0
    skip_empty = 0
    error_cnt = 0

    buffer_rows: List[tuple] = []
    buffer_last_marker: Optional[Tuple[str, date, dt_time, str]] = None
    attempted_total = 0

    _set_stage("parse_files", f"source={source_label} tasks={len(ready_tasks):,}")

    for idx, (best_path, run_id, base_day, start_dt) in enumerate(ready_tasks, start=1):
        if idx % 100 == 0:
            _mark_progress(f"parse_files source={source_label} idx={idx}/{len(ready_tasks)}")

        path_str, run_id2, _base_day2, _start_dt2, rows, parsed_end_day, parsed_end_time, status = _parse_one_file(
            best_path, run_id, base_day, start_dt
        )

        if status == "OK":
            ok_files += 1
            size, _ = _update_stable_state(path_str)
            processed_run_size[run_id2] = int(size) if size is not None else processed_run_size.get(run_id2, 0)

            if rows:
                if buffer_rows and (len(buffer_rows) + len(rows) > batch_size):
                    attempted = _flush_file_buffer(
                        engine, buffer_rows, buffer_last_marker, source_label,
                        total_attempted_before + attempted_total, batch_size
                    )
                    attempted_total += attempted
                    engine = get_engine_insert_blocking()
                    buffer_rows = []
                    buffer_last_marker = None

                buffer_rows.extend(rows)
                if parsed_end_day is not None and parsed_end_time is not None:
                    buffer_last_marker = (run_id2, parsed_end_day, parsed_end_time, path_str)

                _mark_progress(f"parsed file run_id={run_id2}")

        elif status == "SKIP_REMARK":
            skip_remark += 1
        elif status == "SKIP_BADNAME":
            skip_badname += 1
        elif status == "SKIP_EMPTY":
            skip_empty += 1
        else:
            error_cnt += 1

    if buffer_rows:
        attempted = _flush_file_buffer(
            engine, buffer_rows, buffer_last_marker, source_label,
            total_attempted_before + attempted_total, batch_size
        )
        attempted_total += attempted

    log(
        f"[INFO] {source_label} parse_result ok_files={ok_files:,} attempted_rows={attempted_total:,} "
        f"skip_remark={skip_remark:,} skip_badname={skip_badname:,} skip_empty={skip_empty:,} error={error_cnt:,}",
        info="info",
    )
    _mark_progress(f"{source_label} parse_result attempted_rows={attempted_total:,}")
    return attempted_total


def _startup_catchup(engine, processed_run_size: Dict[str, int], total_attempted_before: int) -> int:
    now_dt = datetime.now()
    resume_from = _cursor_resume_from(engine, now_dt)

    _set_stage("startup_catchup_scan", f"collecting catchup candidates from={resume_from} to={now_dt}")
    cand_map = _collect_candidates_between(resume_from, now_dt)
    _mark_progress(f"startup_catchup candidates={len(cand_map):,}")
    log(f"[INFO] startup_catchup resume_from={resume_from.strftime('%Y-%m-%d %H:%M:%S')} now={now_dt.strftime('%Y-%m-%d %H:%M:%S')} candidates={len(cand_map):,}", info="info")

    ready_tasks, ready_runs, skipped_not_ready, skipped_already_done = _prepare_tasks(
        cand_map, processed_run_size, allow_unstable=True
    )
    log(f"[INFO] startup_catchup ready_runs={ready_runs:,} skipped_not_ready={skipped_not_ready:,} already_done={skipped_already_done:,}", info="info")

    if not ready_tasks:
        return 0

    return _run_task_batch(
        engine, ready_tasks, processed_run_size, "startup_catchup",
        total_attempted_before, BACKFILL_INSERT_BATCH_SIZE
    )


def _on_exit():
    global _FAULT_FH
    try:
        _save_watchdog_db("exit")
        line = f"[{_ts()}] [PID:{PID}] [HOST:{HOSTNAME}] [EXIT] atexit called snapshot={_runtime_snapshot_text()}"
        print(line, flush=True)
        _write_local_log(line)
        _write_crash_dump_line(line)
    except Exception:
        pass

    try:
        if _FAULT_FH is not None:
            _FAULT_FH.flush()
            _FAULT_FH.close()
    except Exception:
        pass


atexit.register(_on_exit)


def main():
    _install_fault_handlers()
    _ensure_local_log_dir()

    log("[BOOT] c1_fct_detail_factory db-cursor+watchdog+bulk starting (engine separated)", info="boot")
    log(f"[INFO] HOSTNAME={HOSTNAME} PID={PID}", info="info")
    log(f"[INFO] LOCAL_LOG_FILE={LOCAL_LOG_FILE}", info="info")
    log(f"[INFO] CRASH_DUMP_FILE={CRASH_DUMP_FILE}", info="info")
    log(f"[INFO] MAIN DB={_masked_db_info(DB_CONFIG)}", info="info")
    log(f"[INFO] BASE_DIR={BASE_DIR}", info="info")
    log(f"[INFO] save={SCHEMA_NAME}.{TABLE_NAME}", info="info")
    log(f"[INFO] cursor_table={SCHEMA_NAME}.{CURSOR_TABLE}", info="info")
    log(f"[INFO] watchdog_table={SCHEMA_NAME}.{WATCHDOG_TABLE}", info="info")
    log(f"[INFO] health={LOG_SCHEMA}.{LOG_TABLE}", info="info")
    log(f"[INFO] work_mem={WORK_MEM}", info="info")
    log(f"[INFO] realtime_insert_batch_size={REALTIME_INSERT_BATCH_SIZE} backfill_insert_batch_size={BACKFILL_INSERT_BATCH_SIZE}", info="info")
    log(f"[INFO] heartbeat_interval_sec={HEARTBEAT_INTERVAL_SEC} watchdog_self_exit_sec={WATCHDOG_SELF_EXIT_SEC} cursor_lookback_min={CURSOR_LOOKBACK_MIN}", info="info")

    engine_insert = get_engine_insert_blocking()
    _ = get_engine_watchdog_blocking()
    _ = get_engine_health_blocking()
    _save_watchdog_db("booting")

    health_worker = HealthLogWorker()
    heartbeat_worker = MainHeartbeatWorker()
    stall_watchdog_worker: Optional[StallWatchdogWorker] = None

    health_worker.start()
    heartbeat_worker.start()

    log("[INFO] HealthLogWorker started", info="info")
    log("[INFO] MainHeartbeatWorker started", info="info")

    _fail_fast_if_base_dir_missing()

    log("[INFO] start load_processed_run_sizes", info="info")
    processed_run_size = _load_processed_run_sizes(engine_insert)
    log(f"[INFO] done load_processed_run_sizes count={len(processed_run_size):,}", info="info")

    total_attempted_rows = 0
    catchup_attempted = _startup_catchup(engine_insert, processed_run_size, total_attempted_rows)
    total_attempted_rows += catchup_attempted
    with STATE_LOCK:
        RUNTIME_STATE["total_attempted_rows"] = total_attempted_rows
    _mark_progress(f"startup_catchup attempted_rows={catchup_attempted:,}")
    log(f"[INFO] startup_catchup attempted_rows={catchup_attempted:,}", info="info")

    stall_watchdog_worker = StallWatchdogWorker()
    stall_watchdog_worker.start()
    log("[INFO] StallWatchdogWorker started", info="info")

    loop_count = 0

    try:
        while True:
            loop_t0 = time_mod.perf_counter()
            try:
                loop_count += 1
                with STATE_LOCK:
                    RUNTIME_STATE["last_loop"] = loop_count

                engine_insert = get_engine_insert_blocking()

                _set_stage("collect_candidates", f"loop={loop_count}")
                now_dt = datetime.now()
                cand_map = _collect_realtime_candidates(now_dt, CANDIDATE_WINDOW_SEC)

                if not cand_map:
                    _mark_progress(f"loop={loop_count} no candidates")
                    log(f"[SLEEP] loop={loop_count} no candidates | candidate_window_sec={CANDIDATE_WINDOW_SEC} sleep {LOOP_SLEEP_SEC}s", info="sleep")
                    time_mod.sleep(LOOP_SLEEP_SEC)
                    continue

                ready_tasks, ready_runs, skipped_not_stable, skipped_already_done = _prepare_tasks(
                    cand_map, processed_run_size, allow_unstable=False
                )

                if not ready_tasks:
                    _mark_progress(f"loop={loop_count} no ready tasks")
                    log(f"[SLEEP] loop={loop_count} no ready tasks | cand_runs={len(cand_map):,} not_stable={skipped_not_stable:,} already_done={skipped_already_done:,} sleep {LOOP_SLEEP_SEC}s", info="sleep")
                    time_mod.sleep(LOOP_SLEEP_SEC)
                    continue

                attempted = _run_task_batch(
                    engine_insert, ready_tasks, processed_run_size,
                    f"realtime_loop_{loop_count}", total_attempted_rows, REALTIME_INSERT_BATCH_SIZE
                )
                total_attempted_rows += attempted
                with STATE_LOCK:
                    RUNTIME_STATE["total_attempted_rows"] = total_attempted_rows

                loop_t1 = time_mod.perf_counter()
                _mark_progress(f"loop={loop_count} attempted_rows={attempted:,}")
                log(
                    f"[PERF] loop={loop_count} cand_runs={len(cand_map):,} ready_runs={ready_runs:,} "
                    f"attempted_rows={attempted:,} total_attempted_rows={total_attempted_rows:,} "
                    f"not_stable={skipped_not_stable:,} already_done={skipped_already_done:,} "
                    f"loop={loop_t1 - loop_t0:.3f}s qsize={db_log_queue.qsize()}",
                    info="perf",
                )

            except KeyboardInterrupt:
                log("[STOP] Interrupted by user", info="stop")
                break
            except SystemExit:
                raise
            except Exception as e:
                if _is_connection_error(e):
                    log("[DB][RETRY] loop-level conn error -> rebuild insert engine", info="down")
                    log_exc("[DB][RETRY] loop-level", e)
                    _dispose_engine_insert()
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine_insert = get_engine_insert_blocking()
                else:
                    log("[ERROR] Loop error continue", info="error")
                    log_exc("[ERROR] Loop error", e)

            elapsed = time_mod.perf_counter() - loop_t0
            time_mod.sleep(max(0.0, LOOP_SLEEP_SEC - elapsed))

    finally:
        STOP_EVENT.set()
        _dump_all_threads("main_finally")

        try:
            _save_watchdog_db("stopping")
        except Exception:
            pass

        try:
            heartbeat_worker.join(timeout=15.0)
            log("[INFO] MainHeartbeatWorker stopped", info="info")
        except Exception as e:
            log_exc("[WARN] MainHeartbeatWorker stop failed", e)

        try:
            if stall_watchdog_worker is not None:
                stall_watchdog_worker.join(timeout=15.0)
                log("[INFO] StallWatchdogWorker stopped", info="info")
        except Exception as e:
            log_exc("[WARN] StallWatchdogWorker stop failed", e)

        try:
            health_worker.join(timeout=15.0)
            log("[INFO] HealthLogWorker stopped", info="info")
        except Exception as e:
            log_exc("[WARN] HealthLogWorker stop failed", e)

        _dispose_engine_insert()
        _dispose_engine_watchdog()
        _dispose_engine_health()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[STOP] user interrupted Ctrl+C", info="stop")
    except SystemExit:
        raise
    except Exception as e:
        _dump_all_threads("top_level_unhandled")
        log("[UNHANDLED] fatal error", info="error")
        log_exc("[UNHANDLED]", e)
        raise
