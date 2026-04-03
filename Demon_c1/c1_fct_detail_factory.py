# -*- coding: utf-8 -*-
"""
c1_fct_detail_factory.py
============================================================
FCT Detail TXT Parser -> PostgreSQL 적재 (d4 스타일 운영본 최종)

핵심 특징
- multiprocessing 미사용
- SQLAlchemy engine 기반 main / health 분리
- advisory lock 유지(전용 psycopg2 세션)
- DDL 제거 (테이블/인덱스/컬럼은 이미 존재한다고 가정)
- BASE_DIR 없거나 접근 불가 시 ERROR 로그 후 즉시 종료
- DB 접속 실패/끊김 시 무한 재시도 후 엔진 재생성
- health log는 queue + worker thread 비동기 적재
- run_id 시작 로딩은 최근 N일만 조회
- 재기동 시 현재 날짜 폴더 전체 재스캔 후 UPSERT(기존 유지, 신규만 적재)
- INSERT는 100행 단위 chunk 저장 + batch 로그 출력
- AUTOCOMMIT 기반으로 idle in transaction 방지
"""

from __future__ import annotations

import os
import re
import time as time_mod
import queue
import threading
import traceback
import urllib.parse
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


# =========================================================
# 환경 / 상수
# =========================================================
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
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "c1_log"

CANDIDATE_WINDOW_SEC = int(os.getenv("C1_CANDIDATE_WINDOW_SEC", "3600"))
STABLE_REQUIRED = int(os.getenv("C1_STABLE_REQUIRED", "3"))
LOOP_SLEEP_SEC = int(os.getenv("C1_LOOP_SLEEP_SEC", "5"))
DB_RETRY_INTERVAL_SEC = int(os.getenv("DB_RETRY_INTERVAL_SEC", "5"))
RUNID_LOOKBACK_DAYS = int(os.getenv("C1_RUNID_LOOKBACK_DAYS", "3"))
INSERT_BATCH_SIZE = int(os.getenv("C1_INSERT_BATCH_SIZE", "100"))

WORK_MEM = os.getenv("PG_WORK_MEM", "8MB")
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

ADVISORY_LOCK_KEY1 = 91031
ADVISORY_LOCK_KEY2 = 1001
LOCK_APP_NAME = "c1_fct_detail_loader_factory_lock"
MAIN_APP_NAME = "c1_fct_detail_loader_factory"
HEALTH_APP_NAME = "c1_fct_detail_loader_factory_health"

LINE_RE = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{1,3})\]\s(.+)$")
FNAME_RE = re.compile(r"^(.*)_(\d{8})_(\d{6}(?:\.\d{1,3})?)\.txt$", re.IGNORECASE)

_ENGINE_MAIN = None
_ENGINE_HEALTH = None
_LOCK_CONN = None
_LOCK_HELD = False

FILE_STATE: Dict[str, Tuple[int, int]] = {}

db_log_queue: queue.Queue = queue.Queue(maxsize=max(1000, DB_LOG_QUEUE_MAX))
_db_log_enabled = True


# =========================================================
# 유틸
# =========================================================
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
    if "[stop]" in m:
        return "stop"
    if "sleep" in m:
        return "sleep"
    if "[warn]" in m:
        return "warn"
    if "[perf]" in m:
        return "perf"
    return "info"


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
    ]
    return any(k in msg for k in keywords)


def _now_kst() -> datetime:
    return datetime.now(tz=KST)


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


# =========================================================
# 로그
# =========================================================
def _enqueue_db_log(info: str, contents: str):
    global _db_log_enabled
    if not _db_log_enabled:
        return

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
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] [WARN] db_log_queue full. health log dropped.", flush=True)


def log(msg: str, info: str | None = None):
    ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        tag = _normalize_info(info) if info else _infer_info_from_msg(msg)
        _enqueue_db_log(tag, msg)
    except Exception:
        pass


def log_exc(prefix: str, e: Exception):
    log(f"{prefix}: {type(e).__name__}: {repr(e)}", info="error")
    tb = traceback.format_exc()
    for line in tb.rstrip().splitlines():
        log(f"{prefix} TRACE: {line}", info="error")


# =========================================================
# Engine / Advisory Lock
# =========================================================
def _build_engine(cfg=DB_CONFIG, application_name: str = MAIN_APP_NAME):
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


def _dispose_engine_main():
    global _ENGINE_MAIN
    try:
        if _ENGINE_MAIN is not None:
            _ENGINE_MAIN.dispose()
    except Exception:
        pass
    _ENGINE_MAIN = None


def _dispose_engine_health():
    global _ENGINE_HEALTH
    try:
        if _ENGINE_HEALTH is not None:
            _ENGINE_HEALTH.dispose()
    except Exception:
        pass
    _ENGINE_HEALTH = None


def _safe_close_lock_conn():
    global _LOCK_CONN, _LOCK_HELD
    try:
        if _LOCK_CONN is not None:
            _LOCK_CONN.close()
    except Exception:
        pass
    _LOCK_CONN = None
    _LOCK_HELD = False


def _acquire_lock_blocking_or_exit():
    global _LOCK_CONN, _LOCK_HELD

    if psycopg2 is None:
        log("[ERROR] psycopg2 import failed. advisory lock unavailable", info="error")
        raise SystemExit(1)

    while True:
        try:
            if _LOCK_CONN is not None and getattr(_LOCK_CONN, "closed", 1) == 0 and _LOCK_HELD:
                with _LOCK_CONN.cursor() as cur:
                    cur.execute("SELECT 1")
                return
        except Exception:
            _safe_close_lock_conn()

        try:
            _LOCK_CONN = psycopg2.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                dbname=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                connect_timeout=5,
                application_name=LOCK_APP_NAME,
                keepalives=PG_KEEPALIVES,
                keepalives_idle=PG_KEEPALIVES_IDLE,
                keepalives_interval=PG_KEEPALIVES_INTERVAL,
                keepalives_count=PG_KEEPALIVES_COUNT,
                options=(
                    f"-c work_mem={WORK_MEM} "
                    f"-c statement_timeout={STMT_TIMEOUT_MS} "
                    f"-c lock_timeout={LOCK_TIMEOUT_MS} "
                    f"-c idle_in_transaction_session_timeout={IDLE_TX_TIMEOUT_MS}"
                ),
            )
            _LOCK_CONN.autocommit = True
            with _LOCK_CONN.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (ADVISORY_LOCK_KEY1, ADVISORY_LOCK_KEY2))
                row = cur.fetchone()
            locked = bool(row[0]) if row else False
            if not locked:
                log(
                    f"[LOCK][EXIT] another instance is already running. lock=({ADVISORY_LOCK_KEY1},{ADVISORY_LOCK_KEY2})",
                    info="warn",
                )
                _safe_close_lock_conn()
                raise SystemExit(0)
            _LOCK_HELD = True
            log(f"[LOCK][OK] advisory lock acquired ({ADVISORY_LOCK_KEY1}, {ADVISORY_LOCK_KEY2})", info="boot")
            return
        except SystemExit:
            raise
        except Exception as e:
            log("[DB][RETRY] advisory lock connect/acquire failed", info="down")
            log_exc("[DB][RETRY] advisory lock", e)
            _safe_close_lock_conn()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def _release_lock():
    global _LOCK_HELD
    if _LOCK_CONN is None or not _LOCK_HELD:
        return
    try:
        with _LOCK_CONN.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s, %s)", (ADVISORY_LOCK_KEY1, ADVISORY_LOCK_KEY2))
            row = cur.fetchone()
        unlocked = bool(row[0]) if row else False
        log(f"[LOCK][REL] advisory lock released={unlocked}", info="stop")
    except Exception as e:
        log_exc("[LOCK][WARN] release failed", e)
    finally:
        _safe_close_lock_conn()


def get_engine_main_blocking():
    global _ENGINE_MAIN

    while _ENGINE_MAIN is not None:
        try:
            with _ENGINE_MAIN.connect() as conn:
                conn.execute(text("SELECT 1"))
            return _ENGINE_MAIN
        except Exception as e:
            log("[DB][RETRY] main engine ping failed -> rebuild", info="down")
            log_exc("[DB][RETRY] main ping error", e)
            _dispose_engine_main()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE_MAIN = _build_engine(DB_CONFIG, MAIN_APP_NAME)
            with _ENGINE_MAIN.connect() as conn:
                conn.execute(text("SELECT 1"))
            log(
                f"[DB][OK] main engine ready pool_size=1 max_overflow=0 work_mem={WORK_MEM} "
                f"stmt_timeout_ms={STMT_TIMEOUT_MS} lock_timeout_ms={LOCK_TIMEOUT_MS} idle_tx_timeout_ms={IDLE_TX_TIMEOUT_MS}",
                info="boot",
            )
            return _ENGINE_MAIN
        except Exception as e:
            log("[DB][RETRY] main engine create/connect failed", info="down")
            log_exc("[DB][RETRY] main connect error", e)
            _dispose_engine_main()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def get_engine_health_blocking():
    global _ENGINE_HEALTH

    while _ENGINE_HEALTH is not None:
        try:
            with _ENGINE_HEALTH.connect() as conn:
                conn.execute(text("SELECT 1"))
            return _ENGINE_HEALTH
        except Exception as e:
            ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] health engine ping failed: {type(e).__name__}: {e}", flush=True)
            _dispose_engine_health()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE_HEALTH = _build_engine(DB_CONFIG, HEALTH_APP_NAME)
            with _ENGINE_HEALTH.connect() as conn:
                conn.execute(text("SELECT 1"))
            log("[DB][OK] health engine ready dedicated for health log", info="boot")
            return _ENGINE_HEALTH
        except Exception as e:
            ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] health engine create/connect failed: {type(e).__name__}: {e}", flush=True)
            _dispose_engine_health()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================================================
# Health log worker
# =========================================================
class HealthLogWorker:
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="HealthLogWorker", daemon=True)
        self._local_buffer: list[dict] = []

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()

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
                        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)  # type: ignore
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
                ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [WARN] health log flush failed: {type(e).__name__}: {e}", flush=True)
                if _is_connection_error(e):
                    _dispose_engine_health()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    def _drain_queue(self, max_items: int):
        drained = 0
        while drained < max_items:
            try:
                item = db_log_queue.get_nowait()
                self._local_buffer.append(item)
                drained += 1
            except queue.Empty:
                break

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

                self._drain_queue(max_items=max(0, DB_LOG_BATCH_SIZE - len(self._local_buffer)))
                now_ts = time_mod.time()
                need_flush = False
                if len(self._local_buffer) >= DB_LOG_BATCH_SIZE:
                    need_flush = True
                elif self._local_buffer and (now_ts - last_flush_ts) >= DB_LOG_FLUSH_INTERVAL_SEC:
                    need_flush = True
                if need_flush:
                    batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                    del self._local_buffer[:len(batch)]
                    self._flush_batch(batch)
                    last_flush_ts = now_ts
            except Exception as e:
                ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [WARN] HealthLogWorker loop error: {type(e).__name__}: {e}", flush=True)
                time_mod.sleep(1.0)

        try:
            while True:
                try:
                    item = db_log_queue.get_nowait()
                    self._local_buffer.append(item)
                except queue.Empty:
                    break
            while self._local_buffer:
                batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                del self._local_buffer[:len(batch)]
                self._flush_batch(batch)
        except Exception as e:
            ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] HealthLogWorker final flush error: {type(e).__name__}: {e}", flush=True)


# =========================================================
# 초기 검사 / DB 조회
# =========================================================
def _fail_fast_if_base_dir_missing():
    try:
        exists = BASE_DIR.exists()
    except OSError as e:
        log(f"[ERROR] BASE_DIR access error. stop. path={BASE_DIR} err={type(e).__name__}: {e}", info="error")
        raise SystemExit(1)

    if exists:
        log(f"[INFO] BASE_DIR exists: {BASE_DIR}", info="info")
        return

    log(f"[ERROR] BASE_DIR not found. stop. path={BASE_DIR}", info="error")
    raise SystemExit(1)


def _load_processed_run_sizes(engine) -> Dict[str, int]:
    out: Dict[str, int] = {}
    lookback_day = (_now_kst().date() - timedelta(days=RUNID_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    sql = text(
        f"""
        SELECT DISTINCT run_id
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE run_id IS NOT NULL
          AND run_id <> ''
          AND end_day >= :lookback_day
        """
    )

    while True:
        try:
            with engine.connect() as conn:
                rows = conn.execute(sql, {"lookback_day": lookback_day}).fetchall()
            for row in rows:
                run_id = row[0]
                if run_id:
                    out[str(run_id)] = 0
            return out
        except Exception as e:
            if _is_connection_error(e):
                log("load_processed_run_ids conn error -> reconnect", info="down")
                log_exc("load_processed_run_ids", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_main_blocking()
                continue
            log("load_processed_run_ids db fatal", info="error")
            log_exc("load_processed_run_ids", e)
            raise


# =========================================================
# 파일 상태 / 스캔 / 파싱
# =========================================================
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


def _collect_candidates(now_dt: datetime, window_sec: int) -> Dict[str, List[str]]:
    start_dt = now_dt - timedelta(seconds=window_sec)
    day_set = {start_dt.date(), now_dt.date()}
    out: Dict[str, List[str]] = {}

    for day in day_set:
        day_dir = _day_dir(day)
        try:
            if not day_dir.exists():
                continue
        except OSError as e:
            log(f"[ERROR] day_dir access error: {day_dir} | {type(e).__name__}: {e}", info="error")
            raise

        try:
            for fp in day_dir.rglob("*.txt"):
                if not fp.is_file():
                    continue
                info = _file_info_from_filename(fp)
                if info is None:
                    continue
                run_id, _, _, start_time_dt = info
                if start_dt <= start_time_dt <= now_dt:
                    out.setdefault(run_id, []).append(str(fp))
        except OSError as e:
            log(f"[ERROR] rglob access error: {day_dir} | {type(e).__name__}: {e}", info="error")
            raise
    return out


def _collect_today_backfill_candidates(now_dt: datetime) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    day_dir = _day_dir(now_dt.date())

    try:
        if not day_dir.exists():
            return out
    except OSError as e:
        log(f"[ERROR] today backfill path access error: {day_dir} | {type(e).__name__}: {e}", info="error")
        raise

    try:
        for fp in day_dir.rglob("*.txt"):
            if not fp.is_file():
                continue
            info = _file_info_from_filename(fp)
            if info is None:
                continue
            run_id, _, _, start_time_dt = info
            if start_time_dt <= now_dt:
                out.setdefault(run_id, []).append(str(fp))
    except OSError as e:
        log(f"[ERROR] today backfill rglob error: {day_dir} | {type(e).__name__}: {e}", info="error")
        raise

    return out


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


def _parse_one_file(path_str: str, run_id: str, base_day: date):
    p = Path(path_str)
    remark = _infer_remark_strict(p)
    if remark is None:
        return path_str, run_id, [], "SKIP_REMARK"

    m = FNAME_RE.match(p.name)
    if not m:
        return path_str, run_id, [], "SKIP_BADNAME"

    barcode = m.group(1).strip()
    try:
        lines = _safe_read_lines(p)
    except Exception:
        return path_str, run_id, [], "ERROR"

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
        return path_str, run_id, [], "SKIP_EMPTY"

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

    return path_str, run_id, rows, "OK"


def _prepare_tasks(cand_map: Dict[str, List[str]], processed_run_size: Dict[str, int], allow_unstable: bool = False):
    ready_tasks: List[Tuple[str, str, date]] = []
    ready_runs = 0
    skipped_not_stable = 0
    skipped_already_done = 0

    for run_id, paths in cand_map.items():
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
        _, base_day, _, _ = info
        ready_tasks.append((best_path, run_id, base_day))
        ready_runs += 1

    return ready_tasks, ready_runs, skipped_not_stable, skipped_already_done


# =========================================================
# Insert
# =========================================================
def _insert_rows(engine, rows: List[tuple], source_label: str = "realtime") -> int:
    if not rows:
        return 0

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
        (barcode_information, remark, end_day, end_time, contents, test_ct, test_time, file_path, run_id)
        VALUES
        (:barcode_information, :remark, :end_day, :end_time, :contents, :test_ct, :test_time, :file_path, :run_id)
        ON CONFLICT (run_id, test_time, contents) DO NOTHING
        """
    )

    total = len(rows)
    batches = (total + INSERT_BATCH_SIZE - 1) // INSERT_BATCH_SIZE
    inserted_total = 0
    log(
        f"[INFO] insert_rows start source={source_label} total_rows={total:,} "
        f"batch_size={INSERT_BATCH_SIZE} batches={batches}",
        info="info",
    )

    for idx, batch in enumerate(_chunks(rows, INSERT_BATCH_SIZE), start=1):
        params = [
            {
                "barcode_information": r[0],
                "remark": r[1],
                "end_day": r[2],
                "end_time": r[3],
                "contents": r[4],
                "test_ct": r[5],
                "test_time": r[6],
                "file_path": r[7],
                "run_id": r[8],
            }
            for r in batch
        ]

        while True:
            try:
                with engine.connect() as conn:
                    conn.execute(sql, params)
                inserted_total += len(batch)
                log(
                    f"[INFO] insert_rows batch {idx}/{batches} source={source_label} "
                    f"attempted_rows={len(batch):,} cumulative={inserted_total:,}",
                    info="info",
                )
                break
            except Exception as e:
                if _is_connection_error(e):
                    log(f"insert conn error -> reconnect source={source_label} batch={idx}/{batches}", info="down")
                    log_exc("insert", e)
                    _dispose_engine_main()
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine = get_engine_main_blocking()
                    continue
                log(f"insert db fatal source={source_label} batch={idx}/{batches}", info="error")
                log_exc("insert", e)
                raise

    log(f"[INFO] insert_rows done source={source_label} total_attempted={inserted_total:,}", info="info")
    return inserted_total


def _run_task_batch(engine, ready_tasks: List[Tuple[str, str, date]], processed_run_size: Dict[str, int], source_label: str) -> int:
    parsed_rows_all: List[tuple] = []
    ok_files = 0
    skip_remark = 0
    skip_badname = 0
    skip_empty = 0
    error_cnt = 0

    for best_path, run_id, base_day in ready_tasks:
        path_str, run_id2, rows, status = _parse_one_file(best_path, run_id, base_day)
        if status == "OK":
            ok_files += 1
            if rows:
                parsed_rows_all.extend(rows)
            size, _ = _update_stable_state(path_str)
            processed_run_size[run_id2] = int(size) if size is not None else processed_run_size.get(run_id2, 0)
        elif status == "SKIP_REMARK":
            skip_remark += 1
        elif status == "SKIP_BADNAME":
            skip_badname += 1
        elif status == "SKIP_EMPTY":
            skip_empty += 1
        else:
            error_cnt += 1

    attempted = _insert_rows(engine, parsed_rows_all, source_label=source_label)
    log(
        f"[INFO] {source_label} parse_result ok_files={ok_files:,} attempted_rows={attempted:,} "
        f"skip_remark={skip_remark:,} skip_badname={skip_badname:,} skip_empty={skip_empty:,} error={error_cnt:,}",
        info="info",
    )
    return attempted


# =========================================================
# Startup backfill
# =========================================================
def _startup_backfill_today(engine, processed_run_size: Dict[str, int]) -> int:
    now_dt = datetime.now()
    cand_map = _collect_today_backfill_candidates(now_dt)
    log(f"[INFO] startup_backfill_today candidates={len(cand_map):,}", info="info")

    ready_tasks, ready_runs, skipped_not_ready, skipped_already_done = _prepare_tasks(
        cand_map, processed_run_size, allow_unstable=True
    )
    log(
        f"[INFO] startup_backfill_today ready_runs={ready_runs:,} "
        f"skipped_not_ready={skipped_not_ready:,} already_done={skipped_already_done:,}",
        info="info",
    )

    if not ready_tasks:
        return 0

    return _run_task_batch(engine, ready_tasks, processed_run_size, source_label="startup_backfill_today")


# =========================================================
# main
# =========================================================
def main():
    log("[BOOT] c1_fct_detail_factory d4-style starting", info="boot")
    log(f"[INFO] MAIN DB = {_masked_db_info(DB_CONFIG)}", info="info")
    log(f"[INFO] BASE_DIR = {BASE_DIR}", info="info")
    log(f"[INFO] save = {SCHEMA_NAME}.{TABLE_NAME}", info="info")
    log(f"[INFO] health = {LOG_SCHEMA}.{LOG_TABLE}", info="info")
    log(f"[INFO] work_mem={WORK_MEM}", info="info")
    log(
        f"[INFO] stmt_timeout_ms={STMT_TIMEOUT_MS} lock_timeout_ms={LOCK_TIMEOUT_MS} "
        f"idle_tx_timeout_ms={IDLE_TX_TIMEOUT_MS}",
        info="info",
    )
    log(f"[INFO] candidate_window_sec={CANDIDATE_WINDOW_SEC} stable_required={STABLE_REQUIRED}", info="info")
    log(f"[INFO] runid_lookback_days={RUNID_LOOKBACK_DAYS} insert_batch_size={INSERT_BATCH_SIZE}", info="info")

    _acquire_lock_blocking_or_exit()

    engine_main = get_engine_main_blocking()
    _ = get_engine_health_blocking()

    health_worker = HealthLogWorker()
    health_worker.start()
    log("[INFO] HealthLogWorker started", info="info")

    _fail_fast_if_base_dir_missing()

    log("[INFO] start load_processed_run_sizes", info="info")
    processed_run_size = _load_processed_run_sizes(engine_main)
    log(f"[INFO] done load_processed_run_sizes count={len(processed_run_size):,}", info="info")

    startup_attempted = _startup_backfill_today(engine_main, processed_run_size)
    log(f"[INFO] startup_backfill_today attempted_rows={startup_attempted:,}", info="info")

    total_attempted_rows = startup_attempted
    loop_count = 0

    try:
        while True:
            loop_t0 = time_mod.perf_counter()
            try:
                engine_main = get_engine_main_blocking()
                _acquire_lock_blocking_or_exit()

                loop_count += 1
                now_dt = datetime.now()
                cand_map = _collect_candidates(now_dt, CANDIDATE_WINDOW_SEC)
                if not cand_map:
                    log(
                        f"[SLEEP] loop={loop_count} no candidates | "
                        f"candidate_window_sec={CANDIDATE_WINDOW_SEC} sleep {LOOP_SLEEP_SEC}s",
                        info="sleep",
                    )
                    time_mod.sleep(LOOP_SLEEP_SEC)
                    continue

                ready_tasks, ready_runs, skipped_not_stable, skipped_already_done = _prepare_tasks(
                    cand_map, processed_run_size, allow_unstable=False
                )

                if not ready_tasks:
                    log(
                        f"[SLEEP] loop={loop_count} no ready tasks | cand_runs={len(cand_map):,} "
                        f"not_stable={skipped_not_stable:,} already_done={skipped_already_done:,} sleep {LOOP_SLEEP_SEC}s",
                        info="sleep",
                    )
                    time_mod.sleep(LOOP_SLEEP_SEC)
                    continue

                attempted = _run_task_batch(
                    engine_main,
                    ready_tasks,
                    processed_run_size,
                    source_label=f"realtime_loop_{loop_count}",
                )
                total_attempted_rows += attempted

                loop_t1 = time_mod.perf_counter()
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
                    log("[DB][RETRY] loop-level conn error -> rebuild main engine", info="down")
                    log_exc("[DB][RETRY] loop-level", e)
                    _dispose_engine_main()
                    _safe_close_lock_conn()
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine_main = get_engine_main_blocking()
                else:
                    log("[ERROR] Loop error continue", info="error")
                    log_exc("[ERROR] Loop error", e)

            elapsed = time_mod.perf_counter() - loop_t0
            time_mod.sleep(max(0.0, LOOP_SLEEP_SEC - elapsed))

    finally:
        try:
            health_worker.stop()
            health_worker.join(timeout=15.0)
            log("[INFO] HealthLogWorker stopped", info="info")
        except Exception as e:
            log_exc("[WARN] HealthLogWorker stop failed", e)

        _release_lock()
        _dispose_engine_main()
        _dispose_engine_health()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[STOP] user interrupted Ctrl+C", info="stop")
    except Exception as e:
        log("[UNHANDLED] fatal error", info="error")
        log_exc("[UNHANDLED]", e)
        raise
