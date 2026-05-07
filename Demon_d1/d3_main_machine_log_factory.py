# -*- coding: utf-8 -*-
"""
d3_main_machine_log_factory.py

변경 사항
- ✅ Main log ingest
- ✅ 중복 키 발생 시 예외 중단하지 않고 ON CONFLICT DO NOTHING 처리
- ✅ 메모리 캐시도 end_time 단일값이 아니라 (end_day, station, end_time, contents) dedup key 기준으로 관리
- ✅ Progress logs + idle heartbeat logs
- ✅ Health logs persisted to DB: k_demon_heath_check.d3_log
- ✅ SQLAlchemy pool_size=2 for DDL/health flush
"""

from __future__ import annotations

import os
import re
import time
import threading
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple, Set, Dict

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =========================
# CONFIG
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\Main")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": os.getenv("PG_PASSWORD", ""),
}

SCHEMA_NAME = "d1_machine_log"
TABLE_NAME_QUOTED = '"Main_machine_log"'
FQN = f'{SCHEMA_NAME}.{TABLE_NAME_QUOTED}'
STATION = "Main"

HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "d3_log"
HEALTH_FQN = f"{HEALTH_SCHEMA}.{HEALTH_TABLE}"

SLEEP_SEC = 5
DB_RETRY_INTERVAL_SEC = 5
CONNECT_TIMEOUT_SEC = int(os.getenv("PG_CONNECT_TIMEOUT_SEC", "5"))

PG_WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

VALUES_PAGE_START = int(os.getenv("D3_VALUES_PAGE_START", "100"))
VALUES_PAGE_MIN = int(os.getenv("D3_VALUES_PAGE_MIN", "1"))
VALUES_SLEEP = float(os.getenv("D3_VALUES_SLEEP", "0.01"))

IDLE_LOG_SEC = int(os.getenv("D3_IDLE_LOG_SEC", "60"))

HEALTH_FLUSH_SEC = int(os.getenv("D3_HEALTH_FLUSH_SEC", "5"))
HEALTH_BUFFER_MAX = int(os.getenv("D3_HEALTH_BUFFER_MAX", "500"))
HEALTH_WORKER_STALE_SEC = int(os.getenv("D3_HEALTH_WORKER_STALE_SEC", "60"))
LOCAL_LOG_PATH = Path(os.getenv(
    "D3_LOCAL_LOG_PATH",
    str(Path(__file__).with_suffix(".runtime.log")),
))

LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)\]\s*(.*)$")

Row = Tuple[str, str, str, str]         # (end_day, station, end_time, contents)
DedupKey = Tuple[str, str, str, str]    # (end_day, station, end_time, contents)

# =========================
# LOG (console + DB buffer)
# =========================
_health_buf: List[Dict[str, str]] = []
_last_health_flush_ts: float = 0.0


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _health_row(info: str, contents: str) -> Dict[str, str]:
    now = datetime.now()
    return {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": (info or "info").strip().lower()[:50] or "info",
        "contents": str(contents)[:4000],
    }


def _log_console(level: str, msg: str) -> None:
    line = f"[{_ts()}] [{level}] {msg}"
    print(line, flush=True)
    try:
        with LOCAL_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def logx(level: str, msg: str) -> None:
    _log_console(level, msg)
    try:
        _health_buf.append(_health_row(level, msg))
        if len(_health_buf) >= HEALTH_BUFFER_MAX:
            flush_health_logs(force=True)
    except Exception:
        pass


def boot(msg: str) -> None:
    logx("BOOT", msg)


def info(msg: str) -> None:
    logx("INFO", msg)


def warn(msg: str) -> None:
    logx("WARN", msg)


def retry(msg: str) -> None:
    logx("RETRY", msg)


def err(msg: str) -> None:
    logx("ERROR", msg)


def err_exc(prefix: str, e: Exception) -> None:
    err(f"{prefix}: {type(e).__name__}: {repr(e)}")
    tb = traceback.format_exc()
    for ln in tb.rstrip().splitlines():
        err(f"{prefix} TRACE: {ln}")


# =========================
# SQLAlchemy Engine (DDL/health flush용)
# =========================
_ENGINE: Optional[Engine] = None
_HEALTH_ENGINE: Optional[Engine] = None
_health_flush_running: bool = False
_health_flush_started_ts: float = 0.0
_health_flush_thread: Optional[threading.Thread] = None
_health_flush_generation: int = 0
_health_flush_lock = threading.Lock()


def _build_engine(application_name: str) -> Engine:
    user = DB_CONFIG["user"]
    pw = DB_CONFIG["password"]
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    db = DB_CONFIG["dbname"]
    conn_str = (
        f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
        f"?connect_timeout={CONNECT_TIMEOUT_SEC}"
        f"&application_name={application_name}"
    )
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
    )


def _dispose_engine() -> None:
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None


def _dispose_health_engine() -> None:
    global _HEALTH_ENGINE
    try:
        if _HEALTH_ENGINE is not None:
            _HEALTH_ENGINE.dispose()
    except Exception:
        pass
    _HEALTH_ENGINE = None


def get_health_engine() -> Engine:
    global _HEALTH_ENGINE
    if _HEALTH_ENGINE is None:
        _HEALTH_ENGINE = _build_engine("d3_main_machine_log_factory_health")
    return _HEALTH_ENGINE


def is_health_flush_stale(now_ts: Optional[float] = None) -> bool:
    if now_ts is None:
        now_ts = time.time()
    return (
        _health_flush_running
        and _health_flush_started_ts > 0
        and (now_ts - _health_flush_started_ts) >= HEALTH_WORKER_STALE_SEC
    )


def get_engine_blocking() -> Engine:
    global _ENGINE
    while True:
        try:
            if _ENGINE is None:
                _ENGINE = _build_engine("d3_main_machine_log_factory")

            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem = :wm"), {"wm": str(PG_WORK_MEM)})
                conn.execute(text("SELECT 1"))
            return _ENGINE

        except Exception as e:
            _log_console("RETRY", f"DB connect failed: {type(e).__name__}: {str(e).strip()}")
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


def ensure_target_table(engine: Engine) -> None:
    ddl = text(f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

        CREATE TABLE IF NOT EXISTS {FQN} (
            end_day   TEXT,
            station   TEXT,
            end_time  TEXT,
            contents  TEXT
        );
    """)
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem = :wm"), {"wm": str(PG_WORK_MEM)})
                conn.execute(ddl)
            info(f"DDL ensured for {FQN}")
            return
        except Exception as e:
            retry(f"DDL(target) failed: {type(e).__name__}: {str(e).strip()}")
            time.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


def ensure_health_table(engine: Engine) -> None:
    ddl = text(f"""
        CREATE SCHEMA IF NOT EXISTS {HEALTH_SCHEMA};

        CREATE TABLE IF NOT EXISTS {HEALTH_FQN} (
            id       BIGSERIAL PRIMARY KEY,
            end_day  TEXT NOT NULL,
            end_time TEXT NOT NULL,
            info     TEXT NOT NULL,
            contents TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_{HEALTH_TABLE}_day_time
        ON {HEALTH_FQN} (end_day, end_time);
    """)
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem = :wm"), {"wm": str(PG_WORK_MEM)})
                conn.execute(ddl)
            info(f"DDL ensured for {HEALTH_FQN} (id,end_day,end_time,info,contents)")
            return
        except Exception as e:
            _log_console("RETRY", f"DDL(health) failed: {type(e).__name__}: {str(e).strip()}")
            time.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


def flush_health_logs(force: bool = False) -> None:
    global _last_health_flush_ts, _health_buf, _health_flush_running
    global _health_flush_started_ts, _health_flush_thread, _health_flush_generation
    if not _health_buf:
        return

    now_ts = time.time()
    if (not force) and (now_ts - _last_health_flush_ts) < HEALTH_FLUSH_SEC:
        return

    with _health_flush_lock:
        if _health_flush_running:
            if is_health_flush_stale(now_ts):
                _log_console(
                    "WARN",
                    f"[LOG-DB][WATCHDOG] health flush stale "
                    f"age={now_ts - _health_flush_started_ts:.1f}s -> restart allowed",
                )
                _dispose_health_engine()
                _health_flush_running = False
                _health_flush_started_ts = 0.0
                _health_flush_thread = None
                _health_flush_generation += 1
            else:
                if len(_health_buf) > HEALTH_BUFFER_MAX:
                    _health_buf = _health_buf[-HEALTH_BUFFER_MAX:]
                _last_health_flush_ts = now_ts
                _log_console("WARN", "[LOG-DB][SKIP] previous health flush is still running")
                return

        if len(_health_buf) > HEALTH_BUFFER_MAX:
            dropped = len(_health_buf) - HEALTH_BUFFER_MAX
            _health_buf = _health_buf[-HEALTH_BUFFER_MAX:]
            _log_console("WARN", f"[LOG-DB][DROP] health buffer overflow dropped={dropped}")

        rows = list(_health_buf)
        _health_buf = []
        _health_flush_running = True
        _health_flush_started_ts = now_ts
        _health_flush_generation += 1
        generation = _health_flush_generation
        _last_health_flush_ts = now_ts

    def _worker(rows_to_write: List[Dict[str, str]], my_generation: int) -> None:
        global _health_buf, _health_flush_running, _health_flush_started_ts, _health_flush_thread
        sql = text(f"""
            INSERT INTO {HEALTH_FQN} (end_day, end_time, info, contents)
            VALUES (:end_day, :end_time, :info, :contents)
        """)
        try:
            engine = get_health_engine()
            with engine.begin() as conn:
                conn.execute(text("SET work_mem = :wm"), {"wm": str(PG_WORK_MEM)})
                conn.execute(sql, rows_to_write)
        except Exception as e:
            _dispose_health_engine()
            _log_console("WARN", f"[LOG-DB][SKIP] {type(e).__name__}: {str(e).strip()}")
            with _health_flush_lock:
                _health_buf = rows_to_write + _health_buf
                if len(_health_buf) > HEALTH_BUFFER_MAX:
                    _health_buf = _health_buf[-HEALTH_BUFFER_MAX:]
        finally:
            with _health_flush_lock:
                if my_generation == _health_flush_generation:
                    _health_flush_running = False
                    _health_flush_started_ts = 0.0
                    _health_flush_thread = None

    try:
        thread = threading.Thread(target=_worker, args=(rows, generation), daemon=True)
        with _health_flush_lock:
            _health_flush_thread = thread
        thread.start()
    except Exception as e:
        with _health_flush_lock:
            _health_buf = rows + _health_buf
            _health_flush_running = False
            _health_flush_started_ts = 0.0
            _health_flush_thread = None
            _health_flush_generation += 1
        _log_console("WARN", f"[LOG-DB][SKIP] failed to start async flush | {type(e).__name__}: {e}")


def check_health_worker() -> None:
    global _health_flush_running, _health_flush_started_ts, _health_flush_thread, _health_flush_generation
    now_ts = time.time()
    with _health_flush_lock:
        if not is_health_flush_stale(now_ts):
            return
        _log_console(
            "WARN",
            f"[LOG-DB][WATCHDOG] health worker stale "
            f"age={now_ts - _health_flush_started_ts:.1f}s -> marking dead",
        )
        _dispose_health_engine()
        _health_flush_running = False
        _health_flush_started_ts = 0.0
        _health_flush_thread = None
        _health_flush_generation += 1


def get_psycopg2_conn_once():
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            connect_timeout=CONNECT_TIMEOUT_SEC,
        )
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET work_mem = %s", (str(PG_WORK_MEM),))
            cur.execute("SET client_encoding = 'UTF8'")
        return conn
    except Exception as e:
        retry(f"psycopg2 connect failed: {type(e).__name__}: {str(e).strip()}")
        flush_health_logs(force=False)
        return None


# =========================
# psycopg2 (insert + preload)
# =========================
def get_psycopg2_conn_blocking():
    while True:
        conn = get_psycopg2_conn_once()
        if conn is not None:
            return conn
        time.sleep(DB_RETRY_INTERVAL_SEC)


def close_conn_quietly(conn) -> None:
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


def preload_existing_keys(conn, day_ymd: str) -> Set[DedupKey]:
    """
    DB????? 鈺곕똻???롫뮉 ?諭??dedup key preload
    dedup 疫꿸퀣?: (end_day, station, end_time, contents)
    """
    sql = f"""
        SELECT end_day, station, end_time, contents
        FROM {FQN}
        WHERE end_day=%s
          AND station=%s
    """
    out: Set[DedupKey] = set()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (day_ymd, STATION))
            rows = cur.fetchall()

        for end_day, station, end_time, contents in rows:
            key: DedupKey = (
                "" if end_day is None else str(end_day),
                "" if station is None else str(station),
                "" if end_time is None else str(end_time),
                "" if contents is None else str(contents),
            )
            out.add(key)

        conn.commit()
        return out
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


INSERT_SQL = f"""
INSERT INTO {FQN}
    (end_day, station, end_time, contents)
VALUES %s
ON CONFLICT DO NOTHING
"""


def insert_execute_values_with_progress(conn, rows: List[Row], page: int, phase: str) -> Tuple[int, int]:
    """
    ON CONFLICT DO NOTHING 疫꿸퀣?
    獄쏆꼹??
      inserted_total, skipped_total
    """
    inserted_total = 0
    skipped_total = 0
    total = len(rows)
    i = 0

    while i < total:
        batch = rows[i:i + page]

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                INSERT_SQL,
                batch,
                page_size=len(batch),
            )
            affected = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0

        conn.commit()

        batch_total = len(batch)
        batch_inserted = affected
        batch_skipped = batch_total - batch_inserted

        inserted_total += batch_inserted
        skipped_total += batch_skipped
        i += batch_total

        info(
            f"[{phase}] progress {i}/{total} "
            f"(inserted={inserted_total}, skipped_dup={skipped_total}, page={page})"
        )
        flush_health_logs(force=False)

        if VALUES_SLEEP > 0:
            time.sleep(VALUES_SLEEP)

    return inserted_total, skipped_total


# =========================
# WINDOW + PARSE
# =========================
@dataclass
class Window:
    day_ymd: str
    now_dt: datetime


def window_now() -> Window:
    now = datetime.now()
    return Window(day_ymd=now.strftime("%Y%m%d"), now_dt=now)


def build_today_file_path(day_ymd: str) -> Path:
    now = datetime.now()
    return BASE_DIR / f"{now.year:04d}" / f"{now.month:02d}" / f"{day_ymd}_Main_Machine_Log.txt"


def _decode_line(b: bytes) -> str:
    try:
        return b.decode("cp949", errors="ignore")
    except Exception:
        return b.decode("utf-8", errors="ignore")


def normalize_contents(contents_raw: str) -> str:
    return " ".join(
        str(contents_raw)
        .replace("\x00", " ")
        .replace("\r", " ")
        .replace("\t", " ")
        .strip()
        .split()
    )


def scan_file_rows_filtered(file_path: Path, day_ymd: str, existing_keys: Set[DedupKey]) -> List[Row]:
    out: List[Row] = []
    local_seen: Set[DedupKey] = set()

    with file_path.open("rb") as f:
        for bline in f:
            line = _decode_line(bline).rstrip("\r\n")
            mm = LINE_PATTERN.match(line)
            if not mm:
                continue

            end_time_str, contents_raw = mm.groups()
            contents = normalize_contents(contents_raw)

            key: DedupKey = (day_ymd, STATION, end_time_str, contents)

            # ??? DB????뉕탢????苡???쇳떔 ?룐뫂遊?癒?퐣 ??? ??곷릭??겹늺 skip
            if key in existing_keys or key in local_seen:
                continue

            out.append(key)
            local_seen.add(key)

    return out


# =========================
# MAIN
# =========================
def main() -> None:
    boot("d3 ingest start (dedup continue mode + progress + idle heartbeat + healthlog DB)")

    engine = get_engine_blocking()
    ensure_target_table(engine)
    ensure_health_table(engine)

    info(f"BASE_DIR={BASE_DIR}")
    info(f"TARGET={FQN} cols=end_day,station,end_time,contents")
    info(f"HEALTH={HEALTH_FQN} cols=id,end_day,end_time,info,contents")
    info(
        f"WORK_MEM={PG_WORK_MEM} | SLEEP={SLEEP_SEC}s | "
        f"PAGE_START={VALUES_PAGE_START} | IDLE_LOG={IDLE_LOG_SEC}s | "
        f"HEALTH_FLUSH={HEALTH_FLUSH_SEC}s | HEALTH_STALE={HEALTH_WORKER_STALE_SEC}s"
    )
    info(f"LOCAL_LOG_PATH={LOCAL_LOG_PATH}")

    current_day: Optional[str] = None
    existing_keys: Set[DedupKey] = set()
    bootstrapped = False
    last_idle_log_ts = 0.0

    while True:
        loop_t0 = time.perf_counter()

        try:
            check_health_worker()
            w = window_now()

            if current_day != w.day_ymd:
                current_day = w.day_ymd
                existing_keys.clear()
                bootstrapped = False
                info(f"[WINDOW] day switched -> {current_day}")
                flush_health_logs(force=False)

            fp = build_today_file_path(w.day_ymd)

            if not fp.is_file():
                now_ts = time.time()
                if now_ts - last_idle_log_ts >= IDLE_LOG_SEC:
                    info(f"[IDLE] file_missing day={w.day_ymd} path={fp}")
                    last_idle_log_ts = now_ts
                    flush_health_logs(force=False)
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue

            if not bootstrapped:
                conn = get_psycopg2_conn_once()
                if conn is None:
                    warn("[BOOTSTRAP] DB unavailable -> retry next loop")
                    time.sleep(DB_RETRY_INTERVAL_SEC)
                    continue
                try:
                    info("[BOOTSTRAP] preload existing dedup keys from DB...")
                    existing_keys = preload_existing_keys(conn, w.day_ymd)
                    info(f"[BOOTSTRAP] existing_keys={len(existing_keys)}")
                except Exception as e:
                    warn(f"[BOOTSTRAP] preload failed -> retry next loop | {type(e).__name__}: {str(e).strip()}")
                    time.sleep(DB_RETRY_INTERVAL_SEC)
                    continue
                finally:
                    close_conn_quietly(conn)
                bootstrapped = True
                flush_health_logs(force=False)

            rows_new = scan_file_rows_filtered(fp, w.day_ymd, existing_keys)

            if rows_new:
                info(f"[FETCH] new_rows={len(rows_new)} (will insert)")
                flush_health_logs(force=False)

                page = VALUES_PAGE_START
                conn = get_psycopg2_conn_once()
                if conn is None:
                    warn("[WRITE] DB unavailable -> retry next loop")
                    time.sleep(DB_RETRY_INTERVAL_SEC)
                    continue

                while True:
                    try:
                        inserted, skipped = insert_execute_values_with_progress(
                            conn,
                            rows_new,
                            page=page,
                            phase="INCR",
                        )

                        # ??苡???쇳떔 ???怨? insert ?源껊궗/餓λ쵎??skip ????? ?怨???곸뵠
                        # ??곸젫 DB????뉕탢??揶쏆늿? ???嚥?筌ｌ꼶???袁⑥┷??野껉퍔?앮에?揶쏄쑴竊?
                        for row in rows_new:
                            existing_keys.add(row)

                        info(
                            f"[WRITE] done total={len(rows_new)} "
                            f"inserted={inserted} skipped_dup={skipped} (page={page})"
                        )
                        last_idle_log_ts = 0.0
                        flush_health_logs(force=False)
                        break

                    except Exception as e:
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                        warn(
                            f"[WRITE] insert failed (page={page}) -> shrink & reconnect | "
                            f"{type(e).__name__}: {str(e).strip()}"
                        )
                        flush_health_logs(force=False)

                        page = max(VALUES_PAGE_MIN, page // 2)

                        close_conn_quietly(conn)
                        conn = get_psycopg2_conn_once()
                        if conn is None:
                            warn("[WRITE] reconnect failed -> retry next loop")
                            break
                        time.sleep(DB_RETRY_INTERVAL_SEC)

                close_conn_quietly(conn)

            else:
                now_ts = time.time()
                if now_ts - last_idle_log_ts >= IDLE_LOG_SEC:
                    info(f"[IDLE] no_new_rows day={w.day_ymd} seen_keys={len(existing_keys)}")
                    last_idle_log_ts = now_ts
                    flush_health_logs(force=False)

        except KeyboardInterrupt:
            info("Interrupted by user.")
            flush_health_logs(force=True)
            return

        except Exception as e:
            err_exc("loop error", e)
            flush_health_logs(force=False)
            time.sleep(DB_RETRY_INTERVAL_SEC)

        check_health_worker()
        flush_health_logs(force=False)

        elapsed = time.perf_counter() - loop_t0
        time.sleep(max(0.0, SLEEP_SEC - elapsed))


if __name__ == "__main__":
    main()
