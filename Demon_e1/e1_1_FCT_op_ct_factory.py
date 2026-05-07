# -*- coding: utf-8 -*-
"""
e1_1_FCT_op_ct_factory_schedule.py

[스케줄 실행 방식]
- 매일 08:22:00 1회 실행
- 매일 20:22:00 1회 실행
- 그 외 시간에는 대기
- 2회 모두 끝나도 종료하지 않고 다음날까지 계속 대기

[수정 반영]
1. 대문자 스키마명 "e1_FCT_ct" 안전 처리
2. wait 로그는 scheduler_wait 1행만 UPDATE
3. execute_values 기반 UPSERT 유지
4. main SELECT / target UPSERT / normal log / heartbeat 연결 분리
5. DB 접속 실패/끊김 시 재시도
6. 세션마다 work_mem 설정
7. 진단용 체크포인트 추가
8. scheduler_wait.updated_at 추가
9. 실행 중에도 1분 heartbeat 유지
10. C:\\AptivAgent 파일 로그 저장
11. health 장기 연결 재사용 제거
"""

import os
import time
import urllib.parse
import threading
import traceback
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict

import numpy as np
import pandas as pd

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from pandas.api.types import CategoricalDtype


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e1_FCT_ct"

TBL_OPCT_LATEST = "fct_op_ct"
TBL_WHOLE_LATEST = "fct_whole_op_ct"

TBL_OPCT_HIST = "fct_op_ct_hist"
TBL_WHOLE_HIST = "fct_whole_op_ct_hist"

OPCT_MAX_SEC = 600

RUN_TIME_1 = dtime(8, 22, 0)
RUN_TIME_2 = dtime(20, 22, 0)

ALL_STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")
DB_RETRY_INTERVAL_SEC = 5

STATEMENT_TIMEOUT_MS = int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "60000"))
LOG_STATEMENT_TIMEOUT_MS = int(os.getenv("PG_LOG_STATEMENT_TIMEOUT_MS", "20000"))
HEARTBEAT_STATEMENT_TIMEOUT_MS = int(os.getenv("PG_HEARTBEAT_STATEMENT_TIMEOUT_MS", "15000"))
LOCK_TIMEOUT_MS = int(os.getenv("PG_LOCK_TIMEOUT_MS", "10000"))
IDLE_TX_TIMEOUT_MS = int(os.getenv("PG_IDLE_TX_TIMEOUT_MS", "60000"))

PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "e1_1_log"
WAIT_ROW_KEY = "scheduler_wait"

AGENT_LOG_DIR = os.getenv("APTIV_AGENT_LOG_DIR", r"C:\AptivAgent")
AGENT_NAME = "e1_1_fct_op_ct_schedule"
HEARTBEAT_INTERVAL_SEC = 60

_MAIN_ENGINE = None

_HB_STOP = threading.Event()
_HB_THREAD = None
_HB_LOCK = threading.Lock()
_HB_STATE = {
    "mode": "boot",
    "detail": "starting",
    "updated_at": "",
}


# =========================
# 1) 유틸
# =========================
def _normalize_info(info: str) -> str:
    s = (info or "info").strip().lower()
    return s if s else "info"


def _masked_db_info() -> str:
    return f'postgresql://{DB_CONFIG["user"]}:***@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'


def _safe_repr(e: Exception) -> str:
    return f"{type(e).__name__}: {repr(e)}"


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, (OperationalError, DBAPIError, psycopg2.OperationalError, psycopg2.InterfaceError)):
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
        "canceling statement due to statement timeout",
        "ssl connection has been closed",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
        "closed the connection unexpectedly",
    ]
    return any(k in msg for k in keywords)


def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


def current_yyyymm(now: datetime | None = None) -> str:
    now = now or datetime.now()
    return now.strftime("%Y%m")


def _cast_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df

    out = df.copy()

    if "month" in out.columns:
        out["month"] = out["month"].astype(str)

    for c in out.columns:
        if isinstance(out[c].dtype, CategoricalDtype):
            out[c] = out[c].astype(str)

    out = out.replace({np.nan: None})
    return out


# =========================
# 2) 파일 로그
# =========================
def _ensure_agent_log_dir():
    try:
        os.makedirs(AGENT_LOG_DIR, exist_ok=True)
    except Exception:
        pass


def _agent_log_path(now: datetime | None = None) -> str:
    now = now or datetime.now()
    return os.path.join(AGENT_LOG_DIR, f"{AGENT_NAME}_{now:%Y%m%d}.log")


def _append_agent_log(line: str):
    try:
        _ensure_agent_log_dir()
        with open(_agent_log_path(), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _format_log_line(level: str, msg: str) -> str:
    now = datetime.now()
    return f"{now:%Y-%m-%d %H:%M:%S} | pid={os.getpid()} | {level.upper()} | {msg}"


# =========================
# 3) heartbeat 상태
# =========================
def _set_heartbeat_state(mode: str, detail: str):
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _HB_LOCK:
        _HB_STATE["mode"] = str(mode)
        _HB_STATE["detail"] = str(detail)
        _HB_STATE["updated_at"] = now_s


def _get_heartbeat_message() -> str:
    with _HB_LOCK:
        mode = _HB_STATE.get("mode", "unknown")
        detail = _HB_STATE.get("detail", "")
        updated_at = _HB_STATE.get("updated_at", "")
    return f"[HEARTBEAT] mode={mode} | detail={detail} | state_updated_at={updated_at} | pid={os.getpid()}"


# =========================
# 4) DB 연결/세션 설정
# =========================
def _pg_options(statement_timeout_ms: int) -> str:
    return (
        f"-c statement_timeout={statement_timeout_ms} "
        f"-c lock_timeout={LOCK_TIMEOUT_MS} "
        f"-c idle_in_transaction_session_timeout={IDLE_TX_TIMEOUT_MS}"
    )


def _connect_psycopg(role: str, statement_timeout_ms: int):
    return psycopg2.connect(
        **DB_CONFIG,
        connect_timeout=5,
        keepalives=PG_KEEPALIVES,
        keepalives_idle=PG_KEEPALIVES_IDLE,
        keepalives_interval=PG_KEEPALIVES_INTERVAL,
        keepalives_count=PG_KEEPALIVES_COUNT,
        application_name=f"e1_1_fct_op_ct_schedule_{role}",
        options=_pg_options(statement_timeout_ms),
    )


def _connect_target_db():
    return _connect_psycopg("target_write", STATEMENT_TIMEOUT_MS)


def _connect_log_db():
    return _connect_psycopg("normal_log", LOG_STATEMENT_TIMEOUT_MS)


def _connect_heartbeat_db():
    return _connect_psycopg("heartbeat", HEARTBEAT_STATEMENT_TIMEOUT_MS)


def _connect_schema_db():
    return _connect_psycopg("schema_ensure", STATEMENT_TIMEOUT_MS)


def _apply_session_settings_sqlalchemy(conn):
    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
    conn.execute(text("SET statement_timeout TO :stm"), {"stm": str(STATEMENT_TIMEOUT_MS)})
    conn.execute(text("SET lock_timeout TO :ltm"), {"ltm": str(LOCK_TIMEOUT_MS)})
    conn.execute(text("SET idle_in_transaction_session_timeout TO :itm"), {"itm": str(IDLE_TX_TIMEOUT_MS)})


def _apply_session_settings_psycopg(cur):
    cur.execute("SET work_mem TO %s", (WORK_MEM,))


# =========================
# 5) 진단 로그
# =========================
def diag(stage: str, detail: str = "", save_db: bool = False, info: str = "info"):
    msg = f"[DIAG] stage={stage}"
    if detail:
        msg += f" | {detail}"

    print(msg, flush=True)
    _append_agent_log(_format_log_line("diag", msg))
    _set_heartbeat_state(stage, detail or "running")

    if save_db:
        try:
            _save_log_to_db(info, msg, retry_forever=False)
        except Exception as e:
            warn_msg = f"[WARN] diag db write skipped: {_safe_repr(e)}"
            print(warn_msg, flush=True)
            _append_agent_log(_format_log_line("warn", warn_msg))


def diag_exception(stage: str, e: Exception):
    msg = f"[EXCEPTION] stage={stage} | {_safe_repr(e)}"
    print(msg, flush=True)
    _append_agent_log(_format_log_line("error", msg))
    _append_agent_log(traceback.format_exc().rstrip())
    _set_heartbeat_state(f"error:{stage}", str(e))


# =========================
# 6) 메인 엔진
# =========================
def _dispose_main_engine():
    global _MAIN_ENGINE
    try:
        if _MAIN_ENGINE is not None:
            _MAIN_ENGINE.dispose()
    except Exception:
        pass
    _MAIN_ENGINE = None


def _build_main_engine(config=DB_CONFIG):
    pw = urllib.parse.quote_plus(config["password"])
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{pw}"
        f"@{config['host']}:{config['port']}/{config['dbname']}?connect_timeout=5"
    )

    print(f"[INFO] MAIN DB = {_masked_db_info()}", flush=True)
    _append_agent_log(_format_log_line("info", f"MAIN DB = {_masked_db_info()}"))

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args={
            "connect_timeout": 5,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": "e1_1_fct_op_ct_schedule_main_select",
            "options": _pg_options(STATEMENT_TIMEOUT_MS),
        },
    )


def get_main_engine_blocking(config=DB_CONFIG):
    global _MAIN_ENGINE

    if _MAIN_ENGINE is not None:
        while True:
            try:
                diag("main_engine_ping", "checking existing engine")
                with _MAIN_ENGINE.connect() as conn:
                    _apply_session_settings_sqlalchemy(conn)
                    conn.execute(text("SELECT 1"))
                return _MAIN_ENGINE
            except Exception as e:
                diag_exception("main_engine_ping", e)
                print(f"[DB][RETRY] main engine ping failed -> rebuild: {_safe_repr(e)}", flush=True)
                _dispose_main_engine()
                time.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            diag("main_engine_create", "building engine")
            _MAIN_ENGINE = _build_main_engine(config)
            with _MAIN_ENGINE.connect() as conn:
                _apply_session_settings_sqlalchemy(conn)
                conn.execute(text("SELECT 1"))
            ok_msg = (
                f"[DB][OK] main engine ready (pool_size=1, max_overflow=0, "
                f"work_mem={WORK_MEM}, statement_timeout={STATEMENT_TIMEOUT_MS}ms)"
            )
            print(ok_msg, flush=True)
            _append_agent_log(_format_log_line("info", ok_msg))
            return _MAIN_ENGINE
        except Exception as e:
            diag_exception("main_engine_create", e)
            print(f"[DB][RETRY] main engine create/connect failed: {_safe_repr(e)}", flush=True)
            _dispose_main_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 7) 로그 테이블/로그 저장
# =========================
def ensure_log_table():
    while True:
        conn = None
        try:
            diag("ensure_log_table", "start", save_db=False)

            conn = _connect_log_db()
            conn.autocommit = False
            with conn.cursor() as cur:
                _apply_session_settings_psycopg(cur)

                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(LOG_SCHEMA)
                    )
                )

                cur.execute(
                    sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGSERIAL PRIMARY KEY,
                            end_day TEXT NOT NULL,
                            end_time TEXT NOT NULL,
                            info TEXT NOT NULL,
                            contents TEXT,
                            created_at TIMESTAMP DEFAULT now(),
                            updated_at TIMESTAMP DEFAULT now(),
                            wait_row_key TEXT
                        )
                    """).format(
                        sql.Identifier(LOG_SCHEMA),
                        sql.Identifier(LOG_TABLE)
                    )
                )

                cur.execute(
                    sql.SQL("""
                        ALTER TABLE {}.{}
                        ADD COLUMN IF NOT EXISTS wait_row_key TEXT
                    """).format(
                        sql.Identifier(LOG_SCHEMA),
                        sql.Identifier(LOG_TABLE)
                    )
                )

                cur.execute(
                    sql.SQL("""
                        ALTER TABLE {}.{}
                        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now()
                    """).format(
                        sql.Identifier(LOG_SCHEMA),
                        sql.Identifier(LOG_TABLE)
                    )
                )

                cur.execute(
                    sql.SQL("""
                        CREATE INDEX IF NOT EXISTS {} ON {}.{} (end_day, end_time)
                    """).format(
                        sql.Identifier(f"ix_{LOG_TABLE}_day_time"),
                        sql.Identifier(LOG_SCHEMA),
                        sql.Identifier(LOG_TABLE),
                    )
                )

                cur.execute(
                    sql.SQL("""
                        CREATE INDEX IF NOT EXISTS {} ON {}.{} (info)
                    """).format(
                        sql.Identifier(f"ix_{LOG_TABLE}_info"),
                        sql.Identifier(LOG_SCHEMA),
                        sql.Identifier(LOG_TABLE),
                    )
                )

                cur.execute(
                    sql.SQL("""
                        CREATE INDEX IF NOT EXISTS {} ON {}.{} (updated_at)
                    """).format(
                        sql.Identifier(f"ix_{LOG_TABLE}_updated_at"),
                        sql.Identifier(LOG_SCHEMA),
                        sql.Identifier(LOG_TABLE),
                    )
                )

                cur.execute(
                    sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (wait_row_key)
                    """).format(
                        sql.Identifier(f"ux_{LOG_TABLE}_wait_row_key"),
                        sql.Identifier(LOG_SCHEMA),
                        sql.Identifier(LOG_TABLE),
                    )
                )

            conn.commit()
            print(f"[OK] log table ensured: {LOG_SCHEMA}.{LOG_TABLE}", flush=True)
            _append_agent_log(_format_log_line("info", f"log table ensured: {LOG_SCHEMA}.{LOG_TABLE}"))
            return

        except Exception as e:
            diag_exception("ensure_log_table", e)
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            print(f"[DB][RETRY] ensure_log_table failed: {_safe_repr(e)}", flush=True)
            time.sleep(DB_RETRY_INTERVAL_SEC)

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def _upsert_wait_log_once(contents: str):
    now = datetime.now()

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            end_day, end_time, info, contents, created_at, updated_at, wait_row_key
        )
        VALUES (%s, %s, %s, %s, now(), now(), %s)
        ON CONFLICT (wait_row_key)
        DO UPDATE SET
            end_day    = EXCLUDED.end_day,
            end_time   = EXCLUDED.end_time,
            info       = EXCLUDED.info,
            contents   = EXCLUDED.contents,
            updated_at = now()
    """).format(
        sql.Identifier(LOG_SCHEMA),
        sql.Identifier(LOG_TABLE)
    )

    conn = None
    try:
        conn = _connect_heartbeat_db()
        conn.autocommit = False
        with conn.cursor() as cur:
            _apply_session_settings_psycopg(cur)
            cur.execute(
                stmt,
                (
                    now.strftime("%Y%m%d"),
                    now.strftime("%H:%M:%S"),
                    "wait",
                    str(contents),
                    WAIT_ROW_KEY,
                )
            )
        conn.commit()
        return True

    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        diag_exception("upsert_wait_log_once", e)
        return False

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _upsert_wait_log(contents: str, retry_forever: bool = True):
    while True:
        ok = _upsert_wait_log_once(contents)
        if ok:
            return True
        if not retry_forever:
            return False
        time.sleep(DB_RETRY_INTERVAL_SEC)


def _insert_normal_log_once(info: str, contents: str):
    now = datetime.now()
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": _normalize_info(info),
        "contents": str(contents),
    }

    insert_sql = sql.SQL("""
        INSERT INTO {}.{} (end_day, end_time, info, contents, created_at, updated_at)
        VALUES %s
    """).format(sql.Identifier(LOG_SCHEMA), sql.Identifier(LOG_TABLE))

    conn = None
    try:
        conn = _connect_log_db()
        conn.autocommit = False
        with conn.cursor() as cur:
            _apply_session_settings_psycopg(cur)
            execute_values(
                cur,
                insert_sql.as_string(conn),
                [(row["end_day"], row["end_time"], row["info"], row["contents"], now, now)],
                template="(%s, %s, %s, %s, %s, %s)",
                page_size=1,
            )
        conn.commit()
        return True

    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        diag_exception("insert_normal_log_once", e)
        return False

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _insert_normal_log(info: str, contents: str, retry_forever: bool = True):
    while True:
        ok = _insert_normal_log_once(info, contents)
        if ok:
            return True
        if not retry_forever:
            return False
        time.sleep(DB_RETRY_INTERVAL_SEC)


def _save_log_to_db(info: str, contents: str, retry_forever: bool = True):
    info_n = _normalize_info(info)
    if info_n == "wait":
        return _upsert_wait_log(contents, retry_forever=retry_forever)
    return _insert_normal_log(info_n, contents, retry_forever=retry_forever)


def log(msg: str, info: str = "info", save_db: bool = True, retry_forever: bool = False):
    info_n = _normalize_info(info)
    print(msg, flush=True)
    _append_agent_log(_format_log_line(info_n, msg))

    if save_db:
        ok = _save_log_to_db(info_n, msg, retry_forever=retry_forever)
        if not ok:
            warn_msg = f"[WARN] log db write failed once and skipped | info={info_n}"
            print(warn_msg, flush=True)
            _append_agent_log(_format_log_line("warn", warn_msg))


# =========================
# 8) 백그라운드 heartbeat
# =========================
def _heartbeat_loop():
    _append_agent_log(_format_log_line("info", "heartbeat thread started"))

    while not _HB_STOP.is_set():
        hb_msg = _get_heartbeat_message()
        ok = _upsert_wait_log(hb_msg, retry_forever=False)

        if ok:
            _append_agent_log(_format_log_line("heartbeat", hb_msg))
        else:
            _append_agent_log(_format_log_line("heartbeat_fail", hb_msg))

        _HB_STOP.wait(HEARTBEAT_INTERVAL_SEC)

    _append_agent_log(_format_log_line("info", "heartbeat thread stopped"))


def start_heartbeat_thread():
    global _HB_THREAD

    if _HB_THREAD is not None and _HB_THREAD.is_alive():
        return

    _HB_STOP.clear()
    _HB_THREAD = threading.Thread(
        target=_heartbeat_loop,
        name="scheduler-heartbeat",
        daemon=True,
    )
    _HB_THREAD.start()


def stop_heartbeat_thread():
    _HB_STOP.set()


# =========================
# 9) 스케줄 계산/대기
# =========================
def _schedule_candidates_for_day(day: date) -> List[datetime]:
    return [
        datetime(day.year, day.month, day.day, RUN_TIME_1.hour, RUN_TIME_1.minute, RUN_TIME_1.second),
        datetime(day.year, day.month, day.day, RUN_TIME_2.hour, RUN_TIME_2.minute, RUN_TIME_2.second),
    ]


def _next_run_datetime(now_dt: datetime) -> datetime:
    today = now_dt.date()
    t1, t2 = _schedule_candidates_for_day(today)

    if now_dt < t1:
        return t1
    if now_dt < t2:
        return t2

    tomorrow = today + timedelta(days=1)
    return _schedule_candidates_for_day(tomorrow)[0]


def _sleep_until(target_dt: datetime):
    while True:
        now = datetime.now()
        if now >= target_dt:
            return

        remaining = (target_dt - now).total_seconds()

        if remaining > 3600:
            sleep_sec = 300.0
        elif remaining > 600:
            sleep_sec = 60.0
        elif remaining > 120:
            sleep_sec = 30.0
        else:
            sleep_sec = 5.0

        msg = (
            f"until={target_dt:%Y-%m-%d %H:%M:%S} | "
            f"remaining={int(remaining)}s | sleep={sleep_sec:.0f}s"
        )
        _set_heartbeat_state("waiting", msg)
        _append_agent_log(_format_log_line("wait", f"[WAITING] {msg}"))

        time.sleep(sleep_sec)


# =========================
# 10) 테이블/인덱스/ID 보정
# =========================
def ensure_schema(conn, schema_name: str):
    with conn.cursor() as cur:
        _apply_session_settings_psycopg(cur)
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
    conn.commit()


def ensure_tables_and_indexes():
    while True:
        try:
            diag("ensure_tables_and_indexes", "start", save_db=True)

            with _connect_schema_db() as conn:
                ensure_schema(conn, TARGET_SCHEMA)

                with conn.cursor() as cur:
                    _apply_session_settings_psycopg(cur)

                    diag("ensure_table", TBL_OPCT_LATEST)
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            station TEXT NOT NULL,
                            remark TEXT NOT NULL,
                            month TEXT NOT NULL,
                            sample_amount INT,
                            op_ct_lower_outlier TEXT,
                            q1 NUMERIC(12,2),
                            median NUMERIC(12,2),
                            q3 NUMERIC(12,2),
                            op_ct_upper_outlier TEXT,
                            del_out_op_ct_av NUMERIC(12,2)
                        )
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

                    cur.execute(sql.SQL("""
                        ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now()
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

                    cur.execute(sql.SQL("""
                        ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now()
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (station, remark, month)
                    """).format(
                        sql.Identifier("ux_fct_op_ct_key"),
                        sql.Identifier(TARGET_SCHEMA),
                        sql.Identifier(TBL_OPCT_LATEST)
                    ))

                    diag("ensure_table", TBL_WHOLE_LATEST)
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            station TEXT NOT NULL,
                            remark TEXT NOT NULL,
                            month TEXT NOT NULL,
                            ct_eq NUMERIC(12,2),
                            uph NUMERIC(12,2),
                            final_ct NUMERIC(12,2)
                        )
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

                    cur.execute(sql.SQL("""
                        ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now()
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

                    cur.execute(sql.SQL("""
                        ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now()
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (station, remark, month)
                    """).format(
                        sql.Identifier("ux_fct_whole_op_ct_key"),
                        sql.Identifier(TARGET_SCHEMA),
                        sql.Identifier(TBL_WHOLE_LATEST)
                    ))

                    diag("ensure_table", TBL_OPCT_HIST)
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            snapshot_day TEXT NOT NULL,
                            snapshot_ts TIMESTAMP DEFAULT now(),
                            station TEXT NOT NULL,
                            remark TEXT NOT NULL,
                            month TEXT NOT NULL,
                            sample_amount INT,
                            op_ct_lower_outlier TEXT,
                            q1 NUMERIC(12,2),
                            median NUMERIC(12,2),
                            q3 NUMERIC(12,2),
                            op_ct_upper_outlier TEXT,
                            del_out_op_ct_av NUMERIC(12,2)
                        )
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (snapshot_day, station, remark, month)
                    """).format(
                        sql.Identifier("ux_fct_op_ct_hist_day_key"),
                        sql.Identifier(TARGET_SCHEMA),
                        sql.Identifier(TBL_OPCT_HIST)
                    ))

                    diag("ensure_table", TBL_WHOLE_HIST)
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            snapshot_day TEXT NOT NULL,
                            snapshot_ts TIMESTAMP DEFAULT now(),
                            station TEXT NOT NULL,
                            remark TEXT NOT NULL,
                            month TEXT NOT NULL,
                            ct_eq NUMERIC(12,2),
                            uph NUMERIC(12,2),
                            final_ct NUMERIC(12,2)
                        )
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (snapshot_day, station, remark, month)
                    """).format(
                        sql.Identifier("ux_fct_whole_op_ct_hist_day_key"),
                        sql.Identifier(TARGET_SCHEMA),
                        sql.Identifier(TBL_WHOLE_HIST)
                    ))

                conn.commit()
            break

        except Exception as e:
            diag_exception("ensure_tables_and_indexes", e)
            log(f"[DB][RETRY] ensure_tables_and_indexes failed: {_safe_repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)

    fix_id_sequence(TARGET_SCHEMA, TBL_OPCT_LATEST, "fct_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_WHOLE_LATEST, "fct_whole_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_OPCT_HIST, "fct_op_ct_hist_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_WHOLE_HIST, "fct_whole_op_ct_hist_id_seq")

    log(f'[OK] target tables ensured in schema "{TARGET_SCHEMA}"', info="info")


def fix_id_sequence(schema: str, table: str, seq_name: str):
    do_sql = f"""
    DO $$
    DECLARE
      v_schema TEXT := {schema!r};
      v_table  TEXT := {table!r};
      v_seq    TEXT := {seq_name!r};
      v_full_table TEXT := quote_ident(v_schema) || '.' || quote_ident(v_table);
      v_full_seq   TEXT := quote_ident(v_schema) || '.' || quote_ident(v_seq);
      v_max BIGINT;
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = v_schema
          AND table_name = v_table
          AND column_name = 'id'
      ) THEN
        EXECUTE 'ALTER TABLE ' || v_full_table || ' ADD COLUMN id BIGINT';
      END IF;

      IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname = v_schema
          AND c.relname = v_seq
      ) THEN
        EXECUTE 'CREATE SEQUENCE ' || v_full_seq;
      END IF;

      EXECUTE 'ALTER TABLE ' || v_full_table ||
              ' ALTER COLUMN id SET DEFAULT nextval(''' || v_full_seq || ''')';

      EXECUTE 'ALTER SEQUENCE ' || v_full_seq ||
              ' OWNED BY ' || v_full_table || '.id';

      EXECUTE 'UPDATE ' || v_full_table || ' SET id = nextval(''' || v_full_seq || ''') WHERE id IS NULL';

      EXECUTE 'SELECT COALESCE(MAX(id), 0) FROM ' || v_full_table INTO v_max;
      EXECUTE 'SELECT setval(''' || v_full_seq || ''', ' || (v_max + 1) || ', false)';
    END $$;
    """

    while True:
        try:
            diag("fix_id_sequence", f"{schema}.{table}")
            with _connect_schema_db() as conn:
                with conn.cursor() as cur:
                    _apply_session_settings_psycopg(cur)
                    cur.execute(do_sql)
                conn.commit()
            return
        except Exception as e:
            diag_exception("fix_id_sequence", e)
            log(f"[DB][RETRY] fix_id_sequence({schema}.{table}) failed: {_safe_repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 11) 소스 로딩 및 op_ct 계산
# =========================
def load_month_df(engine, stations: List[str], run_month: str) -> pd.DataFrame:
    sql_query = f"""
    SELECT
        station,
        remark,
        end_day,
        end_time
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE station = ANY(:stations)
      AND remark IN ('PD','Non-PD')
      AND result <> 'FAIL'
      AND goodorbad <> 'BadFile'
      AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
    ORDER BY station ASC, remark ASC, end_time ASC
    """

    while True:
        try:
            diag("load_month_df", f"month={run_month}, stations={stations}", save_db=True)
            with engine.connect() as conn:
                _apply_session_settings_sqlalchemy(conn)
                df = pd.read_sql(
                    text(sql_query),
                    conn,
                    params={"stations": stations, "run_month": run_month},
                )
            break
        except Exception as e:
            diag_exception("load_month_df", e)
            if _is_connection_error(e):
                log(f"[DB][RETRY] load_month_df conn error -> rebuild: {_safe_repr(e)}", info="down")
                _dispose_main_engine()
            else:
                log(f"[DB][RETRY] load_month_df failed: {_safe_repr(e)}", info="error")
            time.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_main_engine_blocking(DB_CONFIG)

    if df.empty:
        return df

    diag("load_month_df_postprocess", f"rows={len(df):,}")

    df["end_day"] = pd.to_datetime(df["end_day"], errors="coerce").dt.date
    df["end_time_str"] = df["end_time"].astype(str)
    df["end_ts"] = pd.to_datetime(df["end_day"].astype(str) + " " + df["end_time_str"], errors="coerce")

    df = df.dropna(subset=["end_ts"]).copy()
    df = df.sort_values(["station", "remark", "end_ts"], ascending=True).reset_index(drop=True)
    df["op_ct"] = df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    df["month"] = df["end_ts"].dt.strftime("%Y%m")
    return df


# =========================
# 12) 통계 요약
# =========================
def _range_str(values: pd.Series):
    values = values.dropna()
    if len(values) == 0:
        return None
    return f"{float(values.min()):.1f}~{float(values.max()):.1f}"


def summarize_group(g: pd.DataFrame) -> Dict:
    s = g["op_ct"].dropna()
    s = s[s <= OPCT_MAX_SEC]

    sample_amount = len(s)
    if sample_amount == 0:
        return {
            "sample_amount": 0,
            "op_ct_lower_outlier": None,
            "q1": None,
            "median": None,
            "q3": None,
            "op_ct_upper_outlier": None,
            "del_out_op_ct_av": None,
        }

    q1 = float(s.quantile(0.25))
    med = float(s.quantile(0.50))
    q3 = float(s.quantile(0.75))
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    lower_outliers = s[s < lower_bound]
    upper_outliers = s[s > upper_bound]

    s_wo = s[(s >= lower_bound) & (s <= upper_bound)]
    avg_wo = float(s_wo.mean()) if len(s_wo) else None

    return {
        "sample_amount": int(sample_amount),
        "op_ct_lower_outlier": _range_str(lower_outliers),
        "q1": round(q1, 2),
        "median": round(med, 2),
        "q3": round(q3, 2),
        "op_ct_upper_outlier": _range_str(upper_outliers),
        "del_out_op_ct_av": round(avg_wo, 2) if avg_wo is not None else None,
    }


def build_summary_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=[
            "station", "remark", "month",
            "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
            "op_ct_upper_outlier", "del_out_op_ct_av"
        ])

    diag("build_summary_df", f"raw_rows={len(df_raw):,}")

    rows = []
    for (station, remark, month), g in df_raw.groupby(["station", "remark", "month"], sort=True):
        rows.append({
            "station": station,
            "remark": remark,
            "month": month,
            **summarize_group(g)
        })

    out = pd.DataFrame(rows)
    out = out.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    diag("build_summary_df_done", f"summary_rows={len(out):,}")
    return out


# =========================
# 13) UPH / CTeq 계산
# =========================
def parallel_uph(ct_series: pd.Series) -> float:
    ct = ct_series.dropna()
    ct = ct[ct > 0]
    if len(ct) == 0:
        return np.nan
    return 3600.0 * (1.0 / ct).sum()


def build_final_df_86(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df is None or summary_df.empty:
        return pd.DataFrame(columns=["station", "remark", "month", "ct_eq", "uph", "final_ct"])

    diag("build_final_df_86", f"summary_rows={len(summary_df):,}")

    b = summary_df[summary_df["station"].isin(["FCT1", "FCT2", "FCT3", "FCT4"])].copy()
    b["del_out_op_ct_av"] = pd.to_numeric(b["del_out_op_ct_av"], errors="coerce")

    side_map = {"FCT1": "left", "FCT2": "left", "FCT3": "right", "FCT4": "right"}
    b["side"] = b["station"].map(side_map)

    grp_side = (
        b.groupby(["month", "remark", "side"], as_index=False)
         .agg(ct_list=("del_out_op_ct_av", lambda x: list(x)))
    )

    grp_side["uph"] = grp_side["ct_list"].apply(lambda lst: parallel_uph(pd.Series(lst)))
    grp_side["ct_eq"] = np.where(grp_side["uph"] > 0, 3600.0 / grp_side["uph"], np.nan)

    grp_side["uph"] = grp_side["uph"].round(2)
    grp_side["ct_eq"] = grp_side["ct_eq"].round(2)

    left_right_df = grp_side.rename(columns={"side": "station"})[
        ["station", "remark", "month", "ct_eq", "uph"]
    ].copy()
    left_right_df["final_ct"] = np.nan

    whole_df = left_right_df.groupby(["month", "remark"], as_index=False)["uph"].sum()
    whole_df["station"] = "whole"
    whole_df["ct_eq"] = np.nan
    whole_df["final_ct"] = np.where(
        whole_df["uph"] > 0,
        (3600.0 / whole_df["uph"]).round(2),
        np.nan
    )

    final_df = pd.concat(
        [
            left_right_df[["station", "remark", "month", "ct_eq", "uph", "final_ct"]],
            whole_df[["station", "remark", "month", "ct_eq", "uph", "final_ct"]],
        ],
        ignore_index=True
    )

    station_order = pd.CategoricalDtype(["left", "right", "whole"], ordered=True)
    final_df["station"] = final_df["station"].astype(station_order)
    final_df = final_df.sort_values(["month", "remark", "station"]).reset_index(drop=True)

    diag("build_final_df_86_done", f"whole_rows={len(final_df):,}")
    return final_df


# =========================
# 14) execute_values 기반 UPSERT
# =========================
def _df_to_tuples(df: pd.DataFrame, cols: List[str]):
    if df is None or df.empty:
        return []
    return [tuple(row[c] for c in cols) for _, row in df[cols].iterrows()]


def _execute_target_values_forever(stage: str, stmt, values, template: str):
    while True:
        try:
            with _connect_target_db() as conn:
                with conn.cursor() as cur:
                    _apply_session_settings_psycopg(cur)
                    execute_values(
                        cur,
                        stmt.as_string(conn),
                        values,
                        template=template,
                        page_size=max(1, len(values)),
                    )
                conn.commit()
            return
        except Exception as e:
            diag_exception(stage, e)
            log(f"[DB][RETRY] {stage} failed: {_safe_repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)


def upsert_latest_opct(df: pd.DataFrame):
    if df is None or df.empty:
        return

    diag("upsert_latest_opct", f"rows={len(df):,}", save_db=True)

    df = _cast_df_for_db(df)

    cols = [
        "station", "remark", "month",
        "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
        "op_ct_upper_outlier", "del_out_op_ct_av"
    ]

    rows = _df_to_tuples(df, cols)
    if not rows:
        return

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            station, remark, month,
            sample_amount, op_ct_lower_outlier, q1, median, q3,
            op_ct_upper_outlier, del_out_op_ct_av,
            created_at, updated_at
        )
        VALUES %s
        ON CONFLICT (station, remark, month)
        DO UPDATE SET
            sample_amount       = EXCLUDED.sample_amount,
            op_ct_lower_outlier = EXCLUDED.op_ct_lower_outlier,
            q1                  = EXCLUDED.q1,
            median              = EXCLUDED.median,
            q3                  = EXCLUDED.q3,
            op_ct_upper_outlier = EXCLUDED.op_ct_upper_outlier,
            del_out_op_ct_av    = EXCLUDED.del_out_op_ct_av,
            updated_at          = now()
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST))

    now_ts = datetime.now()
    values = [r + (now_ts, now_ts) for r in rows]

    _execute_target_values_forever(
        "upsert_latest_opct",
        stmt,
        values,
        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )


def upsert_latest_whole(df: pd.DataFrame):
    if df is None or df.empty:
        return

    diag("upsert_latest_whole", f"rows={len(df):,}", save_db=True)

    df = _cast_df_for_db(df)

    cols = ["station", "remark", "month", "ct_eq", "uph", "final_ct"]
    rows = _df_to_tuples(df, cols)
    if not rows:
        return

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            station, remark, month,
            ct_eq, uph, final_ct,
            created_at, updated_at
        )
        VALUES %s
        ON CONFLICT (station, remark, month)
        DO UPDATE SET
            ct_eq      = EXCLUDED.ct_eq,
            uph        = EXCLUDED.uph,
            final_ct   = EXCLUDED.final_ct,
            updated_at = now()
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST))

    now_ts = datetime.now()
    values = [r + (now_ts, now_ts) for r in rows]

    _execute_target_values_forever(
        "upsert_latest_whole",
        stmt,
        values,
        "(%s,%s,%s,%s,%s,%s,%s,%s)"
    )


def upsert_hist_opct_daily(df: pd.DataFrame, snapshot_day: str):
    if df is None or df.empty:
        return

    diag("upsert_hist_opct_daily", f"rows={len(df):,}, snapshot_day={snapshot_day}", save_db=True)

    df = _cast_df_for_db(df)

    cols = [
        "station", "remark", "month",
        "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
        "op_ct_upper_outlier", "del_out_op_ct_av"
    ]
    rows = _df_to_tuples(df, cols)
    if not rows:
        return

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            snapshot_day, snapshot_ts,
            station, remark, month,
            sample_amount, op_ct_lower_outlier, q1, median, q3,
            op_ct_upper_outlier, del_out_op_ct_av
        )
        VALUES %s
        ON CONFLICT (snapshot_day, station, remark, month)
        DO UPDATE SET
            snapshot_ts         = EXCLUDED.snapshot_ts,
            sample_amount       = EXCLUDED.sample_amount,
            op_ct_lower_outlier = EXCLUDED.op_ct_lower_outlier,
            q1                  = EXCLUDED.q1,
            median              = EXCLUDED.median,
            q3                  = EXCLUDED.q3,
            op_ct_upper_outlier = EXCLUDED.op_ct_upper_outlier,
            del_out_op_ct_av    = EXCLUDED.del_out_op_ct_av
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST))

    now_ts = datetime.now()
    values = [(snapshot_day, now_ts) + r for r in rows]

    _execute_target_values_forever(
        "upsert_hist_opct_daily",
        stmt,
        values,
        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )


def upsert_hist_whole_daily(df: pd.DataFrame, snapshot_day: str):
    if df is None or df.empty:
        return

    diag("upsert_hist_whole_daily", f"rows={len(df):,}, snapshot_day={snapshot_day}", save_db=True)

    df = _cast_df_for_db(df)

    cols = ["station", "remark", "month", "ct_eq", "uph", "final_ct"]
    rows = _df_to_tuples(df, cols)
    if not rows:
        return

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            snapshot_day, snapshot_ts,
            station, remark, month,
            ct_eq, uph, final_ct
        )
        VALUES %s
        ON CONFLICT (snapshot_day, station, remark, month)
        DO UPDATE SET
            snapshot_ts = EXCLUDED.snapshot_ts,
            ct_eq       = EXCLUDED.ct_eq,
            uph         = EXCLUDED.uph,
            final_ct    = EXCLUDED.final_ct
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST))

    now_ts = datetime.now()
    values = [(snapshot_day, now_ts) + r for r in rows]

    _execute_target_values_forever(
        "upsert_hist_whole_daily",
        stmt,
        values,
        "(%s,%s,%s,%s,%s,%s,%s,%s)"
    )


# =========================
# 15) 1회 실행
# =========================
def run_pipeline_once(label: str, run_month: str):
    snapshot_day = today_yyyymmdd()
    _set_heartbeat_state("run_start", f"label={label}, month={run_month}")

    log(f"[RUN] {label} | snapshot_day={snapshot_day} | month={run_month} | START", info="info")

    diag("pipeline_get_engine", f"label={label}, month={run_month}", save_db=True)
    eng = get_main_engine_blocking(DB_CONFIG)

    diag("pipeline_load_source", f"month={run_month}", save_db=True)
    log(f"[RUN] {label} | loading source month={run_month}", info="info")
    df_raw = load_month_df(eng, ALL_STATIONS, run_month)

    if df_raw is None or df_raw.empty:
        _set_heartbeat_state("run_no_data", f"label={label}, month={run_month}")
        log(f"[RUN] {label} | no source rows (month={run_month}) -> skip save", info="info")
        return

    _set_heartbeat_state("run_build_summary", f"label={label}, source_rows={len(df_raw):,}")
    log(f"[RUN] {label} | source rows={len(df_raw):,}", info="info")

    summary_df = build_summary_df(df_raw)

    _set_heartbeat_state("run_build_whole", f"label={label}, summary_rows={len(summary_df):,}")
    whole_df = build_final_df_86(summary_df)

    log(f"[RUN] {label} | summary rows={len(summary_df):,} whole rows={len(whole_df):,}", info="info")

    _set_heartbeat_state("run_upsert_latest_opct", f"label={label}, rows={len(summary_df):,}")
    upsert_latest_opct(summary_df)

    _set_heartbeat_state("run_upsert_latest_whole", f"label={label}, rows={len(whole_df):,}")
    upsert_latest_whole(whole_df)

    _set_heartbeat_state("run_upsert_hist_opct", f"label={label}, rows={len(summary_df):,}")
    upsert_hist_opct_daily(summary_df, snapshot_day=snapshot_day)

    _set_heartbeat_state("run_upsert_hist_whole", f"label={label}, rows={len(whole_df):,}")
    upsert_hist_whole_daily(whole_df, snapshot_day=snapshot_day)

    _set_heartbeat_state("run_done", f"label={label}, summary={len(summary_df):,}, whole={len(whole_df):,}")
    log(f"[RUN] {label} | DONE (latest+hist upsert) | summary={len(summary_df):,} whole={len(whole_df):,}", info="info")


# =========================
# 16) main
# =========================
def main():
    _ensure_agent_log_dir()
    _append_agent_log(_format_log_line("info", "process boot"))

    ensure_log_table()

    _set_heartbeat_state("startup", "log table ensured")
    start_heartbeat_thread()

    start_dt = datetime.now()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}", info="start")
    log("=== ALWAYS-ON MODE: run at 08:22 and 20:22 EVERY DAY (no exit) ===", info="info")
    log(
        f"work_mem={WORK_MEM} | statement_timeout={STATEMENT_TIMEOUT_MS}ms | "
        f"log_statement_timeout={LOG_STATEMENT_TIMEOUT_MS}ms | "
        f"heartbeat_statement_timeout={HEARTBEAT_STATEMENT_TIMEOUT_MS}ms | "
        f"main/log/heartbeat/target separated | agent_log_dir={AGENT_LOG_DIR}",
        info="info"
    )

    ensure_tables_and_indexes()

    done_map: Dict[str, set] = {}

    while True:
        now = datetime.now()
        next_dt = _next_run_datetime(now)

        target_day = next_dt.strftime("%Y%m%d")
        target_label = next_dt.strftime("%H:%M:%S")

        done_set = done_map.setdefault(target_day, set())

        if target_label in done_set:
            _set_heartbeat_state("idle_done", f"day={target_day}, done={sorted(list(done_set))}")
            time.sleep(1.0)
            continue

        if len(done_map) > 5:
            for k in sorted(list(done_map.keys()))[:-3]:
                done_map.pop(k, None)

        log(f"[WAIT] next_run={next_dt:%Y-%m-%d %H:%M:%S} | done({target_day})={sorted(list(done_set))}", info="wait")

        _set_heartbeat_state("waiting_next_run", f"next_run={next_dt:%Y-%m-%d %H:%M:%S}")
        _sleep_until(next_dt)

        run_month = current_yyyymm()

        while True:
            try:
                _set_heartbeat_state("run_triggered", f"day={target_day}, time={target_label}, month={run_month}")
                run_pipeline_once(label=target_label, run_month=run_month)
                done_map.setdefault(target_day, set()).add(target_label)
                log(f"[OK] done day={target_day} time={target_label} -> {sorted(list(done_map[target_day]))}", info="info")
                break
            except Exception as e:
                diag_exception("main_run_loop", e)
                if _is_connection_error(e):
                    log(f"[RUN][RETRY] {target_label} conn error -> rebuild main engine: {_safe_repr(e)}", info="down")
                    _dispose_main_engine()
                    _ = get_main_engine_blocking(DB_CONFIG)
                else:
                    log(f"[RUN][RETRY] {target_label} failed: {_safe_repr(e)}", info="error")
                time.sleep(DB_RETRY_INTERVAL_SEC)

        if done_map.get(target_day) == {"08:22:00", "20:22:00"}:
            _set_heartbeat_state("day_end", f"{target_day} both runs completed")
            log(f"[DAY-END] {target_day} both runs completed. keep alive for next day.", info="end")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        stop_heartbeat_thread()
        raise
    except Exception as e:
        diag_exception("__main__", e)
        stop_heartbeat_thread()
        raise
