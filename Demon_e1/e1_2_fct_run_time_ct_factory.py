# -*- coding: utf-8 -*-
"""
e1_2_fct_runtime_ct_daemon.py

운영 방식
- 평소에는 대기
- heartbeat 전용 프로세스가 1분마다 별도 DB 커넥션으로 heartbeat 로그 기록
- 메인 프로세스는 매일 자정 직후(00:00:10 이후) 전일 데이터를 기준으로 일자 단위 백필 수행
- last_processed_day 기반 누락 일자 복구 지원
- 월 전체 통계는 "월초 ~ 전일" 데이터를 일자 단위로 모아 계산

핵심 포인트
- batch fetch timeout 분리
- heartbeat 전용 프로세스 분리
- application_name 분리
- print 는 DB 기록 뒤에만 수행
- 커넥션 재진입 버그 수정
"""

from __future__ import annotations

import os
import time as time_mod
import urllib.parse
import multiprocessing as mp
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

import plotly.graph_objects as go
import plotly.io as pio


# =========================
# TZ / Constants
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5
FETCH_LIMIT_PER_DAY = int(os.getenv("E1_2_FETCH_LIMIT_PER_DAY", "200000"))
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

APP_NAME_MAIN = os.getenv("E1_2_APP_NAME_MAIN", "e1_2_fct_runtime_ct_main")
APP_NAME_LOG = os.getenv("E1_2_APP_NAME_LOG", "e1_2_fct_runtime_ct_log")
APP_NAME_HEARTBEAT = os.getenv("E1_2_APP_NAME_HEARTBEAT", "e1_2_fct_runtime_ct_heartbeat")

GENERAL_STATEMENT_TIMEOUT_MS = int(os.getenv("E1_2_GENERAL_STMT_TIMEOUT_MS", "60000"))
BATCH_FETCH_TIMEOUT_MS = int(os.getenv("E1_2_BATCH_FETCH_TIMEOUT_MS", "600000"))   # 10분
UPSERT_TIMEOUT_MS = int(os.getenv("E1_2_UPSERT_TIMEOUT_MS", "300000"))              # 5분
LOCK_TIMEOUT_MS = int(os.getenv("PG_LOCK_TIMEOUT_MS", "5000"))
LOG_TIMEOUT_MS = int(os.getenv("E1_2_LOG_TIMEOUT_MS", "5000"))

IDLE_LOG_INTERVAL_SEC = int(os.getenv("E1_2_IDLE_LOG_INTERVAL_SEC", "60"))
SLEEP_LOG_INTERVAL_SEC = int(os.getenv("E1_2_SLEEP_LOG_INTERVAL_SEC", "60"))
HEARTBEAT_INTERVAL_SEC = int(os.getenv("E1_2_HEARTBEAT_INTERVAL_SEC", "60"))

ENABLE_STDOUT_PRINT = os.getenv("E1_2_ENABLE_STDOUT_PRINT", "1") == "1"

DAILY_BATCH_START_HMS = (
    int(os.getenv("E1_2_BATCH_START_HOUR", "0")),
    int(os.getenv("E1_2_BATCH_START_MINUTE", "0")),
    int(os.getenv("E1_2_BATCH_START_SECOND", "10")),
)

STATUS_FILE = os.getenv("E1_2_STATUS_FILE", os.path.join(os.getcwd(), "e1_2_status.txt"))


# =========================
# DB Config
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": os.getenv("PGPASSWORD", ""),
}


def _make_url(app_name: str) -> str:
    pwd = urllib.parse.quote_plus(DB_CONFIG["password"])
    app = urllib.parse.quote_plus(app_name)
    return (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{pwd}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        f"?application_name={app}"
    )


# =========================
# Source / Target / Log / State
# =========================
SRC_SCHEMA = "a2_fct_table"
SRC_TABLE = "fct_table"

TARGET_SCHEMA = "e1_FCT_ct"
TBL_RUN_TIME_LATEST = "fct_run_time_ct"
TBL_RUN_TIME_HIST = "fct_run_time_ct_hist"
TBL_OUTLIER_LIST = "fct_upper_outlier_ct_list"

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "e1_2_log"

STATE_SCHEMA = TARGET_SCHEMA
STATE_TABLE = "e1_2_state"
STATE_FQN = f'"{STATE_SCHEMA}".{STATE_TABLE}'
STATE_KEY = "e1_2_fct_runtime_ct"

STATIONS_ALL = ["FCT1", "FCT2", "FCT3", "FCT4"]

_DATA_ENGINE: Optional[Engine] = None
_LOG_ENGINE: Optional[Engine] = None

_LAST_LOG_TS: Dict[str, float] = {}


# =========================
# Time / Date Helpers
# =========================
def _ts_kst() -> datetime:
    return datetime.now(KST)


def _fmt_now() -> str:
    return _ts_kst().strftime("%Y-%m-%d %H:%M:%S")


def current_yyyymm(now: Optional[datetime] = None) -> str:
    now = now or _ts_kst()
    return now.strftime("%Y%m")


def month_start_day_str(day_str: str) -> str:
    return day_str[:6] + "01"


def to_day_str(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_day_str(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def yesterday_day_str(now: Optional[datetime] = None) -> str:
    now = now or _ts_kst()
    return to_day_str((now.date() - timedelta(days=1)))


def batch_ready(now: Optional[datetime] = None) -> bool:
    now = now or _ts_kst()
    h, m, s = DAILY_BATCH_START_HMS
    return now.timetz().replace(tzinfo=None) >= time(h, m, s)


# =========================
# Status file for heartbeat process
# =========================
def write_status_file(message: str) -> None:
    try:
        tmp = STATUS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(f"{_fmt_now()}|{message}")
        os.replace(tmp, STATUS_FILE)
    except Exception:
        pass


def read_status_file() -> str:
    try:
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            return f.read().strip() or "unknown"
    except Exception:
        return "status_unavailable"


# =========================
# Session apply helpers
# =========================
def apply_general_session(conn) -> None:
    conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))
    conn.execute(text(f"SET statement_timeout = '{GENERAL_STATEMENT_TIMEOUT_MS}ms'"))
    conn.execute(text(f"SET lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))


def apply_log_session(conn) -> None:
    conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))
    conn.execute(text(f"SET statement_timeout = '{LOG_TIMEOUT_MS}ms'"))
    conn.execute(text(f"SET lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))


def apply_batch_fetch_session(conn) -> None:
    conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))
    conn.execute(text(f"SET statement_timeout = '{BATCH_FETCH_TIMEOUT_MS}ms'"))
    conn.execute(text(f"SET lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))


def apply_upsert_session(conn) -> None:
    conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))
    conn.execute(text(f"SET statement_timeout = '{UPSERT_TIMEOUT_MS}ms'"))
    conn.execute(text(f"SET lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))


# =========================
# Engine builders
# =========================
def make_data_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")
    return create_engine(
        _make_url(APP_NAME_MAIN),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
    )


def make_log_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")
    return create_engine(
        _make_url(APP_NAME_LOG),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
    )


def make_heartbeat_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")
    return create_engine(
        _make_url(APP_NAME_HEARTBEAT),
        pool_size=1,
        max_overflow=1,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=10,
    )


# =========================
# Logging
# =========================
def _should_log_rate_limited(key: str, interval_sec: int) -> bool:
    now_ts = time_mod.time()
    last_ts = _LAST_LOG_TS.get(key, 0.0)
    if (now_ts - last_ts) >= interval_sec:
        _LAST_LOG_TS[key] = now_ts
        return True
    return False


def _safe_print(msg: str) -> None:
    if not ENABLE_STDOUT_PRINT:
        return
    try:
        print(msg, flush=True)
    except Exception:
        pass


def insert_health_log_with_engine(
    engine: Engine,
    level_tag: str,
    info: str,
    msg: str,
) -> None:
    now = _ts_kst()
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": (info or "info").lower(),
        "contents": str(msg)[:4000],
    }
    sql = text(f"""
        INSERT INTO "{LOG_SCHEMA}".{LOG_TABLE}
        (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
    """)
    with engine.begin() as conn:
        apply_log_session(conn)
        conn.execute(sql, [row])


def _emit(level_tag: str, info: str, msg: str, persist: bool = True) -> None:
    global _LOG_ENGINE
    db_log_err: Optional[str] = None

    if persist and _LOG_ENGINE is not None:
        try:
            insert_health_log_with_engine(_LOG_ENGINE, level_tag, info, msg)
        except Exception as e:
            db_log_err = f"{type(e).__name__}: {e}"

    _safe_print(f"{_fmt_now()} [{level_tag}] {msg}")

    if db_log_err:
        _safe_print(f"{_fmt_now()} [WARN] health-log insert failed: {db_log_err}")


def log_boot(msg: str) -> None:
    _emit("BOOT", "boot", msg, persist=True)


def log_info(msg: str) -> None:
    _emit("INFO", "info", msg, persist=True)


def log_retry(msg: str) -> None:
    _emit("RETRY", "down", msg, persist=True)


def log_warn(msg: str) -> None:
    _emit("WARN", "warn", msg, persist=True)


def log_error(msg: str) -> None:
    _emit("WARN", "error", msg, persist=True)


def log_sleep(msg: str, rate_limited: bool = False) -> None:
    if rate_limited:
        if _should_log_rate_limited("sleep", SLEEP_LOG_INTERVAL_SEC):
            _emit("INFO", "sleep", msg, persist=True)
        return
    _emit("INFO", "sleep", msg, persist=True)


def log_heartbeat_local(msg: str) -> None:
    if _should_log_rate_limited("heartbeat_local", IDLE_LOG_INTERVAL_SEC):
        _emit("INFO", "heartbeat", msg, persist=True)


# =========================
# DB identity / DDL
# =========================
def log_db_identity(engine: Engine) -> None:
    sql = """
    SELECT
      current_database()       AS db,
      current_user             AS usr,
      inet_server_addr()::text AS server_ip,
      inet_server_port()       AS server_port,
      inet_client_addr()::text AS client_ip,
      current_setting('application_name', true) AS app_name,
      current_setting('statement_timeout', true) AS statement_timeout,
      current_setting('lock_timeout', true) AS lock_timeout
    """
    with engine.connect() as conn:
        apply_general_session(conn)
        row = conn.execute(text(sql)).mappings().first()

    log_info(
        f"[DB_ID] db={row['db']} usr={row['usr']} "
        f"server={row['server_ip']}:{row['server_port']} client={row['client_ip']} "
        f"app={row['app_name']} stmt_to={row['statement_timeout']} lock_to={row['lock_timeout']}"
    )


def ensure_log_table(engine: Engine) -> None:
    with engine.begin() as conn:
        apply_log_session(conn)
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}";'))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{LOG_SCHEMA}".{LOG_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                end_day    TEXT NOT NULL,
                end_time   TEXT NOT NULL,
                info       TEXT NOT NULL,
                contents   TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS ix_{LOG_TABLE}_day_time
            ON "{LOG_SCHEMA}".{LOG_TABLE} (end_day, end_time);
        """))


def ensure_state_table(engine: Engine) -> None:
    with engine.begin() as conn:
        apply_general_session(conn)
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{STATE_SCHEMA}";'))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {STATE_FQN} (
                key                TEXT PRIMARY KEY,
                run_month          TEXT,
                last_end_ts        TIMESTAMPTZ,
                last_processed_day TEXT,
                updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            ALTER TABLE {STATE_FQN}
            ADD COLUMN IF NOT EXISTS last_processed_day TEXT;
        """))


def ensure_target_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        apply_general_session(conn)
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";'))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (
                id BIGSERIAL PRIMARY KEY,
                station                TEXT NOT NULL,
                remark                 TEXT NOT NULL,
                month                  TEXT NOT NULL,
                sample_amount          INTEGER,
                run_time_lower_outlier TEXT,
                q1                     DOUBLE PRECISION,
                median                 DOUBLE PRECISION,
                q3                     DOUBLE PRECISION,
                run_time_upper_outlier TEXT,
                del_out_run_time_av    DOUBLE PRECISION,
                plotly_json            JSONB,
                created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_RUN_TIME_LATEST}_key
            ON "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (station, remark, month);
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (
                id BIGSERIAL PRIMARY KEY,
                snapshot_day           TEXT NOT NULL,
                snapshot_ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
                station                TEXT NOT NULL,
                remark                 TEXT NOT NULL,
                month                  TEXT NOT NULL,
                sample_amount          INTEGER,
                run_time_lower_outlier TEXT,
                q1                     DOUBLE PRECISION,
                median                 DOUBLE PRECISION,
                q3                     DOUBLE PRECISION,
                run_time_upper_outlier TEXT,
                del_out_run_time_av    DOUBLE PRECISION,
                plotly_json            JSONB
            );
        """))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_RUN_TIME_HIST}_day_key
            ON "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (snapshot_day, station, remark, month);
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (
                id BIGSERIAL PRIMARY KEY,
                station             TEXT NOT NULL,
                remark              TEXT NOT NULL,
                barcode_information TEXT NOT NULL,
                end_day             TEXT NOT NULL,
                end_time            TEXT NOT NULL,
                run_time            DOUBLE PRECISION,
                month               TEXT,
                upper_threshold     DOUBLE PRECISION,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_OUTLIER_LIST}_key
            ON "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (station, remark, barcode_information, end_day, end_time);
        """))


# =========================
# State
# =========================
def load_state(engine: Engine) -> Tuple[Optional[str], Optional[datetime], Optional[str]]:
    sql = text(f"""
        SELECT run_month, last_end_ts, last_processed_day
        FROM {STATE_FQN}
        WHERE key=:k
    """)
    with engine.connect() as conn:
        apply_general_session(conn)
        row = conn.execute(sql, {"k": STATE_KEY}).mappings().first()

    if not row:
        return None, None, None
    return row["run_month"], row["last_end_ts"], row["last_processed_day"]


def save_state(
    engine: Engine,
    run_month: str,
    last_end_ts: Optional[datetime],
    last_processed_day: Optional[str],
) -> None:
    sql = text(f"""
        INSERT INTO {STATE_FQN} (key, run_month, last_end_ts, last_processed_day)
        VALUES (:k, :m, :ts, :d)
        ON CONFLICT (key) DO UPDATE SET
          run_month          = EXCLUDED.run_month,
          last_end_ts        = EXCLUDED.last_end_ts,
          last_processed_day = EXCLUDED.last_processed_day,
          updated_at         = now()
    """)
    with engine.begin() as conn:
        apply_general_session(conn)
        conn.execute(
            sql,
            {
                "k": STATE_KEY,
                "m": run_month,
                "ts": last_end_ts,
                "d": last_processed_day,
            },
        )


# =========================
# Timeout helper
# =========================
def _is_statement_timeout_error(exc: Exception) -> bool:
    s = f"{type(exc).__name__}: {exc}".lower()
    return (
        "querycanceled" in s
        or "statement timeout" in s
        or "canceling statement due to statement timeout" in s
        or "명령실행시간 초과" in s
        or "작업을 취소합니다" in s
    )


# =========================
# Fetch - day based
# =========================
def fetch_one_day(engine: Engine, stations: List[str], target_day: str) -> pd.DataFrame:
    run_month = target_day[:6]

    q = text(f"""
    WITH base AS (
        SELECT
            file_path,
            station,
            remark,
            barcode_information,
            step_description,
            result,
            regexp_replace(COALESCE(end_day::text, ''), '\\D', '', 'g') AS end_day_digits,
            regexp_replace(split_part(COALESCE(end_time::text, ''), '.', 1), '\\D', '', 'g') AS end_time_digits,
            end_time::text AS raw_end_time,
            run_time
        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE
            station = ANY(:stations)
            AND barcode_information LIKE 'B%%'
            AND result <> 'FAIL'
            AND (
                (remark = 'PD' AND step_description = '1.36 Test iqz(uA)')
                OR
                (remark = 'Non-PD' AND step_description = '1.32 Test iqz(uA)')
            )
            AND substring(regexp_replace(COALESCE(end_day::text, ''), '\\D', '', 'g') from 1 for 6) = :run_month
            AND regexp_replace(COALESCE(end_day::text, ''), '\\D', '', 'g') = :target_day
    ),
    valid AS (
        SELECT
            file_path,
            station,
            remark,
            barcode_information,
            step_description,
            result,
            end_day_digits AS end_day,
            end_time_digits AS end_time_digits,
            raw_end_time,
            run_time,
            to_timestamp(
                end_day_digits || ' ' ||
                substr(end_time_digits, 1, 2) || ':' ||
                substr(end_time_digits, 3, 2) || ':' ||
                substr(end_time_digits, 5, 2),
                'YYYYMMDD HH24:MI:SS'
            ) AS end_ts
        FROM base
        WHERE
            length(end_day_digits) = 8
            AND end_day_digits ~ '^[0-9]{{8}}$'
            AND length(end_time_digits) = 6
            AND end_time_digits ~ '^[0-9]{{6}}$'
            AND substring(end_time_digits from 1 for 2)::int BETWEEN 0 AND 23
            AND substring(end_time_digits from 3 for 2)::int BETWEEN 0 AND 59
            AND substring(end_time_digits from 5 for 2)::int BETWEEN 0 AND 59
    )
    SELECT
        file_path,
        station,
        remark,
        barcode_information,
        step_description,
        result,
        end_day,
        substr(end_time_digits, 1, 2) || ':' ||
        substr(end_time_digits, 3, 2) || ':' ||
        substr(end_time_digits, 5, 2) AS end_time,
        run_time,
        end_ts
    FROM valid
    ORDER BY end_ts ASC, file_path ASC
    LIMIT :limit
    """)

    with engine.connect() as conn:
        apply_batch_fetch_session(conn)
        df = pd.read_sql_query(
            q,
            conn,
            params={
                "stations": stations,
                "run_month": run_month,
                "target_day": target_day,
                "limit": FETCH_LIMIT_PER_DAY,
            },
        )

    if df is None or df.empty:
        return pd.DataFrame()

    df["file_path"] = df["file_path"].astype(str).str.strip()
    df = df[df["file_path"].notna() & (df["file_path"] != "")].copy()

    df["end_day"] = df["end_day"].astype(str).str.zfill(8)
    df["end_time"] = df["end_time"].astype(str).str.strip()
    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")

    df = df.dropna(subset=["run_time", "end_ts"]).reset_index(drop=True)
    df["month"] = df["end_day"].str.slice(0, 6)

    return df


def month_days_upto(target_day: str) -> List[str]:
    start_day = parse_day_str(month_start_day_str(target_day))
    end_day = parse_day_str(target_day)
    cur = start_day
    out: List[str] = []
    while cur <= end_day:
        out.append(to_day_str(cur))
        cur += timedelta(days=1)
    return out


def build_month_df_upto(engine: Engine, stations: List[str], target_day: str) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    days = month_days_upto(target_day)

    log_info(f"[BATCH] build month dataset start month={target_day[:6]} days={len(days)} upto={target_day}")
    write_status_file(f"batch_running build_month month={target_day[:6]} upto={target_day} days={len(days)}")

    for d in days:
        write_status_file(f"batch_running day_fetch={d}")
        df_day = fetch_one_day(engine, stations, d)
        log_info(f"[DAY_FETCH] day={d} rows={len(df_day)}")
        if not df_day.empty:
            parts.append(df_day)

    if not parts:
        return pd.DataFrame()

    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["file_path"], keep="last").reset_index(drop=True)
    return out


# =========================
# Analysis
# =========================
def _round2(x):
    if x is None:
        return None
    try:
        if np.isnan(x):
            return None
    except Exception:
        pass
    return round(float(x), 2)


def _outlier_range_str(outlier_values: np.ndarray):
    if outlier_values.size == 0:
        return None
    mn = round(float(np.min(outlier_values)), 2)
    mx = round(float(np.max(outlier_values)), 2)
    return f"{mn}~{mx}"


def _make_plotly_box_json(values: np.ndarray, name: str):
    fig = go.Figure(data=[go.Box(y=values.tolist(), name=name, boxpoints=False)])
    return pio.to_json(fig, validate=False)


def boxplot_summary_from_values(vals: np.ndarray, name: str) -> dict:
    n = int(vals.size)
    q1 = float(np.percentile(vals, 25))
    median = float(np.percentile(vals, 50))
    q3 = float(np.percentile(vals, 75))
    iqr = q3 - q1

    lower_th = q1 - 1.5 * iqr
    upper_th = q3 + 1.5 * iqr

    lower_outliers = vals[vals < lower_th]
    upper_outliers = vals[vals > upper_th]

    inliers = vals[(vals >= lower_th) & (vals <= upper_th)]
    del_out_mean = float(np.mean(inliers)) if inliers.size > 0 else None

    return {
        "sample_amount": n,
        "run_time_lower_outlier": _outlier_range_str(lower_outliers),
        "q1": _round2(q1),
        "median": _round2(median),
        "q3": _round2(q3),
        "run_time_upper_outlier": _outlier_range_str(upper_outliers),
        "del_out_run_time_av": _round2(del_out_mean),
        "plotly_json": _make_plotly_box_json(vals, name=name),
    }


def build_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "station", "remark", "month", "sample_amount",
                "run_time_lower_outlier", "q1", "median", "q3",
                "run_time_upper_outlier", "del_out_run_time_av", "plotly_json"
            ]
        )

    rows = []
    for (station, remark, month), g in df.groupby(["station", "remark", "month"], sort=True):
        vals = g["run_time"].astype(float).to_numpy()
        d = boxplot_summary_from_values(vals, name=f"{station}_{remark}_{month}")
        d.update({"station": station, "remark": remark, "month": month})
        rows.append(d)

    return pd.DataFrame(rows).sort_values(["station", "remark", "month"]).reset_index(drop=True)


def build_upper_outlier_df(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty or summary_df is None or summary_df.empty:
        return pd.DataFrame()

    upper_rows = []
    for _, row in summary_df.iterrows():
        station = row["station"]
        remark = row["remark"]
        month = row["month"]

        g = df_raw[
            (df_raw["station"] == station) &
            (df_raw["remark"] == remark) &
            (df_raw["month"] == month)
        ].copy()

        if g.empty:
            continue

        vals = g["run_time"].astype(float).to_numpy()
        q1 = np.percentile(vals, 25)
        q3 = np.percentile(vals, 75)
        iqr = q3 - q1
        upper_th = q3 + 1.5 * iqr

        out_df = g[g["run_time"] > upper_th].copy()
        if out_df.empty:
            continue

        out_df["upper_threshold"] = round(float(upper_th), 2)
        upper_rows.append(out_df)

    if not upper_rows:
        return pd.DataFrame()

    out = pd.concat(upper_rows, ignore_index=True)
    out = out[[
        "station", "remark", "barcode_information",
        "end_day", "end_time", "run_time", "month", "upper_threshold"
    ]]
    return out.sort_values(["station", "remark", "month", "end_day", "end_time"]).reset_index(drop=True)


# =========================
# DB Save
# =========================
def upsert_latest(engine: Engine, summary_df: pd.DataFrame) -> None:
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None)
    rows = df.to_dict("records")

    sql = text(f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (
        station, remark, month,
        sample_amount,
        run_time_lower_outlier,
        q1, median, q3,
        run_time_upper_outlier,
        del_out_run_time_av,
        plotly_json,
        created_at, updated_at
    )
    VALUES (
        :station, :remark, :month,
        :sample_amount,
        :run_time_lower_outlier,
        :q1, :median, :q3,
        :run_time_upper_outlier,
        :del_out_run_time_av,
        CAST(:plotly_json AS jsonb),
        now(), now()
    )
    ON CONFLICT (station, remark, month)
    DO UPDATE SET
        sample_amount          = EXCLUDED.sample_amount,
        run_time_lower_outlier = EXCLUDED.run_time_lower_outlier,
        q1                     = EXCLUDED.q1,
        median                 = EXCLUDED.median,
        q3                     = EXCLUDED.q3,
        run_time_upper_outlier = EXCLUDED.run_time_upper_outlier,
        del_out_run_time_av    = EXCLUDED.del_out_run_time_av,
        plotly_json            = EXCLUDED.plotly_json,
        updated_at             = now()
    """)
    with engine.begin() as conn:
        apply_upsert_session(conn)
        conn.execute(sql, rows)


def upsert_hist(engine: Engine, summary_df: pd.DataFrame, snapshot_day: str) -> None:
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    df.insert(0, "snapshot_day", snapshot_day)
    rows = df.to_dict("records")

    sql = text(f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (
        snapshot_day, snapshot_ts,
        station, remark, month,
        sample_amount,
        run_time_lower_outlier,
        q1, median, q3,
        run_time_upper_outlier,
        del_out_run_time_av,
        plotly_json
    )
    VALUES (
        :snapshot_day, now(),
        :station, :remark, :month,
        :sample_amount,
        :run_time_lower_outlier,
        :q1, :median, :q3,
        :run_time_upper_outlier,
        :del_out_run_time_av,
        CAST(:plotly_json AS jsonb)
    )
    ON CONFLICT (snapshot_day, station, remark, month)
    DO UPDATE SET
        snapshot_ts            = now(),
        sample_amount          = EXCLUDED.sample_amount,
        run_time_lower_outlier = EXCLUDED.run_time_lower_outlier,
        q1                     = EXCLUDED.q1,
        median                 = EXCLUDED.median,
        q3                     = EXCLUDED.q3,
        run_time_upper_outlier = EXCLUDED.run_time_upper_outlier,
        del_out_run_time_av    = EXCLUDED.del_out_run_time_av,
        plotly_json            = EXCLUDED.plotly_json
    """)
    with engine.begin() as conn:
        apply_upsert_session(conn)
        conn.execute(sql, rows)


def insert_outliers(engine: Engine, outlier_df: pd.DataFrame) -> None:
    if outlier_df is None or outlier_df.empty:
        return

    df = outlier_df.where(pd.notnull(outlier_df), None)
    rows = df.to_dict("records")

    sql = text(f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (
        station, remark, barcode_information, end_day, end_time,
        run_time, month, upper_threshold
    )
    VALUES (
        :station, :remark, :barcode_information, :end_day, :end_time,
        :run_time, :month, :upper_threshold
    )
    ON CONFLICT (station, remark, barcode_information, end_day, end_time)
    DO NOTHING
    """)
    with engine.begin() as conn:
        apply_upsert_session(conn)
        conn.execute(sql, rows)


# =========================
# Main helpers
# =========================
def connect_blocking() -> Tuple[Engine, Engine]:
    global _DATA_ENGINE, _LOG_ENGINE

    data_engine = make_data_engine()
    log_engine = make_log_engine()

    while True:
        try:
            with data_engine.connect() as conn:
                apply_general_session(conn)
            with log_engine.connect() as conn:
                apply_log_session(conn)

            ensure_log_table(log_engine)

            _DATA_ENGINE = data_engine
            _LOG_ENGINE = log_engine

            log_info(f"DB connected (work_mem={WORK_MEM})")
            log_db_identity(data_engine)
            log_info(f'health log ready -> "{LOG_SCHEMA}".{LOG_TABLE}')
            return data_engine, log_engine

        except Exception as e:
            _DATA_ENGINE = None
            _LOG_ENGINE = None
            _safe_print(f"{_fmt_now()} [RETRY] DB connect failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

            data_engine = make_data_engine()
            log_engine = make_log_engine()


def reconnect_data_engine() -> Tuple[Engine, Engine]:
    while True:
        try:
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect", rate_limited=True)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

            data_engine, log_engine = connect_blocking()
            ensure_state_table(data_engine)
            ensure_target_tables(data_engine)

            log_info("DB reconnected")
            log_db_identity(data_engine)
            return data_engine, log_engine

        except Exception as e:
            log_retry(f"reconnect failed: {type(e).__name__}: {e}")


def missing_days_to_process(last_processed_day: Optional[str], target_day: str) -> List[str]:
    target_dt = parse_day_str(target_day)
    month_start_dt = parse_day_str(month_start_day_str(target_day))

    if not last_processed_day:
        start_dt = month_start_dt
    else:
        last_dt = parse_day_str(last_processed_day)
        if last_dt >= target_dt:
            return []
        if last_dt < month_start_dt:
            start_dt = month_start_dt
        else:
            start_dt = last_dt + timedelta(days=1)

    out: List[str] = []
    cur = start_dt
    while cur <= target_dt:
        out.append(to_day_str(cur))
        cur += timedelta(days=1)
    return out


def run_daily_batch(
    engine: Engine,
    last_processed_day: Optional[str],
    target_day: str,
) -> str:
    to_process = missing_days_to_process(last_processed_day, target_day)

    if not to_process:
        log_info(f"[BATCH] already up-to-date target_day={target_day}")
        write_status_file(f"idle up_to_date target_day={target_day}")
        return last_processed_day or target_day

    log_info(
        f"[BATCH] start target_day={target_day} "
        f"last_processed_day={last_processed_day} "
        f"missing_days={len(to_process)} first={to_process[0]} last={to_process[-1]}"
    )
    write_status_file(
        f"batch_running target_day={target_day} "
        f"last_processed_day={last_processed_day} "
        f"missing_days={len(to_process)}"
    )

    month_df = build_month_df_upto(engine, STATIONS_ALL, target_day)

    write_status_file(f"batch_running build_summary target_day={target_day} rows={len(month_df)}")
    summary_df = build_summary_df(month_df)
    outlier_df = build_upper_outlier_df(month_df, summary_df)

    write_status_file(f"batch_running upsert target_day={target_day} groups={len(summary_df)} outliers={len(outlier_df)}")
    upsert_latest(engine, summary_df)
    upsert_hist(engine, summary_df, snapshot_day=target_day)
    insert_outliers(engine, outlier_df)

    log_info(
        f"[BATCH] done target_day={target_day} "
        f"rows={len(month_df)} groups={len(summary_df)} outliers={len(outlier_df)}"
    )
    write_status_file(f"idle batch_done target_day={target_day}")
    return target_day


# =========================
# Heartbeat process
# =========================
def heartbeat_worker() -> None:
    engine: Optional[Engine] = None

    while True:
        try:
            if engine is None:
                engine = make_heartbeat_engine()
                ensure_log_table(engine)

            status = read_status_file()
            msg = f"alive {status}"

            insert_health_log_with_engine(engine, "INFO", "heartbeat", msg)
            _safe_print(f"{_fmt_now()} [INFO] HEARTBEAT {msg}")

        except Exception as e:
            _safe_print(f"{_fmt_now()} [WARN] heartbeat worker error: {type(e).__name__}: {e}")
            engine = None

        time_mod.sleep(HEARTBEAT_INTERVAL_SEC)


# =========================
# Main
# =========================
def main() -> None:
    write_status_file("booting")
    log_boot("e1_2 fct_runtime_ct daemon starting (daily batch mode + heartbeat process)")

    heartbeat_proc = mp.Process(target=heartbeat_worker, name="e1_2_heartbeat", daemon=True)
    heartbeat_proc.start()

    data_engine, _ = connect_blocking()

    while True:
        try:
            ensure_state_table(data_engine)
            ensure_target_tables(data_engine)
            log_info(f'state table ready: {STATE_FQN}')
            log_info(f'target tables ready: "{TARGET_SCHEMA}".*')
            break
        except OperationalError as e:
            if _is_statement_timeout_error(e):
                log_warn(f"DDL statement timeout: {type(e).__name__}: {e}")
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            else:
                log_retry(f"DDL DB connection error: {type(e).__name__}: {e}")
                data_engine, _ = reconnect_data_engine()
        except DBAPIError as e:
            if _is_statement_timeout_error(e):
                log_warn(f"DDL statement timeout: {type(e).__name__}: {e}")
            else:
                log_warn(f"DDL SQL/data error: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
        except Exception as e:
            log_warn(f"DDL unexpected error: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    run_month = current_yyyymm()
    last_end_ts: Optional[datetime] = None
    last_processed_day: Optional[str] = None

    try:
        st_month, st_ts, st_last_day = load_state(data_engine)
        run_month = st_month or run_month
        last_end_ts = st_ts
        last_processed_day = st_last_day
        log_info(
            f"[STATE] loaded run_month={run_month} "
            f"last_end_ts={last_end_ts} last_processed_day={last_processed_day}"
        )
    except OperationalError as e:
        if _is_statement_timeout_error(e):
            log_warn(f"state load statement timeout: {type(e).__name__}: {e}")
        else:
            log_retry(f"state load connection error: {type(e).__name__}: {e}")
            data_engine, _ = reconnect_data_engine()
    except Exception as e:
        log_warn(f"state load failed: {type(e).__name__}: {e}")

    while True:
        loop_start = time_mod.time()

        try:
            now = _ts_kst()
            cur_month = current_yyyymm(now)
            target_day = yesterday_day_str(now)

            if batch_ready(now):
                write_status_file(
                    f"ready_for_batch now={now.strftime('%H:%M:%S')} "
                    f"target_day={target_day} last_processed_day={last_processed_day}"
                )

                new_last_processed_day = run_daily_batch(
                    data_engine,
                    last_processed_day=last_processed_day,
                    target_day=target_day,
                )

                if new_last_processed_day != last_processed_day:
                    last_processed_day = new_last_processed_day
                    run_month = new_last_processed_day[:6]
                    save_state(
                        data_engine,
                        run_month=run_month,
                        last_end_ts=last_end_ts,
                        last_processed_day=last_processed_day,
                    )
                    log_info(
                        f"[STATE] saved run_month={run_month} "
                        f"last_processed_day={last_processed_day}"
                    )
                else:
                    log_heartbeat_local(
                        f"alive waiting next daily batch current_month={cur_month} "
                        f"last_processed_day={last_processed_day}"
                    )
            else:
                write_status_file(
                    f"idle waiting_batch_window start="
                    f"{DAILY_BATCH_START_HMS[0]:02d}:{DAILY_BATCH_START_HMS[1]:02d}:{DAILY_BATCH_START_HMS[2]:02d} "
                    f"now={now.strftime('%H:%M:%S')} last_processed_day={last_processed_day}"
                )
                log_heartbeat_local(
                    f"alive waiting batch_window start="
                    f"{DAILY_BATCH_START_HMS[0]:02d}:{DAILY_BATCH_START_HMS[1]:02d}:{DAILY_BATCH_START_HMS[2]:02d} "
                    f"now={now.strftime('%H:%M:%S')} last_processed_day={last_processed_day}"
                )

        except OperationalError as e:
            if _is_statement_timeout_error(e):
                write_status_file("warn statement_timeout during daily batch")
                log_warn(f"statement timeout during daily batch: {type(e).__name__}: {e}")
            else:
                write_status_file("retry db_connection_error")
                log_retry(f"DB connection error: {type(e).__name__}: {e}")
                data_engine, _ = reconnect_data_engine()

        except DBAPIError as e:
            if _is_statement_timeout_error(e):
                write_status_file("warn statement_timeout during daily batch")
                log_warn(f"statement timeout during daily batch: {type(e).__name__}: {e}")
            else:
                write_status_file("warn sql_data_error")
                log_warn(f"SQL/data error: {type(e).__name__}: {e}")

        except Exception as e:
            write_status_file("error unhandled_exception")
            log_error(f"Unhandled error: {type(e).__name__}: {e}")

        elapsed = time_mod.time() - loop_start
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec > 0:
            log_sleep(f"loop sleep {sleep_sec:.2f}s", rate_limited=True)
            time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    mp.freeze_support()
    main()