# app/job/snapshot_mailer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import time
import json
import smtplib
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright

KST = ZoneInfo("Asia/Seoul")

# =============================================================================
# dotenv loader (✅ user fixed path first)
# =============================================================================
def _load_env_file_force_app_env() -> Optional[str]:
    """
    1) ✅ 고정 경로: C:\\Users\\user\\PycharmProjects\\PythonProject\\app\\.env
    2) ✅ fallback: 현재 파일 기준 app/.env
    - override=False (이미 환경변수로 들어온 값은 유지)
    """
    try:
        from pathlib import Path

        fixed = Path(r"C:\Users\user\PycharmProjects\PythonProject\app\.env")
        here = Path(__file__).resolve()          # .../app/job/snapshot_mailer.py
        app_dir = here.parents[1]               # .../app
        fallback = app_dir / ".env"

        env_path = None
        for p in (fixed, fallback):
            if p.exists():
                env_path = p
                break

        if env_path is None:
            return None

        lines = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line in lines:
            s = (line or "").strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = (k or "").strip()
            v = (v or "").strip()
            if not k:
                continue

            # strip quotes
            if (len(v) >= 2) and (v[0] == v[-1]) and v[0] in ("'", '"'):
                v = v[1:-1]

            # override=False
            if os.getenv(k) is None:
                os.environ[k] = v

        return str(env_path)
    except Exception:
        return None


# =============================================================================
# ENV helpers
# =============================================================================
def _env(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is None:
            continue
        s = str(v).strip()
        if s != "":
            return s
    return default


def _env_int(*keys: str, default: int) -> int:
    s = _env(*keys, default=str(default))
    try:
        return int(str(s).strip())
    except Exception:
        return int(default)


def _env_float(*keys: str, default: float) -> float:
    s = _env(*keys, default=str(default))
    try:
        return float(str(s).strip())
    except Exception:
        return float(default)


def _env_bool(*keys: str, default: bool = False) -> bool:
    s = _env(*keys, default=("1" if default else "0")).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


# ✅ 공백 split 금지: 콤마/세미콜론/줄바꿈만 분리
def _split_tokens_keep_spaces(s: str) -> List[str]:
    if not s:
        return []
    parts = re.split(r"[;,|\n]+", str(s))
    out: List[str] = []
    for p in parts:
        t = (p or "").strip()
        if t:
            out.append(t)
    return out


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def _extract_emails_any(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        s = " ".join([str(i) for i in x])
        return _EMAIL_RE.findall(s)
    if isinstance(x, dict):
        s = " ".join([str(v) for v in x.values()])
        return _EMAIL_RE.findall(s)
    return _EMAIL_RE.findall(str(x))


# =============================================================================
# Config (dotenv load 먼저!)
# =============================================================================
_DOTENV_LOADED = _load_env_file_force_app_env()

STREAMLIT_BASE_URL = _env(
    "SNAP_STREAMLIT_BASE_URL",
    "STREAMLIT_BASE_URL",
    "SNAPSHOT_STREAMLIT_BASE_URL",
    default="http://127.0.0.1:8501",
).rstrip("/")

API_BASE_URL = _env(
    "API_BASE_URL",
    "SNAP_API_BASE_URL",
    "SNAPSHOT_API_BASE_URL",
    default="http://127.0.0.1:8000",
).rstrip("/")

SNAP_OUTPUT_DIR = _env("SNAP_OUTPUT_DIR", "SNAPSHOT_OUTPUT_DIR", default=r"C:\AptivAgent\snapshot").strip()

SNAP_TIMEOUT_MS = _env_int("SNAP_TIMEOUT_MS", "SNAPSHOT_READY_TIMEOUT_MS", default=180000)
SNAP_STABLE_MS = _env_int("SNAP_STABLE_MS", "SNAPSHOT_READY_STABLE_MS", default=1500)
SNAP_EPSILON_PX = _env_int("SNAP_EPSILON_PX", "SNAPSHOT_EPSILON_PX", default=8)

SNAP_VIEWPORT_W = _env_int("SNAP_VIEWPORT_W", "SNAPSHOT_VIEWPORT_W", default=1920)
SNAP_VIEWPORT_H = _env_int("SNAP_VIEWPORT_H", "SNAPSHOT_VIEWPORT_H", default=1080)

SNAPSHOT_WAIT_SEC = _env_float("SNAPSHOT_WAIT_SEC", "SNAP_WAIT_SEC", default=0.0)
SNAPSHOT_AFTER_READY_WAIT_SEC = _env_float("SNAPSHOT_AFTER_READY_WAIT_SEC", "SNAP_AFTER_READY_WAIT_SEC", default=0.0)

SNAP_READY_BLOCK_TEXT = _env(
    "SNAP_READY_BLOCK_TEXT",
    default="데이터 조회 중...,데이터 조회 중,Loading...,Please wait,로딩 중,잠시만,조회중,조회 중,불러오는 중,데이터조회중,로딩중",
).strip()
SNAP_READY_MUST_HAVE_TEXT = _env("SNAP_READY_MUST_HAVE_TEXT", default="").strip()

SNAP_MODE = _env("SNAP_MODE", "SNAPSHOT_MODE", default="mail").strip().lower()
if SNAP_MODE not in ("mail", "print"):
    SNAP_MODE = "mail"

ADMIN_PASS = _env("ADMIN_PASS", default="").strip()

SMTP_HOST = _env("SMTP_HOST", default="").strip()
SMTP_PORT = _env_int("SMTP_PORT", default=587)
SMTP_USER = _env("SMTP_USER", default="").strip()
SMTP_PASS = _env("SMTP_PASS", default="").strip()
SMTP_FROM = _env("SMTP_FROM", default=(SMTP_USER or "")).strip()
SMTP_TLS = _env_bool("SMTP_TLS", default=True)

PDF_FORMAT = _env("SNAPSHOT_PDF_FORMAT", "SNAP_PDF_FORMAT", default="A4").strip().upper()
PDF_LANDSCAPE = _env_bool("SNAPSHOT_PDF_LANDSCAPE", "SNAP_PDF_LANDSCAPE", default=True)
PDF_SCALE = _env_float("SNAPSHOT_PDF_SCALE", "SNAP_PDF_SCALE", default=0.70)
PDF_MARGIN_MM = _env_int("SNAPSHOT_PDF_MARGIN_MM_PRINT", "SNAP_PDF_MARGIN_MM_PRINT", default=8)

LOG_DIR = _env("SNAPSHOT_LOG_DIR", "SNAP_LOG_DIR", default="").strip()
LOCK_FILE = _env("SNAPSHOT_LOCK_FILE", "SNAP_LOCK_FILE", default="").strip()
SENT_FILE = _env("SNAPSHOT_SENT_FILE", "SNAP_SENT_FILE", default="").strip()

CONTINUE_ON_ERROR = _env_bool("SNAP_CONTINUE_ON_ERROR", "SNAPSHOT_CONTINUE_ON_ERROR", default=True)
SKIP_IF_ALREADY_SENT = _env_bool("SNAP_SKIP_IF_ALREADY_SENT", "SNAPSHOT_SKIP_IF_ALREADY_SENT", default=True)
FORCE_SEND = _env_bool("SNAP_FORCE_SEND", "SNAPSHOT_FORCE_SEND", default=False)

MIN_PDF_BYTES = _env_int("SNAP_MIN_PDF_BYTES", "SNAPSHOT_MIN_PDF_BYTES", default=20_000)
DEBUG_SAVE_ARTIFACTS = _env_bool("SNAP_DEBUG_SAVE_ARTIFACTS", default=True)

EMAIL_LIST_CONNECT_TIMEOUT = _env_float("SNAP_EMAIL_LIST_CONNECT_TIMEOUT_SEC", default=3.0)
EMAIL_LIST_READ_TIMEOUT = _env_float("SNAP_EMAIL_LIST_READ_TIMEOUT_SEC", default=30.0)
EMAIL_LIST_RETRY = _env_int("SNAP_EMAIL_LIST_RETRY", default=3)
EMAIL_LIST_PATH = _env("SNAP_EMAIL_LIST_PATH", default="/email_list").strip() or "/email_list"
EXCLUDE_EMAILS = _env("SNAP_EXCLUDE_EMAILS", "SNAPSHOT_EXCLUDE_EMAILS", default="").strip()

TS_OVERLAY_ENABLE = _env_bool("SNAP_TS_OVERLAY_ENABLE", default=True)
TS_OVERLAY_FMT = _env("SNAP_TS_OVERLAY_FMT", default="%Y-%m-%d %H:%M:%S KST").strip()

# =============================================================================
# ✅ 운영형: Scheduler / Status / Event Log
# =============================================================================
SNAP_RUNNER_MODE = _env("SNAP_RUNNER_MODE", default="once").strip().lower()
if SNAP_RUNNER_MODE not in ("once", "daemon"):
    SNAP_RUNNER_MODE = "once"

SNAP_SCHED_TIMES = _env("SNAP_SCHED_TIMES", default="08:29:59,20:29:59").strip()
SNAP_SCHED_POLL_SEC = _env_float("SNAP_SCHED_POLL_SEC", default=1.0)
SNAP_SCHED_WINDOW_SEC = _env_float("SNAP_SCHED_WINDOW_SEC", default=2.0)

SNAP_SCHED_CATCHUP = _env_bool("SNAP_SCHED_CATCHUP", default=True)
SNAP_SCHED_CATCHUP_LOOKBACK_MIN = _env_int("SNAP_SCHED_CATCHUP_LOOKBACK_MIN", default=240)  # 최근 4h
SNAP_SCHED_CATCHUP_GRACE_SEC = _env_int("SNAP_SCHED_CATCHUP_GRACE_SEC", default=5400)       # 90min

SNAP_STATUS_FILE = _env("SNAP_STATUS_FILE", default=r"C:\AptivAgent\_state\snapshot_status.json").strip()
SNAP_EVENT_LOG_URL = _env("SNAP_EVENT_LOG_URL", default="").strip()  # 선택: FastAPI로 POST

# =============================================================================
# Logging
# =============================================================================
_LOG_FH = None

def _safe_print(msg: str) -> None:
    try:
        print(msg, flush=True)
    except Exception:
        pass

def _open_log_file(run_id: str) -> None:
    global _LOG_FH
    if _LOG_FH is not None:
        return
    if not LOG_DIR:
        return
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        fp = os.path.join(LOG_DIR, f"snapshot_mailer_{run_id}.log")
        _LOG_FH = open(fp, "a", encoding="utf-8")
    except Exception:
        _LOG_FH = None

def _log(msg: str) -> None:
    ts = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    _safe_print(line)
    try:
        if _LOG_FH is not None:
            _LOG_FH.write(line + "\n")
            _LOG_FH.flush()
    except Exception:
        pass

def _close_log_file() -> None:
    global _LOG_FH
    try:
        if _LOG_FH is not None:
            _LOG_FH.flush()
            _LOG_FH.close()
    except Exception:
        pass
    _LOG_FH = None


# =============================================================================
# ✅ Status file helpers (Streamlit 배지용)
# =============================================================================
def _write_status(state: str, **extra: Any) -> None:
    """
    state 예:
      - idle / waiting / firing / running / success / error
    """
    try:
        if not SNAP_STATUS_FILE:
            return
        os.makedirs(os.path.dirname(SNAP_STATUS_FILE), exist_ok=True)
        payload = {
            "ts": datetime.now(tz=KST).isoformat(),
            "state": state,
        }
        payload.update(extra or {})
        with open(SNAP_STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def get_snapshot_scheduler_status() -> Dict[str, Any]:
    """
    Streamlit에서 읽어 배지로 표시할 수 있게 공개 함수.
    """
    try:
        if not SNAP_STATUS_FILE or (not os.path.exists(SNAP_STATUS_FILE)):
            return {"state": "unknown", "ts": None}
        with open(SNAP_STATUS_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or {"state": "unknown", "ts": None}
    except Exception:
        return {"state": "unknown", "ts": None}


# =============================================================================
# ✅ Event log (선택): FastAPI로 실행 결과 남김
# =============================================================================
def _post_event_log(event: str, payload: Dict[str, Any]) -> None:
    if not SNAP_EVENT_LOG_URL:
        return
    try:
        data = {"event": event, "payload": payload, "ts": datetime.now(tz=KST).isoformat()}
        requests.post(SNAP_EVENT_LOG_URL, json=data, timeout=(2.0, 5.0))
    except Exception:
        pass


# =============================================================================
# Lock / Sent marker
# =============================================================================
def _acquire_lock() -> bool:
    if not LOCK_FILE:
        return True
    try:
        os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
    except Exception:
        pass
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, str(os.getpid()).encode("utf-8", errors="ignore"))
        finally:
            os.close(fd)
        return True
    except Exception:
        return False

def _release_lock() -> None:
    if not LOCK_FILE:
        return
    try:
        os.remove(LOCK_FILE)
    except Exception:
        pass

def _sent_key(prod_day: str, shift_type: str) -> str:
    return f"{prod_day}_{shift_type}"

def _read_last_sent() -> str:
    if not SENT_FILE:
        return ""
    try:
        with open(SENT_FILE, "r", encoding="utf-8") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""

def _write_last_sent(key: str) -> None:
    if not SENT_FILE:
        return
    try:
        os.makedirs(os.path.dirname(SENT_FILE), exist_ok=True)
    except Exception:
        pass
    try:
        with open(SENT_FILE, "w", encoding="utf-8") as f:
            f.write(key)
    except Exception:
        pass


# =============================================================================
# Time rule (KST) - 기준 끝 시각 고정: 08:29:59 & 20:29:59
# =============================================================================
def _now_prod_day_shift_kst(now: Optional[datetime] = None) -> Tuple[str, str]:
    if now is None:
        now = datetime.now(tz=KST)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    else:
        now = now.astimezone(KST)

    day_end = now.replace(hour=20, minute=29, second=59, microsecond=0)
    night_end = now.replace(hour=8, minute=29, second=59, microsecond=0)

    if (now > night_end) and (now <= day_end):
        return now.strftime("%Y%m%d"), "day"

    if now > day_end:
        return now.strftime("%Y%m%d"), "night"

    prev_day = (now - timedelta(days=1)).strftime("%Y%m%d")
    return prev_day, "night"


def _norm_day(v: Any) -> str:
    s = str(v or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""

def _norm_shift(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in ("day", "night") else "day"

def _ts_id(now: Optional[datetime] = None) -> str:
    n = now.astimezone(KST) if (now and now.tzinfo) else (now or datetime.now(tz=KST))
    if n.tzinfo is None:
        n = n.replace(tzinfo=KST)
    return n.strftime("%Y%m%d_%H%M%S")

def _ensure_dir(p: str) -> str:
    p = (p or "").strip() or r"C:\AptivAgent\snapshot"
    os.makedirs(p, exist_ok=True)
    return p

def _url_escape(s: str) -> str:
    return (
        str(s)
        .replace("%", "%25")
        .replace(" ", "%20")
        .replace("!", "%21")
        .replace("#", "%23")
        .replace("&", "%26")
        .replace("+", "%2B")
        .replace("=", "%3D")
        .replace("?", "%3F")
        .replace("/", "%2F")
    )


# =============================================================================
# Pages
# =============================================================================
@dataclass
class PageSpec:
    name: str
    page_id: str

def _default_pages() -> List[PageSpec]:
    return [
        PageSpec(name="01_production_status", page_id="01_production_status"),
        PageSpec(name="02_production_info", page_id="02_production_info"),
        PageSpec(name="03_production_analysis", page_id="03_production_analysis"),
    ]

def _build_url(page_id: str, prod_day: str, shift_type: str, end_day: str, ts_seed: int) -> str:
    qs: Dict[str, str] = {
        "page": page_id,
        "snap": "1",
        "prod_day": prod_day,
        "shift_type": shift_type,
        "end_day": end_day,
        "ts": str(ts_seed),
    }
    if ADMIN_PASS:
        qs["token"] = ADMIN_PASS
    q = "&".join([f"{k}={_url_escape(v)}" for k, v in qs.items()])
    return f"{STREAMLIT_BASE_URL}/?{q}"


# =============================================================================
# FastAPI: GET /email_list?end_day=YYYYMMDD&shift_type=day|night
# =============================================================================
_http = requests.Session()

def _fetch_email_list_from_api(end_day: str, shift_type: str) -> List[str]:
    base = API_BASE_URL.rstrip("/")
    path = EMAIL_LIST_PATH if EMAIL_LIST_PATH.startswith("/") else ("/" + EMAIL_LIST_PATH)
    url = f"{base}{path}"
    timeout = (float(EMAIL_LIST_CONNECT_TIMEOUT), float(EMAIL_LIST_READ_TIMEOUT))

    last_err: Optional[Exception] = None
    for i in range(int(EMAIL_LIST_RETRY)):
        _log(
            f"[MAIL] fetch recipients attempt {i+1}/{EMAIL_LIST_RETRY} "
            f"url={url} end_day={end_day} shift_type={shift_type} timeout=(connect={timeout[0]}, read={timeout[1]})"
        )
        try:
            r = _http.get(url, params={"end_day": end_day, "shift_type": shift_type}, timeout=timeout)
            r.raise_for_status()
            try:
                data = r.json()
            except Exception:
                data = r.text

            candidates = _extract_emails_any(data)

            exclude_set = {e.strip().lower() for e in _extract_emails_any(EXCLUDE_EMAILS)}
            out: List[str] = []
            seen = set()
            for e in candidates:
                el = e.lower()
                if el in exclude_set:
                    continue
                if el in seen:
                    continue
                seen.add(el)
                out.append(e)
            return out

        except Exception as e:
            last_err = e
            time.sleep(0.6)

    if last_err:
        raise last_err
    return []


# =============================================================================
# DOM ready + DATA ready
# =============================================================================
def _wait_dom_ready(page, timeout_ms: int, stable_ms: int, epsilon_px: int) -> None:
    deadline = time.time() + (timeout_ms / 1000.0)

    try:
        page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=timeout_ms)
    except Exception:
        page.wait_for_selector("body", timeout=timeout_ms)

    sample_gap = 0.25
    stable_need = max(0.8, stable_ms / 1000.0)

    last_h: Optional[int] = None
    stable_since: Optional[float] = None
    last_log = 0.0

    def spinner_count() -> int:
        try:
            return page.locator('[data-testid="stSpinner"]').count()
        except Exception:
            return 0

    def get_height() -> int:
        try:
            return int(page.evaluate("() => document.documentElement.scrollHeight || 0"))
        except Exception:
            return 0

    while time.time() < deadline:
        sc = spinner_count()
        h = get_height()

        if last_h is None:
            last_h = h
            stable_since = time.time()
            time.sleep(sample_gap)
            continue

        if abs(h - last_h) <= epsilon_px:
            if stable_since is None:
                stable_since = time.time()
        else:
            stable_since = time.time()

        last_h = h

        if time.time() - last_log >= 5.0:
            last_log = time.time()
            st_for = 0.0 if stable_since is None else (time.time() - stable_since)
            _log(f"[DOM] spinner={sc} height={h} stable_for={st_for:.1f}s")

        if sc <= 1 and stable_since is not None and (time.time() - stable_since) >= stable_need:
            return

        time.sleep(sample_gap)

    raise TimeoutError("DOM ready timeout (spinner/height not stable)")


def _wait_data_ready(page, timeout_ms: int) -> None:
    # 1) READY marker
    try:
        page.wait_for_selector("#__snap_ready__", timeout=timeout_ms)
        return
    except Exception:
        pass

    # 2) fallback heuristic
    deadline = time.time() + (timeout_ms / 1000.0)
    block_texts = _split_tokens_keep_spaces(SNAP_READY_BLOCK_TEXT)
    must_texts = _split_tokens_keep_spaces(SNAP_READY_MUST_HAVE_TEXT)

    last_log = 0.0
    while time.time() < deadline:
        try:
            body = page.inner_text("body")
        except Exception:
            body = ""

        try:
            sc = page.locator('[data-testid="stSpinner"]').count()
        except Exception:
            sc = 0

        hit_block = ""
        for t in block_texts:
            if t and (t in body):
                hit_block = t
                break

        if hit_block or sc > 0:
            if time.time() - last_log >= 5.0:
                last_log = time.time()
                _log(f"[DATA] waiting: spinner={sc} block={hit_block[:12]+'...' if hit_block else ''}")
            time.sleep(0.35)
            continue

        if must_texts:
            ok = any((t in body) for t in must_texts if t)
            if not ok:
                if time.time() - last_log >= 5.0:
                    last_log = time.time()
                    _log(f"[DATA] waiting: must_have_text not found (need one of {must_texts})")
                time.sleep(0.35)
                continue

        return

    raise TimeoutError("DATA ready timeout (block/spinner/markers)")


def _detect_streamlit_error(page) -> Optional[str]:
    try:
        if page.locator('[data-testid="stException"]').count() > 0:
            return "stException"
    except Exception:
        pass

    strong_patterns = [
        "Traceback (most recent call last)",
        "StreamlitAPIException",
        "ModuleNotFoundError:",
        "NotImplementedError:",
        "SyntaxError:",
    ]
    try:
        txt = page.inner_text("body")
        for ptn in strong_patterns:
            if ptn in txt:
                return ptn
    except Exception:
        return None

    return None


def _dump_debug_artifacts(page, out_dir: str, tag: str) -> Tuple[Optional[str], Optional[str]]:
    if not DEBUG_SAVE_ARTIFACTS:
        return None, None

    ss_fp = None
    html_fp = None

    try:
        ss_fp = os.path.join(out_dir, f"debug_{tag}.png")
        page.screenshot(path=ss_fp, full_page=True)
    except Exception:
        ss_fp = None

    try:
        html_fp = os.path.join(out_dir, f"debug_{tag}.html")
        content = page.content()
        with open(html_fp, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception:
        html_fp = None

    return ss_fp, html_fp


# =============================================================================
# overlay (02에 1번만)
# =============================================================================
def _inject_pdf_timestamp_overlay(page, text: str) -> None:
    try:
        page.evaluate(
            """() => {
                const id = '__snap_ts_overlay__';
                const old = document.getElementById(id);
                if (old) old.remove();
            }"""
        )
        page.add_style_tag(
            content="""
            #__snap_ts_overlay__{
              position: fixed;
              top: 6px;
              left: 6px;
              z-index: 2147483647;
              font-size: 12px;
              line-height: 1.2;
              padding: 3px 6px;
              background: rgba(255,255,255,0.7);
              color: #111;
              border: 1px solid rgba(0,0,0,0.25);
              border-radius: 2px;
              pointer-events: none;
              user-select: none;
              font-family: Arial, 'Malgun Gothic', 'NanumGothic', sans-serif;
            }
            """
        )
        page.evaluate(
            """(t) => {
                const div = document.createElement('div');
                div.id = '__snap_ts_overlay__';
                div.textContent = t;
                document.body.appendChild(div);
            }""",
            text,
        )
        page.wait_for_timeout(80)
    except Exception:
        pass


def _try_click_button(page, label: str) -> bool:
    try:
        btn = page.get_by_role("button", name=label)
        if btn.count() > 0:
            btn.first.click(timeout=2000)
            _log(f"[CLICK] clicked button: {label}")
            page.wait_for_timeout(150)
            return True
    except Exception:
        pass
    return False


# =============================================================================
# Capture
# =============================================================================
def _capture_pages_pdf_bytes(
    pages: List[PageSpec],
    prod_day: str,
    shift_type: str,
    anchor_now: datetime,
) -> Tuple[List[Tuple[str, bytes]], List[str]]:
    out_dir = _ensure_dir(SNAP_OUTPUT_DIR)
    run_id = _ts_id(anchor_now)
    ts_seed = int(anchor_now.timestamp())  # ✅ anchor 기준 고정

    attachments: List[Tuple[str, bytes]] = []
    errors: List[str] = []
    overlay_done = False

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"],
        )
        context = browser.new_context(
            viewport={"width": SNAP_VIEWPORT_W, "height": SNAP_VIEWPORT_H},
            locale="ko-KR",
        )

        for i, ps in enumerate(pages):
            page = context.new_page()
            url = _build_url(ps.page_id, prod_day=prod_day, shift_type=shift_type, end_day=prod_day, ts_seed=ts_seed + i)
            _log(f"[SNAPSHOT] capture start: {ps.name} url={url}")

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=SNAP_TIMEOUT_MS)

                if SNAPSHOT_WAIT_SEC > 0:
                    time.sleep(float(SNAPSHOT_WAIT_SEC))

                _wait_dom_ready(page, timeout_ms=SNAP_TIMEOUT_MS, stable_ms=SNAP_STABLE_MS, epsilon_px=SNAP_EPSILON_PX)

                if ps.page_id == "01_production_status":
                    _try_click_button(page, "새로고침")
                elif ps.page_id in ("02_production_info", "03_production_analysis"):
                    _try_click_button(page, "검색")

                _wait_data_ready(page, timeout_ms=SNAP_TIMEOUT_MS)

                if SNAPSHOT_AFTER_READY_WAIT_SEC > 0:
                    time.sleep(float(SNAPSHOT_AFTER_READY_WAIT_SEC))

                hit = _detect_streamlit_error(page)
                if hit:
                    tag = f"{prod_day}_{shift_type}_{ps.name}_{run_id}"
                    ss_fp, html_fp = _dump_debug_artifacts(page, out_dir, tag)
                    msg = f"{ps.name}: Streamlit error detected ({hit})"
                    if ss_fp:
                        msg += f" | screenshot={ss_fp}"
                    if html_fp:
                        msg += f" | html={html_fp}"
                    raise RuntimeError(msg)

                if TS_OVERLAY_ENABLE and (not overlay_done) and ps.page_id == "02_production_info":
                    ts_txt = anchor_now.strftime(TS_OVERLAY_FMT)
                    _inject_pdf_timestamp_overlay(page, f"PDF TS: {ts_txt}")
                    overlay_done = True

                fn = f"{prod_day}_{shift_type}_{ps.name}_{run_id}.pdf"
                margin = f"{max(0, int(PDF_MARGIN_MM))}mm"

                pdf_bytes: bytes = page.pdf(
                    format=PDF_FORMAT,
                    landscape=bool(PDF_LANDSCAPE),
                    scale=float(PDF_SCALE),
                    print_background=True,
                    prefer_css_page_size=True,
                    margin={"top": margin, "bottom": margin, "left": margin, "right": margin},
                )

                sz = len(pdf_bytes or b"")
                if sz < int(MIN_PDF_BYTES):
                    tag = f"{prod_day}_{shift_type}_{ps.name}_{run_id}"
                    ss_fp, html_fp = _dump_debug_artifacts(page, out_dir, f"smallpdf_{tag}")
                    raise RuntimeError(
                        f"{ps.name}: PDF too small ({sz} bytes) -> likely capture before data loaded"
                        + (f" | screenshot={ss_fp}" if ss_fp else "")
                        + (f" | html={html_fp}" if html_fp else "")
                    )

                attachments.append((fn, pdf_bytes))
                _log(f"[SNAPSHOT] captured: {fn} ({sz} bytes)")

            except Exception as e:
                err = f"[CAPTURE-ERR] {ps.name}: {e}"
                _log(err)
                errors.append(err)
                if not CONTINUE_ON_ERROR:
                    raise
            finally:
                try:
                    page.close()
                except Exception:
                    pass

        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

    return attachments, errors


# =============================================================================
# Mail
# =============================================================================
def _send_mail_bytes(subject: str, body: str, recipients: List[str], attachments: List[Tuple[str, bytes]]) -> bool:
    if not SMTP_HOST or not SMTP_FROM or not recipients:
        _log("[MAIL] SMTP config missing (host/from/recipients)")
        return False

    recips_norm: List[str] = []
    seen = set()
    for r in recipients:
        for e in _extract_emails_any(r):
            el = e.lower()
            if el in seen:
                continue
            seen.add(el)
            recips_norm.append(e)

    if not recips_norm:
        _log("[MAIL] recipients parsed=0 -> abort")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = ", ".join(recips_norm)
    msg.set_content(body)

    attached = 0
    total_bytes = 0
    for filename, data in attachments:
        try:
            if not data:
                raise ValueError("empty bytes")
            msg.add_attachment(data, maintype="application", subtype="pdf", filename=filename)
            attached += 1
            total_bytes += len(data)
        except Exception as e:
            _log(f"[MAIL] attach fail: {filename} err={e}")

    _log(f"[MAIL] attach count={attached} total_bytes={total_bytes}")
    if attached <= 0:
        _log("[MAIL] no attachments -> abort")
        return False

    for i in range(2):
        try:
            _log(f"[MAIL] send attempt {i+1}/2 host={SMTP_HOST}:{SMTP_PORT} tls={SMTP_TLS} from={SMTP_FROM}")
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=45) as s:
                s.set_debuglevel(0)
                s.ehlo()
                if SMTP_TLS:
                    s.starttls()
                    s.ehlo()
                if SMTP_USER and SMTP_PASS:
                    s.login(SMTP_USER, SMTP_PASS)
                s.send_message(msg)
            return True
        except Exception as e:
            _log(f"[MAIL] send fail attempt {i+1}: {e}")
            time.sleep(1.2)

    return False


# =============================================================================
# Public entry (slot 기준 고정 실행)
# =============================================================================
def run_snapshot_and_mail_at(slot_dt: datetime, pages: Optional[List[PageSpec]] = None) -> List[str]:
    """
    ✅ 핵심: slot_dt(트리거 시각) 기준으로 prod_day/shift를 결정하고,
    실행이 10~20분 걸려도 끝까지 "그 prod_day/shift"로 고정한다.
    """
    if slot_dt.tzinfo is None:
        slot_dt = slot_dt.replace(tzinfo=KST)
    else:
        slot_dt = slot_dt.astimezone(KST)

    prod_day_n, shift_type_n = _now_prod_day_shift_kst(slot_dt)
    return run_snapshot_and_mail(prod_day=prod_day_n, shift_type=shift_type_n, pages=pages, _anchor_now=slot_dt)


def run_snapshot_and_mail(
    prod_day: Optional[str] = None,
    shift_type: Optional[str] = None,
    pages: Optional[List[PageSpec]] = None,
    _anchor_now: Optional[datetime] = None,  # 내부용: slot 고정 실행
) -> List[str]:
    anchor_now = _anchor_now or datetime.now(tz=KST)
    if anchor_now.tzinfo is None:
        anchor_now = anchor_now.replace(tzinfo=KST)
    else:
        anchor_now = anchor_now.astimezone(KST)

    run_id = _ts_id(anchor_now)
    _open_log_file(run_id)

    # BOOT logs
    _write_status("running", anchor_now=anchor_now.isoformat())
    if _DOTENV_LOADED:
        _log(f"[BOOT] dotenv_loaded={_DOTENV_LOADED}")
    else:
        _log("[BOOT] dotenv_loaded=None")

    if not _acquire_lock():
        _log(f"[LOCK] another instance is running (LOCK_FILE={LOCK_FILE}) -> skip")
        _write_status("idle", note="lock_busy")
        _close_log_file()
        return []

    slot_payload_base: Dict[str, Any] = {
        "anchor_now": anchor_now.isoformat(),
        "run_id": run_id,
        "mode": SNAP_MODE,
    }

    try:
        d0, s0 = _now_prod_day_shift_kst(anchor_now)

        prod_day_n = _norm_day(prod_day) or d0
        shift_type_n = _norm_shift(shift_type) if shift_type else s0
        pages = pages or _default_pages()

        slot_payload_base.update({"prod_day": prod_day_n, "shift_type": shift_type_n, "pages": [p.name for p in pages]})

        _log(f"[ANCHOR] now={anchor_now.strftime('%Y-%m-%d %H:%M:%S %Z')} -> prod_day={prod_day_n} shift={shift_type_n}")
        _log(f"[SNAPSHOT] run start day={prod_day_n} shift={shift_type_n} mode={SNAP_MODE}")

        _post_event_log("start", dict(slot_payload_base))

        key = _sent_key(prod_day_n, shift_type_n)
        if (not FORCE_SEND) and SKIP_IF_ALREADY_SENT and SENT_FILE:
            last = _read_last_sent()
            if last == key:
                _log(f"[SENT] already sent for {key} (SENT_FILE={SENT_FILE}) -> skip")
                _write_status("idle", note="already_sent", prod_day=prod_day_n, shift_type=shift_type_n)
                _post_event_log("skip_already_sent", dict(slot_payload_base))
                return []

        recips = _fetch_email_list_from_api(end_day=prod_day_n, shift_type=shift_type_n)
        _log(f"[MAIL] recipients(from api)={len(recips)}")
        slot_payload_base["recipients"] = len(recips)

        if SNAP_MODE == "mail" and not recips:
            _log("[MAIL] recipients=0 -> abort (check /email_list on FastAPI)")
            _write_status("error", note="recipients=0", prod_day=prod_day_n, shift_type=shift_type_n)
            _post_event_log("error", dict(slot_payload_base, error="recipients=0"))
            return []

        attachments, errors = _capture_pages_pdf_bytes(pages, prod_day=prod_day_n, shift_type=shift_type_n, anchor_now=anchor_now)

        if not attachments:
            _log("[SNAPSHOT] no pdf generated")
            _write_status("error", note="no_pdf", prod_day=prod_day_n, shift_type=shift_type_n)
            _post_event_log("error", dict(slot_payload_base, error="no_pdf", errors=errors[:10]))
            return []

        subject = f"[Aptiv Report] {prod_day_n} {shift_type_n} snapshot"
        body_lines = [
            "자동 스냅샷 리포트입니다.",
            f"- anchor_now={anchor_now.strftime('%Y-%m-%d %H:%M:%S %Z')}",
            f"- prod_day(end_day)={prod_day_n}",
            f"- shift_type={shift_type_n}",
            f"- pages={', '.join([p.name for p in pages])}",
            f"- generated={len(attachments)}",
            f"- recipients={len(recips)}",
            f"- api_email_list={API_BASE_URL.rstrip('/')}{EMAIL_LIST_PATH}",
            f"- pdf_landscape={PDF_LANDSCAPE} scale={PDF_SCALE}",
        ]
        if errors:
            body_lines.append("")
            body_lines.append("[WARN] 일부 페이지 캡처 오류:")
            body_lines.extend([f"- {e}" for e in errors[:10]])
        body = "\n".join(body_lines) + "\n"

        mailed = False
        if SNAP_MODE == "mail":
            mailed = _send_mail_bytes(subject, body, recips, attachments)
            if mailed:
                _log(f"[MAIL] sent OK to {len(recips)} recipients")
                if SENT_FILE:
                    _write_last_sent(key)
            else:
                _log("[MAIL] send failed")

        names = [fn for fn, _ in attachments]
        for fn in names:
            _log(f"[SNAPSHOT] OK: {fn}")

        if mailed or (SNAP_MODE == "print"):
            _write_status("success", prod_day=prod_day_n, shift_type=shift_type_n, pdfs=names, mailed=bool(mailed))
            _post_event_log("success", dict(slot_payload_base, mailed=bool(mailed), pdfs=names, errors=errors[:10]))
        else:
            _write_status("error", prod_day=prod_day_n, shift_type=shift_type_n, pdfs=names, mailed=False, note="smtp_fail")
            _post_event_log("error", dict(slot_payload_base, error="smtp_fail", pdfs=names, errors=errors[:10]))

        return names

    except Exception as e:
        _write_status("error", note=str(e)[:200])
        _post_event_log("error", dict(slot_payload_base, error=str(e)[:500]))
        raise
    finally:
        _release_lock()
        _close_log_file()


# =============================================================================
# ✅ Scheduler daemon
# =============================================================================
_SCHED_STARTED = False
_SCHED_LOCK = threading.Lock()
_SCHED_THREAD: Optional[threading.Thread] = None

def _parse_sched_times(s: str) -> List[Tuple[int, int, int]]:
    out: List[Tuple[int, int, int]] = []
    toks = _split_tokens_keep_spaces(s)
    for t in toks:
        parts = [p.strip() for p in t.split(":")]
        if len(parts) == 2:
            hh, mm = parts
            ss = "0"
        elif len(parts) == 3:
            hh, mm, ss = parts
        else:
            continue
        try:
            h = int(hh); m = int(mm); sec = int(ss)
            if 0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59:
                out.append((h, m, sec))
        except Exception:
            continue
    return sorted(list({x for x in out}))

def _make_slot_dt(day: datetime, h: int, m: int, s: int) -> datetime:
    return day.replace(hour=h, minute=m, second=s, microsecond=0)

def _is_already_sent_for_slot(slot_dt: datetime) -> bool:
    """
    ✅ 캐치업/스케줄러 중복 방지.
    sentfile은 prod_day/shift 단위라, 동일 slot에서 중복 실행은 사실상 막힘.
    """
    if not SENT_FILE or not SKIP_IF_ALREADY_SENT:
        return False
    prod_day, shift = _now_prod_day_shift_kst(slot_dt)
    key = _sent_key(prod_day, shift)
    return _read_last_sent() == key

def _catchup_fire_if_needed(times: List[Tuple[int, int, int]]) -> None:
    if not SNAP_SCHED_CATCHUP:
        return

    now = datetime.now(tz=KST)
    lookback = max(1, int(SNAP_SCHED_CATCHUP_LOOKBACK_MIN))
    grace = max(10, int(SNAP_SCHED_CATCHUP_GRACE_SEC))

    start = now - timedelta(minutes=lookback)

    # 후보 슬롯 생성: start ~ now 구간의 날짜들을 훑고, 각 날짜에 times를 대입
    days: List[datetime] = []
    d = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    while d <= end_day:
        days.append(d)
        d = d + timedelta(days=1)

    candidates: List[datetime] = []
    for day0 in days:
        for (hh, mm, ss) in times:
            slot = _make_slot_dt(day0, hh, mm, ss)
            if (slot <= now) and (slot >= start):
                candidates.append(slot)

    candidates.sort()

    # 가장 최근 후보부터 체크하여 1개만 캐치업(너무 많이 보내는 사고 방지)
    for slot in reversed(candidates):
        delta = (now - slot).total_seconds()
        if delta <= grace:
            if _is_already_sent_for_slot(slot):
                _log(f"[SCHED][CATCHUP] slot={slot} within grace but already sent -> skip")
                _write_status("idle", note="catchup_skip_already_sent", slot=slot.isoformat())
                return
            _log(f"[SCHED][CATCHUP] FIRE slot={slot.strftime('%Y-%m-%d %H:%M:%S %Z')} delta={delta:.1f}s")
            _write_status("firing", slot=slot.isoformat(), mode="catchup")
            try:
                run_snapshot_and_mail_at(slot_dt=slot)
            except Exception as e:
                _log(f"[SCHED][CATCHUP] FIRE error: {e}")
                _write_status("error", slot=slot.isoformat(), note=f"catchup_error: {str(e)[:180]}")
            return

def _scheduler_loop() -> None:
    times = _parse_sched_times(SNAP_SCHED_TIMES) or [(8, 29, 59), (20, 29, 59)]
    poll = max(0.2, float(SNAP_SCHED_POLL_SEC))
    window = max(0.5, float(SNAP_SCHED_WINDOW_SEC))

    _log(f"[SCHED] loop start times={times} poll={poll}s window=±{window}s mode={SNAP_MODE}")
    _write_status("idle", note="scheduler_started", times=times)

    # ✅ PC 켜짐/프로세스 재시작 시 캐치업 1회
    _catchup_fire_if_needed(times)

    last_fired_key = ""

    while True:
        now = datetime.now(tz=KST)
        next_slots = []
        for (hh, mm, ss) in times:
            slot = _make_slot_dt(now, hh, mm, ss)
            next_slots.append(slot)

        _write_status(
            "waiting",
            now=now.isoformat(),
            next_slots=[s.isoformat() for s in sorted(next_slots)],
        )

        for (hh, mm, ss) in times:
            slot = _make_slot_dt(now, hh, mm, ss)
            delta = (now - slot).total_seconds()

            if abs(delta) <= window:
                fire_key = slot.strftime("%Y%m%d_%H%M%S")
                if fire_key == last_fired_key:
                    continue

                # ✅ 이미 보낸 prod_day/shift면 skip
                if _is_already_sent_for_slot(slot):
                    _log(f"[SCHED] FIRE slot={slot} but already sent -> skip")
                    last_fired_key = fire_key
                    _write_status("idle", note="already_sent", slot=slot.isoformat())
                    continue

                last_fired_key = fire_key
                _log(f"[SCHED] FIRE slot={slot.strftime('%Y-%m-%d %H:%M:%S %Z')} (delta={delta:+.2f}s)")
                _write_status("firing", slot=slot.isoformat(), mode="schedule")

                try:
                    run_snapshot_and_mail_at(slot_dt=slot)
                except Exception as e:
                    _log(f"[SCHED] FIRE error: {e}")
                    _write_status("error", slot=slot.isoformat(), note=f"schedule_error: {str(e)[:180]}")

        time.sleep(poll)


def start_snapshot_scheduler_once() -> Optional[threading.Thread]:
    """
    Streamlit에서 호출용:
      from app.job.snapshot_mailer import start_snapshot_scheduler_once
      start_snapshot_scheduler_once()
    - 여러 번 호출돼도 thread는 1개만 뜸
    """
    global _SCHED_STARTED, _SCHED_THREAD
    with _SCHED_LOCK:
        if _SCHED_STARTED and _SCHED_THREAD and _SCHED_THREAD.is_alive():
            return _SCHED_THREAD

        _SCHED_STARTED = True
        th = threading.Thread(target=_scheduler_loop, daemon=True, name="snapshot_mailer_scheduler")
        th.start()
        _SCHED_THREAD = th
        _log("[SCHED] started (daemon thread)")
        return th


# =============================================================================
# __main__
# =============================================================================
if __name__ == "__main__":
    if SNAP_RUNNER_MODE == "daemon":
        start_snapshot_scheduler_once()
        while True:
            time.sleep(3600)
    else:
        run_snapshot_and_mail()