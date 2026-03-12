# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext


# ---------------------------------------------------------
# 1) 사용할 Python 3.12 가상환경의 python.exe 경로
# ---------------------------------------------------------
NUITKA_PYTHON = r"C:\venvs\py312_nuitka\Scripts\python.exe"


# ---------------------------------------------------------
# 2) 경로/문자열 유틸
# ---------------------------------------------------------
def _norm(p: str) -> str:
    return os.path.normpath(os.path.abspath(p))


def _exists_file(p: str) -> bool:
    try:
        return os.path.isfile(p)
    except Exception:
        return False


def _exists_dir(p: str) -> bool:
    try:
        return os.path.isdir(p)
    except Exception:
        return False


def safe_join_cmd(cmd_list):
    out = []
    for c in cmd_list:
        c = str(c)
        if any(ch.isspace() for ch in c) and not (c.startswith('"') and c.endswith('"')):
            out.append(f'"{c}"')
        else:
            out.append(c)
    return " ".join(out)


def append_log(msg: str):
    txt_log.insert(tk.END, msg)
    txt_log.see(tk.END)
    root.update_idletasks()


# ---------------------------------------------------------
# 3) 파일/폴더 선택 유틸
# ---------------------------------------------------------
def select_script():
    path = filedialog.askopenfilename(
        title="컴파일할 파이썬 파일 선택",
        filetypes=[("Python files", "*.py"), ("All files", "*.*")]
    )
    if path:
        entry_script.delete(0, tk.END)
        entry_script.insert(0, path)
        _auto_fill_project_root()
        _auto_profile_hint()


def select_output_dir():
    path = filedialog.askdirectory(title="출력 폴더 선택")
    if path:
        entry_output.delete(0, tk.END)
        entry_output.insert(0, path)


def select_icon_ico():
    path = filedialog.askopenfilename(
        title="아이콘(.ico) 파일 선택",
        filetypes=[("Icon files", "*.ico"), ("All files", "*.*")]
    )
    if path:
        entry_icon.delete(0, tk.END)
        entry_icon.insert(0, path)


# ---------------------------------------------------------
# 4) 패키지 경로/루트 자동 감지
# ---------------------------------------------------------
def _is_pkg_dir(p: str) -> bool:
    return _exists_dir(p) and _exists_file(os.path.join(p, "__init__.py"))


def find_project_root_from_script(script_path: str) -> str | None:
    try:
        cur = _norm(os.path.dirname(script_path))
    except Exception:
        return None

    for _ in range(25):
        app_dir = os.path.join(cur, "app")
        if _is_pkg_dir(app_dir):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    return None


def _auto_fill_project_root():
    script_path = entry_script.get().strip()
    if not script_path or not _exists_file(script_path):
        return
    root_dir = find_project_root_from_script(script_path)
    if root_dir:
        entry_root.delete(0, tk.END)
        entry_root.insert(0, root_dir)


def _script_basename() -> str:
    p = entry_script.get().strip()
    if not p:
        return ""
    return os.path.basename(p).lower()


# ---------------------------------------------------------
# 5) 패키지/바이너리 감지
# ---------------------------------------------------------
def get_plotly_validators_dir():
    try:
        import plotly
        base = os.path.dirname(plotly.__file__)
        validators_dir = os.path.join(base, "validators")
        if _exists_dir(validators_dir):
            return validators_dir
    except Exception:
        pass
    return None


def get_plotly_package_dir():
    try:
        import plotly
        base = os.path.dirname(plotly.__file__)
        if _exists_dir(base):
            return base
    except Exception:
        pass
    return None


def get_xgboost_dll_candidates():
    try:
        import xgboost
        base = os.path.dirname(xgboost.__file__)
        cand = []
        for root_dir, _, files in os.walk(base):
            for fn in files:
                low = fn.lower()
                if low.endswith(".dll") and ("xgboost" in low or "libxgboost" in low):
                    cand.append(os.path.join(root_dir, fn))
        return cand
    except Exception:
        return []


def get_lightgbm_dll_candidates():
    try:
        import lightgbm
        base = os.path.dirname(lightgbm.__file__)
        cand = []
        for root_dir, _, files in os.walk(base):
            for fn in files:
                low = fn.lower()
                if low.endswith(".dll") and ("lightgbm" in low or "lib_lightgbm" in low):
                    cand.append(os.path.join(root_dir, fn))
        return cand
    except Exception:
        return []


def get_pyarrow_dll_candidates():
    try:
        import pyarrow

        base = os.path.dirname(pyarrow.__file__)
        py_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"  # ex) cp312

        cand = []
        for root_dir, _, files in os.walk(base):
            for fn in files:
                low = fn.lower()
                if not (low.endswith(".dll") or low.endswith(".pyd")):
                    continue

                # cp 태그가 파일명에 있으면 현재 파이썬 버전과 일치하는 것만 포함
                if low.endswith(".pyd") and ("cp" in low):
                    if py_tag not in low:
                        continue

                cand.append(os.path.join(root_dir, fn))
        return cand
    except Exception:
        return []


# ---------------------------------------------------------
# 6) 빌드 환경 import probe
# ---------------------------------------------------------
def _probe_import(module_name: str) -> bool:
    try:
        r = subprocess.run(
            [NUITKA_PYTHON, "-c", f"import {module_name}; print('OK')"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return (r.returncode == 0) and ("OK" in (r.stdout or ""))
    except Exception:
        return False


# ---------------------------------------------------------
# 7) 빌드 프로필 자동 추천
# ---------------------------------------------------------
def _apply_profile_for_basename(bn: str):
    anti_bloat_disable_var.set(1)
    include_app_var.set(1)
    include_env_var.set(1)
    disable_self_execution_var.set(0)

    if bn == "run_ui.py":
        include_streamlit_app_var.set(1)
        include_streamlit_pkg_var.set(1)
        include_playwright_var.set(1)   # UI에도 playwright 포함
        nofollow_streamlit_var.set(0)
        build_mode_var.set("standalone")
        append_log("[AUTO] run_ui.py 감지: standalone, streamlit_pkg=ON, streamlit_app=ON, playwright=ON, nofollow=OFF\n")
        return

    if bn == "run_mailer.py":
        include_streamlit_app_var.set(0)
        include_streamlit_pkg_var.set(0)
        include_playwright_var.set(1)
        nofollow_streamlit_var.set(1)
        disable_self_execution_var.set(1)  # ✅ snapshot_mailer subprocess 자기호출 대응
        build_mode_var.set("standalone")
        append_log("[AUTO] run_mailer.py 감지: standalone, playwright=ON, streamlit nofollow=ON, self-execution flag disable=ON\n")
        return

    if bn in ("launcher.py", "run_api.py"):
        include_streamlit_app_var.set(0)
        include_streamlit_pkg_var.set(0)
        include_playwright_var.set(0)
        nofollow_streamlit_var.set(1)
        append_log(f"[AUTO] {bn} 감지: streamlit_pkg=OFF, streamlit_app=OFF, playwright=OFF, nofollow=ON\n")
        return

    include_streamlit_pkg_var.set(0)
    include_playwright_var.set(0)


def _auto_profile_hint():
    bn = _script_basename()
    if not bn:
        return
    _apply_profile_for_basename(bn)


# ---------------------------------------------------------
# 8) run_ui 산출물 검증 / 후처리
# ---------------------------------------------------------
def _expected_dist_dir(output_dir: str, script_path: str) -> str:
    base = os.path.splitext(os.path.basename(script_path))[0]
    return os.path.join(_norm(output_dir), f"{base}.dist")


def _verify_run_ui_dist(output_dir: str, script_path: str) -> tuple[bool, str]:
    dist_dir = _expected_dist_dir(output_dir, script_path)
    app_py = os.path.join(dist_dir, "app", "streamlit_app", "app.py")
    pages_dir = os.path.join(dist_dir, "app", "streamlit_app", "pages")

    ok_app = _exists_file(app_py)
    ok_pages = _exists_dir(pages_dir)

    lines = []
    lines.append(f"[VERIFY] dist_dir={dist_dir}")
    lines.append(f"[VERIFY] app.py={'OK' if ok_app else 'MISSING'} -> {app_py}")
    lines.append(f"[VERIFY] pages={'OK' if ok_pages else 'MISSING'} -> {pages_dir}")

    return ok_app, "\n".join(lines) + "\n"


def _copy_streamlit_app_after_build(project_root: str, output_dir: str, script_path: str) -> tuple[bool, str]:
    try:
        src_dir = os.path.join(project_root, "app", "streamlit_app")
        dist_dir = _expected_dist_dir(output_dir, script_path)
        dst_dir = os.path.join(dist_dir, "app", "streamlit_app")

        if not _exists_dir(src_dir):
            return False, f"[POSTCOPY] source missing: {src_dir}\n"

        os.makedirs(os.path.join(dist_dir, "app"), exist_ok=True)

        if os.path.isdir(dst_dir):
            shutil.rmtree(dst_dir)

        shutil.copytree(src_dir, dst_dir)

        app_py = os.path.join(dst_dir, "app.py")
        pages_dir = os.path.join(dst_dir, "pages")

        ok = _exists_file(app_py)
        msg = []
        msg.append(f"[POSTCOPY] src={src_dir}")
        msg.append(f"[POSTCOPY] dst={dst_dir}")
        msg.append(f"[POSTCOPY] app.py={'OK' if _exists_file(app_py) else 'MISSING'} -> {app_py}")
        msg.append(f"[POSTCOPY] pages={'OK' if _exists_dir(pages_dir) else 'MISSING'} -> {pages_dir}")
        return ok, "\n".join(msg) + "\n"

    except Exception as e:
        return False, f"[POSTCOPY][ERROR] {e}\n"


# ---------------------------------------------------------
# 9) 공통 컴파일 실행
# ---------------------------------------------------------
def run_compile(disable_console: bool = False):
    script_path = entry_script.get().strip()
    output_dir = entry_output.get().strip()
    icon_path = entry_icon.get().strip()
    project_root = entry_root.get().strip()

    if not script_path:
        messagebox.showwarning("경고", "컴파일할 파이썬(.py) 파일을 선택하세요.")
        return
    if not _exists_file(script_path):
        messagebox.showerror("에러", f"파일이 존재하지 않습니다.\n{script_path}")
        return

    bn = os.path.basename(script_path).lower()

    if not output_dir:
        output_dir = os.path.dirname(script_path)
        entry_output.delete(0, tk.END)
        entry_output.insert(0, output_dir)

    if icon_path:
        if not _exists_file(icon_path):
            messagebox.showerror("에러", f"아이콘 파일이 존재하지 않습니다.\n{icon_path}")
            return
        if not icon_path.lower().endswith(".ico"):
            messagebox.showerror("에러", "아이콘은 .ico 파일만 지원합니다.")
            return

    mode = build_mode_var.get().strip()
    if mode not in ("onefile", "standalone"):
        messagebox.showerror("에러", "빌드 모드를 선택하세요.")
        return

    jobs = entry_jobs.get().strip()
    if jobs:
        try:
            jobs_int = int(jobs)
            if jobs_int <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("에러", "--jobs 값은 1 이상의 정수여야 합니다.")
            return

    if not project_root:
        auto_root = find_project_root_from_script(script_path)
        if auto_root:
            project_root = auto_root
            entry_root.delete(0, tk.END)
            entry_root.insert(0, project_root)

    use_app_pkg = (include_app_var.get() == 1)
    include_env = (include_env_var.get() == 1)
    include_streamlit_app = (include_streamlit_app_var.get() == 1)
    use_nofollow_streamlit = (nofollow_streamlit_var.get() == 1)
    include_streamlit_pkg = (include_streamlit_pkg_var.get() == 1)
    include_playwright = (include_playwright_var.get() == 1)
    antibloat_disabled = (anti_bloat_disable_var.get() == 1)
    disable_self_execution = (disable_self_execution_var.get() == 1)

    if use_app_pkg:
        if not project_root:
            messagebox.showerror(
                "에러",
                "include-package=app 옵션이 ON인데 PROJECT ROOT를 찾지 못했습니다.\n"
                "PROJECT ROOT를 직접 입력하거나,\n"
                "스크립트가 <root>/app/... 구조 안에 있는지 확인하세요."
            )
            return

        app_dir = os.path.join(project_root, "app")
        if not _is_pkg_dir(app_dir):
            messagebox.showerror(
                "에러",
                f"PROJECT ROOT 아래에 app 패키지가 없습니다.\n"
                f"PROJECT ROOT: {project_root}\n"
                f"필요 조건: {app_dir}\\__init__.py 존재"
            )
            return

    if bn == "run_ui.py":
        mode = "standalone"
        build_mode_var.set("standalone")
        include_streamlit_app = True
        include_streamlit_app_var.set(1)
        include_streamlit_pkg = True
        include_streamlit_pkg_var.set(1)
        include_playwright = True
        include_playwright_var.set(1)
        use_nofollow_streamlit = False
        nofollow_streamlit_var.set(0)

    if bn == "run_mailer.py":
        mode = "standalone"
        build_mode_var.set("standalone")
        include_playwright = True
        include_playwright_var.set(1)
        use_nofollow_streamlit = True
        nofollow_streamlit_var.set(1)
        disable_self_execution = True
        disable_self_execution_var.set(1)

    if bn in ("launcher.py", "run_api.py"):
        use_nofollow_streamlit = True
        nofollow_streamlit_var.set(1)

    cmd = [
        NUITKA_PYTHON, "-m", "nuitka",
        "--mingw64",
        "--follow-imports",
        "--assume-yes-for-downloads",
        "--enable-plugin=multiprocessing",
        "--remove-output",
    ]

    if mode == "onefile":
        cmd.append("--onefile")
    else:
        cmd.append("--standalone")

    if jobs:
        cmd.append(f"--jobs={jobs}")

    if disable_console:
        cmd.append("--windows-console-mode=disable")

    if icon_path:
        cmd.append(f"--windows-icon-from-ico={_norm(icon_path)}")

    # ✅ 자기 자신 subprocess 호출 허용
    # run_mailer.exe 가 run_mailer.exe --snap-run-once ... 형태로 다시 실행될 때 필요
    if disable_self_execution:
        cmd.append("--no-deployment-flag=self-execution")

    # 공통 패키지
    cmd += [
        "--include-module=pandas",
        "--include-module=numpy",
        "--include-module=openpyxl",
        "--include-module=psycopg2",
        "--include-module=sqlalchemy",
        "--include-module=tqdm",
        "--include-module=dateutil",
        "--include-module=requests",

        "--include-package=plotly",
        "--include-package=plotly.io",
        "--include-package=plotly.express",
        "--include-package=plotly.data",
        "--include-package=plotly.validators",
        "--include-package=plotly.graph_objs",
        "--include-package-data=plotly",

        "--include-package=sklearn",

        "--include-package=xgboost",
        "--include-module=xgboost",
        "--include-module=xgboost.core",
        "--include-module=xgboost.sklearn",
        "--include-module=xgboost.callback",
        "--include-package-data=xgboost",

        "--include-package=lightgbm",
        "--include-module=lightgbm",
        "--include-package-data=lightgbm",

        # ✅ pyarrow 보강
        "--include-package=pyarrow",
        "--include-package-data=pyarrow",
        "--include-module=pyarrow",
        "--include-module=pyarrow.lib",
    ]

    if use_app_pkg:
        cmd.append("--include-package=app")

    if antibloat_disabled:
        cmd.append("--disable-plugin=anti-bloat")

    # dotenv
    dotenv_note = "OFF"
    if _probe_import("dotenv"):
        cmd += ["--include-module=dotenv", "--include-module=dotenv.main"]
        dotenv_note = "ON (dotenv, dotenv.main)"
    else:
        dotenv_note = "OFF (build venv에서 import dotenv 실패)"

    # streamlit
    streamlit_pkg_note = "OFF"
    if include_streamlit_pkg:
        if _probe_import("streamlit"):
            cmd += [
                "--include-package=streamlit",
                "--include-package=streamlit.web",
                "--include-package=streamlit.runtime",
                "--include-package=streamlit.components",
                "--include-package-data=streamlit",
            ]
            streamlit_pkg_note = "ON"
        else:
            streamlit_pkg_note = "OFF (build venv에서 import streamlit 실패)"

    # playwright
    playwright_note = "OFF"
    if include_playwright:
        if _probe_import("playwright"):
            cmd += [
                "--include-package=playwright",
                "--include-package=playwright.async_api",
                "--include-package=playwright.sync_api",
                "--include-package=playwright._impl",
                "--include-package-data=playwright",
            ]
            playwright_note = "ON"
        else:
            playwright_note = "OFF (build venv에서 import playwright 실패)"

    # ✅ matplotlib: 설치되어 있을 때만 포함
    matplotlib_note = "OFF"
    if _probe_import("matplotlib"):
        cmd += [
            "--include-package=matplotlib",
            "--include-package=matplotlib.backends",
            "--include-module=matplotlib",
            "--include-package-data=matplotlib",
        ]
        matplotlib_note = "ON"
    else:
        matplotlib_note = "OFF (build venv에서 import matplotlib 실패)"

    streamlit_app_note = "OFF"
    if include_streamlit_app:
        if not project_root:
            messagebox.showerror("에러", "streamlit_app 포함을 켰는데 PROJECT ROOT를 찾지 못했습니다.")
            return

        src_dir = os.path.join(project_root, "app", "streamlit_app")
        src_app_py = os.path.join(src_dir, "app.py")

        if not _exists_dir(src_dir):
            messagebox.showerror("에러", f"streamlit_app 폴더가 없습니다:\n{src_dir}")
            return
        if not _exists_file(src_app_py):
            messagebox.showerror("에러", f"streamlit_app/app.py 파일이 없습니다:\n{src_app_py}")
            return

        # 참고용 include + post-copy 보강
        cmd.append(f"--include-data-dir={_norm(src_dir)}=app/streamlit_app")
        streamlit_app_note = f"ON ({src_dir} -> app/streamlit_app, .py는 post-copy로 보강)"

    nofollow_note = "OFF"
    if use_nofollow_streamlit:
        cmd += [
            "--nofollow-import-to=streamlit",
            "--nofollow-import-to=streamlit.*",
            "--nofollow-import-to=app.streamlit_app",
            "--nofollow-import-to=app.streamlit_app.*",
        ]
        nofollow_note = "ON (streamlit/app.streamlit_app)"

    validators_dir = get_plotly_validators_dir()
    if validators_dir:
        cmd.append(f"--include-data-dir={_norm(validators_dir)}=plotly/validators")

    if include_plotly_all_var.get() == 1:
        plotly_root = get_plotly_package_dir()
        if plotly_root:
            cmd.append(f"--include-data-dir={_norm(plotly_root)}=plotly")

    xgb_dlls = get_xgboost_dll_candidates()
    for dll_path in xgb_dlls:
        dll_name = os.path.basename(dll_path)
        cmd.append(f"--include-data-files={_norm(dll_path)}=xgboost/{dll_name}")

    lgb_dlls = get_lightgbm_dll_candidates()
    for dll_path in lgb_dlls:
        dll_name = os.path.basename(dll_path)
        cmd.append(f"--include-data-files={_norm(dll_path)}=lightgbm/{dll_name}")

    pa_bins = get_pyarrow_dll_candidates()
    for bin_path in pa_bins:
        bn_file = os.path.basename(bin_path)
        cmd.append(f"--include-data-files={_norm(bin_path)}=pyarrow/{bn_file}")

    env_included = False
    env_src = None
    if include_env and project_root:
        cand = os.path.join(project_root, "app", ".env")
        if _exists_file(cand):
            env_src = cand
            cmd.append(f"--include-data-files={_norm(env_src)}=app/.env")
            env_included = True

    cmd.append(f"--output-dir={_norm(output_dir)}")
    cmd.append(_norm(script_path))

    append_log("\n" + "=" * 78 + "\n")
    append_log(f"빌드 대상: {os.path.basename(script_path)}\n")
    append_log(f"빌드 모드: {mode}\n")
    append_log(f"콘솔: {'숨김' if disable_console else '포함'}\n")
    if jobs:
        append_log(f"빌드 jobs: {jobs}\n")

    append_log(f"PROJECT ROOT: {project_root or '(미설정)'}\n")
    append_log(f"include-package=app: {'ON' if use_app_pkg else 'OFF'}\n")
    append_log(f"anti-bloat disable: {'ON' if antibloat_disabled else 'OFF'}\n")
    append_log(f"no-deployment-flag=self-execution: {'ON' if disable_self_execution else 'OFF'}\n")
    append_log(f"dotenv include: {dotenv_note}\n")
    append_log(f"streamlit package include: {streamlit_pkg_note}\n")
    append_log(f"playwright include: {playwright_note}\n")
    append_log(f"matplotlib include: {matplotlib_note}\n")
    append_log(f"streamlit_app include: {streamlit_app_note}\n")
    append_log(f"nofollow streamlit: {nofollow_note}\n")

    if env_included:
        append_log(f".env 포함: {env_src} -> app/.env\n")
    else:
        append_log(".env 포함: OFF 또는 파일 미존재\n")

    append_log(f"XGBoost DLL 감지/포함: {len(xgb_dlls)}개\n")
    append_log(f"LightGBM DLL 감지/포함: {len(lgb_dlls)}개\n")
    append_log(f"pyarrow DLL/.pyd 감지/포함: {len(pa_bins)}개\n")

    if bn == "run_ui.py":
        append_log("[CHECK] run_ui는 dist 안에 app/streamlit_app/app.py 실파일이 반드시 있어야 합니다.\n")
        append_log("[CHECK] run_ui는 빌드 후 POSTCOPY로 streamlit_app 폴더를 강제 복사합니다.\n")
        append_log("[CHECK] run_ui는 matplotlib / pyarrow / playwright 포함으로 보강됩니다(단, matplotlib는 설치 시에만 포함).\n")

    if bn == "run_mailer.py":
        append_log("[CHECK] run_mailer는 playwright 패키지가 빌드 venv에 설치되어 있어야 합니다.\n")
        append_log("[CHECK] 필요 시 별도로 실행: python -m playwright install chromium\n")
        append_log("[CHECK] run_mailer는 exe가 자기 자신을 subprocess로 다시 호출하므로 --no-deployment-flag=self-execution 이 필요합니다.\n")

    append_log("\n실행 명령어:\n")
    append_log(safe_join_cmd(cmd) + "\n")
    append_log("=" * 78 + "\n\n")

    try:
        env = os.environ.copy()
        cwd = None

        if use_app_pkg and project_root:
            old_pp = env.get("PYTHONPATH", "")
            parts = []
            if old_pp.strip():
                parts = [p for p in old_pp.split(os.pathsep) if p.strip()]

            normalized_parts = []
            for p in parts:
                try:
                    normalized_parts.append(_norm(p))
                except Exception:
                    pass

            if _norm(project_root) not in normalized_parts:
                parts.insert(0, project_root)

            env["PYTHONPATH"] = os.pathsep.join(parts)
            env["PROJECT_ROOT"] = project_root
            cwd = project_root

        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        append_log(result.stdout + "\n")

        if result.returncode == 0 and bn == "run_ui.py":
            copy_ok, copy_text = _copy_streamlit_app_after_build(project_root, output_dir, script_path)
            append_log(copy_text)

            ok, verify_text = _verify_run_ui_dist(output_dir, script_path)
            append_log(verify_text)

            if not ok:
                messagebox.showerror(
                    "run_ui 산출물 검증 실패",
                    "빌드는 끝났지만 run_ui.dist 안에 app/streamlit_app/app.py 가 없습니다.\n"
                    "자동 복사 후에도 누락되었습니다.\n\n"
                    "로그 하단 POSTCOPY / VERIFY 결과를 확인하세요."
                )
                return

        if result.returncode == 0:
            messagebox.showinfo("완료", "컴파일이 성공적으로 완료되었습니다.")
        else:
            messagebox.showerror("오류", "컴파일 중 문제가 발생했습니다. 로그를 확인하세요.")

    except Exception as e:
        messagebox.showerror("예외 발생", f"컴파일 실행 중 예외 발생:\n{e}")


# ---------------------------------------------------------
# 10) Tkinter GUI
# ---------------------------------------------------------
root = tk.Tk()
root.title("Nuitka 컴파일러 GUI (run_ui/launcher/api/mailer 안정화)")
root.geometry("1040x1020")

frame_top = tk.Frame(root)
frame_top.pack(fill="x", padx=10, pady=10)

lbl_script = tk.Label(frame_top, text="파이썬 파일(.py)")
lbl_script.grid(row=0, column=0, sticky="w")

entry_script = tk.Entry(frame_top, width=110)
entry_script.grid(row=1, column=0, padx=5, pady=3, sticky="w")

btn_script = tk.Button(frame_top, text="찾아보기", command=select_script)
btn_script.grid(row=1, column=1, padx=5, pady=3)

lbl_root = tk.Label(frame_top, text="PROJECT ROOT (비우면 자동 감지: 상위에서 app/__init__.py 탐색)")
lbl_root.grid(row=2, column=0, sticky="w")

entry_root = tk.Entry(frame_top, width=110)
entry_root.grid(row=3, column=0, padx=5, pady=3, sticky="w")

lbl_output = tk.Label(frame_top, text="출력 폴더 (비우면 py 파일 위치)")
lbl_output.grid(row=4, column=0, sticky="w")

entry_output = tk.Entry(frame_top, width=110)
entry_output.grid(row=5, column=0, padx=5, pady=3, sticky="w")

btn_output = tk.Button(frame_top, text="찾아보기", command=select_output_dir)
btn_output.grid(row=5, column=1, padx=5, pady=3)

lbl_icon = tk.Label(frame_top, text="EXE 아이콘(.ico) (선택)")
lbl_icon.grid(row=6, column=0, sticky="w")

entry_icon = tk.Entry(frame_top, width=110)
entry_icon.grid(row=7, column=0, padx=5, pady=3, sticky="w")

btn_icon = tk.Button(frame_top, text="아이콘 선택", command=select_icon_ico)
btn_icon.grid(row=7, column=1, padx=5, pady=3)

frame_mode = tk.LabelFrame(root, text="빌드 모드")
frame_mode.pack(fill="x", padx=10, pady=5)

build_mode_var = tk.StringVar(value="standalone")
tk.Radiobutton(frame_mode, text="standalone (권장)", value="standalone", variable=build_mode_var).pack(side="left", padx=10, pady=5)
tk.Radiobutton(frame_mode, text="onefile", value="onefile", variable=build_mode_var).pack(side="left", padx=10, pady=5)

frame_opt = tk.LabelFrame(root, text="추가 옵션")
frame_opt.pack(fill="x", padx=10, pady=5)

lbl_jobs = tk.Label(frame_opt, text="빌드 jobs(선택):")
lbl_jobs.grid(row=0, column=0, sticky="w")
entry_jobs = tk.Entry(frame_opt, width=10)
entry_jobs.grid(row=0, column=1, padx=5, sticky="w")
entry_jobs.insert(0, "")

include_plotly_all_var = tk.IntVar(value=0)
tk.Checkbutton(
    frame_opt,
    text="(용량 증가) plotly 전체 데이터 폴더 포함(보험)",
    variable=include_plotly_all_var
).grid(row=0, column=2, padx=10, sticky="w")

include_app_var = tk.IntVar(value=1)
tk.Checkbutton(
    frame_opt,
    text="include-package=app 사용 (PROJECT ROOT 기반 cwd/PYTHONPATH prepend)",
    variable=include_app_var
).grid(row=1, column=0, columnspan=3, sticky="w", padx=5, pady=3)

anti_bloat_disable_var = tk.IntVar(value=1)
tk.Checkbutton(
    frame_opt,
    text="(권장) anti-bloat 비활성화 (--disable-plugin=anti-bloat)",
    variable=anti_bloat_disable_var
).grid(row=2, column=0, columnspan=3, sticky="w", padx=5, pady=3)

include_env_var = tk.IntVar(value=1)
tk.Checkbutton(
    frame_opt,
    text="(있으면) PROJECT ROOT/app/.env 를 app/.env 로 포함",
    variable=include_env_var
).grid(row=3, column=0, columnspan=3, sticky="w", padx=5, pady=3)

include_streamlit_pkg_var = tk.IntVar(value=0)
tk.Checkbutton(
    frame_opt,
    text="(run_ui 권장) streamlit 패키지/데이터 포함",
    variable=include_streamlit_pkg_var
).grid(row=4, column=0, columnspan=3, sticky="w", padx=5, pady=3)

include_streamlit_app_var = tk.IntVar(value=0)
tk.Checkbutton(
    frame_opt,
    text="(run_ui 필수) app/streamlit_app 폴더 포함 + 빌드 후 post-copy 보강",
    variable=include_streamlit_app_var
).grid(row=5, column=0, columnspan=3, sticky="w", padx=5, pady=3)

include_playwright_var = tk.IntVar(value=0)
tk.Checkbutton(
    frame_opt,
    text="(run_mailer/run_ui 필요 시) playwright 패키지/데이터 포함",
    variable=include_playwright_var
).grid(row=6, column=0, columnspan=3, sticky="w", padx=5, pady=3)

nofollow_streamlit_var = tk.IntVar(value=1)
tk.Checkbutton(
    frame_opt,
    text="(launcher/api/mailer 권장) streamlit / app.streamlit_app nofollow 적용",
    variable=nofollow_streamlit_var
).grid(row=7, column=0, columnspan=3, sticky="w", padx=5, pady=3)

disable_self_execution_var = tk.IntVar(value=0)
tk.Checkbutton(
    frame_opt,
    text="(run_mailer 권장) Nuitka self-execution 보호 해제 (--no-deployment-flag=self-execution)",
    variable=disable_self_execution_var
).grid(row=8, column=0, columnspan=3, sticky="w", padx=5, pady=3)

btn_run_console = tk.Button(root, text="콘솔 포함 EXE 만들기", command=lambda: run_compile(False), height=2)
btn_run_console.pack(fill="x", padx=10, pady=6)

btn_run_no_console = tk.Button(root, text="콘솔 숨김 EXE 만들기", command=lambda: run_compile(True), height=2)
btn_run_no_console.pack(fill="x", padx=10, pady=6)

frame_log = tk.LabelFrame(root, text="로그 출력")
frame_log.pack(fill="both", expand=True, padx=10, pady=5)

txt_log = scrolledtext.ScrolledText(frame_log, wrap="word")
txt_log.pack(fill="both", expand=True, padx=5, pady=5)

append_log("[INFO] onefile/standalone 동시 사용 금지로 옵션 충돌 제거\n")
append_log("[INFO] include-package=app ON 시 build cwd=PROJECT ROOT, PYTHONPATH는 prepend만 수행\n")
append_log("[INFO] run_ui는 dist 안에 app/streamlit_app/app.py 실파일이 반드시 있어야 함\n")
append_log("[INFO] run_ui는 빌드 후 POSTCOPY로 streamlit_app 폴더를 강제 복사\n")
append_log("[INFO] run_ui는 pyarrow / playwright 포함으로 보강, matplotlib는 설치 시에만 포함\n")
append_log("[INFO] run_mailer는 playwright 포함이 필수\n")
append_log("[INFO] launcher/run_api/run_mailer 는 streamlit nofollow 권장\n")
append_log("[INFO] python-dotenv는 include-package=dotenv 대신 include-module=dotenv, dotenv.main 사용\n")
append_log("[INFO] anti-bloat는 기본 비활성화 유지\n")
append_log("[INFO] run_mailer가 자기 자신 exe를 subprocess로 다시 호출하면 --no-deployment-flag=self-execution 이 필요\n")
append_log("[INFO] run_ui 빌드 성공 후 POSTCOPY / VERIFY 로그를 반드시 확인\n\n")

root.mainloop()