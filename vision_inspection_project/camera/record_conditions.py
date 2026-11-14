import json
import math
from pathlib import Path

RECORD_CONDITION_FILE = Path(r"C:\vision_inspection\test\info\record_condition.txt")


def condition_files(directory=None):
    directory = Path(directory) if directory is not None else RECORD_CONDITION_FILE.parent
    paths = list(directory.glob("*_record_condition.txt"))
    legacy = directory / "record_condition.txt"
    if legacy.exists():
        paths.append(legacy)
    return sorted(paths, key=lambda path: (path.stat().st_mtime_ns, path.name))


def named_condition_path(name, directory=None):
    directory = Path(directory) if directory is not None else RECORD_CONDITION_FILE.parent
    name = name.strip()
    if name.endswith(".txt"):
        name = name[:-4]
    if name.endswith("_record_condition"):
        name = name[:-len("_record_condition")]
    if not name or any(char in name for char in '<>:"/\\|?*') or name.endswith((".", " ")):
        raise ValueError("Enter a valid condition name without path separators.")
    if name.split(".")[0].upper() in {"CON", "PRN", "AUX", "NUL", *[f"COM{i}" for i in range(1, 10)], *[f"LPT{i}" for i in range(1, 10)]}:
        raise ValueError("This condition name is reserved by Windows.")
    return directory / f"{name}_record_condition.txt"


def validate_conditions(data):
    result = {"fps": float(data["fps"]), "total_time": float(data["total_time"]),
              "initial": {"zoom": float(data["initial"]["zoom"]),
                          "focus": float(data["initial"]["focus"]),
                          "auto_focus": bool(data["initial"]["auto_focus"])}, "zoom_intervals": []}
    bot = data.get('switchbot',{})
    result['switchbot'] = dict(enabled=bool(bot.get('enabled',False)),address=str(bot.get('address','')).strip(),intervals=[])
    if result['switchbot']['enabled'] and not result['switchbot']['address']:
        raise ValueError('Select a SwitchBot BLE device before saving/starting.')
    for item in bot.get('intervals',[]):
        stop,go = float(item['stop']),float(item['go'])
        if not all(math.isfinite(n) for n in (stop,go)) or not 0 < stop < go < result['total_time']:
            raise ValueError('SwitchBot intervals require 0 < STOP < GO < Total time.')
        result['switchbot']['intervals'].append(dict(stop=stop,go=go))
    result['switchbot']['intervals'].sort(key=lambda item:item['stop'])
    for previous,current in zip(result['switchbot']['intervals'],result['switchbot']['intervals'][1:]):
        if previous['go'] >= current['stop']:
            raise ValueError('SwitchBot STOP/GO intervals must not overlap or share a time.')
    if not math.isfinite(result["fps"]) or not 0 < result["fps"] <= 30:
        raise ValueError("Record FPS must be greater than 0 and at most 30.")
    if not math.isfinite(result["total_time"]) or result["total_time"] <= 0:
        raise ValueError("Total time must be greater than 0 seconds.")
    if not math.isfinite(result["initial"]["zoom"]) or result["initial"]["zoom"] <= 0:
        raise ValueError("Base zoom must be positive.")
    if not math.isfinite(result["initial"]["focus"]) or result["initial"]["focus"] < 0:
        raise ValueError("Focus must be nonnegative.")
    for item in data.get("zoom_intervals", []):
        zoom, start, end = (float(item[key]) for key in ("zoom", "start", "end"))
        if not all(math.isfinite(value) for value in (zoom, start, end)) or zoom <= 0 or not 0 <= start < end <= result["total_time"]:
            raise ValueError("Zoom intervals require positive zoom and 0 <= start < end <= Total time.")
        result["zoom_intervals"].append(dict(zoom=zoom, start=start, end=end))
    result["zoom_intervals"].sort(key=lambda item: item["start"])
    for previous, current in zip(result["zoom_intervals"], result["zoom_intervals"][1:]):
        if previous["end"] > current["start"]:
            raise ValueError("Zoom intervals must not overlap.")
    return result


def load_conditions(path=RECORD_CONDITION_FILE, default_fps=23):
    if not path.exists():
        return validate_conditions({"fps": default_fps, "total_time": 10,
                                    "initial": {"zoom": 1, "focus": 0, "auto_focus": True}, "zoom_intervals": []})
    return validate_conditions(json.loads(path.read_text(encoding="utf-8")))


def save_conditions(data, path=RECORD_CONDITION_FILE):
    data = validate_conditions(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2), encoding="utf-8")
    temporary.replace(path)


def recording_actions(data):
    data = validate_conditions(data)
    events = []
    for interval in data["zoom_intervals"]:
        events.append({"seconds": interval["start"], "values": dict(data["initial"], zoom=interval["zoom"])})
        events.append({"seconds": interval["end"], "values": dict(data["initial"])})
    return {"initial": data["initial"], "events": sorted(events, key=lambda item: item["seconds"]),
            "duration": data["total_time"],"switchbot":data['switchbot']}
