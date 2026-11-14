import json
from pathlib import Path

from inspection.performance import result_directory


def name_live_results(saved, name, group, fps, threshold, log_dir, old_stem):
    saved = Path(saved)
    old_folder = saved.parent
    target = result_directory(old_folder.parent,name,group,fps,threshold)
    if target.exists():
        raise FileExistsError(f'Result folder already exists: {target}')
    new_stem = name + '_live_test'
    replacements = [(str(old_folder),str(target)),(old_stem,new_stem)]
    def replace(text):
        for old,new in replacements:
            text = text.replace(old,new)
        return text
    # Select this run's logs before moving anything, never unrelated test logs.
    logs = list(Path(log_dir).glob(old_stem+'*.txt')) + list(Path(log_dir).glob(old_stem+'*.pipeline.json'))
    for path in logs:
        destination = path.with_name(path.name.replace(old_stem,new_stem))
        if destination != path and destination.exists():
            raise FileExistsError(f'Result log already exists: {destination}')
    old_folder.rename(target)
    for path in list(target.iterdir()):
        if path.is_file() and old_stem in path.name:
            path.rename(path.with_name(path.name.replace(old_stem,new_stem)))
    for path in list(target.rglob('*.json')) + logs:
        if path.suffix == '.json':
            data = json.loads(path.read_text(encoding='utf-8'))
            def rewrite(value):
                if isinstance(value,str):
                    return replace(value)
                if isinstance(value,list):
                    return [rewrite(item) for item in value]
                if isinstance(value,dict):
                    return {key:rewrite(item) for key,item in value.items()}
                return value
            path.write_text(json.dumps(rewrite(data),ensure_ascii=False,indent=2),encoding='utf-8')
        else:
            path.write_text(replace(path.read_text(encoding='utf-8')),encoding='utf-8')
        if path in logs:
            path.rename(path.with_name(path.name.replace(old_stem,new_stem)))
    return target / saved.name.replace(old_stem,new_stem)
