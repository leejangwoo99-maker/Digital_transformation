"""Frame-group copy combinations and transactional, type-scoped absorption."""
import copy
import json
import math
import os
import re
import shutil
import tempfile
from pathlib import Path

from inspection.frame_conditions import group_fps
from inspection.special_checks import annotation_dir


def combined_name(name, sources):
    name = name.strip()
    if not name or name in ('.','..') or re.search(r'[<>:"/\\|?*\x00-\x1f]',name) or name.endswith('.'):
        raise ValueError('Enter a valid new group name.')
    if re.fullmatch(r'(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])',name,re.I):
        raise ValueError('Reserved Windows group name.')
    if len(sources) != 2 or sources[0] == sources[1]:
        raise ValueError('Select two different frame groups.')
    fps = group_fps(sources[0])
    if not math.isclose(fps,group_fps(sources[1]),abs_tol=1e-9):
        raise ValueError('Cannot combine groups with different FPS.')
    if re.search(r'_\d+(?:\.\d+)?fps$',name):
        if not math.isclose(group_fps(name),fps,abs_tol=1e-9):
            raise ValueError('New group FPS must match the source groups.')
    else:
        name += f'_{fps:g}fps'
    if name.casefold() in {source.casefold() for source in sources}:
        raise ValueError('The destination must not be one of the source groups.')
    if any(record_path(Path('.'),name).name.casefold() == record_path(Path('.'),source).name.casefold() for source in sources):
        raise ValueError('The destination must not share a recording-condition name with a source group.')
    return name


def record_path(info, group):
    base = re.sub(r'_\d+(?:\.\d+)?fps$','',group)
    return Path(info)/f'{base}_record_condition.txt'


def owned_files(root, image_dir, label_dir, info, target):
    root,image_dir,label_dir,info = map(Path,(root,image_dir,label_dir,info))
    result = []
    pattern = re.compile(re.escape(target)+r'_\d{6}\.(?:jpg|jpeg|png|bmp|txt|rotation\.json)$',re.I)
    for directory in (image_dir,label_dir):
        if directory.exists():
            result.extend(path for path in directory.iterdir() if path.is_file() and pattern.fullmatch(path.name))
    special = root/'dataset/special_labels'/target
    if special.exists():
        result.extend(path for path in special.rglob('*.json') if path.is_file())
    for kind in ('OCR','conn','label'):
        result.extend(info/f'{target}_{kind}_inspection_{suffix}.txt' for suffix in ('frame','time'))
    result.extend([info/f'{target}_test_condition.txt',record_path(info,target),info/f'{target}_combine.json'])
    weights = root/'weights'
    if weights.exists():
        exact = re.compile(re.escape(target)+r'_(?:algorith|conn_algorith|proto)\.(?:pt|json)$',re.I)
        prefixes = [f'{target}_{part}' for part in ('OCR_mark_algorith.','OCR_text_image.','OCR_text_image_','label_text_image.','label_text_image_')]
        result.extend(path for path in weights.iterdir() if path.is_file() and
                      (exact.fullmatch(path.name) or any(path.name.casefold().startswith(prefix.casefold()) for prefix in prefixes)))
    return list(dict.fromkeys(path for path in result if path.is_file()))


def combine_groups(root, image_dir, label_dir, info, sources, groups, name, choices, overwrite=False, progress=None):
    root,image_dir,label_dir,info = map(lambda p:Path(p).resolve(),(root,image_dir,label_dir,info))
    target = combined_name(name,sources)
    if any(choices.get(key) not in sources for key in ('inspection','record','test')):
        raise ValueError('Choose a source group for every condition.')
    if any(not groups.get(group) for group in sources):
        raise ValueError('Both source groups must contain extracted frames.')
    old = owned_files(root,image_dir,label_dir,info,target)
    if old and not overwrite:
        raise FileExistsError(f'Group already exists: {target}')
    # All destructive commit operations are file-only and restricted to these owners.
    allowed = (image_dir,label_dir,root/'dataset/special_labels'/target,info,root/'weights')
    def safe(path):
        path = Path(path)
        resolved = path.resolve()
        if path.is_symlink() or not any(resolved.is_relative_to(base.resolve()) for base in allowed):
            raise ValueError(f'Unsafe combine path: {path}')
        return path
    for path in old:
        safe(path)
    dataset = root/'dataset'
    dataset.mkdir(parents=True,exist_ok=True)
    paths,missing = [],[]
    with tempfile.TemporaryDirectory(prefix='.combine_',dir=dataset) as temporary:
        staging = Path(temporary)
        if not staging.resolve().is_relative_to(dataset.resolve()):
            raise ValueError('Unsafe combine staging directory')
        writes = {}
        def stage(destination,source=None,data=None):
            destination = safe(destination)
            staged = staging/f'new_{len(writes)}'
            if source is not None:
                shutil.copy2(source,staged)
            else:
                staged.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
            writes[destination] = staged
        for group in sources:
            for source in sorted(map(Path,groups[group]),key=lambda path:path.name):
                if not source.is_file():
                    raise ValueError(f'Missing source image: {source}')
                number = len(paths)
                destination = image_dir/f'{target}_{number:06d}{source.suffix.lower()}'
                paths.append(destination)
                stage(destination,source=source)
                txt = label_dir/f'{source.stem}.txt'
                if txt.exists():
                    stage(label_dir/f'{destination.stem}.txt',source=txt)
                rotation = txt.with_suffix('.rotation.json')
                if rotation.exists():
                    stage((label_dir/f'{destination.stem}.txt').with_suffix('.rotation.json'),source=rotation)
                for kind in ('OCR','conn','label'):
                    annotation = annotation_dir(root,group,kind)/f'{source.stem}.json'
                    if annotation.exists():
                        data = json.loads(annotation.read_text(encoding='utf-8'))
                        data.update(group=target,image=str(destination),frame_number=number+1)
                        stage(annotation_dir(root,target,kind)/f'{destination.stem}.json',data=data)
                if progress:
                    progress(f'Combine: staged frame {number+1}')
        for category in ('inspection','record','test'):
            donor = choices[category]
            pairs = []
            if category == 'inspection':
                for kind in ('OCR','conn','label'):
                    for suffix in ('frame','time'):
                        pairs.append((info/f'{donor}_{kind}_inspection_{suffix}.txt',info/f'{target}_{kind}_inspection_{suffix}.txt'))
            elif category == 'record':
                pairs = [(record_path(info,donor),record_path(info,target))]
            else:
                pairs = [(info/f'{donor}_test_condition.txt',info/f'{target}_test_condition.txt')]
            copied = 0
            for source,destination in pairs:
                if source.exists():
                    stage(destination,source=source)
                    copied += 1
            if not copied:
                missing.append(category)
        for kind in ('OCR','conn','label'):
            if any(path.parent == annotation_dir(root,target,kind) for path in writes) and info/f'{target}_{kind}_inspection_frame.txt' not in writes:
                missing.append(f'inspection ({kind})')
        stage(info/f'{target}_combine.json',data=dict(sources=sources,choices=choices,
            frame_counts={group:len(groups[group]) for group in sources},inspection_ranges_copied_verbatim=True,
            missing_conditions=missing,requires_training=True))
        backups = {}
        for path in old:
            backup = staging/f'backup_{len(backups)}'
            shutil.copy2(path,backup)
            backups[path] = backup
        changed = []
        try:
            for destination,staged in writes.items():
                safe(destination).parent.mkdir(parents=True,exist_ok=True)
                os.replace(staged,destination)
                changed.append(destination)
            for path in old:
                if path not in writes:
                    safe(path).unlink()
        except Exception:
            for path in changed:
                if path not in backups and path.exists():
                    safe(path).unlink()
            for path,backup in backups.items():
                shutil.copy2(backup,safe(path))
            raise
    return target,paths,missing


def appearance_group_absorbed(info, group, paths, label_dir):
    marker = Path(info)/f'{group}_open_labeler_absorbed.json'
    return marker.is_file() and not any(
        (label := Path(label_dir)/f'{Path(image).stem}.txt').is_file()
        and label.read_text(encoding='utf-8').strip() for image in paths)


def absorb_group(root, image_dir, label_dir, info, source, target, kind, progress=None):
    """Append one annotation type; preserve destination conditions and other types."""
    import hashlib
    from video_dataset_builder import find_frame_groups, frame_number
    from inspection.special_checks import labeled_records
    root,image_dir,label_dir,info = [Path(p).resolve() for p in (root,image_dir,label_dir,info)]
    if kind not in ('open_labeler','OCR','label','conn'):
        raise ValueError('Invalid absorption type.')
    for group in (source,target):
        if not group or Path(group).name != group or re.search(r'[<>:"/\\|?*\x00-\x1f]',group):
            raise ValueError('Invalid frame group name.')
    if source.casefold() == target.casefold():
        raise ValueError('Choose two different groups.')
    if not math.isclose(group_fps(source),group_fps(target),abs_tol=1e-9):
        raise ValueError('Cannot absorb groups with different FPS.')
    groups = find_frame_groups(image_dir)
    if not groups.get(source) or not groups.get(target):
        raise ValueError('Both groups must contain extracted frames.')
    if kind == 'open_labeler' and appearance_group_absorbed(info,source,groups[source],label_dir):
        raise ValueError('This appearance group has already been absorbed.')
    records = {} if kind == 'open_labeler' else {
        Path(data['image']).resolve():(path,data) for path,data in labeled_records(root,source,kind)}
    images = groups[source] if kind == 'open_labeler' else sorted(records,key=lambda p:(frame_number(p),p.name))
    if not images:
        raise ValueError('The selected source has no labels to absorb.')
    special_root = root/'dataset/special_labels'
    allowed = (image_dir,label_dir,special_root,info)
    def safe(path):
        path = Path(path)
        if (path.is_symlink() or any(parent.is_symlink() for parent in path.parents
                                    if any(parent.is_relative_to(base) for base in allowed))
                or not any(path.resolve().is_relative_to(base) for base in allowed)):
            raise ValueError(f'Unsafe absorption path: {path}')
        return path
    for image in images:
        safe(image)
        if image.resolve().parent != image_dir or not image.is_file():
            raise ValueError(f'Missing or external source image: {image}')
    destination_paths = list(groups[target])
    next_number = max(frame_number(path) for path in destination_paths)+1
    dataset = root/'dataset'
    dataset.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.absorb_',dir=dataset) as temporary:
        staging = Path(temporary)
        if not staging.resolve().is_relative_to(dataset):
            raise ValueError('Unsafe absorption staging directory.')
        writes,deletes = {},set()
        def stage(destination,source_path=None,data=None,replace=False):
            destination = safe(destination)
            if destination.exists() and not replace:
                raise FileExistsError(f'Absorption would overwrite existing data: {destination}')
            staged = staging/f'new_{len(writes)}'
            if source_path is not None:
                source_path = safe(source_path)
                shutil.copy2(source_path,staged)
                with source_path.open('rb') as original,staged.open('rb') as copied:
                    if hashlib.file_digest(original,'sha256').digest() != hashlib.file_digest(copied,'sha256').digest():
                        raise IOError(f'Copy verification failed: {source_path}')
            else:
                staged.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
                json.loads(staged.read_text(encoding='utf-8'))
            writes[destination] = staged
        for offset,image in enumerate(images):
            destination = image_dir/f'{target}_{next_number+offset:06d}{image.suffix.lower()}'
            if destination.exists():
                raise FileExistsError(destination)
            stage(destination,image)
            destination_paths.append(destination)
            if kind == 'open_labeler':
                for suffix in ('.txt','.rotation.json'):
                    path = label_dir/f'{image.stem}{suffix}'
                    if path.exists():
                        stage(label_dir/f'{destination.stem}{suffix}',path)
                        deletes.add(path)
            else:
                path,data = records[image.resolve()]
                data = copy.deepcopy(data)
                data.update(group=target,image=str(destination),frame_number=len(destination_paths))
                stage(annotation_dir(root,target,kind)/f'{destination.stem}.json',data=data)
                deletes.add(path)
            if progress:
                progress(f'Absorb {kind}: verified frame {offset+1}/{len(images)}')
        if kind == 'open_labeler':
            # Images can also belong to OCR/Label/Conn. Keep shared originals.
            referenced = set()
            for annotation in special_root.rglob('*.json') if special_root.exists() else ():
                data = json.loads(safe(annotation).read_text(encoding='utf-8'))
                if data.get('image'):
                    referenced.add(Path(data['image']).resolve())
            deletes.update(image for image in images if image.resolve() not in referenced)
            stage(info/f'{source}_open_labeler_absorbed.json',data=dict(target=target,kind=kind),replace=True)
        else:
            deletes.update(annotation_dir(root,source,kind).glob('*.json'))
        audit_path = info/f'{target}_{kind}_absorption.json'
        audit = json.loads(audit_path.read_text(encoding='utf-8')) if audit_path.exists() else {'events':[]}
        audit['events'].append(dict(source=source,target=target,kind=kind,added_frames=len(images),
                                   destination_conditions_preserved=True,requires_training=True))
        stage(audit_path,data=audit,replace=True)
        backups = {}
        for path in set(writes)|deletes:
            safe(path)
            if path.exists():
                backup = staging/f'backup_{len(backups)}'
                shutil.copy2(path,backup)
                backups[path] = backup
        expected = {}
        for path,staged in writes.items():
            with staged.open('rb') as handle:
                expected[path] = hashlib.file_digest(handle,'sha256').digest()
        changed = []
        try:
            for destination,staged in writes.items():
                safe(destination).parent.mkdir(parents=True,exist_ok=True)
                os.replace(staged,destination)
                changed.append(destination)
            # Verify every committed write before removing any source data.
            for destination in changed:
                with safe(destination).open('rb') as handle:
                    if hashlib.file_digest(handle,'sha256').digest() != expected[destination]:
                        raise IOError(f'Incomplete absorption: {destination}')
            for path in deletes:
                safe(path).unlink()
        except Exception:
            for path in changed:
                if path not in backups and path.exists():
                    safe(path).unlink()
            for path,backup in backups.items():
                shutil.copy2(backup,safe(path))
            raise
    return destination_paths
