"""Serial naming is resolved from actual reads, never annotation transcripts."""
import json
import re
from datetime import datetime
from pathlib import Path


def safe_serial(value):
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_',value).strip(' .')
    if not value:
        return 'no_reading'
    if value.split('.')[0].upper() in {'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))}:
        value = '_'+value
    return value[:100]


def recognized_serial(inspectors):
    required,reads = set(),{}
    for inspector in inspectors:
        records = [getattr(inspector,'references',{}).get('label',{})]
        records += list(getattr(inspector,'annotation_frames',{}).get('label',{}).values())
        for record in records:
            required.update(int(r['serial_order']) for r in record.get('regions',[]) if r.get('serial_order') and r['role']=='qr')
        for order,value in getattr(inspector,'serial_reads',{}).items():
            if isinstance(value,str) and value.strip():
                reads.setdefault(int(order),value)
    if not required or not required.issubset(reads):
        return 'no_reading'
    return safe_serial(''.join(reads[n] for n in sorted(required)))


def claim_result_folder(base, numbered=True, global_sequence=False):
    base.parent.mkdir(parents=True,exist_ok=True)
    if numbered:
        pattern = re.compile(r'(\d+)st_'+(r'.+' if global_sequence else re.escape(base.name))+r'$')
        numbers = [int(match[1]) for path in base.parent.iterdir()
                   if (match := pattern.fullmatch(path.name))]
        number = max(numbers+[1 if base.exists() else 0])+1
    else:
        number = 0
    while True:
        candidate = base if number == 0 else base.with_name(f'{number}st_{base.name}')
        try:
            candidate.mkdir()
            return candidate,number
        except FileExistsError:
            if numbered:
                numbers = [int(match[1]) for path in base.parent.iterdir()
                           if (match := pattern.fullmatch(path.name))]
                number = max([number]+numbers)+1
            else:
                number += 1


def plan_results(settings,stage,output,photos,serial,fps,passed,recorder,status='completed'):
    from inspection.performance import result_directory
    from inspection.confidence_runs import reserve_run
    live = bool(settings.get('live_photo_inspection'))
    stamp = settings.get('live_log_timestamp') if live else settings.get('inspection_timestamp')
    stamp = stamp or datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    group = settings['special_group']
    threshold = float(settings.get('judge',{}).get('min_confidence',.5))*100
    parent = result_directory(stage.parent,f'{serial}_{stamp}' if live else serial,group,fps,threshold,
                              fraction=bool(settings.get('confidence_history') and not live))
    if not live:
        source = settings.get('test_source_name') or (Path(recorder.data['source']).stem if recorder is not None else serial)
        container = stage.parent/safe_serial(source)
        parent = container/parent.name
    parent,collision = claim_result_folder(parent,numbered=not live,global_sequence=not live)
    number = collision if not live else collision+1
    reservation,run_number = reserve_run(parent,number=number if not live else None)
    if not live:
        settings['live_inspection_count'] = number
    value = f'{threshold/100:.2f}' if not live and settings.get('confidence_history') else f'{threshold:g}'
    base = f'{serial}_{stamp}_live_test' if live else f'{number}st_{serial}_{group}_threshold_{value}_result'
    verdict = 'p' if passed else 'f'
    mapping = {}
    final_output = parent/f'{base}_{verdict}_[{fps:g}fps]{"" if live else "_"+stamp}{output.suffix}'
    if output.suffix.lower() in ('.mp4','.avi'):
        mapping[Path(output)] = final_output
    for index,photo in enumerate(photos):
        old = Path(photo['output'])
        part = photo.get('inspection_task',photo['kind'])
        target = parent/f'{base}_{part}_{"p" if photo["result"]=="OK" else "f"}_[{fps:g}fps]{"" if live else "_"+stamp}.png'
        mapping[old] = target
        photo['output'] = str(target)
        if old == output:
            final_output = target
    log_name = (f'{serial}_{group}_live_test_threshold_{threshold:g}_result_{verdict}_[{fps:g}fps]_{stamp}' if live
                else f'{base}_{verdict}_[{fps:g}fps]_{stamp}')
    if live and collision:
        log_name = log_name[:-len(stamp)]+f'{collision}st_'+stamp

    def finish():
        for old,new in mapping.items():
            if new.exists():
                raise FileExistsError(f'Result already exists: {new}')
            old.replace(new)
        if recorder is not None:
            reservation.rmdir()
            recorder.directory.replace(reservation)
            recorder.directory = reservation
            recorder.run = reservation,run_number
            recorder.data.update(run_number=run_number,parent_name=parent.name,serial_number=serial,
                                 original_source=recorder.data['source'],source=serial)
            recorder.finish(status)
        else:
            reservation.rmdir()
        stage.rmdir()
    return final_output,log_name,finish


def finish_media_audit(settings,error):
    from time import monotonic
    path = settings.get('_live_result_log')
    if not path:
        return
    path = Path(path)
    audit = path.with_suffix('.pipeline.json')
    data = json.loads(audit.read_text(encoding='utf-8'))
    data.update(media_save_status='failed' if error else 'completed',media_save_error=error)
    started = settings.get('_live_started_at',settings.get('_file_started_at'))
    if started is not None:
        data['media_completed_seconds'] = monotonic()-started
    timing = settings.get('_live_performance')
    if timing is not None:
        data['performance'].update(timing.report())
    temporary = audit.with_suffix('.tmp')
    temporary.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    temporary.replace(audit)
    if error:
        path.with_suffix('.media_error.txt').write_text(error,encoding='utf-8')
