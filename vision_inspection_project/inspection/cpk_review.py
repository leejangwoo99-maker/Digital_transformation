"""Repeated confidence reproducibility; scores and tolerance use percentage points."""
import math
import statistics
from pathlib import Path


def grade(value):
    return '우수' if value >= 1.67 else '양호' if value >= 1.33 else '주의' if value >= 1 else '불합격'


def capability(scores, center=None):
    if not scores or any(not math.isfinite(v) or not 0 <= v <= 100 for v in scores):
        raise ValueError('Invalid confidence scores')
    median = statistics.median(scores) if center is None else center
    mean = statistics.mean(scores)
    low,high = median-.05,median+.05
    sigma = statistics.stdev(scores) if len(scores) > 1 else None
    if sigma is None:
        cpk,verdict = None,'계산 불가'
    elif sigma == 0:
        inside = low <= mean <= high
        cpk,verdict = (math.inf,'변동 없음·합격') if inside else (-math.inf,'변동 없음·불합격')
    else:
        cpk = min(high-mean,mean-low)/(3*sigma)
        verdict = grade(cpk)
    return dict(median=median,mean=mean,stddev=sigma,lsl=low,usl=high,cpk=cpk,verdict=verdict)


def geometry(detection):
    box = list(map(float,detection['xyxy']))
    if len(box) != 4 or any(not math.isfinite(v) for v in box) or box[2] <= box[0] or box[3] <= box[1]:
        raise ValueError('Invalid detection box')
    return ((box[0]+box[2])/2,(box[1]+box[3])/2),math.hypot(box[2]-box[0],box[3]-box[1])


def match_detections(tracks, detections, run):
    """Only a detection's closest same-class track may own it; no second-choice filling."""
    import copy
    snapshot = list(tracks)
    claims,new = {},[]
    for detection_index,detection in sorted(enumerate(detections),key=lambda pair:(pair[1]['class_name'],*geometry(pair[1])[0])):
        center,size = geometry(detection)
        score = round(float(detection['confidence'])*100,2)
        if not math.isfinite(score) or not 0 <= score <= 100:
            raise ValueError('Invalid detection confidence')
        candidates = []
        for index,track in enumerate(snapshot):
            if track['class_name'] != detection['class_name']:
                continue
            distance = math.dist(center,track['center'])
            candidates.append((distance,index,track))
        nearest = min(candidates,key=lambda v:(v[0],v[1])) if candidates else None
        if nearest and nearest[0] <= max(12.,min(size,nearest[2]['size'])*.75):
            distance,index,_ = nearest
            if index not in claims or distance < claims[index][0]:
                claims[index] = distance,score,detection_index
            # Conflicts are not reassigned to another, more distant label.
        else:
            new.append(dict(class_name=detection['class_name'],center=center,size=size,
                            box=copy.deepcopy(detection['xyxy']),scores={run:score},detections={run:detection_index}))
    for index,(_,score,detection_index) in claims.items():
        tracks[index]['scores'][run] = score
        tracks[index].setdefault('detections',{})[run] = detection_index
    tracks.extend(new)


def build_cpk(runs):
    if not runs:
        return dict(rows=[],runs=[],verdict='계산 불가')
    numbers = [data.get('run_number',index+1) for index,(_,data) in enumerate(runs)]
    frames = []
    expected = None
    for _,data in runs:
        mapping = {item['frame_number']:item for item in data['frames']}
        if len(mapping) != len(data['frames']) or not mapping:
            raise ValueError('Missing or duplicate frame records')
        if expected is not None and set(mapping) != expected:
            raise ValueError('All runs must contain the same inspected frame numbers.')
        expected = set(mapping)
        frames.append(mapping)
    rows = []
    for frame in sorted(expected):
        tracks = []
        for number,mapping in zip(numbers,frames):
            match_detections(tracks,mapping[frame]['detections'],number)
        if not tracks:
            tracks = [dict(class_name='검출 없음',center=(0,0),scores={})]
        for label,track in enumerate(tracks,1):
            scores = [track['scores'].get(number,0.) for number in numbers]
            rows.append(dict(file_name=Path(runs[0][1]['source']).name,frame_number=frame,
                label=f"{track['class_name']} #{label}",label_number=label,class_name=track['class_name'],scores=scores,
                center=track['center'],detections=track.get('detections',{}),**capability(scores)))
    if len(runs) < 2 or not rows:
        verdict = '계산 불가'
    else:
        worst = min(rows,key=lambda row:row['cpk'])
        verdict = worst['verdict']
    return dict(rows=rows,runs=numbers,verdict=verdict)


def save_label_points(parent):
    import json
    from inspection.confidence_runs import load_runs
    report = build_cpk(load_runs(parent))
    data = dict(runs=report['runs'],score_unit='percent',tolerance=.05,
                matching='same-class nearest-only 1:1; conflicts remain missing (0)',
                frames=[{key:row[key] for key in ('file_name','frame_number','label_number','class_name','center','scores')}
                        for row in report['rows']])
    path = Path(parent)/'label_points.json'
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    temporary.replace(path)


def table_data(report):
    headers = ['파일 이름','프레임 번호','라벨']+[f'{n}st 점수 (%)' for n in report['runs']]+[
        '중앙값 (%)','평균 (%)','표준편차','하한 LSL (%)','상한 USL (%)','Cpk','판정']
    rows = [[r['file_name'],r['frame_number'],r['label'],*r['scores'],r['median'],r['mean'],
             r['stddev'],r['lsl'],r['usl'],r['cpk'],r['verdict']] for r in report['rows']]
    return headers,rows


def display_value(value):
    if value is None:
        return ''
    if isinstance(value,float):
        return ('∞' if value > 0 else '-∞') if math.isinf(value) else f'{value:.6f}'
    return str(value)


def export_cpk(report,path):
    # Runtime export for the installed desktop app, independent of authoring tools.
    from openpyxl import Workbook
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    book = Workbook()
    sheet = book.active
    sheet.title = 'Cpk'
    headers,rows = table_data(report)
    sheet.append(headers)
    for row in rows:
        sheet.append([display_value(v) if isinstance(v,float) and not math.isfinite(v) else v for v in row])
    sheet.freeze_panes = 'D2'
    sheet.auto_filter.ref = sheet.dimensions
    for cell in sheet[1]:
        cell.font = Font(bold=True,color='FFFFFF')
        cell.fill = PatternFill('solid',fgColor='334155')
    for index in range(1,len(headers)+1):
        sheet.column_dimensions[get_column_letter(index)].width = 30 if index in (1,3,len(headers)) else 19
    for row in sheet.iter_rows(min_row=2,min_col=4,max_col=len(headers)-1):
        for cell in row:
            cell.number_format = '0.000000' if cell.column > 3+len(report['runs']) else '0.00'
    for row in sheet:
        for cell in row:
            if isinstance(cell.value,str):
                cell.data_type = 's'
    notes = book.create_sheet('Conditions')
    for row in [('판정',report['verdict']),('허용오차 (퍼센트포인트)',.05),
                ('규격','각 프레임·라벨의 전체 회차 중앙값 ±0.05'),
                ('계산','min(USL-평균, 평균-LSL)/(3*표본 표준편차); ddof=1'),
                ('미검출',0),('대응','동일 프레임·종류, 가장 가까운 위치에 1:1 대응; 차선 라벨 배정 없음'),
                ('거리 한계','max(12px, 두 박스 중 작은 대각선의 75%)'),
                ('용도','검출 점수의 반복 재현성 지표; 제품 불량률 판정과 별개')]:
        notes.append(row)
    notes.column_dimensions['A'].width = 30
    notes.column_dimensions['B'].width = 100
    book.save(path)


def show_cpk(parent,report):
    import tkinter as tk
    from tkinter import ttk
    window = tk.Toplevel(parent)
    window.title('Cpk view')
    window.geometry('1400x650')
    headers,rows = table_data(report)
    frame = ttk.Frame(window)
    frame.pack(fill='both',expand=True)
    ids = [str(i) for i in range(len(headers))]
    tree = ttk.Treeview(frame,columns=ids,show='headings')
    for key,title in zip(ids,headers):
        tree.heading(key,text=title)
        tree.column(key,width=150,stretch=False)
    vertical = ttk.Scrollbar(frame,orient='vertical',command=tree.yview)
    horizontal = ttk.Scrollbar(frame,orient='horizontal',command=tree.xview)
    tree.configure(yscrollcommand=vertical.set,xscrollcommand=horizontal.set)
    tree.grid(row=0,column=0,sticky='nsew')
    vertical.grid(row=0,column=1,sticky='ns')
    horizontal.grid(row=1,column=0,sticky='ew')
    frame.rowconfigure(0,weight=1)
    frame.columnconfigure(0,weight=1)
    for row in rows:
        tree.insert('',tk.END,values=[display_value(v) for v in row])
    return window
