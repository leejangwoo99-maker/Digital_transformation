"""Presence checks against explicitly labeled normal reference crops."""
import json
from pathlib import Path

import cv2
import numpy as np

from inspection.rotated_regions import raw_region_polygon
from inspection.shape_registration import ShapeRegistration


def load_essential_shapes(paths, label_dir, classes):
    ids = {str(key) for key,name in classes.items() if name.lower() == 'essential'}
    schedule = {}
    for number,image in enumerate(paths,1):
        label = label_dir/f'{image.stem}.txt'
        if not label.is_file():
            continue
        text = label.read_text(encoding='utf-8')
        rows = [line.split() for line in text.splitlines() if line.strip()]
        if not any(row[0] in ids for row in rows):
            continue
        sidecar = label.with_suffix('.rotation.json')
        data = json.loads(sidecar.read_text(encoding='utf-8')) if sidecar.is_file() else {}
        geometry = data.get('geometry',[]) if data.get('label_text') == text else []
        groups = data.get('essential_groups',[]) if data.get('label_text') == text else []
        boxes = []
        for index,row in enumerate(rows):
            if row[0] in ids:
                if len(row) != 5:
                    raise ValueError(f'Essential: invalid box on frame {number}, row {index+1}.')
                if len(geometry) == len(rows):
                    region = geometry[index]
                else:
                    x,y,w,h = map(float,row[1:])
                    region = dict(box=[x-w/2,y-h/2,x+w/2,y+h/2])
                group = groups[index] if index < len(groups) else None
                if type(group) is not int or group < 1:
                    group = None
                boxes.append(dict(region,id=f'F{number}_B{len(boxes)+1}',group=group))
        schedule[number] = dict(image=str(image),regions=boxes)
    return schedule


def _crop(image, polygon, size, padding=0):
    height,width = image.shape[:2]
    if ((polygon < 0).any() or (polygon[:,0] >= width).any()
            or (polygon[:,1] >= height).any()):
        return None
    w,h = size
    target = np.float32([[0,0],[w-1,0],[w-1,h-1],[0,h-1]])+padding
    matrix = cv2.getPerspectiveTransform(np.float32(polygon),target)
    return cv2.warpPerspective(image,matrix,(w+2*padding,h+2*padding))


def _edges(image):
    gray = cv2.cvtColor(image,cv2.COLOR_BGR2GRAY)
    return cv2.Canny(cv2.GaussianBlur(gray,(3,3),0),40,100)


def shape_score(reference, frame, region, matrix):
    polygon = raw_region_polygon(region,reference.shape).astype(np.float32)
    w = int(round(np.linalg.norm(polygon[1]-polygon[0])))
    h = int(round(np.linalg.norm(polygon[3]-polygon[0])))
    if min(w,h) < 8:
        return None
    scale = min(1.,256/max(w,h))
    size = max(8,round(w*scale)),max(8,round(h*scale))
    target = (cv2.perspectiveTransform(polygon[None],matrix)[0] if matrix.shape == (3,3)
              else cv2.transform(polygon[None],matrix)[0])
    normal = _crop(reference,polygon,size)
    current = _crop(frame,target,size,padding=3)
    if normal is None or current is None:
        return None
    template = _edges(normal)
    if np.count_nonzero(template) < 8:
        return None
    return float(cv2.matchTemplate(_edges(current),template,cv2.TM_CCOEFF_NORMED).max())


class EssentialShapes:
    def __init__(self, originals, sampling, threshold=.60):
        self.schedule = sampling.schedule({int(n):value for n,value in originals.items()})
        self.items = {r['id']:dict(result='unavailable',attempts=0,score=None)
                      for value in originals.values() for r in value['regions']}
        self.groups = {r['group']:dict(result='unavailable') for value in originals.values()
                       for r in value['regions'] if r.get('group') is not None}
        self.grouped_boxes = {r['id'] for value in originals.values() for r in value['regions']
                              if r.get('group') is not None}
        self.threshold = float(threshold)
        if not 0 < self.threshold <= 1:
            raise ValueError('Essential score threshold must be greater than 0 and at most 1.')
        self.registration = ShapeRegistration(tracking=False)

    def check(self, frame, number):
        record = self.schedule.get(number)
        if record is None:
            return
        regions = [r for r in record['regions'] if self.items[r['id']]['result'] != 'OK'
                   and self.groups.get(r.get('group'),{}).get('result') != 'OK']
        if not regions or not Path(record['image']).is_file():
            return
        reference = cv2.imread(record['image'])
        if reference is None:
            return
        fit = self.registration.fit(frame,record['image'])
        if fit is None:
            return
        for region in regions:
            score = shape_score(reference,frame,region,fit[0])
            if score is None:
                continue
            item = self.items[region['id']]
            item['attempts'] += 1
            item['score'] = max(score,item['score']) if item['score'] is not None else score
            item['result'] = 'OK' if score >= self.threshold else 'missing'
            if item['result'] == 'OK':
                item['test_frame'] = number
        for group in {r['group'] for r in regions if r.get('group') is not None}:
            results = [self.items[r['id']]['result'] for r in record['regions'] if r.get('group') == group]
            if all(result == 'OK' for result in results):
                self.groups[group].update(result='OK',test_frame=number)
            elif 'missing' in results:
                self.groups[group]['result'] = 'missing'

    def failures(self):
        return ([f'Essential_{number}_{item["result"]}' for number,item in sorted(self.items.items())
                 if number not in self.grouped_boxes and item['result'] != 'OK']
                + [f'Essential_G{number}_{item["result"]}' for number,item in sorted(self.groups.items())
                   if item['result'] != 'OK'])

    @property
    def required_count(self):
        return len(self.items)-len(self.grouped_boxes)+len(self.groups)

    def report(self):
        return dict(threshold=self.threshold,items=self.items,groups=self.groups,
                    scheduled_test_frames=sorted(self.schedule),result='NG' if self.failures() else 'OK')
