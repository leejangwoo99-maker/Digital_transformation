"""Read-only positioning overlays from shared original OCR/Label frame numbers."""
from pathlib import Path

from PIL import Image

ROLES = {'OCR':{'text','mark'},'label':{'text','qr'}}
COLORS = {'OCR':'#00d9ed','label':'#ffdf32'}


def common_frames(settings):
    from inspection.special_checks import labeled_frame_schedule
    if not settings.get('root_dir') or not settings.get('special_group'):
        return {}
    schedules = {kind:labeled_frame_schedule(Path(settings['root_dir']),settings['special_group'],kind,
                  settings.get('special_frame_paths',[])) for kind in ROLES}
    return {number:{kind:schedules[kind][number] for kind in ROLES}
            for number in sorted(schedules['OCR'].keys() & schedules['label'].keys())
            if all(any(r.get('role') in ROLES[kind] for r in schedules[kind][number]['regions']) for kind in ROLES)}


def guide_layers(records):
    layers = []
    for kind,record in records.items():
        with Image.open(record['image']) as image:
            size = image.size
        layers.append(dict(kind=kind,size=size,regions=[dict(r) for r in record['regions'] if r.get('role') in ROLES[kind]]))
    return layers


def projected_guides(layers, preview_size):
    from inspection.rotated_regions import region_polygon
    width,height = preview_size
    result = []
    for layer in layers:
        original_width,original_height = layer['size']
        scale = min(width/original_width,height/original_height)
        offset = [(width-original_width*scale)/2,(height-original_height*scale)/2]
        for index,region in enumerate(layer['regions'],1):
            polygon = region_polygon(region,(original_height,original_width))*scale+offset
            if len(polygon) < 3:
                continue
            result.append(dict(points=polygon,color=COLORS[layer['kind']],
                               title=f"{layer['kind']} {region['number'] if region.get('number') is not None else index}"))
    return result
