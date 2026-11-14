"""Run saved-video validation with Live-style point saving disabled; no hardware."""
import argparse
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    import yaml
    from main import inspect_video
    parser = argparse.ArgumentParser()
    parser.add_argument('video',type=Path)
    parser.add_argument('--group',default='Proto_23fps')
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--automatic',action='store_true')
    parser.add_argument('--repeat',type=int,default=1)
    args = parser.parse_args()
    settings = yaml.safe_load((ROOT/'config/settings.yaml').read_text(encoding='utf-8'))
    settings.update(root_dir=str(ROOT),special_group=args.group,
        special_info_dir=r'C:\vision_inspection\test\info',group_fps=23,test_fps=10,
        model=dict(weights=str(ROOT/'weights'/f'{args.group}_algorith.pt'),confidence=.3,
                   iou=.45,image_size=640,original_resolution=False,render_predictions=False),
        judge=dict(settings['judge'],min_confidence=.3),cpu_threads=4,mark_workers=3,mark_spread=2,
        resource_limits=True,special_labeled_frames=True,special_frame_ranges=False,
        conn_inspection_frame=True,special_photo_inspection=True,photo_frame_selection=True,
        save_test_details=True,record_confidence_points=False,require_regular_model=True,
        match_original_format=False,alignment_enabled=False)
    settings['special_frame_paths'] = [str(p) for p in sorted((ROOT/'dataset/images/train').glob(f'{args.group}_*.jpg'))]
    settings['auto_runtime'] = args.automatic
    with tempfile.TemporaryDirectory() as directory:
        import copy
        summaries = []
        for repeat in range(max(1,args.repeat)):
            output = Path(directory)/str(repeat+1)
            output.mkdir()
            inspect_video(args.video,copy.deepcopy(settings),output/'result.mp4',show_window=False,
                          display_width=960,display_height=540,log_dir=output/'logs',file_test_log=True)
            report = json.loads(next((output/'logs').glob('*.pipeline.json')).read_text(encoding='utf-8'))
            summary = dict(run=repeat+1,seconds=report['performance']['total_wall_seconds'],
                preparation=report['performance']['stages']['model_initialization']['total_seconds'],
                results=report.get('photo_kind_results'),automatic=report['performance'].get('automatic_runtime'))
            summaries.append(summary)
            print(json.dumps(summary),flush=True)
        report['benchmark_runs'] = summaries
        report['benchmark_note'] = 'Saved-video validation; temporary media removed; not an actual phone Live test.'
        args.report.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        print(args.report,flush=True)
        print(json.dumps(dict(seconds=report['performance']['total_wall_seconds'],
            results=report.get('photo_kind_results'),backends=report['performance'].get('ocr_backends'),
            tracking=report['performance'].get('reference_tracking'))),flush=True)


if __name__ == '__main__':
    main()
