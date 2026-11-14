import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from inspection.group_combine import combine_groups,combined_name,record_path


class GroupCombineTests(unittest.TestCase):
    def setup_groups(self,root):
        images,labels,info = root/'dataset/images/train',root/'dataset/labels/train',root/'test/info'
        for folder in (images,labels,info):
            folder.mkdir(parents=True)
        groups = {}
        for group in ('A_23fps','B_23fps'):
            paths = []
            for n in range(2):
                image = images/f'{group}_{n:06}.jpg'
                image.write_bytes((group+str(n)).encode())
                (labels/f'{image.stem}.txt').write_text(f'2 .5 .5 .1 .1\n')
                (labels/f'{image.stem}.rotation.json').write_text(json.dumps({'angles':[12],'label_text':'x'}))
                for kind in ('OCR','conn','label'):
                    directory = root/'dataset/special_labels'/group/kind
                    directory.mkdir(parents=True,exist_ok=True)
                    (directory/f'{image.stem}.json').write_text(json.dumps(dict(group=group,image=str(image),
                        frame_number=n+1,regions=[dict(role='text',text=group)],rois=[dict(points=[[0,0],[1,0],[1,1]])])))
                paths.append(image)
            groups[group] = paths
            for kind in ('OCR','conn','label'):
                (info/f'{group}_{kind}_inspection_frame.txt').write_text(json.dumps(dict(start_frame=1,end_frame=2)))
            record_path(info,group).write_text(group)
            (info/f'{group}_test_condition.txt').write_text(group)
        return images,labels,info,groups

    def test_append_all_labels_copy_selected_conditions_without_union_and_keep_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            before = {p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
            choices = dict(inspection='B_23fps',record='A_23fps',test='B_23fps')
            target,paths,missing = combine_groups(root,images,labels,info,list(groups),groups,'C',choices)
            self.assertEqual(target,'C_23fps')
            self.assertEqual(len(paths),4)
            self.assertEqual(missing,[])
            self.assertEqual(paths[2].read_bytes(),b'B_23fps0')
            for kind in ('OCR','conn','label'):
                data = json.loads((root/'dataset/special_labels'/target/kind/f'{target}_000002.json').read_text())
                self.assertEqual(data['frame_number'],3)
                self.assertEqual(data['image'],str(paths[2]))
                self.assertEqual(data['regions'][0]['text'],'B_23fps')
                ranges = json.loads((info/f'{target}_{kind}_inspection_frame.txt').read_text())
                self.assertEqual(ranges,dict(start_frame=1,end_frame=2))
            self.assertEqual(record_path(info,target).read_text(),'A_23fps')
            self.assertEqual((info/f'{target}_test_condition.txt').read_text(),'B_23fps')
            for path,content in before.items():
                self.assertEqual(path.read_bytes(),content)

    def test_overwrite_removes_only_old_destination_labels_and_models(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            choices = dict(inspection='A_23fps',record='A_23fps',test='A_23fps')
            combine_groups(root,images,labels,info,list(groups),groups,'C',choices)
            weights = root/'weights'
            weights.mkdir()
            for name in ('C_23fps_algorith.pt','C_23fps_label_text_image.ocr.json','A_23fps_algorith.pt','C_other.pt'):
                (weights/name).write_text('model')
            stale = images/'C_23fps_000099.jpg'
            stale.write_bytes(b'stale')
            with self.assertRaises(FileExistsError):
                combine_groups(root,images,labels,info,list(groups),groups,'C',choices)
            combine_groups(root,images,labels,info,list(groups),groups,'C',choices,overwrite=True)
            self.assertFalse(stale.exists())
            self.assertFalse((weights/'C_23fps_algorith.pt').exists())
            self.assertFalse((weights/'C_23fps_label_text_image.ocr.json').exists())
            self.assertTrue((weights/'A_23fps_algorith.pt').exists())
            self.assertTrue((weights/'C_other.pt').exists())

    def test_validation_and_failed_commit_restore_previous_group(self):
        for name,sources in (('../C',['A_23fps','B_23fps']),('A',['A_23fps','B_23fps']),
                             ('A_23.0fps',['A_23fps','B_23fps']),('C',['A_23fps','B_10fps'])):
            with self.assertRaises(ValueError):
                combined_name(name,sources)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            choices = dict(inspection='A_23fps',record='A_23fps',test='A_23fps')
            combine_groups(root,images,labels,info,list(groups),groups,'C',choices)
            before = {p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
            import os
            real = os.replace
            calls = []
            def fail_second(*args):
                calls.append(args)
                if len(calls) == 2:
                    raise OSError('simulated commit failure')
                return real(*args)
            with patch('inspection.group_combine.os.replace',side_effect=fail_second),self.assertRaises(OSError):
                combine_groups(root,images,labels,info,list(groups),groups,'C',choices,overwrite=True)
            for path,value in before.items():
                self.assertEqual(path.read_bytes(),value)
