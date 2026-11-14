import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from inspection.group_combine import absorb_group, appearance_group_absorbed
import test_group_combine as fixtures


class GroupAbsorptionTests(unittest.TestCase):
    def setup_groups(self, root):
        return fixtures.GroupCombineTests().setup_groups(root)

    def test_absorbs_only_selected_special_kind_and_preserves_all_conditions(self):
        for kind in ('OCR','label','conn'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                images,labels,info,groups = self.setup_groups(root)
                before = {p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
                source_dir = root/'dataset/special_labels/A_23fps'/kind
                paths = absorb_group(root,images,labels,info,'A_23fps','B_23fps',kind)
                self.assertEqual(len(paths),4)
                self.assertEqual(paths[2].name,'B_23fps_000002.jpg')
                self.assertEqual(paths[2].read_bytes(),groups['A_23fps'][0].read_bytes())
                self.assertEqual(list(source_dir.glob('*.json')),[])
                record = root/f'dataset/special_labels/B_23fps/{kind}/B_23fps_000002.json'
                data = json.loads(record.read_text())
                self.assertEqual(data['frame_number'],3)
                self.assertEqual(data['group'],'B_23fps')
                self.assertEqual(data['image'],str(paths[2]))
                self.assertFalse((labels/'B_23fps_000002.txt').exists())
                for other in ('OCR','label','conn'):
                    if other != kind:
                        self.assertFalse((root/f'dataset/special_labels/B_23fps/{other}/B_23fps_000002.json').exists())
                for path,value in before.items():
                    if path.parent != source_dir:
                        self.assertEqual(path.read_bytes(),value)

    def test_appearance_absorption_removes_only_appearance_labels_and_keeps_shared_images(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            before = {p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
            paths = absorb_group(root,images,labels,info,'A_23fps','B_23fps','open_labeler')
            self.assertEqual(len(paths),4)
            self.assertTrue(appearance_group_absorbed(info,'A_23fps',groups['A_23fps'],labels))
            self.assertFalse(appearance_group_absorbed(info,'B_23fps',paths,labels))
            for image in groups['A_23fps']:
                self.assertTrue(image.exists())
                self.assertFalse((labels/f'{image.stem}.txt').exists())
                self.assertFalse((labels/f'{image.stem}.rotation.json').exists())
            for path,value in before.items():
                if path.parent != labels or not path.name.startswith('A_23fps_'):
                    self.assertEqual(path.read_bytes(),value)
            self.assertEqual((labels/'B_23fps_000002.txt').read_bytes(),before[labels/'A_23fps_000000.txt'])
            self.assertEqual((labels/'B_23fps_000002.rotation.json').read_bytes(),
                             before[labels/'A_23fps_000000.rotation.json'])
            self.assertFalse((root/'dataset/special_labels/B_23fps/OCR/B_23fps_000002.json').exists())
            with self.assertRaises(ValueError):
                absorb_group(root,images,labels,info,'A_23fps','B_23fps','open_labeler')

    def test_failed_commit_rolls_back_without_losing_source_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            before = {p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
            real_replace = os.replace
            calls = []
            def fail_second(*args):
                calls.append(args)
                if len(calls) == 2:
                    raise OSError('simulated failure')
                return real_replace(*args)
            with patch('inspection.group_combine.os.replace',side_effect=fail_second),self.assertRaises(OSError):
                absorb_group(root,images,labels,info,'A_23fps','B_23fps','label')
            self.assertEqual({p:p.read_bytes() for p in root.rglob('*') if p.is_file()},before)

    def test_failed_source_removal_rolls_back_verified_destination_and_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            before = {p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
            original = Path.unlink
            count = [0]
            def unlink(path,*args,**kwargs):
                if path.parent == root/'dataset/special_labels/A_23fps/label':
                    count[0] += 1
                    if count[0] == 2:
                        raise OSError('simulated removal failure')
                return original(path,*args,**kwargs)
            with patch.object(Path,'unlink',unlink),self.assertRaises(OSError):
                absorb_group(root,images,labels,info,'A_23fps','B_23fps','label')
            self.assertEqual({p:p.read_bytes() for p in root.rglob('*') if p.is_file()},before)

    def test_appearance_only_source_images_removed_and_empty_labels_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            for path in (root/'dataset/special_labels/A_23fps').rglob('*.json'):
                path.unlink()
            (labels/'A_23fps_000001.txt').write_text('')
            paths = absorb_group(root,images,labels,info,'A_23fps','B_23fps','open_labeler')
            self.assertTrue(all(not p.exists() for p in groups['A_23fps']))
            self.assertEqual((labels/f'{paths[-1].stem}.txt').read_text(),'')

    def test_rejects_same_group_and_invalid_kind(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            for source,target,kind in (('A_23fps','A_23fps','OCR'),
                                       ('A_23fps','B_23fps','ALL'),
                                       ('../A_23fps','B_23fps','OCR'),
                                       ('A_23fps','B_10fps','OCR')):
                with self.subTest(source=source,target=target,kind=kind),self.assertRaises(ValueError):
                    absorb_group(root,images,labels,info,source,target,kind)

    def test_orphan_destination_label_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels,info,groups = self.setup_groups(root)
            orphan = labels/'B_23fps_000002.txt'
            orphan.write_text('existing label')
            before = {p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
            with self.assertRaises(FileExistsError):
                absorb_group(root,images,labels,info,'A_23fps','B_23fps','open_labeler')
            self.assertEqual({p:p.read_bytes() for p in root.rglob('*') if p.is_file()},before)
