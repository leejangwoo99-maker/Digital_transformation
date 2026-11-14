import tempfile
import unittest
import json
from pathlib import Path
import cv2
import numpy as np

from inspection.rotated_regions import clip_polygon,region_polygon,raw_region_polygon,appearance_obb_labels
from label_images import YoloBoxLabeler
from rotation_interaction import RotationInteraction


class RotationClippingTests(unittest.TestCase):
    def test_moved_box_crosses_edge_and_reloads_with_clipped_geometry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/"frame.png"
            cv2.imwrite(str(source),np.full((200,200,3),180,np.uint8))
            labeler = YoloBoxLabeler(root,root,{0:"scratch"},image_paths=[source])
            labeler._load_current_image()
            labeler.boxes = [(0,10,50,80,150)]
            regions = labeler._rotation_regions()
            labeler.editor.selected = [0]
            _,handles = labeler.editor.geometry(regions,labeler.image.shape,1)
            labeler.editor.begin(handles["move"],regions,labeler.image.shape,1)
            labeler.editor.finish(handles["move"]+[-30,0],regions,labeler.image.shape)
            labeler._apply_edit_regions(regions)
            self.assertEqual(labeler.boxes[0][1],-20)
            labeler._save_label()
            labeler._load_current_image()
            self.assertEqual(labeler.boxes[0][1],-20)
            visible = region_polygon(labeler._rotation_regions()[0],labeler.image.shape)
            self.assertEqual(visible[:,0].min(),0)
            self.assertEqual(visible[:,0].max(),50)

    def test_clip_keeps_only_inside_polygon_not_vertex_clamping(self):
        polygon = np.array([[-30,20],[40,-20],[80,60],[10,100]],np.float32)
        clipped = clip_polygon(polygon,(80,70))
        self.assertGreater(len(clipped),4)
        self.assertTrue((clipped >= 0).all())
        self.assertLessEqual(clipped[:,0].max(),69)
        self.assertLessEqual(clipped[:,1].max(),79)
        self.assertFalse(any(np.allclose(point,[0,0]) for point in clipped))

    def test_yolo_clipped_rotation_reloads_and_exports_valid_polygon(self):
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/"frame.png"
            cv2.imwrite(str(source),np.full((200,200,3),180,np.uint8))
            labeler = YoloBoxLabeler(root,root,{0:"scratch"},image_paths=[source])
            labeler._load_current_image()
            box = (0,5,50,175,160)
            labeler.boxes = [box]
            labeler.rotations = {box:45}
            visible = region_polygon(labeler._rotation_regions()[0],labeler.image.shape)
            self.assertGreater(len(visible),4)
            labeler._save_label()
            text = (root/"frame.txt").read_text()
            metadata = json.loads((root/"frame.rotation.json").read_text())
            exported = appearance_obb_labels(text,metadata["angles"],labeler.image.shape,metadata["geometry"])
            coords = np.asarray(list(map(float,exported.split()[1:]))).reshape(-1,2)
            np.testing.assert_allclose(coords,visible/200,atol=1e-7)
            output = root/"export.txt"
            output.write_text(exported)
            # Keep Ultralytics' process-wide OpenCV settings out of other tests.
            script = ("import json,sys; from ultralytics.data.utils import verify_image_label; "
                      "r=verify_image_label((sys.argv[1],sys.argv[2],'',False,1,0,0,False)); "
                      "print(json.dumps({'valid':r[0] is not None,'points':len(r[3][0]) if r[0] else 0,'message':r[-1]}))")
            checked = json.loads(subprocess.check_output([sys.executable,"-c",script,str(source),str(output)],text=True).strip().splitlines()[-1])
            self.assertTrue(checked["valid"],checked["message"])
            self.assertEqual(checked["points"],len(visible))
            labeler._load_current_image()
            self.assertEqual(labeler.boxes,[box])
            np.testing.assert_allclose(region_polygon(labeler._rotation_regions()[0],labeler.image.shape),visible)

    def test_polygon_roi_rotation_can_cross_border(self):
        regions = [{"role":"label_roi","points":[[.01,.1],[.15,.1],[.15,.9],[.01,.9]]}]
        rotation = RotationInteraction()
        rotation.selected = [0]
        _,center,handle = rotation.geometry(regions,(200,200),1)
        rotation.begin(handle,regions,(200,200),1)
        self.assertTrue(rotation.move(center+[40,0],regions,(200,200)))
        polygon = raw_region_polygon(regions[0],(200,200))
        self.assertTrue((polygon >= 0).all())
        self.assertLessEqual(polygon[:,0].max(),199)
