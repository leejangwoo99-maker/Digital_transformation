import copy
import unittest
import numpy as np
from translation_interaction import TranslationInteraction
from inspection.rotated_regions import region_polygon


class TranslationTests(unittest.TestCase):
    def test_roi_rectangle_overlap_includes_edges_and_crossings_not_empty_bounds(self):
        from inspection.rotated_regions import polygon_overlaps_rectangle
        square = [[.2,.2],[.5,.2],[.5,.5],[.2,.5]]
        self.assertTrue(polygon_overlaps_rectangle(square,[.49,.49,.6,.6]))
        self.assertTrue(polygon_overlaps_rectangle(square,[.5,.3,.6,.4]))
        self.assertTrue(polygon_overlaps_rectangle(square,[.1,.3,.6,.4]))
        self.assertFalse(polygon_overlaps_rectangle(square,[.51,.51,.6,.6]))
        triangle = [[.1,.1],[.8,.1],[.1,.8]]
        self.assertFalse(polygon_overlaps_rectangle(triangle,[.6,.6,.7,.7]))

    def test_yolo_center_drag_saves_position_and_rotation(self):
        import cv2
        import tempfile
        from pathlib import Path
        from label_images import YoloBoxLabeler
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/"frame.png"
            cv2.imwrite(str(source),np.zeros((200,400,3),np.uint8))
            labeler = YoloBoxLabeler(root,root,{0:"defect"},image_paths=[source])
            labeler._load_current_image()
            labeler.boxes = [(0,80,60,120,100),(0,240,60,280,100)]
            labeler.rotations = {labeler.boxes[0]:5,labeler.boxes[1]:0}
            def mouse(event,point,flags=0):
                x,y = np.asarray(point)*labeler.scale+[labeler.offset_x,labeler.offset_y]
                labeler._on_mouse(event,round(x),round(y),flags,None)
            for point,flags in (((85,70),0),((245,70),cv2.EVENT_FLAG_SHIFTKEY)):
                mouse(cv2.EVENT_LBUTTONDOWN,point,flags)
                mouse(cv2.EVENT_LBUTTONUP,point,flags)
            self.assertEqual(labeler.editor.selected,[0,1])
            _,handles = labeler.editor.geometry(labeler._rotation_regions(),labeler.image.shape,labeler.scale)
            center = handles["move"]
            mouse(cv2.EVENT_LBUTTONDOWN,center)
            mouse(cv2.EVENT_LBUTTONUP,center+[20,10])
            self.assertEqual(labeler.boxes,[(0,100,70,140,110),(0,260,70,300,110)])
            self.assertEqual(labeler.rotations[labeler.boxes[0]],5)
            labeler._load_current_image()
            self.assertEqual(labeler.boxes,[(0,100,70,140,110),(0,260,70,300,110)])

    def test_multi_selection_moves_as_group_and_clamps_rotated_edges(self):
        regions = [{"box":[.2,.3,.3,.5],"angle":20,"text":"AB"},
                   {"box":[.6,.3,.7,.5],"angle":0,"role":"mark","mark_id":"A"}]
        original = copy.deepcopy(regions)
        interaction = TranslationInteraction()
        interaction.select((100,80),regions,(200,400))
        interaction.select((260,80),regions,(200,400),True)
        self.assertEqual(interaction.selected,[0,1])
        _,center = interaction.geometry(regions,(200,400))
        self.assertTrue(interaction.begin(center,regions,(200,400),1))
        interaction.move(center+[20,10],regions,(200,400))
        for before,after in zip(original,regions):
            np.testing.assert_allclose(np.asarray(after["box"])-before["box"],[.05,.05,.05,.05])
            self.assertEqual(before.get("angle"),after.get("angle"))
        interaction.move(center+[1000,1000],regions,(200,400))
        for region in regions:
            polygon = region_polygon(region,(200,400))
            self.assertTrue((polygon >= -1e-5).all())
            self.assertLessEqual(polygon[:,0].max(),399.001)
            self.assertLessEqual(polygon[:,1].max(),199.001)
        self.assertEqual(interaction.finish(),original)

