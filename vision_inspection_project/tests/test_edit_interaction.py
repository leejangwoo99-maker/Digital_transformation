import copy
import unittest
import numpy as np
from edit_interaction import EditInteraction
from inspection.rotated_regions import region_polygon


class EditInteractionTests(unittest.TestCase):
    shape = (400,400)

    def setUp(self):
        self.regions = [{"role":"conn_boundary","box":[.2,.3,.3,.5],"angle":10},
            {"role":"pin","shape":"circle","box":[.5,.3,.6,.4]},
            {"role":"label_roi","points":[[.7,.3],[.8,.3],[.75,.5]]}]
        self.editor = EditInteraction()
        self.editor.begin((60,80),self.regions,self.shape,1)
        self.editor.finish((340,240),self.regions,self.shape)
        self.assertEqual(self.editor.selected,[0,1,2])

    def test_resize_all_changes_centers_and_preserves_circle_shape(self):
        original = copy.deepcopy(self.regions)
        _,handles = self.editor.geometry(self.regions,self.shape,1)
        start = handles["e"]
        self.editor.begin(start,self.regions,self.shape,1)
        before = self.editor.finish(start+[15,0],self.regions,self.shape)
        self.assertEqual(before,original)
        for old,new in zip(original,self.regions):
            a,b = region_polygon(old,self.shape),region_polygon(new,self.shape)
            self.assertGreater(((b.min(0)+b.max(0))/2)[0],((a.min(0)+a.max(0))/2)[0])
            self.assertGreater(np.ptp(b[:,0]),np.ptp(a[:,0]))
        circle = self.regions[1]["box"]
        self.assertAlmostEqual(circle[2]-circle[0],circle[3]-circle[1])

    def test_single_box_all_handles_anchor_opposite_side_including_rotation(self):
        opposite = {"nw":"se","n":"s","ne":"sw","e":"w","se":"nw","s":"n","sw":"ne","w":"e"}
        for angle in (0,30):
            for handle,other in opposite.items():
                with self.subTest(angle=angle,handle=handle):
                    regions = [{"box":[.4,.4,.6,.6],"angle":angle}]
                    editor = EditInteraction()
                    editor.selected = [0]
                    _,before = editor.geometry(regions,self.shape,1)
                    target = before[handle]+(before[handle]-before[other])*.2
                    editor.begin(before[handle],regions,self.shape,1)
                    editor.finish(target,regions,self.shape)
                    _,after = editor.geometry(regions,self.shape,1)
                    np.testing.assert_allclose(after[other],before[other],atol=1e-4)
                    np.testing.assert_allclose(after[handle],target,atol=1e-4)

    def test_move_cancel_restores_all_geometry(self):
        original = copy.deepcopy(self.regions)
        _,handles = self.editor.geometry(self.regions,self.shape,1)
        self.editor.begin(handles["move"],self.regions,self.shape,1)
        self.editor.move(handles["move"]+[15,10],self.regions,self.shape)
        self.assertNotEqual(self.regions,original)
        self.editor.cancel(self.regions)
        self.assertEqual(self.regions,original)
        self.assertEqual(self.editor.selected,[])

    def test_group_move_crosses_edge_and_clips_without_clamping_translation(self):
        original = copy.deepcopy(self.regions)
        _,handles = self.editor.geometry(self.regions,self.shape,1)
        self.editor.begin(handles["move"],self.regions,self.shape,1)
        self.editor.move(handles["move"]+[-100,0],self.regions,self.shape)
        self.assertAlmostEqual(self.regions[0]["box"][0],original[0]["box"][0]-.25)
        self.assertAlmostEqual(self.regions[1]["box"][0],original[1]["box"][0]-.25)
        self.assertTrue(self.regions[0]["clip_to_frame"])
        polygon = region_polygon(self.regions[0],self.shape)
        self.assertEqual(polygon[:,0].min(),0)
        self.assertTrue((polygon >= 0).all())
        self.editor.cancel(self.regions)
        self.assertEqual(self.regions,original)

    def test_roi_move_clips_points_on_border_and_keeps_last_visible_position(self):
        regions = [{"role":"label_roi","points":[[.05,.3],[.2,.3],[.2,.5],[.05,.5]]}]
        editor = EditInteraction()
        editor.selected = [0]
        _,handles = editor.geometry(regions,self.shape,1)
        editor.begin(handles["move"],regions,self.shape,1)
        editor.move(handles["move"]+[-40,0],regions,self.shape)
        self.assertAlmostEqual(min(point[0] for point in regions[0]["points"]),0)
        self.assertAlmostEqual(max(point[0] for point in regions[0]["points"]),.1)
        visible = copy.deepcopy(regions)
        editor.move(handles["move"]+[-1000,0],regions,self.shape)
        self.assertEqual(regions,visible)

    def test_rotate_cancel_and_bounds_rejection(self):
        original = copy.deepcopy(self.regions)
        _,handles = self.editor.geometry(self.regions,self.shape,1)
        self.editor.begin(handles["rotate"],self.regions,self.shape,1)
        self.editor.move(handles["move"]+[90,-60],self.regions,self.shape)
        self.editor.cancel(self.regions)
        self.assertEqual(self.regions,original)
        self.editor.selected = [0,1,2]
        _,handles = self.editor.geometry(self.regions,self.shape,1)
        self.editor.begin(handles["e"],self.regions,self.shape,1)
        self.assertIsNone(self.editor.finish(handles["e"]+[1000,0],self.regions,self.shape))
        self.assertEqual(self.regions,original)
