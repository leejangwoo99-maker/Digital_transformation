import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np

from inspection.flexible_fields import accepts,paired_matches
from inspection.special_checks import SpecialInspector


class FlexibleTests(unittest.TestCase):
    def inspector(self,kind,regions,values,code=''):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {kind:dict(regions=regions)}
        inspector.times = {kind:(0,None)}
        inspector.ocr = SimpleNamespace(candidates=lambda image:values)
        inspector.qr = SimpleNamespace(detectAndDecode=lambda image:(code,None,None))
        return inspector

    def test_fixed_text_is_exact_and_missing_rules_do_not_pass(self):
        self.assertTrue(accepts(dict(text='ABC'),'ABC'))
        self.assertFalse(accepts(dict(text='ABC'),'ABD'))
        self.assertFalse(accepts(dict(text='123',flexible=True),'456'))
        self.assertFalse(accepts(dict(text='123',flexible=True),''))

    def test_flexible_unpaired_format_rejects_unread_and_invalid(self):
        for kind,number in (('OCR',None),('label',11)):
            region = dict(role='text',number=number,box=[0,0,1,1],text='11',flexible=True,pattern=['num','num'])
            for value,ok in (('23',True),('2X',False),('',False)):
                with self.subTest(kind=kind,value=value):
                    inspector = self.inspector(kind,[region],[value] if value else [])
                    self.assertEqual(not inspector.check(np.zeros((80,80,3),np.uint8),1),ok)

    def test_flexible_requires_actual_matching_qr_and_format(self):
        regions = [dict(text='PREFIX'),dict(text='11',flexible=True,pattern=['num','num'])]
        self.assertTrue(paired_matches('PREFIX23',regions,[['PREFIX'],['23']]))
        self.assertFalse(paired_matches('PREFIX24',regions,[['PREFIX'],['23']]))
        self.assertFalse(paired_matches('PREFIX2X',regions,[['PREFIX'],['2X']]))
        self.assertFalse(paired_matches('PREFIX23',regions,[[],['23']]))
        self.assertFalse(paired_matches('',regions,[['PREFIX'],['23']]))

    def test_paired_flexible_without_format_uses_actual_code_not_old_text(self):
        text = dict(role='text',number=2,box=[0,0,1,1],text='OLD',flexible=True)
        qr = dict(role='qr',number=1,box=[0,0,1,1],flexible=True)
        for code,ok in (('NEW123',True),('WRONG',False),('',False)):
            inspector = self.inspector('label',[qr,text],['NEW123'],code)
            self.assertEqual(not inspector.check(np.zeros((80,80,3),np.uint8),1),ok)

    def test_flexible_does_not_reject_changed_print_via_old_image_template(self):
        text = dict(role='text',number=11,box=[0,0,1,1],text='11',flexible=True,
                    pattern=['num','num'],verdict='PASS')
        inspector = self.inspector('label',[text],['23'])
        image_model = SimpleNamespace(predict=Mock(return_value={'verdict':'FAIL'}))
        inspector.text_image_models = {'label':image_model}
        self.assertEqual(inspector.check(np.zeros((80,80,3),np.uint8),1),[])
        image_model.predict.assert_not_called()

    def test_legacy_check_rules_are_preserved(self):
        self.assertTrue(accepts(dict(check=True,pattern=['num','num']),'12'))
        self.assertFalse(accepts(dict(check=True,pattern=['num','num']),'AB'))
