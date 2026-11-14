import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from tkinter import ttk
from unittest.mock import patch

from video_inspection_gui import VideoInspectionApp,load_test_conditions


def descendants(parent):
    result = []
    for child in parent.winfo_children():
        result.append(child)
        result.extend(descendants(child))
    return result


class TrainingControlsUITests(unittest.TestCase):
    def setUp(self):
        self.root = tk.Tk()
        self.root.withdraw()
        self.app = VideoInspectionApp(self.root)

    def tearDown(self):
        self.root.destroy()

    def test_training_scope_popup_selection_and_combine_condition_choices(self):
        def select_scope():
            widgets = descendants(self.root)
            next(w for w in widgets if isinstance(w,tk.Radiobutton) and w.cget('text') == 'OCR').invoke()
            next(w for w in widgets if isinstance(w,tk.Button) and w.cget('text') == 'Train').invoke()
        self.root.after(20,select_scope)
        self.assertEqual(self.app.choose_training_scope(),'OCR')
        def select_conditions():
            widgets = descendants(self.root)
            entry = next(w for w in widgets if isinstance(w,tk.Entry))
            entry.delete(0,'end')
            entry.insert(0,'combined')
            combos = [w for w in widgets if isinstance(w,ttk.Combobox)]
            self.assertEqual(len(combos),3)
            combos[0].set('B_23fps')
            combos[2].set('B_23fps')
            next(w for w in widgets if isinstance(w,tk.Button) and w.cget('text') == 'Combine').invoke()
        self.root.after(20,select_conditions)
        result = self.app.choose_combine_options(['A_23fps','B_23fps'])
        self.assertEqual(result,('combined_23fps',dict(inspection='B_23fps',record='A_23fps',test='B_23fps')))

    def test_worker_spread_save_switch_and_file_live_settings(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(self.app,'check_parallel_resources',return_value=True):
            self.app.output_dir = Path(directory)
            self.app.active_group_name = 'A_23fps'
            self.assertFalse(hasattr(self.app,'worker_var'))
            self.assertFalse(hasattr(self.app,'spread_var'))
            self.app.test_fps_var.set('10')
            self.app.save_test_conditions()
            saved = load_test_conditions(self.app.output_dir,'A_23fps')
            self.assertNotIn('worker',saved)
            self.assertNotIn('spread',saved)
            self.assertEqual(saved['iou_percent'],45)
            self.assertEqual(saved['test_fps'],10)
            self.app.active_group_name = 'B_23fps'
            self.app.load_group_test_conditions()
            other = self.app.settings_for_frame_group('A_23fps',[])
            self.assertTrue(other['auto_runtime'])
            self.assertEqual(other['model']['iou'],.45)
            self.assertEqual(other['test_fps'],10)
            self.assertEqual(other['group_fps'],23)
            self.assertTrue(other['resource_limits'])
            self.app.active_group_name = 'A_23fps'
            self.app.load_group_test_conditions()
            settings = self.app._settings_from_ui()
            self.assertTrue(settings['auto_runtime'])

    def test_resource_warning_and_invalid_value(self):
        with patch('video_inspection_gui.messagebox.askokcancel',return_value=False) as warning:
            self.assertTrue(self.app.check_parallel_resources(force=True))
            warning.assert_not_called()
        with patch('video_inspection_gui.messagebox.showerror') as error:
            self.assertTrue(self.app.check_parallel_resources(force=True))
            error.assert_not_called()

    def test_absorption_group_picker_select_and_cancel(self):
        def accept():
            widgets = descendants(self.root)
            choices = next(w for w in widgets if isinstance(w,tk.Listbox))
            choices.selection_clear(0,'end')
            choices.selection_set(1)
            next(w for w in widgets if isinstance(w,tk.Button) and w.cget('text') == 'Select').invoke()
        self.root.after(20,accept)
        self.assertEqual(self.app.choose_absorption_group(self.root,['A_23fps_OCR','B_23fps_OCR'],
                                                        'Source selection'),'B_23fps_OCR')
        def cancel():
            next(w for w in descendants(self.root) if isinstance(w,tk.Button) and w.cget('text') == 'Cancel').invoke()
        self.root.after(20,cancel)
        self.assertIsNone(self.app.choose_absorption_group(self.root,['A_23fps_OCR'],'Cancel selection'))

    def test_training_defaults_to_absorbed_type(self):
        self.app.preferred_training_scope = 'label'
        def accept():
            radios = [w for w in descendants(self.root) if isinstance(w,tk.Radiobutton)]
            variable = radios[0].cget('variable')
            self.assertEqual(self.root.getvar(variable),'label')
            next(w for w in descendants(self.root) if isinstance(w,tk.Button) and w.cget('text') == 'Train').invoke()
        self.root.after(20,accept)
        self.assertEqual(self.app.choose_training_scope(),'label')
