import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import cv2
import numpy as np

from camera.record_conditions import load_conditions,save_conditions,validate_conditions,recording_actions
from camera.switchbot import BleBot,ToggleTimeline,PRESS_COMMAND,WRITE_UUID,NOTIFY_UUID,scan_bots


class FakeBot:
    def __init__(self,address):
        self.calls = 0
        self.closed = False

    async def connect(self):
        pass

    async def press(self):
        self.calls += 1

    async def close(self):
        self.closed = True


class SwitchBotTests(unittest.TestCase):
    def timeline(self,intervals=(),duration=.16,bot_class=FakeBot):
        bot = bot_class('test')
        timeline = ToggleTimeline(dict(address='test',intervals=list(intervals)),duration,transport_factory=lambda address:bot)
        timeline.prepare()
        return timeline,bot

    def test_natural_end_go_stop_and_multiple_intervals(self):
        timeline,bot = self.timeline([dict(stop=.03,go=.06),dict(stop=.09,go=.12)])
        timeline.start(time.monotonic())
        timeline.worker.join(2)
        timeline.close()
        self.assertEqual(bot.calls,6)
        self.assertEqual([e['action'] for e in timeline.events],['GO','STOP','GO','STOP','GO','STOP'])
        self.assertEqual(timeline.report()['state'],'STOP')
        self.assertFalse(timeline.report()['physical_state_verified'])
        self.assertTrue(bot.closed)

    def test_manual_stop_while_stopped_does_not_toggle_again(self):
        timeline,bot = self.timeline([dict(stop=.02,go=.5)],duration=1)
        timeline.start(time.monotonic())
        deadline = time.monotonic()+2
        while bot.calls < 2 and time.monotonic() < deadline:
            time.sleep(.005)
        timeline.close()
        self.assertEqual(bot.calls,2)
        self.assertEqual(timeline.events[-1]['result'],'skipped_already_in_assumed_state')

    def test_manual_stop_while_moving_and_repeated_stop_once(self):
        timeline,bot = self.timeline(duration=10)
        timeline.start(time.monotonic())
        deadline = time.monotonic()+2
        while bot.calls < 1 and time.monotonic() < deadline:
            time.sleep(.005)
        timeline.finish()
        timeline.close()
        timeline.close()
        self.assertEqual(bot.calls,2)

    def test_uncertain_press_is_never_retried_and_pending_actions_cancelled(self):
        class Uncertain(FakeBot):
            async def press(self):
                self.calls += 1
                raise TimeoutError('no response')
        timeline,bot = self.timeline([dict(stop=.03,go=.06)],duration=.1,bot_class=Uncertain)
        timeline.start(time.monotonic())
        timeline.worker.join(2)
        timeline.close()
        self.assertEqual(bot.calls,1)
        self.assertEqual(timeline.report()['state'],'unknown')
        self.assertTrue(any(e.get('result')=='uncertain_no_retry' for e in timeline.events))

    def test_connection_failure_and_cancel_before_start_never_press(self):
        class Offline(FakeBot):
            async def connect(self):
                raise OSError('Bluetooth off')
        timeline,bot = self.timeline(bot_class=Offline)
        timeline.start(time.monotonic())
        timeline.close()
        self.assertEqual(bot.calls,0)
        self.assertTrue(any('connection failed' in e.get('message','') for e in timeline.events))
        timeline,bot = self.timeline()
        timeline.close()
        self.assertEqual(bot.calls,0)

    def test_profile_roundtrip_and_old_profiles_disabled(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'group_record_condition.txt'
            data = load_conditions(path)
            self.assertFalse(data['switchbot']['enabled'])
            data['switchbot'] = dict(enabled=True,address='AA:BB:CC:DD:EE:FF',intervals=[dict(stop=3,go=4),dict(stop=6,go=7)])
            save_conditions(data,path)
            self.assertEqual(load_conditions(path),data)
            self.assertEqual(recording_actions(data)['switchbot'],data['switchbot'])
            for intervals in ([dict(stop=3,go=3)],[dict(stop=0,go=1)],[dict(stop=3,go=11)],
                              [dict(stop=1,go=4),dict(stop=3,go=6)]):
                data['switchbot']['intervals'] = intervals
                with self.assertRaises(ValueError):
                    validate_conditions(data)

    def test_phone_capture_continues_when_bot_connection_fails(self):
        from camera.phone_usb import PhoneCapture
        class Offline(FakeBot):
            async def connect(self):
                raise OSError('Bluetooth off')
        class Camera:
            count = 0
            stopped = False
            def configure(self,values):
                return {}
            def request(self,endpoint,**values):
                if endpoint == 'status':
                    return dict(width=32,height=24,fps_max=30,recording=not self.stopped)
                if endpoint == 'stop':
                    self.stopped = True
                if endpoint == 'frame':
                    if self.stopped:
                        return b'',{}
                    time.sleep(.03)
                    self.count += 1
                    return cv2.imencode('.jpg',np.zeros((24,32,3),np.uint8))[1].tobytes(),{'X-Frame-Number':str(self.count)}
                return {}
        warnings = []
        actions = dict(initial=dict(zoom=1,focus=0,auto_focus=True),events=[],duration=.2,
                       switchbot=dict(enabled=True,address='test',intervals=[]))
        factory = lambda config,duration,warning:ToggleTimeline(config,duration,warning,transport_factory=Offline)
        with patch('camera.switchbot.ToggleTimeline',factory):
            capture = PhoneCapture(Camera(),20,None,actions,True,save_recorded_actions=False,switchbot_warning=warnings.append)
            count = 0
            try:
                while capture.read()[0]:
                    count += 1
            finally:
                capture.release()
        self.assertEqual(count,4)
        self.assertIsNone(capture.error)
        self.assertTrue(warnings)


class BLETests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_uses_discovered_device_not_address_only(self):
        import camera.switchbot as module
        device = SimpleNamespace(address='TEST')
        class Scanner:
            @staticmethod
            async def discover(timeout,return_adv):
                self.assertEqual(timeout,20.)
                self.assertTrue(return_adv)
                return {'device':(device,SimpleNamespace())}
        clients = []
        class Client:
            def __init__(self,target,timeout):
                clients.append((target,timeout))
            async def connect(self):
                pass
        with patch.dict(module._devices,{},clear=True),patch('bleak.BleakScanner',Scanner),patch('bleak.BleakClient',Client):
            await BleBot('TEST').connect()
            await BleBot('TEST').connect()
        self.assertEqual(clients,[(device,20.),(device,20.)])

    async def test_empty_timeout_has_visible_exception_name(self):
        from camera.switchbot import error_text
        self.assertIn('TimeoutError',error_text(TimeoutError()))

    async def test_press_payload_requires_ack_and_never_retries(self):
        class Client:
            writes = []
            services = SimpleNamespace(get_characteristic=lambda uuid:SimpleNamespace(properties=['write']))
            async def start_notify(self,uuid,callback):
                self.callback = callback
                self.notify_uuid = uuid
            async def write_gatt_char(self,uuid,payload,response):
                self.writes.append((uuid,payload,response))
                self.callback(None,b'\x01')
            async def stop_notify(self,uuid):
                pass
        bot = BleBot('test')
        bot.client = Client()
        await bot.press()
        self.assertEqual(bot.client.writes,[(WRITE_UUID,PRESS_COMMAND,True)])
        self.assertEqual(bot.client.notify_uuid,NOTIFY_UUID)

    async def test_negative_ack_is_an_error(self):
        class Client:
            services = SimpleNamespace(get_characteristic=lambda uuid:SimpleNamespace(properties=['write-without-response']))
            async def start_notify(self,uuid,callback):
                self.callback = callback
            async def write_gatt_char(self,*args,**kwargs):
                self.callback(None,b'\x06')
            async def stop_notify(self,uuid):
                pass
        bot = BleBot('test')
        bot.client = Client()
        with self.assertRaisesRegex(RuntimeError,'06'):
            await bot.press()

    async def test_scan_filters_bot_and_preserves_mode_and_address(self):
        class Scanner:
            @staticmethod
            async def discover(**kwargs):
                return {'a':(SimpleNamespace(address='AA',name='Bot'),SimpleNamespace(local_name=None,service_data={'service':b'H\x00\x64'})),
                        'b':(SimpleNamespace(address='BB',name='Bot'),SimpleNamespace(local_name=None,service_data={'service':b'H\x80\x64'})),
                        'c':(SimpleNamespace(address='CC',name='Sensor'),SimpleNamespace(local_name=None,service_data={'service':b'T\x00\x64'}))}
        with patch('bleak.BleakScanner',Scanner):
            result = await scan_bots()
        self.assertEqual([(item['address'],item['mode']) for item in result],[('AA','Press'),('BB','Switch')])
