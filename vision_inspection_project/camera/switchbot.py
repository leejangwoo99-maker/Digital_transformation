"""SwitchBot Bot press-only BLE control. Never retry a toggle automatically."""
import asyncio
import threading
import time

WRITE_UUID = 'cba20002-224d-11e6-9fb8-0002a5d5c51b'
NOTIFY_UUID = 'cba20003-224d-11e6-9fb8-0002a5d5c51b'
PRESS_COMMAND = bytes.fromhex('570100')
_devices = {}
_devices_lock = threading.Lock()


def error_text(error):
    return f'{type(error).__name__}: {str(error) or "operation timed out or returned no details"}'


async def scan_bots():
    from bleak import BleakScanner
    devices = await BleakScanner.discover(timeout=20.,return_adv=True)
    found = []
    for device,advertisement in devices.values():
        data = next((data for data in advertisement.service_data.values() if data and data[0]&0x7f == 0x48),None)
        if data is not None:
            with _devices_lock:
                _devices[device.address.upper()] = device
            found.append(dict(address=device.address,name=advertisement.local_name or device.name or 'SwitchBot Bot',
                              mode='Switch' if len(data)>1 and data[1]&0x80 else 'Press'))
    return found


class BleBot:
    def __init__(self,address):
        self.address = address
        self.client = None

    async def connect(self):
        from bleak import BleakClient,BleakScanner
        with _devices_lock:
            device = _devices.get(self.address.upper())
        if device is None:
            devices = await BleakScanner.discover(timeout=20.,return_adv=True)
            device = next((item for item,_ in devices.values() if item.address.upper() == self.address.upper()),None)
            if device is None:
                raise RuntimeError(f'Bot {self.address} not found during full 20-second BLE scan. Close the phone SwitchBot app and check Bluetooth/range.')
        self.client = BleakClient(device,timeout=20.)
        try:
            await asyncio.wait_for(self.client.connect(),22.)
        except Exception as exc:
            with _devices_lock:
                _devices.pop(self.address.upper(),None)
            raise RuntimeError(f'BLE connection to {self.address} failed: {error_text(exc)}') from exc
        with _devices_lock:
            _devices[self.address.upper()] = device

    async def press(self):
        loop = asyncio.get_running_loop()
        characteristic = self.client.services.get_characteristic(WRITE_UUID)
        if characteristic is None:
            raise RuntimeError('SwitchBot write characteristic not found')
        properties = characteristic.properties
        if not any(value in properties for value in ('write','write-without-response')):
            raise RuntimeError('SwitchBot characteristic is not writable')
        reply = loop.create_future()
        def notified(sender,data):
            def deliver():
                if not reply.done():
                    reply.set_result(bytes(data))
            loop.call_soon_threadsafe(deliver)
        await self.client.start_notify(NOTIFY_UUID,notified)
        try:
            await self.client.write_gatt_char(WRITE_UUID,PRESS_COMMAND,response='write' in properties)
            response = await asyncio.wait_for(reply,3.)
            if not response or response[0] != 1:
                raise RuntimeError(f'Bot did not confirm execution: {response.hex() or "empty response"}')
        finally:
            try:
                await self.client.stop_notify(NOTIFY_UUID)
            except Exception:
                pass

    async def close(self):
        if self.client is not None:
            await self.client.disconnect()


class ToggleTimeline:
    def __init__(self,config,duration,on_warning=None,transport_factory=BleBot):
        self.config,self.duration = config,float(duration)
        self.on_warning = on_warning
        self.transport_factory = transport_factory
        self.ready = threading.Event()
        self.started = threading.Event()
        self.cancelled = threading.Event()
        self.started_at = None
        self.connection_seconds = None
        self.state = False  # User confirms the turntable is stopped before each run.
        self.events = []
        self.worker = threading.Thread(target=lambda:asyncio.run(self.run()),daemon=True)

    def warn(self,message):
        self.events.append(dict(action='WARNING',message=message))
        if self.on_warning:
            try:
                self.on_warning(message)
            except Exception:
                pass

    def prepare(self):
        self.worker.start()
        if not self.ready.wait(48):
            self.cancelled.set()
            self.warn('SwitchBot connection timed out; recording continues without confirmed Bot control.')

    def start(self,started_at):
        self.started_at = started_at
        self.started.set()

    def finish(self):
        self.cancelled.set()

    def close(self):
        self.finish()
        self.worker.join(timeout=48)
        if self.worker.is_alive():
            self.warn('SwitchBot shutdown pending; check the turntable manually.')

    def report(self):
        return dict(address=self.config['address'],state='unknown' if self.state is None else 'GO' if self.state else 'STOP',
                    connection_seconds=self.connection_seconds,physical_state_verified=False,events=list(self.events))

    async def transition(self,transport,moving,scheduled):
        event = dict(action='GO' if moving else 'STOP',scheduled_seconds=scheduled,
                     actual_seconds=time.monotonic()-self.started_at)
        event['lateness_seconds'] = max(0.,event['actual_seconds']-scheduled)
        if moving and self.cancelled.is_set():
            event['result'] = 'cancelled_before_GO'
        elif self.state is None:
            event['result'] = 'skipped_unknown_state'
        elif self.state == moving:
            event['result'] = 'skipped_already_in_assumed_state'
        else:
            try:
                await asyncio.wait_for(transport.press(),5.)
                self.state = moving
                event['result'] = 'bot_acknowledged_not_motion_verified'
            except Exception as exc:
                self.state = None
                event.update(result='uncertain_no_retry',error=str(exc) or type(exc).__name__)
                self.warn(f'SwitchBot {event["action"]} failed or uncertain: {error_text(exc)}. No retry; check turntable manually. Recording continues.')
        event['completed_seconds'] = time.monotonic()-self.started_at
        self.events.append(event)

    async def wait_until(self,deadline):
        while not self.cancelled.is_set():
            delay = deadline-time.monotonic()
            if delay <= 0:
                return True
            await asyncio.sleep(min(.02,delay))
        return False

    async def run(self):
        transport = self.transport_factory(self.config['address'])
        connected = False
        try:
            try:
                connection_started = time.monotonic()
                await asyncio.wait_for(transport.connect(),45.)
                connected = True
            except Exception as exc:
                self.warn(f'SwitchBot connection failed: {error_text(exc)}. Recording continues without Bot control.')
                return
            finally:
                self.connection_seconds = time.monotonic()-connection_started
                self.ready.set()
            while not self.started.is_set() and not self.cancelled.is_set():
                await asyncio.sleep(.01)
            if self.cancelled.is_set():
                return
            await self.transition(transport,True,0.)
            events = [(item['stop'],False) for item in self.config['intervals']]+[(item['go'],True) for item in self.config['intervals']]
            for seconds,moving in sorted(events):
                if not await self.wait_until(self.started_at+seconds):
                    break
                if time.monotonic() >= self.started_at+self.duration:
                    break
                await self.transition(transport,moving,seconds)
            await self.wait_until(self.started_at+self.duration)
        finally:
            if connected and self.started.is_set():
                await self.transition(transport,False,min(self.duration,time.monotonic()-self.started_at))
                if self.state is None:
                    self.warn('Automatic STOP cannot be confirmed; manually check/stop the turntable.')
            try:
                await asyncio.wait_for(transport.close(),3.)
            except Exception as exc:
                self.warn(f'SwitchBot disconnect: {exc}')
            self.ready.set()
