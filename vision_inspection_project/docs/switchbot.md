# SwitchBot Bot BLE Turntable Control

Requires the original SwitchBot Bot in Press mode, a Windows BLE adapter and
the `bleak` dependency from requirements.txt. No Hub or cloud token is used.

In Recording by Phone, enable SwitchBot, scan BLE and select the Press-mode
device. Add STOP/GO pairs between Total time and zoom, then save the named
record condition. Existing conditions default to SwitchBot disabled.
Live Test loads the same group's record condition automatically.

All times are offsets from the first captured frame, not inspection progress:
GO at zero, each scheduled STOP/GO, then STOP at capture end or manual Stop.
Pairs must satisfy `0 < stop < go < total_time` and cannot overlap.
Camera zoom and Bot scheduling run independently of slow inference.

Each run requires confirmation that the turntable is physically stopped and
the Bot is in Press mode. Software tracks only an assumed state. It cannot
measure actual rotation. If already assumed STOP, ending the recording does
not press the toggle again. Pending GO events are cancelled on manual Stop.

There is no automatic toggle retry. A missing/negative acknowledgement makes
the state unknown; remaining automatic presses are suppressed to avoid an
accidental restart. Camera capture and inspection continue with a warning.
Check and stop the turntable manually after any such warning.

BLE and Windows scheduling are not PLC-grade hard real-time control. Bot
acknowledgement does not prove the button moved the turntable. Pipeline JSON
and TXT performance sections include `switchbot` events, scheduled/actual times,
lateness, command outcomes and warnings. Hardware commissioning is required.

Protocol: https://github.com/OpenWonderLabs/SwitchBotAPI-BLE/blob/latest/devicetypes/bot.md
BLE client: https://bleak.readthedocs.io/en/latest/
