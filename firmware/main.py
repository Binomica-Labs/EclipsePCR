"""
EclipsePCR firmware (MicroPython, ESP32-C6 / Seeed XIAO).

On boot, emits a hello record so the host can identify the firmware:
    {"type":"hello","fw":"eclipse_pcr","version":"0.1.0",
     "chip":"ESP32C6","mpy":"1.28.0","mac":"...","reset_cause":"..."}

Then emits telemetry at 10 Hz with ADC oversampling on A0 (GPIO0):
    {"t": <ms>, "tc": <°C>, "set": <°C>, "stage": <str>, "cycle": <int>,
     "motor": <str>, "adc": <raw>}

Commands over USB-CDC (one per line):
    ping | info | reboot | start | stop | pause | set <float> | motor home
"""

import gc
import json
import select
import sys
import time

from machine import ADC, Pin, reset_cause, soft_reset, unique_id

FW_NAME = "eclipse_pcr"
FW_VERSION = "0.1.0"

# Ctrl-C (0x03) stays enabled so `mpremote` and other dev tools can break into
# the REPL for live updates. The main loop catches KeyboardInterrupt itself so
# a stray 0x03 byte can't kill the firmware — it just logs and keeps going.

# --- hardware ---
_ADC_PIN = 0  # XIAO ESP32-C6 A0 == GPIO0 (ADC1_CH0)
_adc = ADC(Pin(_ADC_PIN))
# Older / newer MicroPython ADC APIs disagree — try both shapes, ignore failures.
try:
    _adc.atten(ADC.ATTN_11DB)  # 0..~3.3 V
except (AttributeError, ValueError):
    pass
try:
    _adc.width(ADC.WIDTH_12BIT)
except (AttributeError, ValueError):
    pass

TELEM_PERIOD_MS = 100
ADC_OVERSAMPLE = 8

# --- state ---
_state = {
    "setpoint": 95.0,
    "stage": "idle",
    "cycle": 0,
    "motor": "idle",
    "running": False,
    "paused": False,
}

_RESET_CAUSES = {
    1: "power_on", 2: "ext", 3: "software", 4: "panic",
    5: "int_wdt", 6: "task_wdt", 7: "wdt", 8: "deepsleep",
    9: "brownout", 10: "sdio", 15: "usb_serial_jtag",
}


def _mac_str():
    try:
        return ":".join("{:02x}".format(x) for x in unique_id())
    except Exception:
        return "unknown"


def _reset_cause_str():
    try:
        return _RESET_CAUSES.get(reset_cause(), str(reset_cause()))
    except Exception:
        return "unknown"


def _emit(obj):
    try:
        sys.stdout.write(json.dumps(obj))
        sys.stdout.write("\n")
    except Exception:
        pass


def _log(msg):
    _emit({"t": time.ticks_ms(), "log": msg})


def _read_adc():
    try:
        acc = 0
        for _ in range(ADC_OVERSAMPLE):
            acc += _adc.read_u16()
        return (acc // ADC_OVERSAMPLE) >> 4  # 0..4095
    except Exception:
        return 0


def _adc_to_tc(raw):
    # Fake linear mapping 0..4095 -> 20.0..100.0 °C for UI testing.
    return 20.0 + (raw / 4095.0) * 80.0


def _emit_hello():
    info = {
        "type": "hello",
        "fw": FW_NAME,
        "version": FW_VERSION,
        "chip": "ESP32C6",
        "mac": _mac_str(),
        "reset_cause": _reset_cause_str(),
    }
    try:
        v = sys.implementation.version
        info["mpy"] = ".".join(str(x) for x in v[:3])
    except Exception:
        pass
    _emit(info)


def _emit_info():
    try:
        free = gc.mem_free()
    except Exception:
        free = -1
    _emit({
        "type": "info",
        "t": time.ticks_ms(),
        "fw": FW_NAME,
        "version": FW_VERSION,
        "uptime_ms": time.ticks_ms(),
        "heap_free": free,
    })


def _handle(cmd):
    s = cmd.strip()
    if not s:
        return
    low = s.lower()
    if low == "ping":
        _log("pong")
    elif low == "info":
        _emit_info()
    elif low == "reboot":
        _log("rebooting")
        time.sleep_ms(50)
        try:
            soft_reset()
        except Exception as e:
            _emit({"t": time.ticks_ms(), "err": "reboot failed: {}".format(e)})
    elif low == "start":
        _state["running"] = True
        _state["paused"] = False
        _state["stage"] = "denature"
        _state["cycle"] = 1
        _log("started")
    elif low == "stop":
        _state["running"] = False
        _state["paused"] = False
        _state["stage"] = "idle"
        _state["cycle"] = 0
        _state["motor"] = "idle"
        _log("stopped")
    elif low == "pause":
        _state["paused"] = not _state["paused"]
        _state["stage"] = "hold" if _state["paused"] else "denature"
        _log("paused" if _state["paused"] else "resumed")
    elif low.startswith("set "):
        try:
            v = float(s.split(None, 1)[1])
            _state["setpoint"] = v
            _log("setpoint={:.2f}".format(v))
        except (ValueError, IndexError):
            _emit({"t": time.ticks_ms(), "err": "bad set arg"})
    elif low == "motor home":
        _state["motor"] = "homing"
        _log("motor homing")
    else:
        _log("unknown: " + s)


def main():
    _emit_hello()
    _log("{} {} boot".format(FW_NAME, FW_VERSION))

    poll = select.poll()
    poll.register(sys.stdin, select.POLLIN)

    rx = ""
    last = time.ticks_ms()

    while True:
        try:
            if poll.poll(0):
                try:
                    c = sys.stdin.read(1)
                except Exception:
                    c = ""
                if c:
                    if c == "\r":
                        pass
                    elif c == "\n":
                        if rx:
                            _handle(rx)
                            rx = ""
                    elif len(rx) < 128:
                        rx += c

            now = time.ticks_ms()
            if time.ticks_diff(now, last) >= TELEM_PERIOD_MS:
                last = now
                raw = _read_adc()
                _emit({
                    "t": now,
                    "tc": round(_adc_to_tc(raw), 2),
                    "set": _state["setpoint"],
                    "stage": _state["stage"],
                    "cycle": _state["cycle"],
                    "motor": _state["motor"],
                    "adc": raw,
                })

            time.sleep_ms(5)
        except KeyboardInterrupt:
            # Stray 0x03 from a misbehaving host shouldn't terminate the run.
            _log("keyboard interrupt ignored")
            rx = ""
        except Exception as e:
            _emit({"t": time.ticks_ms(), "err": "{}: {}".format(type(e).__name__, e)})
            time.sleep_ms(100)


try:
    main()
except Exception as e:
    _emit({"t": time.ticks_ms(), "err": "fatal {}: {}".format(type(e).__name__, e)})
    raise
