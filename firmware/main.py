"""
EclipsePCR firmware (MicroPython, ESP32-C6 / Seeed XIAO).

Hardware map — update pin constants below if your wiring differs.

    MAX31856 universal thermocouple amp (Adafruit #3263, SPI):
        Module pin     XIAO pin   GPIO    Notes
        --------------------------------------------------------
        Vin            3V3        —       3.3V in (5V tolerant, use 3V3 here)
        GND            GND        —
        SCK            D8         19      SPI clock
        SDI (MOSI)     D10        18      host → amp
        SDO (MISO)     D9         20      amp → host
        CS             D7         17      chip select (any free GPIO works)
        T+  / T-       —          —       K-type thermocouple leads

    ADC (placeholder until the heater's sense line is wired):
        A0 / GPIO0 — floating for now; used as a "signal alive" ADC field.

Telemetry (one JSON object per line, 10 Hz):
    {"t":ms, "tc":°C|null, "cj":°C|null, "set":°C, "stage":str, "cycle":int,
     "motor":str, "fault":[str,...], "adc":int}

Boot record (emitted once before telemetry starts):
    {"type":"hello","fw":"eclipse_pcr","version":"...","chip":"ESP32C6", ...}

Commands (one per line, case-insensitive where applicable):
    ping | info | reboot
    start | stop | pause
    set <°C>                     — change the setpoint
    motor home
    tc_type <K|J|T|E|N|R|S|B>    — re-init the MAX31856 for a different TC
    tc_rescan                    — retry the MAX31856 init (use after wiring)
"""

import gc
import json
import select
import sys
import time

from machine import ADC, Pin, SPI, reset_cause, soft_reset, unique_id

FW_NAME = "eclipse_pcr"
FW_VERSION = "0.2.0"

# Ctrl-C (0x03) stays enabled so `mpremote` and other dev tools can break into
# the REPL for live updates. KeyboardInterrupt is allowed to propagate out of
# main() — that's what lets the raw REPL handshake actually complete. A stray
# 0x03 from a noisy host can therefore end the run; for production we can
# re-enable kbd_intr(-1) once iteration calms down.

# --- pin map (XIAO ESP32-C6) -----------------------------------------------
_SPI_SCK = 19      # D8
_SPI_MOSI = 18     # D10
_SPI_MISO = 20     # D9
_TC_CS_PIN = 17    # D7
_ADC_PIN = 0       # D0 / A0

TELEM_PERIOD_MS = 100   # 10 Hz telemetry
TC_RETRY_MS = 30_000    # if the MAX31856 isn't seen, retry init every 30s
ADC_OVERSAMPLE = 4

# ---------------------------------------------------------------------------
# MAX31856 thermocouple amplifier driver
# ---------------------------------------------------------------------------
class MAX31856:
    # register addresses (7-bit; write sets bit 7 = 1)
    _CR0 = 0x00
    _CR1 = 0x01
    _MASK = 0x02
    _CJTH = 0x0A
    _CJTL = 0x0B
    _LTCBH = 0x0C
    _LTCBM = 0x0D
    _LTCBL = 0x0E
    _SR = 0x0F

    # CR0: CMODE auto-convert + OCFAULT mode 1 (open-circuit detect <5kΩ)
    _CR0_DEFAULT = 0x90
    # CR1: 4-sample averaging + TC type field
    _CR1_AVG_4 = 0x20

    _TC_TYPES = {
        "B": 0x00, "E": 0x01, "J": 0x02, "K": 0x03,
        "N": 0x04, "R": 0x05, "S": 0x06, "T": 0x07,
    }

    _FAULT_BITS = (
        (0x01, "open"),
        (0x02, "ov_uv"),
        (0x04, "tc_low"),
        (0x08, "tc_high"),
        (0x10, "cj_low"),
        (0x20, "cj_high"),
        (0x40, "tc_range"),
        (0x80, "cj_range"),
    )

    def __init__(self, spi, cs_pin, tc_type="K"):
        self._spi = spi
        self._cs = Pin(cs_pin, Pin.OUT, value=1)
        self.alive = False
        self.tc_type = tc_type
        self._last_init_attempt_ms = time.ticks_ms() - TC_RETRY_MS
        self.init()

    def init(self) -> bool:
        """(Re)initialise the chip. Returns True if it looks present."""
        self._last_init_attempt_ms = time.ticks_ms()
        cr1 = self._CR1_AVG_4 | self._TC_TYPES.get(self.tc_type, 0x03)
        try:
            self._write(self._CR0, self._CR0_DEFAULT)
            self._write(self._CR1, cr1)
            got_cr1 = self._read(self._CR1, 1)[0]
            got_cr0 = self._read(self._CR0, 1)[0]
            # If MISO is floating (chip absent) we'll read 0x00 or 0xFF uniformly.
            self.alive = got_cr1 == cr1 and got_cr0 == self._CR0_DEFAULT
        except Exception:
            self.alive = False
        return self.alive

    def set_tc_type(self, tc_type: str) -> bool:
        tc_type = tc_type.upper()
        if tc_type not in self._TC_TYPES:
            return False
        self.tc_type = tc_type
        return self.init()

    def _read(self, addr, n):
        buf = bytearray(n)
        self._cs(0)
        try:
            self._spi.write(bytes((addr & 0x7F,)))
            self._spi.readinto(buf)
        finally:
            self._cs(1)
        return buf

    def _write(self, addr, value):
        self._cs(0)
        try:
            self._spi.write(bytes((addr | 0x80, value & 0xFF)))
        finally:
            self._cs(1)

    def read_all(self):
        """Return (tc_c, cj_c, fault_names) or None on failure."""
        if not self.alive:
            return None
        try:
            # burst read: CJTH, CJTL, LTCBH, LTCBM, LTCBL, SR
            data = self._read(self._CJTH, 6)
        except Exception:
            self.alive = False
            return None

        # Sanity: all-0x00 or all-0xFF means MISO lost its driver.
        if data == b"\x00\x00\x00\x00\x00\x00" or data == b"\xff\xff\xff\xff\xff\xff":
            self.alive = False
            return None

        cj_raw = (data[0] << 8) | data[1]
        if cj_raw & 0x8000:
            cj_raw -= 0x10000
        cj_c = cj_raw / 256.0  # 2^-8 °C per LSB (bits 1:0 reserved)

        tc_raw = (data[2] << 16) | (data[3] << 8) | data[4]
        if tc_raw & 0x800000:
            tc_raw -= 0x1000000
        tc_c = tc_raw / 4096.0  # 2^-7 °C per LSB (bits 4:0 reserved)

        fault = data[5]
        faults = [name for mask, name in self._FAULT_BITS if fault & mask]
        return tc_c, cj_c, faults

    def maybe_retry(self):
        """Periodically retry init when the sensor is absent (hot-plug)."""
        if self.alive:
            return
        if time.ticks_diff(time.ticks_ms(), self._last_init_attempt_ms) >= TC_RETRY_MS:
            self.init()


# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------
_state = {
    "setpoint": 95.0,
    "stage": "idle",
    "cycle": 0,
    "motor": "idle",
    "running": False,
    "paused": False,
}

_adc = ADC(Pin(_ADC_PIN))
try:
    _adc.atten(ADC.ATTN_11DB)
except (AttributeError, ValueError):
    pass

# SPI bus & MAX31856 — set up once, driver handles the "not wired" case.
try:
    _spi = SPI(
        2, baudrate=1_000_000, polarity=0, phase=1,
        sck=Pin(_SPI_SCK), mosi=Pin(_SPI_MOSI), miso=Pin(_SPI_MISO),
    )
except Exception:
    # Fall back to software SPI if the hw peripheral refuses these pins.
    from machine import SoftSPI
    _spi = SoftSPI(
        baudrate=1_000_000, polarity=0, phase=1,
        sck=Pin(_SPI_SCK), mosi=Pin(_SPI_MOSI), miso=Pin(_SPI_MISO),
    )

_tc = MAX31856(_spi, _TC_CS_PIN, tc_type="K")

_RESET_CAUSES = {
    1: "power_on", 2: "ext", 3: "software", 4: "panic",
    5: "int_wdt", 6: "task_wdt", 7: "wdt", 8: "deepsleep",
    9: "brownout", 10: "sdio", 15: "usb_serial_jtag",
}


# ---------------------------------------------------------------------------
# Serial I/O helpers
# ---------------------------------------------------------------------------
def _emit(obj):
    try:
        sys.stdout.write(json.dumps(obj))
        sys.stdout.write("\n")
    except Exception:
        pass


def _log(msg):
    _emit({"t": time.ticks_ms(), "log": msg})


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


def _read_adc():
    try:
        acc = 0
        for _ in range(ADC_OVERSAMPLE):
            acc += _adc.read_u16()
        return (acc // ADC_OVERSAMPLE) >> 4
    except Exception:
        return 0


def _emit_hello():
    info = {
        "type": "hello",
        "fw": FW_NAME,
        "version": FW_VERSION,
        "chip": "ESP32C6",
        "mac": _mac_str(),
        "reset_cause": _reset_cause_str(),
        "tc_sensor": "max31856" if _tc.alive else "absent",
        "tc_type": _tc.tc_type,
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
        "tc_sensor": "max31856" if _tc.alive else "absent",
        "tc_type": _tc.tc_type,
    })


# ---------------------------------------------------------------------------
# Command handling
# ---------------------------------------------------------------------------
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
    elif low.startswith("tc_type "):
        arg = s.split(None, 1)[1].strip()
        if _tc.set_tc_type(arg):
            _log("tc_type={}  (sensor {})".format(_tc.tc_type, "ok" if _tc.alive else "absent"))
        else:
            _emit({"t": time.ticks_ms(), "err": "unknown tc type: " + arg})
    elif low == "tc_rescan":
        ok = _tc.init()
        _log("tc_rescan: " + ("found" if ok else "still absent"))
    else:
        _log("unknown: " + s)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    _emit_hello()
    _log("{} {} boot  (tc_sensor={})".format(
        FW_NAME, FW_VERSION, "max31856" if _tc.alive else "absent"))

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

                tc_c = None
                cj_c = None
                faults = []
                sensor_result = _tc.read_all()
                if sensor_result is not None:
                    tc_c, cj_c, faults = sensor_result
                else:
                    _tc.maybe_retry()

                frame = {
                    "t": now,
                    "tc": round(tc_c, 3) if tc_c is not None else None,
                    "cj": round(cj_c, 2) if cj_c is not None else None,
                    "set": _state["setpoint"],
                    "stage": _state["stage"],
                    "cycle": _state["cycle"],
                    "motor": _state["motor"],
                    "adc": _read_adc(),
                }
                if faults:
                    frame["fault"] = faults
                _emit(frame)

            time.sleep_ms(5)
        except Exception as e:
            # Survive transient errors; KeyboardInterrupt intentionally escapes
            # so dev tools (mpremote) can drop us into the REPL.
            _emit({"t": time.ticks_ms(), "err": "{}: {}".format(type(e).__name__, e)})
            time.sleep_ms(100)


try:
    main()
except Exception as e:
    _emit({"t": time.ticks_ms(), "err": "fatal {}: {}".format(type(e).__name__, e)})
    raise
