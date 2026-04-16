#!/usr/bin/env python3
"""
eclipse_pcr.py — TUI control panel for EclipsePCR, a 1450 nm IR-driven PCR prototype.

Single-file, pure Python. On first run, auto-installs: textual, pyserial, plotext, esptool.
Works on Linux, macOS, Windows, and WSL2 (via usbipd-win attached devices).

Usage:
    python3 eclipse_pcr.py                       # launch TUI, auto-detect port
    python3 eclipse_pcr.py --port /dev/ttyACM0   # force a port
    python3 eclipse_pcr.py --baud 921600         # non-default baud
    python3 eclipse_pcr.py --simulate            # demo mode (no hardware needed)
    python3 eclipse_pcr.py --list-ports          # print detected ports and exit

Firmware side (expected JSON-lines over serial, one object per line):
    {"t": 12345, "tc": 67.3, "set": 95.0, "stage": "denature", "cycle": 3, "motor": "idle"}
Non-JSON lines are logged verbatim in the Console.
"""

from __future__ import annotations

# =====================================================================================
# Dependency bootstrap — auto-install on first run
# =====================================================================================
import importlib
import importlib.util
import os
import subprocess
import sys

_REQUIRED = [
    ("textual", "textual>=0.80"),
    ("serial", "pyserial>=3.5"),
    ("plotext", "plotext>=5.2"),
    ("esptool", "esptool>=4.8"),
]


def _ensure_deps() -> None:
    def _installed(mod: str) -> bool:
        try:
            return importlib.util.find_spec(mod) is not None
        except (ImportError, ValueError):
            return False

    missing = [spec for mod, spec in _REQUIRED if not _installed(mod)]
    if not missing:
        return
    print(f"[eclipse_pcr] Installing missing dependencies: {', '.join(missing)}", flush=True)
    attempts = [
        [sys.executable, "-m", "pip", "install", "--disable-pip-version-check"],
        [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--user"],
        [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--break-system-packages"],
    ]
    last_err: Exception | None = None
    for base in attempts:
        try:
            subprocess.check_call([*base, *missing])
            importlib.invalidate_caches()
            return
        except subprocess.CalledProcessError as e:
            last_err = e
    raise SystemExit(
        "\n[eclipse_pcr] Could not auto-install dependencies.\n"
        f"  Please run manually:   pip install {' '.join(missing)}\n"
        "  Or use a venv:         python3 -m venv .venv && source .venv/bin/activate\n"
        f"  Last error: {last_err}\n"
    )


_ensure_deps()

# =====================================================================================
# Standard imports (after bootstrap)
# =====================================================================================
import argparse
import json
import math
import platform as _platform
import queue
import random
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable

import serial as pyserial
from serial.tools import list_ports as _list_ports

import plotext as plt

from rich.text import Text
from rich.align import Align

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Grid, VerticalScroll
from textual.reactive import reactive
from textual.widgets import (
    Header,
    Footer,
    Static,
    Label,
    Input,
    Button,
    Select,
    Switch,
    TabbedContent,
    TabPane,
    RichLog,
    Digits,
)

# =====================================================================================
# Constants
# =====================================================================================
APP_NAME = "EclipsePCR"
APP_VERSION = "0.2.0"
DEFAULT_BAUD = 115200
DEFAULT_FLASH_BAUD = 460800
DEFAULT_CHIP = "esp32c6"
CONFIG_PATH = Path.home() / ".eclipse_pcr.json"

# PCR stage conventions (labels & colours)
STAGE_COLORS = {
    "denature": "red",
    "denaturation": "red",
    "anneal": "cyan",
    "annealing": "cyan",
    "extend": "green",
    "extension": "green",
    "elongation": "green",
    "hold": "yellow",
    "idle": "white",
    "cooldown": "blue",
    "init": "magenta",
}

# =====================================================================================
# Platform detection + serial port enumeration
# =====================================================================================
def detect_platform() -> str:
    """Return one of: 'linux', 'wsl', 'macos', 'windows', or the raw system name."""
    s = _platform.system()
    if s == "Linux":
        try:
            with open("/proc/version", "r") as f:
                ver = f.read().lower()
            if "microsoft" in ver or "wsl" in ver:
                return "wsl"
        except OSError:
            pass
        return "linux"
    if s == "Darwin":
        return "macos"
    if s == "Windows":
        return "windows"
    return s.lower()


PLATFORM = detect_platform()


def _port_score(p) -> int:
    """Rank likely ESP boards higher (lower score = earlier in sort)."""
    s = f"{p.device} {p.description or ''} {p.manufacturer or ''} {p.hwid or ''}".lower()
    score = 0
    for kw in ("esp32", "esp ", "jtag", "cp210", "ch340", "ch343", "ch34", "ftdi", "silicon labs"):
        if kw in s:
            score -= 30
    for kw in ("/dev/ttyacm", "/dev/tty.usbmodem", "/dev/tty.usbserial"):
        if kw in s:
            score -= 15
    if p.device.lower().startswith("com"):
        score -= 10
    if "/dev/ttys" in s and "usb" not in s:
        score += 50
    return score


def list_serial_ports() -> list:
    """Enumerate candidate serial ports, ranked (most likely target first)."""
    ports = list(_list_ports.comports())
    ports.sort(key=_port_score)
    return ports


def wsl_helper_hint() -> str:
    return (
        "[WSL2] No serial devices visible. On the Windows host, install usbipd-win then run:\n"
        "  usbipd list\n  usbipd attach --busid <id> --wsl\n"
        "Your ESP32-C6 should then appear as /dev/ttyACM0 inside WSL."
    )

# =====================================================================================
# Serial worker — pyserial in a background thread, line-based I/O
# =====================================================================================
MAX_RX_BUF = 65536       # discard partial line if buffer exceeds this (binary garbage protection)
MAX_LINE_LEN = 4096       # ignore lines longer than this
MAX_TX_QUEUE = 256        # drop oldest if command queue overflows
STALE_TIMEOUT_S = 10.0    # "no data" warning threshold while connected


@dataclass
class SerialStatus:
    state: str = "disconnected"   # disconnected|connecting|connected|error|paused
    detail: str = ""
    port: str = ""
    baud: int = DEFAULT_BAUD


class SerialWorker:
    """Background serial reader/writer. Thread-safe fan-out via callbacks."""

    def __init__(
        self,
        on_line: Callable[[str], None],
        on_status: Callable[[SerialStatus], None],
    ) -> None:
        self._on_line = on_line
        self._on_status = on_status
        self._tx_q: queue.Queue[bytes] = queue.Queue(maxsize=MAX_TX_QUEUE)
        self._stop = threading.Event()
        self._pause = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._ser: pyserial.Serial | None = None
        self._status = SerialStatus()
        self.last_rx_time: float = 0.0
        self.bytes_rx: int = 0
        self.bytes_tx: int = 0
        self.lines_rx: int = 0

    @property
    def status(self) -> SerialStatus:
        return self._status

    @property
    def connected(self) -> bool:
        return self._status.state == "connected"

    @property
    def stale(self) -> bool:
        if not self.connected:
            return False
        return self.last_rx_time > 0 and (time.monotonic() - self.last_rx_time) > STALE_TIMEOUT_S

    def _set_status(self, **kw) -> None:
        for k, v in kw.items():
            setattr(self._status, k, v)
        try:
            self._on_status(self._status)
        except Exception:
            pass

    def connect(self, port: str, baud: int = DEFAULT_BAUD) -> None:
        with self._lock:
            self._stop_and_join()
            self._status = SerialStatus(state="connecting", port=port, baud=baud)
            self.last_rx_time = 0.0
            self.bytes_rx = 0
            self.bytes_tx = 0
            self.lines_rx = 0
            self._flush_tx()
            self._stop.clear()
            self._pause.clear()
            self._set_status(state="connecting", detail="", port=port, baud=baud)
            self._thread = threading.Thread(
                target=self._run, args=(port, baud), name="serial-worker", daemon=True
            )
            self._thread.start()

    def disconnect(self) -> None:
        with self._lock:
            self._stop_and_join()
            self._set_status(state="disconnected", detail="")

    def _stop_and_join(self) -> None:
        self._stop.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=2.0)
            if t.is_alive():
                # thread stuck — force-close the port to unblock read()
                try:
                    if self._ser is not None:
                        self._ser.close()
                except Exception:
                    pass
                t.join(timeout=1.0)
        self._thread = None
        try:
            if self._ser is not None:
                self._ser.close()
        except Exception:
            pass
        self._ser = None

    def _flush_tx(self) -> None:
        try:
            while True:
                self._tx_q.get_nowait()
        except queue.Empty:
            pass

    def pause(self, paused: bool = True) -> None:
        if paused:
            self._pause.set()
            self._set_status(state="paused")
        else:
            self._pause.clear()
            if self._status.state == "paused":
                self._set_status(state="connected" if self._ser else "connecting")

    def send(self, text: str) -> bool:
        if not self.connected:
            return False
        if not text.endswith("\n"):
            text += "\n"
        encoded = text.encode("utf-8", errors="replace")
        try:
            self._tx_q.put_nowait(encoded)
            return True
        except queue.Full:
            # drop oldest to make room
            try:
                self._tx_q.get_nowait()
            except queue.Empty:
                pass
            try:
                self._tx_q.put_nowait(encoded)
                return True
            except queue.Full:
                return False

    def _drain_tx(self) -> None:
        ser = self._ser
        if not ser:
            return
        while True:
            try:
                chunk = self._tx_q.get_nowait()
            except queue.Empty:
                break
            try:
                ser.write(chunk)
                self.bytes_tx += len(chunk)
            except (pyserial.SerialException, OSError):
                raise

    def _run(self, port: str, baud: int) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            buf = b""
            try:
                self._set_status(state="connecting", detail=f"{port} @ {baud}", port=port, baud=baud)
                # Open with DTR/RTS both asserted. On ESP32-C6 USB-Serial/JTAG
                # that matches the chip's "no reset" state, so reconnecting
                # the TUI doesn't reboot the firmware mid-run. (Caveat: boards
                # wired through an external CP210x/CH340 auto-reset RC circuit
                # would do the opposite — flip these to False for that setup.)
                ser = pyserial.Serial()
                ser.port = port
                ser.baudrate = baud
                ser.timeout = 0.1
                ser.dsrdtr = False
                ser.rtscts = False
                ser.dtr = True
                ser.rts = True
                ser.open()
                self._ser = ser
                self._set_status(state="connected", detail=f"{port} @ {baud}")
                backoff = 0.5
                while not self._stop.is_set():
                    if self._pause.is_set():
                        time.sleep(0.1)
                        continue
                    self._drain_tx()
                    try:
                        data = ser.read(4096)
                    except (pyserial.SerialException, OSError):
                        raise
                    if data:
                        self.bytes_rx += len(data)
                        self.last_rx_time = time.monotonic()
                        buf += data
                        # protect against binary flood with no newlines
                        if len(buf) > MAX_RX_BUF:
                            buf = buf[-MAX_RX_BUF:]
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            s = line.decode("utf-8", errors="replace").rstrip("\r")
                            if s and len(s) <= MAX_LINE_LEN:
                                self.lines_rx += 1
                                try:
                                    self._on_line(s)
                                except Exception:
                                    pass
            except (pyserial.SerialException, OSError) as e:
                self._set_status(state="error", detail=_friendly_serial_error(port, e))
            except Exception as e:
                self._set_status(state="error", detail=f"{type(e).__name__}: {e}")
            finally:
                try:
                    if self._ser:
                        self._ser.close()
                except Exception:
                    pass
                self._ser = None
            if self._stop.is_set():
                break
            self._set_status(state="connecting", detail=f"reconnecting in {backoff:.1f}s")
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, 5.0)
        self._set_status(state="disconnected", detail="")

# =====================================================================================
# Telemetry parser
# =====================================================================================
_KV_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^,\s]+)")


def parse_telemetry(line: str) -> dict[str, Any] | None:
    """Parse one serial line. Returns a dict on success or None for raw log lines."""
    s = line.strip()
    if not s:
        return None
    if len(s) > MAX_LINE_LEN:
        return None
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                return _sanitize_telemetry(obj)
        except (json.JSONDecodeError, RecursionError):
            pass
    if "=" in s:
        hits = _KV_RE.findall(s)
        if hits:
            obj: dict[str, Any] = {}
            for k, v in hits:
                try:
                    obj[k] = int(v); continue
                except ValueError:
                    pass
                try:
                    f = float(v)
                    if math.isfinite(f):
                        obj[k] = f
                    else:
                        obj[k] = v
                    continue
                except ValueError:
                    pass
                obj[k] = v
            if obj:
                return obj
    return None


def _sanitize_telemetry(d: dict[str, Any]) -> dict[str, Any]:
    """Clamp/reject bad values in parsed telemetry dicts."""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if not isinstance(k, str):
            continue
        if isinstance(v, float) and not math.isfinite(v):
            continue
        # reject deeply nested objects — telemetry should be flat
        if isinstance(v, (dict, list)):
            continue
        out[k] = v
    return out if out else None

# =====================================================================================
# Simulator — fake PCR telemetry for --simulate
# =====================================================================================
class Simulator:
    """Generates plausible PCR telemetry at ~10 Hz for dev/demo."""

    def __init__(self, on_line: Callable[[str], None]) -> None:
        self._on_line = on_line
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="simulator", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)

    def _run(self) -> None:
        stages = [("denature", 95.0, 10), ("anneal", 55.0, 20), ("extend", 72.0, 25)]
        cycle = 1
        tc = 25.0
        t0 = time.monotonic()
        idx = 0
        stage_end = t0 + stages[idx][2]
        while not self._stop.is_set():
            now = time.monotonic()
            if now >= stage_end:
                idx = (idx + 1) % len(stages)
                if idx == 0:
                    cycle += 1
                stage_end = now + stages[idx][2]
            stage, setpt, _dur = stages[idx]
            alpha = 0.08 if tc < setpt else 0.05
            tc += (setpt - tc) * alpha + random.gauss(0, 0.12)
            msg = {
                "t": int((now - t0) * 1000),
                "tc": round(tc, 2),
                "set": setpt,
                "stage": stage,
                "cycle": cycle,
                "motor": "idle" if idx != 2 else "spin",
            }
            self._on_line(json.dumps(msg))
            if random.random() < 0.01:
                self._on_line(f"[log] heater duty {random.randint(10, 90)}%")
            time.sleep(0.1)

# =====================================================================================
# Config persistence
# =====================================================================================
@dataclass
class Config:
    port: str = ""
    baud: int = DEFAULT_BAUD
    flash_baud: int = DEFAULT_FLASH_BAUD
    chip: str = DEFAULT_CHIP
    autoconnect: bool = True
    plot_window_s: int = 300
    log_max: int = 5000
    flash_entries: list[list[str]] = field(default_factory=lambda: [["0x0", ""]])

    @classmethod
    def load(cls) -> "Config":
        if CONFIG_PATH.exists():
            try:
                data = json.loads(CONFIG_PATH.read_text())
                if not isinstance(data, dict):
                    return cls()
                return cls._from_dict(data)
            except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                pass
        return cls()

    @classmethod
    def _from_dict(cls, data: dict) -> "Config":
        cfg = cls()
        if isinstance(data.get("port"), str):
            cfg.port = data["port"]
        if isinstance(data.get("baud"), (int, float)) and 300 <= int(data["baud"]) <= 4_000_000:
            cfg.baud = int(data["baud"])
        if isinstance(data.get("flash_baud"), (int, float)) and 300 <= int(data["flash_baud"]) <= 4_000_000:
            cfg.flash_baud = int(data["flash_baud"])
        if isinstance(data.get("chip"), str) and data["chip"]:
            cfg.chip = data["chip"]
        if isinstance(data.get("autoconnect"), bool):
            cfg.autoconnect = data["autoconnect"]
        if isinstance(data.get("plot_window_s"), (int, float)) and 1 <= int(data["plot_window_s"]) <= 86400:
            cfg.plot_window_s = int(data["plot_window_s"])
        if isinstance(data.get("log_max"), (int, float)) and 100 <= int(data["log_max"]) <= 100_000:
            cfg.log_max = int(data["log_max"])
        fe = data.get("flash_entries")
        if isinstance(fe, list):
            validated: list[list[str]] = []
            for entry in fe:
                if isinstance(entry, list) and len(entry) == 2 and all(isinstance(e, str) for e in entry):
                    validated.append(entry)
            if validated:
                cfg.flash_entries = validated
        return cfg

    def save(self) -> str | None:
        """Save config. Returns None on success, or an error message."""
        tmp = CONFIG_PATH.parent / (CONFIG_PATH.name + ".tmp")
        try:
            tmp.write_text(json.dumps(asdict(self), indent=2))
            os.replace(tmp, CONFIG_PATH)
            return None
        except OSError as e:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return f"Failed to save config: {e}"

# =====================================================================================
# Widgets — tiles, dashboard, plot, console, flash, settings
# =====================================================================================
class Tile(Static):
    """A dashboard tile: title + big value + units."""

    DEFAULT_CSS = """
    Tile {
        height: 7;
        border: round $primary 50%;
        padding: 0 1;
        content-align: center middle;
    }
    Tile > Label.tile-title {
        color: $text-muted;
        text-style: bold;
        height: 1;
        width: 100%;
        content-align: center middle;
    }
    Tile > Digits.tile-value {
        color: $accent;
        height: 3;
        width: 100%;
        content-align: center middle;
    }
    Tile > Label.tile-value-text {
        color: $accent;
        text-style: bold;
        height: 3;
        width: 100%;
        content-align: center middle;
    }
    Tile > Label.tile-units {
        color: $text-muted;
        height: 1;
        width: 100%;
        content-align: center middle;
    }
    """

    def __init__(self, title: str, initial: str = "—", units: str = "", *, digits: bool = True, tid: str | None = None) -> None:
        super().__init__(id=tid)
        self._title = title
        self._value = initial
        self._units = units
        self._digits = digits

    def compose(self) -> ComposeResult:
        yield Label(self._title, classes="tile-title")
        if self._digits:
            yield Digits(self._value, classes="tile-value")
        else:
            yield Label(self._value, classes="tile-value-text")
        yield Label(self._units, classes="tile-units")

    def update_value(self, value: str, *, color: str | None = None) -> None:
        self._value = value
        try:
            if self._digits:
                d = self.query_one(Digits)
                d.update(value)
                if color:
                    d.styles.color = color
            else:
                lbl = self.query_one(".tile-value-text", Label)
                if color:
                    lbl.update(Text(value, style=f"bold {color}"))
                else:
                    lbl.update(Text(value, style="bold"))
        except Exception:
            pass


class StatusBar(Static):
    """One-line status strip shown beneath the header."""

    DEFAULT_CSS = """
    StatusBar {
        height: 1;
        background: $boost;
        color: $text;
        padding: 0 1;
    }
    """

    status: reactive[SerialStatus] = reactive(SerialStatus())
    sim: reactive[bool] = reactive(False)
    stale: reactive[bool] = reactive(False)
    awaiting: reactive[bool] = reactive(False)
    stats_text: reactive[str] = reactive("")
    fw_info: reactive[str] = reactive("")

    def render(self) -> Text:
        st = self.status
        state_color = {
            "connected": "green",
            "connecting": "yellow",
            "disconnected": "red",
            "error": "red",
            "paused": "magenta",
        }.get(st.state, "white")
        t = Text()
        t.append(f"{APP_NAME} v{APP_VERSION}", style="bold")
        t.append("  │  ", style="dim")
        t.append(f"{PLATFORM}", style="cyan")
        t.append("  │  ", style="dim")
        t.append("● ", style=state_color)
        t.append(st.state, style=f"bold {state_color}")
        if self.awaiting and not self.stale:
            t.append("  ⌛ awaiting telemetry", style="bold cyan")
        if self.stale:
            t.append("  ⚠ STALE", style="bold yellow")
        if self.sim:
            t.append("  [SIMULATING]", style="bold magenta")
        if st.port:
            t.append(f"  │  {st.port}", style="white")
        if st.baud:
            t.append(f" @ {st.baud}", style="dim")
        if self.fw_info:
            t.append(f"  │  fw: {self.fw_info}", style="green")
        if self.stats_text:
            t.append(f"  │  {self.stats_text}", style="dim")
        if st.detail:
            t.append(f"  │  {st.detail}", style="dim")
        return t


class DashboardView(Container):
    DEFAULT_CSS = """
    DashboardView { padding: 1 2; }
    DashboardView > #dash-grid {
        grid-size: 3 2;
        grid-gutter: 1 2;
        grid-rows: 1fr 1fr;
        grid-columns: 1fr 1fr 1fr;
        height: auto;
    }
    DashboardView > #dash-controls {
        height: auto;
        padding: 1 0 0 0;
    }
    DashboardView Button { margin-right: 1; }
    DashboardView #inp-target { width: 28; margin-right: 1; }
    """

    def compose(self) -> ComposeResult:
        with Grid(id="dash-grid"):
            yield Tile("Thermocouple", "—", "°C", tid="tile-tc")
            yield Tile("Setpoint", "—", "°C", tid="tile-set")
            yield Tile("Δ error", "—", "°C", tid="tile-err")
            yield Tile("Stage", "idle", "", digits=False, tid="tile-stage")
            yield Tile("Cycle", "0", "", tid="tile-cycle")
            yield Tile("Motor", "idle", "", digits=False, tid="tile-motor")
        with Horizontal(id="dash-controls"):
            yield Button("Start", id="btn-start", variant="success")
            yield Button("Stop", id="btn-stop", variant="error")
            yield Button("Pause", id="btn-pause", variant="warning")
            yield Button("Home motor", id="btn-home")
            yield Input(placeholder="Set target °C (e.g. 95.0)", id="inp-target")
            yield Button("Apply", id="btn-apply")

    def apply_telemetry(self, data: dict[str, Any]) -> None:
        def fmt(v, prec=2):
            if isinstance(v, (int, float)) and not (isinstance(v, float) and not math.isfinite(v)):
                return f"{v:.{prec}f}"
            return "—"

        tc = _safe_float(data.get("tc"))
        setpt = _safe_float(data.get("set"))
        stage = str(data.get("stage", "")) if data.get("stage") is not None else ""
        cycle = data.get("cycle")
        motor = str(data.get("motor", "")) if data.get("motor") is not None else ""

        # per-tile try so one bad update doesn't kill the rest
        try:
            if tc is not None:
                self.query_one("#tile-tc", Tile).update_value(fmt(tc))
        except Exception:
            pass
        try:
            if setpt is not None:
                self.query_one("#tile-set", Tile).update_value(fmt(setpt))
        except Exception:
            pass
        try:
            if tc is not None and setpt is not None:
                err = tc - setpt
                color = "green" if abs(err) < 1.0 else ("yellow" if abs(err) < 3.0 else "red")
                self.query_one("#tile-err", Tile).update_value(f"{err:+.2f}", color=color)
        except Exception:
            pass
        try:
            if stage:
                color = STAGE_COLORS.get(stage.lower(), "white")
                self.query_one("#tile-stage", Tile).update_value(stage, color=color)
        except Exception:
            pass
        try:
            if cycle is not None:
                self.query_one("#tile-cycle", Tile).update_value(str(int(cycle)))
        except (TypeError, ValueError, Exception):
            pass
        try:
            if motor:
                color = "magenta" if motor.lower() not in ("idle", "stop", "off") else "white"
                self.query_one("#tile-motor", Tile).update_value(motor, color=color)
        except Exception:
            pass


_plotext_lock = threading.Lock()


class PlotView(Static):
    """Rolling temperature/setpoint plot using plotext."""

    DEFAULT_CSS = """
    PlotView {
        padding: 0 1;
        height: 1fr;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self.t_samples: deque[float] = deque(maxlen=4000)
        self.tc_samples: deque[float] = deque(maxlen=4000)
        self.set_samples: deque[float | None] = deque(maxlen=4000)
        self.window_s: int = 300
        self._t0: float | None = None
        self._dirty: bool = True

    def on_mount(self) -> None:
        self.set_interval(0.4, self._maybe_refresh)

    def _maybe_refresh(self) -> None:
        if self._dirty:
            self._dirty = False
            self.refresh()

    def add_point(self, tc: float | None, setpt: float | None) -> None:
        now = time.monotonic()
        if self._t0 is None:
            self._t0 = now
        if tc is None:
            return
        self.t_samples.append(now - self._t0)
        self.tc_samples.append(float(tc))
        self.set_samples.append(float(setpt) if setpt is not None else None)
        self._dirty = True

    def clear(self) -> None:
        self.t_samples.clear()
        self.tc_samples.clear()
        self.set_samples.clear()
        self._t0 = None
        self._dirty = True

    def render(self):
        if not self.tc_samples:
            return Align.center(Text("Waiting for telemetry…  (Console tab shows raw incoming data)", style="dim"))
        try:
            w = max(self.size.width - 2, 40)
            h = max(self.size.height - 2, 8)
            t_last = self.t_samples[-1]
            t_min = t_last - self.window_s if self.window_s > 0 else self.t_samples[0]
            xs, ys, sx, sy = [], [], [], []
            for t, y, s in zip(self.t_samples, self.tc_samples, self.set_samples):
                if t >= t_min:
                    xs.append(t); ys.append(y)
                    if s is not None:
                        sx.append(t); sy.append(s)
            if not xs:
                return Text("No data in current time window.", style="dim")
            with _plotext_lock:
                plt.clear_figure()
                plt.plotsize(w, h)
                plt.theme("clear")
                plt.title(f"Temperature  (window: {self.window_s}s)")
                plt.xlabel("time (s)")
                plt.ylabel("°C")
                if len(sx) >= 2:
                    plt.plot(sx, sy, label="setpoint", color="cyan", marker="dot")
                plt.plot(xs, ys, label="TC", color="red", marker="dot")
                out = plt.build()
            return Text.from_ansi(out)
        except Exception as e:
            return Text(f"Plot error: {e}", style="red")


class ConsoleView(Container):
    DEFAULT_CSS = """
    ConsoleView { padding: 0 1; }
    ConsoleView > RichLog { height: 1fr; border: round $primary 50%; }
    ConsoleView > Horizontal { height: 3; padding-top: 1; }
    ConsoleView Input { width: 1fr; }
    ConsoleView Button { margin-left: 1; }
    """

    def compose(self) -> ComposeResult:
        yield RichLog(id="console-log", highlight=True, markup=False, wrap=False, max_lines=8000)
        with Horizontal():
            yield Input(placeholder="Type a command and press Enter (e.g. start, stop, set 95.0) …", id="cmd-input")
            yield Button("Send", id="btn-send", variant="primary")
            yield Button("Clear", id="btn-clear")

    def log(self, text: str, style: str | None = None) -> None:
        try:
            rl = self.query_one("#console-log", RichLog)
            if style:
                rl.write(Text(text, style=style))
            else:
                rl.write(text)
        except Exception:
            pass

    def clear_log(self) -> None:
        try:
            self.query_one("#console-log", RichLog).clear()
        except Exception:
            pass


class FlashRow(Horizontal):
    """One row in the flash entries list: offset + path + remove button."""
    DEFAULT_CSS = """
    FlashRow { height: 3; width: 100%; }
    FlashRow Input.offset { width: 14; margin-right: 1; }
    FlashRow Input.path { width: 1fr; margin-right: 1; }
    FlashRow Button.row-del { width: 4; }
    """

    def __init__(self, offset: str = "0x0", path: str = "") -> None:
        super().__init__()
        self._offset = offset
        self._path = path

    def compose(self) -> ComposeResult:
        yield Input(value=self._offset, placeholder="0x10000", classes="offset")
        yield Input(value=self._path, placeholder="/path/to/firmware.bin", classes="path")
        yield Button("×", classes="row-del", variant="error")


class FlashView(Container):
    DEFAULT_CSS = """
    FlashView { padding: 1 2; layout: vertical; }
    FlashView #flash-controls {
        grid-size: 2 3;
        grid-columns: 14 1fr;
        grid-rows: 3 3 3;
        grid-gutter: 0 1;
        height: auto;
    }
    FlashView #flash-entries {
        height: auto;
        min-height: 5;
        max-height: 12;
        border: round $primary 50%;
        padding: 0 1;
        margin: 1 0;
    }
    FlashView #flash-buttons { height: 3; }
    FlashView #flash-buttons Button { margin-right: 1; }
    FlashView #flash-log { height: 1fr; border: round $primary 50%; margin-top: 1; }
    """

    def __init__(self, cfg: Config) -> None:
        super().__init__()
        self._cfg = cfg

    def compose(self) -> ComposeResult:
        with Grid(id="flash-controls"):
            yield Label("Chip")
            yield Input(value=self._cfg.chip, id="flash-chip")
            yield Label("Flash baud")
            yield Input(value=str(self._cfg.flash_baud), id="flash-baud")
            yield Label("Firmware entries")
            yield Label("Offset    ·    binary path", classes="mono")
        with VerticalScroll(id="flash-entries"):
            entries = self._cfg.flash_entries or [["0x0", ""]]
            for off, path in entries:
                yield FlashRow(off, path)
        with Horizontal(id="flash-buttons"):
            yield Button("+ Add entry", id="flash-add")
            yield Button("Detect chip", id="flash-chip-id")
            yield Button("Erase flash", id="flash-erase", variant="warning")
            yield Button("Flash firmware", id="flash-go", variant="success")
            yield Button("Abort", id="flash-abort", variant="error", disabled=True)
        yield RichLog(id="flash-log", highlight=False, markup=False, wrap=False, max_lines=10000)

    def current_entries(self) -> list[tuple[str, str]]:
        """Rows with non-empty paths — used when firing a flash operation."""
        return [(off, path) for off, path in self.all_rows() if path]

    def all_rows(self) -> list[tuple[str, str]]:
        """Every row in the UI, including empty paths — used when persisting."""
        out: list[tuple[str, str]] = []
        for row in self.query(FlashRow):
            inputs = row.query(Input)
            if len(inputs) >= 2:
                off = inputs[0].value.strip() or "0x0"
                path = inputs[1].value.strip()
                out.append((off, path))
        return out

    def log(self, text: str, style: str | None = None) -> None:
        try:
            rl = self.query_one("#flash-log", RichLog)
            if style:
                rl.write(Text(text, style=style))
            else:
                rl.write(text)
        except Exception:
            pass

    def chip(self) -> str:
        try:
            return self.query_one("#flash-chip", Input).value.strip() or DEFAULT_CHIP
        except Exception:
            return DEFAULT_CHIP

    def baud(self) -> int:
        try:
            return int(self.query_one("#flash-baud", Input).value.strip())
        except Exception:
            return DEFAULT_FLASH_BAUD

    def refresh_from_cfg(self, cfg: Config) -> None:
        """Write cfg values back into the widgets (used after Discard)."""
        self._cfg = cfg
        try:
            self.query_one("#flash-chip", Input).value = cfg.chip
        except Exception:
            pass
        try:
            self.query_one("#flash-baud", Input).value = str(cfg.flash_baud)
        except Exception:
            pass
        try:
            container = self.query_one("#flash-entries", VerticalScroll)
            for row in list(self.query(FlashRow)):
                row.remove()
            entries = cfg.flash_entries or [["0x0", ""]]
            for off, path in entries:
                container.mount(FlashRow(off, path))
        except Exception:
            pass


class SettingsView(Container):
    DEFAULT_CSS = """
    SettingsView { padding: 1 2; }
    SettingsView Grid {
        grid-size: 2 6;
        grid-columns: 22 1fr;
        grid-rows: 3 3 3 3 3 3;
        grid-gutter: 0 1;
        height: auto;
    }
    SettingsView Input { width: 1fr; }
    SettingsView Switch { width: auto; }
    SettingsView #ports-row {
        height: auto;
        padding-top: 1;
    }
    SettingsView #ports-row Button { margin-right: 1; }
    SettingsView #save-row { height: 3; }
    SettingsView #save-row Button { margin-right: 1; }
    SettingsView #cfg-detected { height: auto; padding: 1 0; color: $text-muted; }
    """

    def __init__(self, cfg: Config, ports_provider: Callable[[], list]):
        super().__init__()
        self._cfg = cfg
        self._ports_provider = ports_provider

    def compose(self) -> ComposeResult:
        port_opts = self._port_options()
        match = next((v for _lbl, v in port_opts if v == self._cfg.port), None)
        yield Label("Serial port and connection", classes="header")
        with Grid():
            yield Label("Port")
            if match is not None:
                yield Select(options=port_opts, value=match, allow_blank=True, id="cfg-port", prompt="auto-detect")
            else:
                yield Select(options=port_opts, allow_blank=True, id="cfg-port", prompt="auto-detect")
            yield Label("Baud")
            yield Input(value=str(self._cfg.baud), id="cfg-baud")
            yield Label("Auto-connect at start")
            yield Switch(value=self._cfg.autoconnect, id="cfg-autoconnect")
            yield Label("Plot window (seconds)")
            yield Input(value=str(self._cfg.plot_window_s), id="cfg-plot")
            yield Label("Console max lines")
            yield Input(value=str(self._cfg.log_max), id="cfg-logmax")
            yield Label("Config path")
            yield Label(str(CONFIG_PATH), classes="mono")
        with Horizontal(id="ports-row"):
            yield Button("Rescan ports", id="cfg-rescan")
            yield Button("Connect", id="cfg-connect", variant="success")
            yield Button("Disconnect", id="cfg-disconnect", variant="warning")
        with Horizontal(id="save-row"):
            yield Button("Save config", id="cfg-save", variant="primary")
            yield Button("Discard changes", id="cfg-discard")
        yield Static(id="cfg-detected")

    def _port_options(self) -> list[tuple[str, str]]:
        ports = self._ports_provider()
        opts: list[tuple[str, str]] = []
        for p in ports:
            desc = p.description or p.hwid or ""
            label = f"{p.device}  —  {desc}" if desc and desc != "n/a" else p.device
            opts.append((label, p.device))
        return opts

    def rescan(self) -> None:
        try:
            sel = self.query_one("#cfg-port", Select)
            sel.set_options(self._port_options())
        except Exception:
            pass
        ports = self._ports_provider()
        lines: list[str] = []
        if not ports:
            lines.append(wsl_helper_hint() if PLATFORM == "wsl" else "No serial ports detected.")
        else:
            lines.append(f"Detected {len(ports)} port(s):")
            for p in ports:
                lines.append(f"  {p.device}   {p.description or ''}   [{p.hwid or ''}]")
        try:
            # pass Text so brackets in hwid aren't parsed as Rich markup
            self.query_one("#cfg-detected", Static).update(Text("\n".join(lines)))
        except Exception:
            pass

    def read_into(self, cfg: Config) -> None:
        def _int(x, fallback):
            try: return int(x)
            except Exception: return fallback
        try:
            v = self.query_one("#cfg-port", Select).value
            cfg.port = str(v) if v not in (None, Select.BLANK, Select.NULL) else ""
        except Exception:
            pass
        try: cfg.baud = _int(self.query_one("#cfg-baud", Input).value, cfg.baud)
        except Exception: pass
        try: cfg.autoconnect = bool(self.query_one("#cfg-autoconnect", Switch).value)
        except Exception: pass
        try: cfg.plot_window_s = _int(self.query_one("#cfg-plot", Input).value, cfg.plot_window_s)
        except Exception: pass
        try: cfg.log_max = _int(self.query_one("#cfg-logmax", Input).value, cfg.log_max)
        except Exception: pass

    def refresh_from_cfg(self, cfg: Config) -> None:
        """Write cfg values back into the widgets (used after Discard)."""
        self._cfg = cfg
        try:
            sel = self.query_one("#cfg-port", Select)
            sel.set_options(self._port_options())
            match = next((v for _lbl, v in self._port_options() if v == cfg.port), None)
            sel.value = match if match is not None else Select.BLANK
        except Exception:
            pass
        for iid, val in (
            ("#cfg-baud", str(cfg.baud)),
            ("#cfg-plot", str(cfg.plot_window_s)),
            ("#cfg-logmax", str(cfg.log_max)),
        ):
            try:
                self.query_one(iid, Input).value = val
            except Exception:
                pass
        try:
            self.query_one("#cfg-autoconnect", Switch).value = cfg.autoconnect
        except Exception:
            pass

# =====================================================================================
# The main app
# =====================================================================================
class EclipsePCRApp(App):

    TITLE = f"{APP_NAME} — 1450nm IR PCR controller"
    SUB_TITLE = "TUI host console"

    CSS = """
    Screen { layers: base overlay; }
    Header { height: 1; }
    #status-row { height: 1; }
    TabbedContent { height: 1fr; }
    TabPane { padding: 0; }
    Label.header { text-style: bold; color: $accent; }
    Label.mono { color: $text-muted; }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=False),
        Binding("c", "toggle_connect", "Connect/Disc.", priority=False),
        Binding("p", "toggle_pause", "Pause rx", priority=False),
        Binding("r", "rescan_ports", "Rescan", priority=False),
        Binding("1", "show_tab('dash')", "Dash", show=False, priority=False),
        Binding("2", "show_tab('plot')", "Plot", show=False, priority=False),
        Binding("3", "show_tab('console')", "Console", show=False, priority=False),
        Binding("4", "show_tab('flash')", "Flash", show=False, priority=False),
        Binding("5", "show_tab('settings')", "Settings", show=False, priority=False),
        Binding("ctrl+l", "clear_log", "Clear console", show=False),
        Binding("ctrl+q", "quit", "Quit", priority=True),
    ]

    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__()
        self.args = args
        self.cfg = Config.load()
        if args.port:
            self.cfg.port = args.port
        if args.baud:
            self.cfg.baud = args.baud
        self.simulate = bool(args.simulate)
        self._serial = SerialWorker(
            on_line=self._on_serial_line_threadsafe,
            on_status=self._on_serial_status_threadsafe,
        )
        self._sim = Simulator(on_line=self._on_serial_line_threadsafe) if self.simulate else None
        self._flash_proc: subprocess.Popen | None = None
        self._flash_pending: bool = False
        self._last_press: dict[str, float] = {}

    # ------------------------------------------------------------------ layout
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield StatusBar(id="status-row")
        with TabbedContent(initial="dash", id="tabs"):
            with TabPane("Dashboard", id="dash"):
                yield DashboardView()
            with TabPane("Plot", id="plot"):
                yield PlotView()
            with TabPane("Console", id="console"):
                yield ConsoleView()
            with TabPane("Flash", id="flash"):
                yield FlashView(self.cfg)
            with TabPane("Settings", id="settings"):
                yield SettingsView(self.cfg, ports_provider=list_serial_ports)
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(StatusBar).sim = self.simulate
        try:
            pv = self.query_one(PlotView)
            pv.window_s = self.cfg.plot_window_s
        except Exception:
            pass
        try:
            self.query_one("#console-log", RichLog).max_lines = self.cfg.log_max
        except Exception:
            pass
        self.query_one(SettingsView).rescan()
        self.set_interval(2.0, self._health_check)

        if self.simulate:
            self._sim.start()
            self.query_one(ConsoleView).log("--- running in SIMULATE mode (no hardware) ---", style="bold magenta")
            return

        if self.cfg.autoconnect:
            port = self.cfg.port
            if not port:
                ports = list_serial_ports()
                if ports:
                    port = ports[0].device
            if port:
                self._connect(port, self.cfg.baud)
            else:
                msg = wsl_helper_hint() if PLATFORM == "wsl" else "No serial ports detected. Open the Settings tab and click Rescan."
                self.query_one(ConsoleView).log(msg, style="yellow")

    # ------------------------------------------------------------------ serial plumbing
    def _on_serial_line_threadsafe(self, line: str) -> None:
        try:
            self.call_from_thread(self._on_serial_line, line)
        except Exception:
            pass

    def _on_serial_status_threadsafe(self, status: SerialStatus) -> None:
        try:
            self.call_from_thread(self._on_serial_status, status)
        except Exception:
            pass

    def _on_serial_line(self, line: str) -> None:
        console = self.query_one(ConsoleView)
        data = parse_telemetry(line)
        if data is None:
            console.log(f"< {line}", style="dim")
            return

        msg_type = data.get("type") if isinstance(data.get("type"), str) else None
        if msg_type == "hello":
            self._on_hello(data)
            return
        if msg_type == "info":
            self._on_info(data)
            return

        console.log(f"< {line}")
        self.query_one(DashboardView).apply_telemetry(data)
        try:
            self.query_one(PlotView).add_point(
                tc=_safe_float(data.get("tc")),
                setpt=_safe_float(data.get("set")),
            )
        except Exception:
            pass
        if "log" in data:
            console.log(f"! {data['log']}", style="yellow")
        if "err" in data or "error" in data:
            msg = data.get("err") or data.get("error")
            console.log(f"!! {msg}", style="bold red")

    def _on_hello(self, data: dict[str, Any]) -> None:
        fw = str(data.get("fw") or "unknown")
        ver = str(data.get("version") or "?")
        parts = [f"{fw} {ver}"]
        for key in ("chip", "mpy", "mac", "reset_cause"):
            v = data.get(key)
            if v:
                parts.append(f"{key}={v}")
        self.query_one(ConsoleView).log("[fw] " + "  ".join(parts), style="bold green")
        try:
            self.query_one(StatusBar).fw_info = f"{fw} {ver}"
        except Exception:
            pass

    def _on_info(self, data: dict[str, Any]) -> None:
        parts = []
        for key in ("fw", "version", "uptime_ms", "heap_free"):
            v = data.get(key)
            if v is not None:
                parts.append(f"{key}={v}")
        self.query_one(ConsoleView).log("[fw info] " + "  ".join(parts), style="cyan")

    def _on_serial_status(self, status: SerialStatus) -> None:
        try:
            # reactive on dataclass: reassign a copy so Textual notices the change
            sb = self.query_one(StatusBar)
            sb.status = SerialStatus(state=status.state, detail=status.detail, port=status.port, baud=status.baud)
        except Exception:
            pass
        self.query_one(ConsoleView).log(f"[serial] {status.state} {status.detail}".rstrip(), style="cyan")

    def _connect(self, port: str, baud: int) -> None:
        self.cfg.port = port
        self.cfg.baud = baud
        self._serial.connect(port, baud)

    def _disconnect(self) -> None:
        self._serial.disconnect()

    # ------------------------------------------------------------------ button wiring
    @on(Button.Pressed, "#btn-start")
    def _btn_start(self) -> None: self._send_cmd("start")

    @on(Button.Pressed, "#btn-stop")
    def _btn_stop(self) -> None: self._send_cmd("stop")

    @on(Button.Pressed, "#btn-pause")
    def _btn_pause(self) -> None: self._send_cmd("pause")

    @on(Button.Pressed, "#btn-home")
    def _btn_home(self) -> None: self._send_cmd("motor home")

    @on(Button.Pressed, "#btn-apply")
    def _btn_apply(self) -> None:
        inp = self.query_one("#inp-target", Input)
        v = inp.value.strip()
        if not v: return
        try:
            f = float(v)
            self._send_cmd(f"set {f}")
            inp.value = ""
        except ValueError:
            self.query_one(ConsoleView).log(f"Invalid target: {v}", style="red")

    @on(Button.Pressed, "#btn-send")
    def _btn_send(self) -> None:
        inp = self.query_one("#cmd-input", Input)
        v = inp.value
        if v.strip():
            self._send_cmd(v)
            inp.value = ""

    @on(Input.Submitted, "#cmd-input")
    def _cmd_submitted(self, event: Input.Submitted) -> None:
        v = event.value
        if v.strip():
            self._send_cmd(v)
        event.input.value = ""

    @on(Input.Submitted, "#inp-target")
    def _target_submitted(self, event: Input.Submitted) -> None:
        self._btn_apply()

    @on(Button.Pressed, "#btn-clear")
    def _btn_clear(self) -> None:
        self.query_one(ConsoleView).clear_log()

    # Settings tab
    @on(Button.Pressed, "#cfg-rescan")
    def _cfg_rescan(self) -> None:
        self.query_one(SettingsView).rescan()

    @on(Button.Pressed, "#cfg-connect")
    def _cfg_connect(self) -> None:
        sv = self.query_one(SettingsView)
        sv.read_into(self.cfg)
        port = self.cfg.port
        if not port:
            ports = list_serial_ports()
            if ports:
                port = ports[0].device
        if not port:
            self.query_one(ConsoleView).log("No port selected.", style="red")
            return
        self._connect(port, self.cfg.baud)

    @on(Button.Pressed, "#cfg-disconnect")
    def _cfg_disconnect(self) -> None:
        self._disconnect()

    @on(Button.Pressed, "#cfg-save")
    def _cfg_save(self) -> None:
        sv = self.query_one(SettingsView)
        sv.read_into(self.cfg)
        # persist every row the user sees, including empty-path placeholders
        self.cfg.flash_entries = [[off, path] for off, path in self.query_one(FlashView).all_rows()] or [["0x0", ""]]
        try:
            self.cfg.chip = self.query_one(FlashView).chip()
            self.cfg.flash_baud = self.query_one(FlashView).baud()
        except Exception:
            pass
        err = self.cfg.save()
        try:
            self.query_one(PlotView).window_s = self.cfg.plot_window_s
        except Exception:
            pass
        try:
            self.query_one("#console-log", RichLog).max_lines = self.cfg.log_max
        except Exception:
            pass
        if err:
            self.query_one(ConsoleView).log(err, style="bold red")
        else:
            self.query_one(ConsoleView).log(f"Config saved to {CONFIG_PATH}", style="green")

    @on(Button.Pressed, "#cfg-discard")
    def _cfg_discard(self) -> None:
        self.cfg = Config.load()
        try:
            self.query_one(SettingsView).refresh_from_cfg(self.cfg)
        except Exception:
            pass
        try:
            self.query_one(FlashView).refresh_from_cfg(self.cfg)
        except Exception:
            pass
        try:
            self.query_one(PlotView).window_s = self.cfg.plot_window_s
        except Exception:
            pass
        try:
            self.query_one("#console-log", RichLog).max_lines = self.cfg.log_max
        except Exception:
            pass
        self.query_one(ConsoleView).log("Config reloaded from disk.", style="yellow")

    # Flash tab
    @on(Button.Pressed, "#flash-add")
    def _flash_add(self) -> None:
        fv = self.query_one(FlashView)
        container = fv.query_one("#flash-entries", VerticalScroll)
        container.mount(FlashRow())

    @on(Button.Pressed, ".row-del")
    def _flash_row_del(self, event: Button.Pressed) -> None:
        row = event.button.parent
        if row is not None:
            row.remove()

    @on(Button.Pressed, "#flash-chip-id")
    def _flash_chipid(self) -> None:
        self._run_esptool(["chip_id"])

    @on(Button.Pressed, "#flash-erase")
    def _flash_erase(self) -> None:
        self._run_esptool(
            ["erase_flash"],
            require_confirm="CONFIRM: erase_flash will wipe the entire ESP32-C6 flash. Press Erase again within 5s to confirm.",
        )

    @on(Button.Pressed, "#flash-go")
    def _flash_go(self) -> None:
        fv = self.query_one(FlashView)
        entries = fv.current_entries()
        if not entries:
            fv.log("No firmware entries configured. Add a .bin with its flash offset.", style="red")
            return
        for off, path in entries:
            if not _valid_hex_offset(off):
                fv.log(f"Invalid flash offset '{off}' — expected hex like 0x10000", style="red")
                return
            p = Path(path).expanduser()
            if not p.exists():
                fv.log(f"Missing file: {path}", style="red")
                return
            if not p.is_file():
                fv.log(f"Not a regular file: {path}", style="red")
                return
        args = ["--baud", str(fv.baud()), "write_flash"]
        for off, path in entries:
            args.extend([off, str(Path(path).expanduser())])
        self._run_esptool(args)

    @on(Button.Pressed, "#flash-abort")
    def _flash_abort(self) -> None:
        proc = self._flash_proc
        if proc and proc.poll() is None:
            fv = self.query_one(FlashView)
            try:
                proc.terminate()
                fv.log("Abort: SIGTERM sent.", style="yellow")
            except Exception:
                pass
            # escalate to SIGKILL after 3s if still alive
            self._escalate_kill(proc)

    @work(thread=True, group="esptool-kill")
    def _escalate_kill(self, proc: subprocess.Popen) -> None:
        time.sleep(3.0)
        if proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                return
            self.call_from_thread(self._log_flash_kill)

    def _log_flash_kill(self) -> None:
        try:
            self.query_one(FlashView).log(
                "Abort: SIGKILL sent (process did not respond to SIGTERM).",
                style="bold red",
            )
        except Exception:
            pass

    # ------------------------------------------------------------------ esptool runner
    def _run_esptool(self, extra_args: list[str], *, require_confirm: str | None = None) -> None:
        fv = self.query_one(FlashView)
        if self._flash_pending or (self._flash_proc and self._flash_proc.poll() is None):
            fv.log("An esptool operation is already running.", style="red")
            return
        port = self.cfg.port
        if not port:
            ports = list_serial_ports()
            port = ports[0].device if ports else ""
        if not port:
            fv.log("No serial port selected. Set one in the Settings tab.", style="red")
            return
        chip = fv.chip()

        if require_confirm:
            key = ",".join(extra_args)
            now = time.monotonic()
            last = self._last_press.get(key, 0.0)
            if now - last > 5.0:
                self._last_press[key] = now
                fv.log(require_confirm, style="bold yellow")
                return
            self._last_press.pop(key, None)

        was_connected = self._serial.connected
        if was_connected:
            fv.log("[serial] releasing port for esptool…", style="cyan")
            self._disconnect()

        argv = [sys.executable, "-m", "esptool", "--chip", chip, "--port", port, *extra_args]
        fv.log(f"$ {' '.join(argv)}", style="bold")
        self._flash_pending = True
        try:
            self.query_one("#flash-abort", Button).disabled = False
            self.query_one("#flash-go", Button).disabled = True
            self.query_one("#flash-erase", Button).disabled = True
            self.query_one("#flash-chip-id", Button).disabled = True
        except Exception:
            pass

        self._run_esptool_worker(argv, was_connected)

    @work(exclusive=True, thread=True, group="esptool")
    def _run_esptool_worker(self, argv: list[str], reconnect_after: bool) -> None:
        try:
            self._flash_proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            assert self._flash_proc.stdout is not None
            for line in iter(self._flash_proc.stdout.readline, ""):
                line = line.rstrip("\r\n")
                if line:
                    self.call_from_thread(self._flash_log_line, line)
            rc = self._flash_proc.wait()
            self.call_from_thread(self._flash_log_line, f"[esptool exited with code {rc}]")
        except FileNotFoundError as e:
            self.call_from_thread(self._flash_log_line, f"ERROR: {e}")
        except Exception as e:
            self.call_from_thread(self._flash_log_line, f"ERROR: {type(e).__name__}: {e}")
        finally:
            self._flash_proc = None
            self.call_from_thread(self._flash_finished, reconnect_after)

    def _flash_log_line(self, line: str) -> None:
        style = None
        low = line.lstrip().lower()
        # esptool prefixes real errors with "A fatal error" or "error:"; prefix match
        # avoids false positives like "no errors encountered".
        if low.startswith("error") or low.startswith("a fatal error") or low.startswith("fatal"):
            style = "red"
        elif "wrote" in low or "hash of data verified" in low or "leaving" in low:
            style = "green"
        elif line.startswith("$"):
            style = "bold"
        try:
            self.query_one(FlashView).log(line, style=style)
        except Exception:
            pass

    def _flash_finished(self, reconnect: bool) -> None:
        self._flash_pending = False
        try:
            self.query_one("#flash-abort", Button).disabled = True
            self.query_one("#flash-go", Button).disabled = False
            self.query_one("#flash-erase", Button).disabled = False
            self.query_one("#flash-chip-id", Button).disabled = False
        except Exception:
            pass
        if reconnect and self.cfg.port:
            self.query_one(FlashView).log("[serial] re-opening port…", style="cyan")
            self._connect(self.cfg.port, self.cfg.baud)

    # ------------------------------------------------------------------ health
    def _health_check(self) -> None:
        try:
            sb = self.query_one(StatusBar)
            sb.stale = self._serial.stale
            s = self._serial
            # Connected but the firmware hasn't said anything yet.
            sb.awaiting = s.connected and s.lines_rx == 0
            if s.connected or s.bytes_rx > 0:
                sb.stats_text = f"rx:{s.bytes_rx:,}B  tx:{s.bytes_tx:,}B  lines:{s.lines_rx:,}"
            else:
                sb.stats_text = ""
                sb.fw_info = ""
        except Exception:
            pass

    # ------------------------------------------------------------------ helpers
    def _send_cmd(self, cmd: str) -> None:
        console = self.query_one(ConsoleView)
        if self.simulate:
            console.log(f"> {cmd}  (simulate — not sent)", style="dim magenta")
            return
        if not self._serial.connected:
            console.log(f"(not connected) dropped: {cmd}", style="red")
            return
        if not self._serial.send(cmd):
            console.log(f"(queue full) dropped: {cmd}", style="red")
            return
        console.log(f"> {cmd}", style="bold green")

    # ------------------------------------------------------------------ actions
    def action_toggle_connect(self) -> None:
        if self._serial.connected or self._serial.status.state in ("connecting", "paused"):
            self._disconnect()
        else:
            ports = list_serial_ports()
            port = self.cfg.port or (ports[0].device if ports else "")
            if port:
                self._connect(port, self.cfg.baud)

    def action_toggle_pause(self) -> None:
        self._serial.pause(self._serial.status.state != "paused")

    def action_rescan_ports(self) -> None:
        self.query_one(SettingsView).rescan()

    def action_show_tab(self, tab_id: str) -> None:
        try:
            self.query_one(TabbedContent).active = tab_id
        except Exception:
            pass

    def action_clear_log(self) -> None:
        self.query_one(ConsoleView).clear_log()

    # ------------------------------------------------------------------ tear down
    def on_unmount(self) -> None:
        try:
            if self._sim:
                self._sim.stop()
        except Exception:
            pass
        try:
            self._serial.disconnect()
        except Exception:
            pass

# =====================================================================================
# Utilities
# =====================================================================================
_HEX_OFFSET_RE = re.compile(r"^0x[0-9a-fA-F]+$|^[0-9]+$")


def _valid_hex_offset(s: str) -> bool:
    return bool(_HEX_OFFSET_RE.match(s.strip()))


def _safe_float(x) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except (TypeError, ValueError, OverflowError):
        return None


def _friendly_serial_error(port: str, exc: BaseException) -> str:
    """Turn a raw pyserial/OSError into a message with a concrete next step."""
    msg = str(exc)
    low = msg.lower()
    if "permission denied" in low or (isinstance(exc, OSError) and getattr(exc, "errno", None) == 13):
        hint = "sudo usermod -a -G dialout $USER  (then log out/in)"
        if PLATFORM == "wsl":
            hint += "  — or restart WSL after detach/attach"
        return f"{msg}  |  {hint}"
    if (
        "no such file" in low
        or "could not open port" in low
        or (isinstance(exc, OSError) and getattr(exc, "errno", None) == 2)
    ):
        tail = wsl_helper_hint() if PLATFORM == "wsl" else f"port {port} not present; check USB"
        return f"{msg}  |  {tail}"
    if "device busy" in low or (isinstance(exc, OSError) and getattr(exc, "errno", None) == 16):
        return f"{msg}  |  another program is holding the port (screen/minicom/esptool?)"
    return msg

# =====================================================================================
# CLI
# =====================================================================================
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="eclipse_pcr",
        description=f"{APP_NAME} — TUI host console for the 1450nm IR PCR prototype.",
    )
    ap.add_argument("--port", help="Serial port (e.g. /dev/ttyACM0, COM4)")
    ap.add_argument("--baud", type=int, default=None, help=f"Serial baud (default {DEFAULT_BAUD})")
    ap.add_argument("--simulate", action="store_true", help="Run with simulated telemetry (no hardware)")
    ap.add_argument("--list-ports", action="store_true", help="Print detected serial ports and exit")
    ap.add_argument("--version", action="version", version=f"{APP_NAME} {APP_VERSION}")
    args = ap.parse_args(argv)

    if args.list_ports:
        ports = list_serial_ports()
        print(f"Platform: {PLATFORM}")
        if not ports:
            print("No ports detected.")
            if PLATFORM == "wsl":
                print(wsl_helper_hint())
            return 0
        for p in ports:
            print(f"  {p.device:20s}  {p.description or ''}  [{p.hwid or ''}]")
        return 0

    app = EclipsePCRApp(args)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
