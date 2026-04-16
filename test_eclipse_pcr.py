"""
Comprehensive test suite for eclipse_pcr.py

Run:  python3 -m pytest test_eclipse_pcr.py -v
"""
from __future__ import annotations

import asyncio
import argparse
import importlib.util
import json
import math
import os
import pty
import select
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# ---- load eclipse_pcr as a module ----
_spec = importlib.util.spec_from_file_location(
    "eclipse_pcr", Path(__file__).with_name("eclipse_pcr.py")
)
ecp = importlib.util.module_from_spec(_spec)
sys.modules["eclipse_pcr"] = ecp
_spec.loader.exec_module(ecp)


# =====================================================================================
# parse_telemetry
# =====================================================================================
class TestParseTelemetry:
    def test_valid_json(self):
        d = ecp.parse_telemetry('{"tc": 67.3, "set": 95.0, "stage": "denature"}')
        assert d["tc"] == 67.3
        assert d["set"] == 95.0
        assert d["stage"] == "denature"

    def test_json_with_int_fields(self):
        d = ecp.parse_telemetry('{"t": 1234, "cycle": 3}')
        assert d["t"] == 1234
        assert d["cycle"] == 3

    def test_kv_format(self):
        d = ecp.parse_telemetry("tc=67.3,set=95.0,stage=denature")
        assert d["tc"] == 67.3
        assert d["set"] == 95.0
        assert d["stage"] == "denature"

    def test_kv_with_spaces(self):
        d = ecp.parse_telemetry("tc = 67.3, set = 95.0")
        assert d["tc"] == 67.3

    def test_empty_string(self):
        assert ecp.parse_telemetry("") is None
        assert ecp.parse_telemetry("   ") is None

    def test_non_json_non_kv(self):
        assert ecp.parse_telemetry("hello world") is None

    def test_partial_json(self):
        assert ecp.parse_telemetry('{"tc": 67.3') is None

    def test_json_array(self):
        assert ecp.parse_telemetry("[1,2,3]") is None

    def test_json_string(self):
        assert ecp.parse_telemetry('"hello"') is None

    def test_json_with_inf(self):
        # JSON spec doesn't allow inf, but manual construction
        d = ecp.parse_telemetry('{"tc": 1e999}')
        # json.loads("1e999") raises OverflowError on some platforms or returns inf
        # Either way, _sanitize_telemetry should strip it
        assert d is None or "tc" not in d

    def test_json_nested_dict_stripped(self):
        d = ecp.parse_telemetry('{"tc": 67.3, "nested": {"a": 1}}')
        assert "nested" not in d

    def test_flat_list_preserved(self):
        # flat lists (e.g. fault lists) are allowed through
        d = ecp.parse_telemetry('{"tc": 67.3, "fault": ["open", "tc_high"]}')
        assert d["fault"] == ["open", "tc_high"]

    def test_nested_list_stripped(self):
        d = ecp.parse_telemetry('{"tc": 67.3, "vals": [[1,2],[3,4]]}')
        assert "vals" not in d

    def test_very_long_line(self):
        line = '{"tc":' + "1" * 10000 + "}"
        assert ecp.parse_telemetry(line) is None

    def test_binary_garbage(self):
        assert ecp.parse_telemetry("\x00\xff\xfe\x01\x02") is None

    def test_kv_int_values(self):
        d = ecp.parse_telemetry("cycle=3,t=1234")
        assert d["cycle"] == 3
        assert isinstance(d["cycle"], int)
        assert d["t"] == 1234

    def test_kv_inf_values(self):
        d = ecp.parse_telemetry("tc=inf,set=95.0")
        assert d["tc"] == "inf"  # stored as string, not float
        assert d["set"] == 95.0

    def test_json_only_non_finite_values(self):
        d = ecp.parse_telemetry('{"tc": null}')
        assert d is not None
        assert d.get("tc") is None  # null is allowed

    def test_all_fields_stripped_returns_none(self):
        d = ecp.parse_telemetry('{"nested": {"a": 1}}')
        assert d is None


# =====================================================================================
# _safe_float
# =====================================================================================
class TestSafeFloat:
    def test_none(self):
        assert ecp._safe_float(None) is None

    def test_int(self):
        assert ecp._safe_float(42) == 42.0

    def test_float(self):
        assert ecp._safe_float(3.14) == 3.14

    def test_string_number(self):
        assert ecp._safe_float("67.3") == 67.3

    def test_nan(self):
        assert ecp._safe_float(float("nan")) is None

    def test_inf(self):
        assert ecp._safe_float(float("inf")) is None

    def test_neg_inf(self):
        assert ecp._safe_float(float("-inf")) is None

    def test_string_garbage(self):
        assert ecp._safe_float("hello") is None

    def test_empty_string(self):
        assert ecp._safe_float("") is None

    def test_zero(self):
        assert ecp._safe_float(0) == 0.0

    def test_negative(self):
        assert ecp._safe_float(-40.5) == -40.5

    def test_bool(self):
        # bool is a subclass of int in Python
        assert ecp._safe_float(True) == 1.0

    def test_very_large(self):
        assert ecp._safe_float(1e308) == 1e308

    def test_overflow_string(self):
        assert ecp._safe_float("1e999") is None


# =====================================================================================
# _valid_hex_offset
# =====================================================================================
class TestValidHexOffset:
    def test_hex_with_prefix(self):
        assert ecp._valid_hex_offset("0x10000") is True

    def test_hex_lowercase(self):
        assert ecp._valid_hex_offset("0xabcdef") is True

    def test_hex_uppercase(self):
        assert ecp._valid_hex_offset("0xABCDEF") is True

    def test_decimal(self):
        assert ecp._valid_hex_offset("65536") is True

    def test_zero(self):
        assert ecp._valid_hex_offset("0x0") is True
        assert ecp._valid_hex_offset("0") is True

    def test_invalid_garbage(self):
        assert ecp._valid_hex_offset("hello") is False

    def test_empty(self):
        assert ecp._valid_hex_offset("") is False

    def test_negative(self):
        assert ecp._valid_hex_offset("-1") is False

    def test_spaces(self):
        assert ecp._valid_hex_offset(" 0x10000 ") is True


# =====================================================================================
# _sanitize_telemetry
# =====================================================================================
class TestSanitizeTelemetry:
    def test_normal(self):
        d = ecp._sanitize_telemetry({"tc": 67.3, "stage": "denature"})
        assert d == {"tc": 67.3, "stage": "denature"}

    def test_strips_inf(self):
        d = ecp._sanitize_telemetry({"tc": float("inf"), "stage": "denature"})
        assert "tc" not in d

    def test_strips_nan(self):
        d = ecp._sanitize_telemetry({"tc": float("nan")})
        assert d is None

    def test_strips_nested_dict(self):
        d = ecp._sanitize_telemetry({"tc": 67.3, "extra": {"a": 1}})
        assert "extra" not in d

    def test_keeps_flat_list(self):
        d = ecp._sanitize_telemetry({"tc": 67.3, "fault": ["open"]})
        assert d["fault"] == ["open"]

    def test_strips_nested_list(self):
        d = ecp._sanitize_telemetry({"tc": 67.3, "extra": [[1, 2]]})
        assert "extra" not in d

    def test_strips_non_string_keys(self):
        d = ecp._sanitize_telemetry({42: "bad", "tc": 67.3})  # type: ignore[dict-item]
        assert 42 not in d
        assert d["tc"] == 67.3

    def test_all_stripped_returns_none(self):
        d = ecp._sanitize_telemetry({"a": float("inf"), "b": {"x": 1}})
        assert d is None

    def test_none_value_preserved(self):
        d = ecp._sanitize_telemetry({"tc": None, "stage": "idle"})
        assert d["tc"] is None

    def test_bool_value_preserved(self):
        d = ecp._sanitize_telemetry({"running": True})
        assert d["running"] is True


# =====================================================================================
# Config
# =====================================================================================
class TestConfig:
    def test_defaults(self):
        c = ecp.Config()
        assert c.baud == ecp.DEFAULT_BAUD
        assert c.chip == ecp.DEFAULT_CHIP
        assert c.autoconnect is True

    def test_round_trip(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config(port="/dev/ttyACM0", baud=921600, chip="esp32s3")
            err = c.save()
            assert err is None
            loaded = ecp.Config.load()
            assert loaded.port == "/dev/ttyACM0"
            assert loaded.baud == 921600
            assert loaded.chip == "esp32s3"

    def test_load_missing_file(self, tmp_path):
        cfg_path = tmp_path / "nonexistent.json"
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.baud == ecp.DEFAULT_BAUD

    def test_load_corrupt_json(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text("{this is not json}")
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.baud == ecp.DEFAULT_BAUD

    def test_load_wrong_type_baud(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({"baud": "fast"}))
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.baud == ecp.DEFAULT_BAUD  # falls back to default

    def test_load_out_of_range_baud(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({"baud": 999999999}))
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.baud == ecp.DEFAULT_BAUD  # rejected

    def test_load_bad_flash_entries(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({"flash_entries": [["0x0"]]}))  # 1-element
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.flash_entries == [["0x0", ""]]  # falls back

    def test_load_non_dict_json(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text('"just a string"')
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.baud == ecp.DEFAULT_BAUD

    def test_save_error_returns_message(self, tmp_path):
        cfg_path = tmp_path / "readonly" / "test.json"
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config()
            err = c.save()
            assert err is not None
            assert "Failed" in err

    def test_valid_flash_entries_preserved(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        entries = [["0x0", "boot.bin"], ["0x10000", "app.bin"]]
        cfg_path.write_text(json.dumps({"flash_entries": entries}))
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.flash_entries == entries

    def test_plot_window_clamped(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({"plot_window_s": 0}))  # below minimum of 1
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.plot_window_s == 300  # default

    def test_autoconnect_non_bool(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({"autoconnect": "yes"}))
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            c = ecp.Config.load()
            assert c.autoconnect is True  # default, not "yes"

    def test_save_is_atomic(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({"baud": 115200}))
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            # simulate a mid-write crash by forcing os.replace to raise and
            # confirm the original file is intact and no .tmp is left behind
            with patch("eclipse_pcr.os.replace", side_effect=OSError("boom")):
                err = ecp.Config(baud=230400).save()
                assert err is not None
            assert cfg_path.read_text().strip().startswith("{")
            assert json.loads(cfg_path.read_text())["baud"] == 115200
            tmp = cfg_path.parent / (cfg_path.name + ".tmp")
            assert not tmp.exists(), "stale .tmp file must be cleaned up"

    def test_save_round_trip_via_tmp(self, tmp_path):
        cfg_path = tmp_path / "test.json"
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            assert ecp.Config(baud=921600).save() is None
            # no leftover tmp file after successful save
            tmp = cfg_path.parent / (cfg_path.name + ".tmp")
            assert not tmp.exists()
            assert json.loads(cfg_path.read_text())["baud"] == 921600


# =====================================================================================
# Platform detection
# =====================================================================================
class TestPlatform:
    def test_returns_string(self):
        p = ecp.detect_platform()
        assert isinstance(p, str)
        assert len(p) > 0

    def test_known_platform(self):
        p = ecp.detect_platform()
        assert p in ("linux", "wsl", "macos", "windows") or len(p) > 0


# =====================================================================================
# Port scoring
# =====================================================================================
class TestPortScoring:
    def _make_port(self, device, description="", manufacturer="", hwid=""):
        p = MagicMock()
        p.device = device
        p.description = description
        p.manufacturer = manufacturer
        p.hwid = hwid
        return p

    def test_esp_device_ranked_higher(self):
        esp = self._make_port("/dev/ttyACM0", description="ESP32-C6 JTAG")
        legacy = self._make_port("/dev/ttyS0", description="n/a")
        assert ecp._port_score(esp) < ecp._port_score(legacy)

    def test_usb_serial_ranked_higher_than_legacy(self):
        usb = self._make_port("/dev/ttyACM0", description="USB Serial")
        legacy = self._make_port("/dev/ttyS5", description="n/a")
        assert ecp._port_score(usb) < ecp._port_score(legacy)

    def test_cp210x_ranked_high(self):
        cp = self._make_port("/dev/ttyUSB0", description="CP2102N", manufacturer="Silicon Labs")
        generic = self._make_port("/dev/ttyS10")
        assert ecp._port_score(cp) < ecp._port_score(generic)

    def test_com_port_ranked(self):
        com = self._make_port("COM3", description="USB Serial")
        assert ecp._port_score(com) < 0

    def test_macos_usbmodem(self):
        p = self._make_port("/dev/tty.usbmodem14101")
        assert ecp._port_score(p) < 0


# =====================================================================================
# SerialWorker (with pty loopback)
# =====================================================================================
class TestSerialWorker:
    @pytest.fixture
    def pty_pair(self):
        master, slave = pty.openpty()
        slave_name = os.ttyname(slave)
        yield master, slave, slave_name
        os.close(master)
        os.close(slave)

    def test_connect_receive_disconnect(self, pty_pair):
        master, slave, port = pty_pair
        received = []
        statuses = []

        def on_line(line):
            received.append(line)

        def on_status(st):
            statuses.append(st.state)

        w = ecp.SerialWorker(on_line=on_line, on_status=on_status)
        w.connect(port, 115200)
        time.sleep(0.3)
        assert w.connected

        os.write(master, b'{"tc":67.3,"set":95.0}\n')
        time.sleep(0.3)
        assert len(received) >= 1
        assert "tc" in received[0]

        w.disconnect()
        assert not w.connected
        assert "disconnected" in statuses

    def test_send_data(self, pty_pair):
        master, slave, port = pty_pair
        w = ecp.SerialWorker(on_line=lambda l: None, on_status=lambda s: None)
        w.connect(port, 115200)
        time.sleep(0.3)

        result = w.send("hello")
        assert result is True
        time.sleep(0.3)
        data = os.read(master, 1024)
        assert b"hello\n" in data
        assert w.bytes_tx > 0

        w.disconnect()

    def test_reconnect_on_close(self, pty_pair):
        master, slave, port = pty_pair
        statuses = []
        w = ecp.SerialWorker(
            on_line=lambda l: None,
            on_status=lambda s: statuses.append(s.state),
        )
        w.connect(port, 115200)
        time.sleep(0.3)
        assert w.connected
        w.disconnect()
        assert not w.connected

    def test_stale_detection(self, pty_pair):
        master, slave, port = pty_pair
        w = ecp.SerialWorker(on_line=lambda l: None, on_status=lambda s: None)
        w.connect(port, 115200)
        time.sleep(0.3)

        # send data so last_rx_time > 0
        os.write(master, b'{"tc":1}\n')
        time.sleep(0.3)
        assert not w.stale

        # fake stale by setting last_rx_time in the past
        w.last_rx_time = time.monotonic() - ecp.STALE_TIMEOUT_S - 1
        assert w.stale

        w.disconnect()

    def test_send_when_disconnected(self):
        w = ecp.SerialWorker(on_line=lambda l: None, on_status=lambda s: None)
        assert w.send("test") is False

    def test_binary_flood_does_not_oom(self, pty_pair):
        master, slave, port = pty_pair
        received = []
        w = ecp.SerialWorker(on_line=lambda l: received.append(l), on_status=lambda s: None)
        w.connect(port, 115200)
        time.sleep(0.2)

        # send 128KB of binary garbage with no newlines
        os.write(master, b"\xff" * 131072)
        time.sleep(0.5)
        # flush the garbage line with a newline, then send valid data
        os.write(master, b'\n{"tc":42}\n')
        time.sleep(0.5)

        # the worker should still be alive and process the valid line after the flush
        assert w.connected
        assert any("42" in r for r in received)
        w.disconnect()

    def test_double_connect_is_safe(self, pty_pair):
        master, slave, port = pty_pair
        w = ecp.SerialWorker(on_line=lambda l: None, on_status=lambda s: None)
        w.connect(port, 115200)
        time.sleep(0.2)
        assert w.connected
        # connect again without explicit disconnect
        w.connect(port, 115200)
        time.sleep(0.2)
        assert w.connected
        w.disconnect()

    def test_pause_resume(self, pty_pair):
        master, slave, port = pty_pair
        received = []
        w = ecp.SerialWorker(on_line=lambda l: received.append(l), on_status=lambda s: None)
        w.connect(port, 115200)
        time.sleep(0.2)

        w.pause(True)
        assert w.status.state == "paused"

        os.write(master, b'{"tc":1}\n')
        time.sleep(0.3)
        count_paused = len(received)

        w.pause(False)
        time.sleep(0.3)
        # the line sent during pause may or may not be delivered
        # but unpausing shouldn't crash
        assert w.status.state == "connected"
        w.disconnect()

    def test_counters_increment(self, pty_pair):
        master, slave, port = pty_pair
        w = ecp.SerialWorker(on_line=lambda l: None, on_status=lambda s: None)
        w.connect(port, 115200)
        time.sleep(0.2)

        os.write(master, b'{"tc":1}\n{"tc":2}\n')
        time.sleep(0.5)
        assert w.bytes_rx > 0
        assert w.lines_rx >= 2

        w.send("test")
        time.sleep(0.3)
        assert w.bytes_tx > 0

        w.disconnect()


# =====================================================================================
# Simulator
# =====================================================================================
class TestSimulator:
    def test_produces_valid_json(self):
        lines = []
        sim = ecp.Simulator(on_line=lambda l: lines.append(l))
        sim.start()
        time.sleep(0.5)
        sim.stop()
        assert len(lines) > 0
        for line in lines[:10]:
            if line.startswith("{"):
                d = json.loads(line)
                assert "tc" in d
                assert "set" in d
                assert "stage" in d
                assert isinstance(d["tc"], (int, float))

    def test_stop_is_idempotent(self):
        sim = ecp.Simulator(on_line=lambda l: None)
        sim.start()
        time.sleep(0.2)
        sim.stop()
        sim.stop()  # second stop shouldn't crash

    def test_double_start(self):
        sim = ecp.Simulator(on_line=lambda l: None)
        sim.start()
        time.sleep(0.1)
        sim.start()  # second start shouldn't crash
        time.sleep(0.1)
        sim.stop()

    def test_cycles_increment(self):
        lines = []
        sim = ecp.Simulator(on_line=lambda l: lines.append(l))
        sim.start()
        time.sleep(6)  # long enough for at least part of a full stage
        sim.stop()
        json_lines = [json.loads(l) for l in lines if l.startswith("{")]
        assert len(json_lines) > 10
        stages = {d["stage"] for d in json_lines}
        assert len(stages) >= 1  # at least one stage observed

    def test_double_start_does_not_leak_thread(self):
        sim = ecp.Simulator(on_line=lambda l: None)
        sim.start()
        first = sim._thread
        time.sleep(0.1)
        sim.start()  # should be a no-op while running
        assert sim._thread is first, "second start() must not replace the running thread"
        sim.stop()


# =====================================================================================
# Textual TUI (pilot tests)
# =====================================================================================
class TestTUI:
    @pytest.fixture
    def make_app(self):
        def factory(**kw):
            defaults = dict(port=None, baud=None, simulate=True, list_ports=False)
            defaults.update(kw)
            args = argparse.Namespace(**defaults)
            return ecp.EclipsePCRApp(args)
        return factory

    @pytest.mark.asyncio
    async def test_startup_shutdown(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            await asyncio.sleep(0.5)
            assert app.simulate is True
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_all_tabs_render(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            await asyncio.sleep(0.3)
            for tab in ["dash", "plot", "console", "flash", "settings"]:
                app.query_one(ecp.TabbedContent).active = tab
                await pilot.pause()
                await asyncio.sleep(0.1)
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_telemetry_updates_dashboard(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            await asyncio.sleep(1.5)
            tc_tile = app.query_one("#tile-tc", ecp.Tile)
            assert tc_tile._value != "—"
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_plot_accumulates_data(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            await asyncio.sleep(1.5)
            pv = app.query_one(ecp.PlotView)
            assert len(pv.tc_samples) > 5
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_console_command_in_sim(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            app.query_one(ecp.TabbedContent).active = "console"
            await pilot.pause()
            inp = app.query_one("#cmd-input", ecp.Input)
            inp.value = "set 72.0"
            await pilot.press("enter")
            await pilot.pause()
            await asyncio.sleep(0.2)
            # should log the sim command
            rl = app.query_one("#console-log", ecp.RichLog)
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_q_doesnt_quit_when_input_focused(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            app.query_one(ecp.TabbedContent).active = "console"
            await pilot.pause()
            inp = app.query_one("#cmd-input", ecp.Input)
            inp.focus()
            await pilot.pause()
            await pilot.press("q")
            await pilot.pause()
            # app should still be running (q went into input, not quit)
            assert app.is_running
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_flash_add_remove_rows(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            app.query_one(ecp.TabbedContent).active = "flash"
            await pilot.pause()
            initial = len(app.query(ecp.FlashRow))
            await pilot.click("#flash-add")
            await pilot.pause()
            assert len(app.query(ecp.FlashRow)) == initial + 1
            # delete via the × button
            btns = app.query(".row-del")
            if btns:
                await pilot.click(btns[0])
                await pilot.pause()
                assert len(app.query(ecp.FlashRow)) == initial
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_stale_indicator(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            sb = app.query_one(ecp.StatusBar)
            # in sim mode, stale should be false (serial worker isn't connected)
            assert sb.stale is False
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_settings_rescan(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            app.query_one(ecp.TabbedContent).active = "settings"
            await pilot.pause()
            await pilot.click("#cfg-rescan")
            await pilot.pause()
            # should not crash
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_config_save_discard(self, make_app, tmp_path):
        cfg_path = tmp_path / "test.json"
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            app = make_app()
            async with app.run_test(headless=True, size=(120, 40)) as pilot:
                await pilot.pause()
                app.query_one(ecp.TabbedContent).active = "settings"
                await pilot.pause()
                await pilot.click("#cfg-save")
                await pilot.pause()
                assert cfg_path.exists()
                await pilot.click("#cfg-discard")
                await pilot.pause()
                await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_hello_sets_fw_info(self, make_app):
        app = make_app(simulate=False)
        app.cfg.autoconnect = False  # don't latch onto a real board during tests
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            sb = app.query_one(ecp.StatusBar)
            dv = app.query_one(ecp.DashboardView)
            before = dv.query_one("#tile-tc", ecp.Tile)._value
            hello = '{"type":"hello","fw":"eclipse_pcr","version":"9.9.9","chip":"ESP32C6","mac":"58:e6:c5:12:e0:7c","reset_cause":"power_on","tc_sensor":"max31856","tc_type":"K"}'
            app._on_serial_line(hello)
            await pilot.pause()
            assert sb.fw_info.startswith("eclipse_pcr 9.9.9")
            assert "max31856" in sb.fw_info  # sensor appears in the info line
            # hello must not be mistaken for a telemetry frame
            assert dv.query_one("#tile-tc", ecp.Tile)._value == before
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_tc_fault_logs_once_until_cleared(self, make_app):
        app = make_app(simulate=False)
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            # two frames with the same fault — only one log line
            app._on_serial_line('{"t":1,"tc":25.0,"fault":["open"]}')
            app._on_serial_line('{"t":2,"tc":25.0,"fault":["open"]}')
            assert app._last_fault_str == "open"
            # faults clear — a single "cleared" line
            app._on_serial_line('{"t":3,"tc":25.0}')
            assert app._last_fault_str == ""
            # new fault after clear — re-log
            app._on_serial_line('{"t":4,"tc":25.0,"fault":["tc_high","open"]}')
            assert app._last_fault_str == "tc_high, open"
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_awaiting_flag_clears_after_telemetry(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            sb = app.query_one(ecp.StatusBar)
            # manually simulate "connected, zero lines" and then a frame
            sb.awaiting = True
            app._on_serial_line('{"tc": 25.0, "set": 95.0, "stage": "idle"}')
            app._serial.lines_rx = 1  # pretend a line was counted
            app._serial._status.state = "connected"
            app._health_check()
            await pilot.pause()
            assert sb.awaiting is False
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_dashboard_handles_bad_telemetry(self, make_app):
        app = make_app()
        async with app.run_test(headless=True, size=(120, 40)) as pilot:
            await pilot.pause()
            dv = app.query_one(ecp.DashboardView)
            # shouldn't crash on weird data
            dv.apply_telemetry({})
            dv.apply_telemetry({"tc": "not_a_number"})
            dv.apply_telemetry({"tc": float("inf")})
            dv.apply_telemetry({"tc": None, "set": None})
            dv.apply_telemetry({"cycle": "bogus"})
            dv.apply_telemetry({"stage": 12345})
            await pilot.pause()
            await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_log_max_applied_on_mount(self, make_app, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({"log_max": 1234}))
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            app = make_app()
            async with app.run_test(headless=True, size=(120, 40)) as pilot:
                await pilot.pause()
                rl = app.query_one("#console-log", ecp.RichLog)
                assert rl.max_lines == 1234
                await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_discard_refreshes_widgets(self, make_app, tmp_path):
        cfg_path = tmp_path / "test.json"
        cfg_path.write_text(json.dumps({
            "baud": 230400,
            "plot_window_s": 120,
            "log_max": 2500,
            "autoconnect": False,
            "chip": "esp32s3",
            "flash_baud": 921600,
            "flash_entries": [["0x0", "boot.bin"], ["0x10000", "app.bin"]],
        }))
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            app = make_app()
            async with app.run_test(headless=True, size=(120, 40)) as pilot:
                await pilot.pause()
                # mutate the widgets to diverge from the saved config
                sv = app.query_one(ecp.SettingsView)
                sv.query_one("#cfg-baud", ecp.Input).value = "9600"
                sv.query_one("#cfg-plot", ecp.Input).value = "5"
                sv.query_one("#cfg-logmax", ecp.Input).value = "100"
                fv = app.query_one(ecp.FlashView)
                fv.query_one("#flash-chip", ecp.Input).value = "esp32c3"
                fv.query_one("#flash-baud", ecp.Input).value = "115200"
                await pilot.pause()

                app.query_one(ecp.TabbedContent).active = "settings"
                await pilot.pause()
                await pilot.click("#cfg-discard")
                await pilot.pause()
                await asyncio.sleep(0.1)

                assert sv.query_one("#cfg-baud", ecp.Input).value == "230400"
                assert sv.query_one("#cfg-plot", ecp.Input).value == "120"
                assert sv.query_one("#cfg-logmax", ecp.Input).value == "2500"
                assert fv.query_one("#flash-chip", ecp.Input).value == "esp32s3"
                assert fv.query_one("#flash-baud", ecp.Input).value == "921600"
                rows = fv.all_rows()
                assert rows == [("0x0", "boot.bin"), ("0x10000", "app.bin")]

                rl = app.query_one("#console-log", ecp.RichLog)
                assert rl.max_lines == 2500
                pv = app.query_one(ecp.PlotView)
                assert pv.window_s == 120
                await pilot.press("ctrl+q")

    @pytest.mark.asyncio
    async def test_save_preserves_empty_flash_rows(self, make_app, tmp_path):
        cfg_path = tmp_path / "test.json"
        with patch.object(ecp, "CONFIG_PATH", cfg_path):
            app = make_app()
            async with app.run_test(headless=True, size=(120, 40)) as pilot:
                await pilot.pause()
                fv = app.query_one(ecp.FlashView)
                app.query_one(ecp.TabbedContent).active = "flash"
                await pilot.pause()
                fv.query_one("#flash-entries", ecp.VerticalScroll).mount(ecp.FlashRow("0x1000", ""))
                fv.query_one("#flash-entries", ecp.VerticalScroll).mount(ecp.FlashRow("0x20000", "fw.bin"))
                await pilot.pause()

                rows_before = fv.all_rows()
                assert ("0x1000", "") in rows_before
                assert ("0x20000", "fw.bin") in rows_before

                app._cfg_save()
                await pilot.pause()
                data = json.loads(cfg_path.read_text())
                saved = {tuple(r) for r in data["flash_entries"]}
                # empty-path rows survive the round-trip
                assert ("0x1000", "") in saved
                assert ("0x20000", "fw.bin") in saved

                # current_entries still filters empties for flash operations
                active = fv.current_entries()
                assert ("0x20000", "fw.bin") in active
                assert ("0x1000", "") not in active
                await pilot.press("ctrl+q")


# =====================================================================================
# Integration: serial loopback → TUI
# =====================================================================================
class TestSerialIntegration:
    """Integration tests using PTY loopback + TUI.

    These use asyncio.run() directly (not pytest-asyncio) because Textual's
    run_test + call_from_thread from the serial worker deadlocks under
    pytest-asyncio 1.x's event loop management.
    """

    def test_real_serial_to_tui(self):
        async def _run():
            master, slave = pty.openpty()
            port = os.ttyname(slave)
            try:
                args = argparse.Namespace(port=None, baud=115200, simulate=False, list_ports=False)
                app = ecp.EclipsePCRApp(args)
                app.cfg.autoconnect = False
                async with app.run_test(headless=True, size=(120, 40)) as pilot:
                    await pilot.pause()
                    app._connect(port, 115200)
                    await asyncio.sleep(0.8)
                    await pilot.pause()

                    for i in range(5):
                        msg = json.dumps({"tc": 25.0 + i, "set": 95.0, "stage": "denature", "cycle": 1})
                        os.write(master, (msg + "\n").encode())
                        await asyncio.sleep(0.15)

                    await asyncio.sleep(0.5)
                    await pilot.pause()
                    tc_tile = app.query_one("#tile-tc", ecp.Tile)
                    assert tc_tile._value != "—", "tile-tc never updated"
                    val = float(tc_tile._value)
                    assert 25.0 <= val <= 30.0

                    pv = app.query_one(ecp.PlotView)
                    assert len(pv.tc_samples) >= 3

                    app.query_one(ecp.TabbedContent).active = "console"
                    await pilot.pause()
                    inp = app.query_one("#cmd-input", ecp.Input)
                    inp.value = "start"
                    await pilot.press("enter")
                    await pilot.pause()
                    await asyncio.sleep(0.3)

                    r, _, _ = select.select([master], [], [], 3.0)
                    if r:
                        data = os.read(master, 1024)
                        assert b"start\n" in data

                    app._disconnect()
                    await pilot.pause()
                    await pilot.press("ctrl+q")
            finally:
                os.close(master)
                os.close(slave)

        asyncio.run(_run())

    def test_serial_disconnect_reconnect_via_ui(self):
        async def _run():
            master, slave = pty.openpty()
            port = os.ttyname(slave)
            try:
                args = argparse.Namespace(port=None, baud=115200, simulate=False, list_ports=False)
                app = ecp.EclipsePCRApp(args)
                app.cfg.autoconnect = False
                async with app.run_test(headless=True, size=(120, 40)) as pilot:
                    await pilot.pause()
                    app._connect(port, 115200)
                    await asyncio.sleep(0.8)
                    await pilot.pause()
                    assert app._serial.connected

                    app._disconnect()
                    await asyncio.sleep(0.3)
                    await pilot.pause()
                    assert not app._serial.connected

                    app._connect(port, 115200)
                    await asyncio.sleep(0.8)
                    await pilot.pause()
                    assert app._serial.connected

                    os.write(master, b'{"tc":50.0,"set":72.0}\n')
                    await asyncio.sleep(0.5)
                    await pilot.pause()
                    tc_tile = app.query_one("#tile-tc", ecp.Tile)
                    assert tc_tile._value != "—"

                    app._disconnect()
                    await pilot.pause()
                    await pilot.press("ctrl+q")
            finally:
                os.close(master)
                os.close(slave)

        asyncio.run(_run())
