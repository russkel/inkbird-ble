# /// script
# requires-python = ">=3.11"
# dependencies = ["inkbird-ble", "textual>=0.80"]
#
# [tool.uv.sources]
# inkbird-ble = { path = "..", editable = true }
# ///
"""Live terminal dashboard for an INKBIRD INT-11I-B probe.

Holds a GATT connection open and subscribes to the ``ff01`` temperature and
``2a19`` battery characteristics, decoding each update with
``INKBIRDBluetoothDeviceData`` so the values shown match the library.

Usage::

    uv run examples/int_11i_b_tui.py [ADDRESS]

With no address, the first device advertising as ``INT-11I-B`` is used.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar

from bleak import BleakScanner
from bleak.exc import BleakError
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, Static

from inkbird_ble import INKBIRDBluetoothDeviceData, Model
from inkbird_ble.parser import (
    INT_11I_B_BATTERY_CHARACTERISTIC_UUID,
    INT_11I_B_STATUS_CHARACTERISTIC_UUID,
    INT_11I_B_TEMP_CHARACTERISTIC_UUID,
)

if TYPE_CHECKING:
    from bleak import BLEDevice
    from bleak.backends.characteristic import BleakGATTCharacteristic
    from textual.binding import BindingType

SCAN_TIMEOUT = 20.0
MAX_BACKOFF = 30.0
TEMP_UUID = str(INT_11I_B_TEMP_CHARACTERISTIC_UUID)
BATTERY_UUID = str(INT_11I_B_BATTERY_CHARACTERISTIC_UUID)
STATUS_UUID = str(INT_11I_B_STATUS_CHARACTERISTIC_UUID)


class ProbeApp(App[None]):
    TITLE = "INKBIRD INT-11I-B"
    CSS = """
    Screen { align: center middle; }
    #panel { width: 60; height: auto; border: round $accent; padding: 1 2; }
    #status { color: $text-muted; margin-bottom: 1; }
    #temp { text-style: bold; content-align: center middle; height: 3;
            border: tall $primary; margin-bottom: 1; }
    #batteries Static { width: 1fr; }
    #meta { color: $text-muted; margin-top: 1; }
    """
    BINDINGS: ClassVar[list[BindingType]] = [
        ("u", "toggle_units", "°C/°F"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, address: str | None) -> None:
        super().__init__()
        self.address = address
        self.temperature = b""
        self.battery = b""
        self.status = b""
        self.celsius: float | None = None
        self.charging: bool | None = None
        self.batteries: dict[str, float] = {}
        self.fahrenheit_units = False

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="panel"):
            yield Static("Starting…", id="status")
            yield Static("--", id="temp")
            with Horizontal(id="batteries"):
                yield Static("Station: --", id="station")
                yield Static("Probe: --", id="probe")
            yield Static("Probe: --", id="dock")
            yield Static("", id="meta")
        yield Footer()

    def on_mount(self) -> None:
        self.run_worker(self.connection_loop(), exclusive=True)

    def action_toggle_units(self) -> None:
        self.fahrenheit_units = not self.fahrenheit_units
        self.render_values()

    def set_status(self, text: str) -> None:
        self.query_one("#status", Static).update(text)

    async def find_device(self) -> BLEDevice | None:
        if self.address:
            return await BleakScanner.find_device_by_address(
                self.address, timeout=SCAN_TIMEOUT
            )
        return await BleakScanner.find_device_by_filter(
            lambda dev, adv: (adv.local_name or dev.name or "").lower() == "int-11i-b",
            timeout=SCAN_TIMEOUT,
        )

    async def connection_loop(self) -> None:
        backoff = 1.0
        while True:
            target = self.address or "an INT-11I-B"
            self.set_status(f"Scanning for {target}…")
            try:
                device = await self.find_device()
                if device is None:
                    self.set_status(f"{target} not found, retrying…")
                else:
                    await self.stream(device)
                    backoff = 1.0
            except (BleakError, TimeoutError) as err:
                self.set_status(f"Error: {err or type(err).__name__}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

    async def stream(self, device: BLEDevice) -> None:
        """Connect, read initial values, then follow notifications."""
        self.address = device.address
        self.set_status(f"Connecting to {device.address}…")
        disconnected = asyncio.Event()
        client = await establish_connection(
            BleakClientWithServiceCache,
            device,
            device.name or device.address,
            disconnected_callback=lambda _: disconnected.set(),
        )
        try:
            self.temperature = bytes(await client.read_gatt_char(TEMP_UUID))
            self.battery = bytes(await client.read_gatt_char(BATTERY_UUID))
            self.status = bytes(await client.read_gatt_char(STATUS_UUID))
            self.decode()
            await client.start_notify(TEMP_UUID, self.on_temperature)
            await client.start_notify(BATTERY_UUID, self.on_battery)
            await client.start_notify(STATUS_UUID, self.on_status)
            self.set_status(f"Connected to {device.name} ({device.address})")
            await disconnected.wait()
            self.set_status("Disconnected, reconnecting…")
        finally:
            with contextlib.suppress(BleakError):
                await client.disconnect()

    def on_temperature(self, _: BleakGATTCharacteristic, data: bytearray) -> None:
        self.temperature = bytes(data)
        self.decode()

    def on_battery(self, _: BleakGATTCharacteristic, data: bytearray) -> None:
        self.battery = bytes(data)
        self.decode()

    def on_status(self, _: BleakGATTCharacteristic, data: bytearray) -> None:
        self.status = bytes(data)
        self.decode()

    def decode(self) -> None:
        # Same concatenated payload the library's poll path builds
        # (temperature bytes 0-1, battery bytes 0-1, ff03 byte 0) so its
        # decoder applies. A fresh parser per decode: SensorData accumulates
        # values across updates, so a reused one would keep reporting the last
        # temperature after the library starts dropping it (probe docked).
        payload = self.temperature[:2] + self.battery[:2]
        if len(payload) == 4:  # noqa: PLR2004
            payload += self.status[:1]
        parser = INKBIRDBluetoothDeviceData(Model.INT_11I_B)
        parser._update_int_11i_b_from_raw(payload)  # noqa: SLF001
        update = parser._finish_update()  # noqa: SLF001
        self.celsius = None
        for key, value in update.entity_values.items():
            if key.key == "temperature":
                self.celsius = value.native_value
            elif key.key in ("station_battery", "probe_battery"):
                self.batteries[key.key] = value.native_value
        self.charging = None
        for key, value in update.binary_entity_values.items():
            if key.key == "probe_charging":
                self.charging = value.native_value
        self.render_values()

    def render_values(self) -> None:
        if self.celsius is not None:
            if self.fahrenheit_units:
                temp = f"{self.celsius * 9 / 5 + 32:.1f} °F"
            else:
                temp = f"{self.celsius:.1f} °C"
            self.query_one("#temp", Static).update(temp)
        elif self.temperature:
            self.query_one("#temp", Static).update("In cradle / no reading")
        if self.charging is not None:
            state = "In cradle, charging" if self.charging else "Out of cradle"
            self.query_one("#dock", Static).update(f"Probe: {state}")
        for key, label, widget in (
            ("station_battery", "Station", "#station"),
            ("probe_battery", "Probe", "#probe"),
        ):
            if key in self.batteries:
                text = f"{label}: {self.batteries[key]:.0f}%"
                self.query_one(widget, Static).update(text)
        now = datetime.now().astimezone().strftime("%H:%M:%S")
        self.query_one("#meta", Static).update(
            f"Updated {now}  ·  ff01={self.temperature.hex()}"
            f"  2a19={self.battery.hex()}  ff03={self.status.hex()}"
        )


def main() -> None:
    arg_parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    arg_parser.add_argument("address", nargs="?", help="Bluetooth address")
    ProbeApp(arg_parser.parse_args().address).run()


if __name__ == "__main__":
    main()
