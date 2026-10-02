"""Unit-Tests für den Power-Cycle nach einem Computer-Shutdown.

Pinnt den Fix vom 2026-08-21 (Hörraum-Ausfall): Nach einem erfolgreichen
Soft-Shutdown schaltet der Manager den PDU-Feed 10 s später ab und hält ihn
30 s stromlos (vorher 10 s). Bei 10 s blieb 3900-zg-re-02 WoL-taub, eine
Trennung über 19,6 s weckte ihn.

Keine Hardware / kein Broker / kein Manager nötig — alles gemockt. Wie
test_scram.py im Manager-Image ausführen (hat die Import-Abhängigkeiten):

    pip install pytest && python -m pytest tests/test_power_cycle.py -v
"""
import asyncio
from unittest import mock

from devices.computer import Computer
from devices.mixins import power_mixin
from devices.mixins.power_mixin import PowerMixin
from devices.state import DeviceState


def _run_cycle(**kwargs):
    """async_power_cycle ausführen, Schlafzeiten und Feed-Schaltungen mitschreiben."""
    m = PowerMixin.__new__(PowerMixin)    # __init__ braucht Manager + Client
    sleeps, power = [], []

    async def set_power(state):
        power.append(state)
        return True

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    m.set_power = set_power
    with mock.patch.object(power_mixin.asyncio, 'sleep', fake_sleep):
        asyncio.run(m.async_power_cycle(**kwargs))
    return sleeps, power


class FakeComputer:
    """Minimaler Ersatz mit genau dem, was Computer.shutdown anfasst."""

    name = 'pc'

    def __init__(self, online=DeviceState.ON):
        self.is_online = online
        self.tasks = {}
        self.cycles = []                  # power_cycle(wait, off_wait)-Aufrufe

    async def cancel(self):
        pass

    async def set_should_shutdown(self, _):
        pass

    async def _try_method(self, method, error_cb=None, **kwargs):
        if asyncio.iscoroutine(error_cb):
            error_cb.close()              # nur im Fehlerfall gebraucht
        await method(**kwargs)

    async def _shutdown(self):
        pass

    def _delete_task(self, name):
        def wrap(_):
            self.tasks.pop(name, None)
        return wrap

    def power_cycle(self, wait=10, off_wait=None):
        self.cycles.append((wait, off_wait))


def _shutdown(dev):
    async def scenario():
        await Computer.shutdown(dev)
        task = dev.tasks.get('shutdown')
        if task:
            await task
            await asyncio.sleep(0)        # Done-Callbacks laufen per call_soon
    asyncio.run(scenario())


# --- PowerMixin ------------------------------------------------------------

def test_power_cycle_off_wait_sets_dead_time():
    sleeps, power = _run_cycle(wait=10, off_wait=30)
    assert sleeps == [10, 30], "erst 10 s Marge, dann 30 s stromlos"
    assert power == [False, True]


def test_power_cycle_without_off_wait_is_unchanged():
    # Bisheriges Verhalten für alle anderen Aufrufer: stromlos so lang wie wait.
    sleeps, power = _run_cycle(wait=10)
    assert sleeps == [10, 10]
    assert power == [False, True]


# --- Computer.shutdown -----------------------------------------------------

def test_shutdown_power_cycles_with_30s_dead_time():
    dev = FakeComputer(online=DeviceState.ON)
    _shutdown(dev)
    assert dev.cycles == [(10, 30)]
    assert 'shutdown' not in dev.tasks, "Shutdown-Task muss aufgeräumt sein"


def test_shutdown_of_offline_computer_does_not_power_cycle():
    dev = FakeComputer(online=DeviceState.OFF)
    _shutdown(dev)
    assert dev.cycles == []
