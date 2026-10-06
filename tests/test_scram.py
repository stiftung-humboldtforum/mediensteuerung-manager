"""Fire-safety unit tests for Tag.scram / Tag.unscram (manager/tags.py).

Pins the behaviour fixed 2026-07-08 after an adversarial multi-agent review of
the first scram rewrite. A fire alarm must:
  * reach 'ctrl mon' control-only devices — dispatch on `_capabilities`, NOT the
    public `capabilities` property (which returns [] to hide them from the UI);
  * classify by CAPABILITY, not NetBox role (live roles like 'Video Player' /
    'Medienstation Linux Easire' match none of the role classes);
  * cut power (PDU feed) for devices that can neither mute nor shut down
    (reboot-only signage, wake-only, driverless);
  * leave power/network infrastructure (PDUs, switches) UP;
  * never let one device's exception abort the alarm for the rest.

No hardware / broker / manager needed — devices are mocked. Run inside the
manager image (has the import deps):

    pip install pytest && python -m pytest tests/test_scram.py -v
"""
import asyncio

from devices.state import DeviceState
from tags import Tag


class FakeDevice:
    """Minimal stand-in exposing exactly what Tag.scram touches."""

    def __init__(self, name, caps, role='PC', has_pdu=False, raises=None):
        self.name = name
        self._capabilities = list(caps)       # scram dispatches on THIS
        self.role = {'name': role}            # _role_name() reads role['name']
        self._has_pdu = has_pdu               # set_power() returns this
        self._raises = raises                 # method name that should raise
        self.calls = []                       # dispatched methods, in order
        self.power = []                       # set_power(state) history
        self._state = {'is_online': DeviceState.ON}

    def _make(self, method):
        async def _m():
            if self._raises == method:
                raise RuntimeError(f'{self.name}.{method} boom')
            self.calls.append(method)
        return _m

    def __getattr__(self, name):
        # mute/shutdown/wake/unmute/reboot are dispatched via getattr(dev, name)
        if name in ('mute', 'unmute', 'shutdown', 'wake', 'reboot'):
            return self._make(name)
        raise AttributeError(name)

    async def set_power(self, state):
        self.power.append(state)
        return self._has_pdu

    async def wait_for(self, *states):
        return  # already terminal for the test


def _tag(devices):
    t = Tag.__new__(Tag)          # bypass __init__ (it needs a live manager)
    t.name = 'testtag'
    t.manager = None
    t.devices = list(devices)
    return t


def _run(coro):
    return asyncio.run(coro)


# --- the fixed behaviour ---------------------------------------------------

def test_ctrl_mon_device_is_reached():
    # A 'ctrl mon' device hides its public capabilities ([]) but keeps real
    # _capabilities. scram dispatches on _capabilities -> it must be reached.
    d = FakeDevice('ctrl-mon', ['mute', 'shutdown'], role='PC')
    _run(_tag([d]).scram())
    assert 'mute' in d.calls, "ctrl-mon device must still be muted in an alarm"


def test_role_unmatched_device_is_shut_down():
    # Live role matches no role class; capability-based dispatch still reaches it.
    d = FakeDevice('mst', ['wake', 'shutdown', 'reboot'],
                   role='Medienstation Linux Easire')
    _run(_tag([d]).scram())
    assert d.calls == ['shutdown']


def test_mutable_is_muted_not_shut_down():
    d = FakeDevice('audio', ['mute', 'shutdown'], role='PC')
    _run(_tag([d]).scram())
    assert d.calls == ['mute'], "a mute-capable device is muted, not shut down"


def test_reboot_only_gets_power_cut():
    # BrightSign-style: only 'reboot' -> neither muted nor shut down -> PDU cut.
    d = FakeDevice('signage', ['reboot'], role='Video Player', has_pdu=True)
    _run(_tag([d]).scram())
    assert d.calls == [], "reboot is never a scram action"
    assert d.power == [False], "reboot-only device must have its power cut"


def test_driverless_no_pdu_is_attempted_then_unreached():
    d = FakeDevice('cam', [], role='Kamera', has_pdu=False)
    _run(_tag([d]).scram())
    assert d.calls == []
    assert d.power == [False]  # power-cut attempted; returns False -> unreached


def test_infrastructure_left_up():
    pdu = FakeDevice('pdu', ['shutdown'], role='PDU')
    sw = FakeDevice('sw', ['shutdown'], role='Netzwerkswitch')
    _run(_tag([pdu, sw]).scram())
    assert pdu.calls == [] and pdu.power == []
    assert sw.calls == [] and sw.power == []


def test_one_device_exception_does_not_abort_the_alarm():
    bad = FakeDevice('bad', ['shutdown'], role='PC', raises='shutdown')
    good = FakeDevice('good', ['shutdown'], role='PC')
    _run(_tag([bad, good]).scram())   # must not raise
    assert 'shutdown' in good.calls, "a raising device must not skip the others"


def test_unscram_unmutes_and_wakes_and_powers_on():
    muted = FakeDevice('audio', ['mute', 'unmute', 'shutdown'], role='PC')
    off = FakeDevice('pc', ['wake', 'shutdown'], role='PC')
    signage = FakeDevice('signage', ['reboot'], role='Video Player', has_pdu=True)
    _run(_tag([muted, off, signage]).unscram())
    assert muted.calls == ['unmute']
    assert off.calls == ['wake']
    assert signage.power == [True]   # power restored to the power-cut device
