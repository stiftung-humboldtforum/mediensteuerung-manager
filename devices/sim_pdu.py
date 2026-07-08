from misc import logger

from .state import DeviceState
from .snmp_gude import GudePDU

# Dummy PDUs may not carry a NetBox model string that GudePDU can map to an
# outlet count, so we fix a default instead of deriving it from the model. Keep
# this in sync with seed_netbox.py seed_power() feeds_per_panel so the PDU
# detail renders exactly the wired outlets (no phantom always-on switches). Any
# outlet index the power-seed references must be < this.
DEFAULT_OUTLETS = 8


class SimPDU(GudePDU):
    """Simulated Gude PDU for the dummy-mode stack.

    Keeps GudePDU's powerfeed state and `powerfeeds` event contract (so the
    frontend renders the same power switches and the PowerMixin round-trip works
    unchanged) but replaces all SNMP I/O with in-memory state. Outlets start
    energized (ON) and the PDU is always reachable — it self-manages `is_online`
    and does NOT consume the sim_probes ping stream, so there is no collision
    with the external probe swarm.
    """

    @property
    def num_powerfeeds(self):
        return DEFAULT_OUTLETS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, should_icmp=False, **kwargs)
        # override GudePDU's [-1]*N "unknown" init: a simulated PDU boots with
        # every outlet energized.
        self._state['powerfeeds'] = [True] * self.num_powerfeeds

    async def send_icmp(self):
        # A simulated PDU is always reachable — no ping.
        await self.set_is_online(DeviceState.ON)

    async def _read_powerfeeds(self):
        # In-memory state is authoritative; nothing to read back over SNMP.
        return

    async def _write_powerfeeds(self, powerfeeds=None):
        if powerfeeds is None:
            return
        self._state['powerfeeds'] = list(powerfeeds)
        logger.debug('%s powerfeeds %s', self.name, self._state['powerfeeds'])
        await self.event('powerfeeds', self._state['powerfeeds'])
