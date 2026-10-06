import asyncio
import time

from misc import logger, memoize

from .state import DeviceState
from .pjlink import PJLink

# Simulated transition times (short, for a snappy dummy — real projectors take
# tens of seconds to minutes).
WARM_TIME = 20   # OFF -> WARMING(PARTIAL) -> ON
COOL_TIME = 20   # ON  -> COOLING(PARTIAL) -> OFF
LAMP_INTERVAL = 15  # seconds between lamp-hour re-checks while ON


class SimProjector(PJLink):
    """Simulated PJLink projector for the dummy-mode stack.

    Keeps PJLink's state/event contract — tri-state `is_online`,
    `should_wake`/`should_shutdown`, `lamps`, warming/cooling — but is driven by
    timers instead of a real PJLink interface. Starts OFF; `wake` runs
    WARMING(PARTIAL) -> ON, `shutdown` runs COOLING(PARTIAL) -> OFF. Lamp hours
    accumulate from REAL elapsed ON-time (seeded with a plausible per-device
    baseline so the value is non-zero and varies between projectors). If the
    projector is wired to a (Sim)PDU power feed, wake energizes it and shutdown
    cuts it after cool-down.

    It self-manages `is_online` and does NOT listen to the sim_probes ping
    stream, so the external swarm cannot fight the transition state machine.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Deterministic, varied baseline so the UI shows realistic (non-zero)
        # lamp hours without needing randomness.
        base_hours = 400 + (self.id * 137) % 6000
        self._lamp_base_seconds = base_hours * 3600
        self._lamp_on_since = None
        self._state['lamps'] = [[base_hours, 0]]
        # Replace PJLink's interface poller ('PJLink watch') with a lamp ticker.
        self.update_methods = [(name, method)
                               for name, method in self.update_methods
                               if name != 'PJLink watch']
        self.update_methods.append(('SimProjector tick', self._sim_tick))

    async def online_event(self, _, event_type, value):
        # SimProjector owns warming/cooling/lamps itself. Suppress PJLink's
        # reset-on-not-ON behaviour, which would wipe the simulated state the
        # moment we set PARTIAL during a warm-up/cool-down.
        return

    def _lamp_hours(self):
        total = self._lamp_base_seconds
        if self._lamp_on_since is not None:
            total += time.time() - self._lamp_on_since
        return int(total // 3600)

    @memoize(LAMP_INTERVAL)
    async def _sim_tick(self):
        # Re-publish lamp hours while ON; the count tracks REAL elapsed ON-time
        # (not the update cadence, which was the earlier racing bug). Memoized to
        # the codebase's watcher pattern so it re-runs at most every LAMP_INTERVAL.
        if self.is_online == DeviceState.ON:
            hours = self._lamp_hours()
            if not self._state['lamps'] or hours != self._state['lamps'][0][0]:
                self._state['lamps'] = [[hours, 1]]
                await self.event('lamps', self._state['lamps'])

    async def _warm_up(self):
        self._state['cooling'] = False
        self._state['warming'] = True
        await self.set_is_online(DeviceState.PARTIAL)
        await asyncio.sleep(WARM_TIME)
        self._state['warming'] = False
        self._lamp_on_since = time.time()
        await self.set_is_online(DeviceState.ON)
        # surface the lamp as "on" immediately
        self._state['lamps'] = [[self._lamp_hours(), 1]]
        await self.event('lamps', self._state['lamps'])
        await self.set_should_wake(False)

    async def _cool_down(self):
        self._state['warming'] = False
        self._state['cooling'] = True
        # bank the ON-time accumulated so far
        if self._lamp_on_since is not None:
            self._lamp_base_seconds += time.time() - self._lamp_on_since
            self._lamp_on_since = None
        await self.set_is_online(DeviceState.PARTIAL)
        await asyncio.sleep(COOL_TIME)
        self._state['cooling'] = False
        # Bypass the 3-strike OFF debounce (meant for flaky pings) so a
        # deterministic sim shutdown registers immediately.
        self._offline_counter = 3
        await self.set_is_online(DeviceState.OFF)
        await self.set_should_shutdown(False)

    async def wake(self, *_, **__):
        await self.cancel()
        if self.is_online == DeviceState.ON:
            return
        logger.debug('SimProjector waking %s', self.name)
        await self.set_should_wake(True)
        await self.set_power(True)   # energize the PDU feed if the projector is wired to one
        if 'wake' in self.tasks:
            self.tasks['wake'].cancel()
        task = asyncio.create_task(self._try_method(self._warm_up))
        self.tasks['wake'] = task
        task.add_done_callback(self._delete_task('wake'))

    async def shutdown(self, *_, **__):
        await self.cancel()
        if self.is_online == DeviceState.OFF:
            return
        logger.debug('SimProjector shutting down %s', self.name)
        await self.set_should_shutdown(True)
        if 'shutdown' in self.tasks:
            self.tasks['shutdown'].cancel()
        task = asyncio.create_task(self._try_method(self._cool_down))
        self.tasks['shutdown'] = task

        def power_off_done(finished):
            logger.debug('%s power_off_done', self.name)
            self.power_off()   # cut the PDU feed after cool-down
            self._delete_task('shutdown')(finished)
        task.add_done_callback(power_off_done)
