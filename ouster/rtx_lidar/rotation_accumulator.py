"""Assemble Isaac Sim RTX lidar GenericModelOutput (GMO) frames into complete Ouster rotations.

The node only needs one thing from the sensor: for every rotation, an (H, W) image of first-return
range and intensity indexed by (emitter, tick) -- the staggered layout a real Ouster puts in its
packets. How that image is obtained depends on the Isaac Sim version:

* SensorRotationAccumulator (Isaac Sim 6.0+): the sensor model accumulates the rotation itself
  (omni:sensor:Core:accumulateOutputs=True) and the annotator delivers one complete rotation per
  scan period, which stays readable for several node reads.
* BatchRotationAccumulator (Isaac Sim 5.1, or any prim without accumulateOutputs): the annotator
  delivers one batch of ticks per render frame and the rotation is stitched together here.

create_rotation_accumulator() picks the implementation from the prim's schema and authors the
attribute that implementation depends on; call it before the render product is created, since the
sensor output attributes are "runtime changes not supported".
"""
import numpy as np

GMO_MAGIC = 0x4E474D4F
_FLAG_VALID = 1 << 6  # ElementFlags::VALID in GenericModelOutputTypes.h
_ACCUMULATE_OUTPUTS = "omni:sensor:Core:accumulateOutputs"

_gmo_parser = None


def parse_gmo(data):
    """Parse GenericModelOutput annotator data on Isaac Sim 5.1 and 6.0+.

    6.0 deprecates isaacsim.sensors.rtx (get_gmo_data) in favour of
    isaacsim.sensors.experimental.rtx (parse_generic_model_output_data); the
    struct fields used here are identical in both. The import is resolved once
    and cached: a failed import is not cached by Python and would re-scan
    sys.path on every tick under 5.1.
    """
    global _gmo_parser
    if _gmo_parser is None:
        try:
            from isaacsim.sensors.experimental.rtx import parse_generic_model_output_data as parser
        except ImportError:
            from isaacsim.sensors.rtx import get_gmo_data as parser
        _gmo_parser = parser
    return _gmo_parser(data)


def schema_defines(prim, name):
    """True if the prim's applied schemas define the attribute `name`.

    HasAttribute() is also true for an opinion authored against another schema
    version, e.g. a 5.1-authored asset (or a stage this node saved on 5.1, which
    carries auxOutputType=BASIC) opened in 6.0 -- exactly the case the version
    branches have to tell apart.
    """
    return bool(prim.GetPrimDefinition().GetAttributeDefinition(name))


def create_rotation_accumulator(prim, width, height, max_returns):
    """Choose the accumulation strategy for this OmniLidar prim and configure the prim for it."""
    if schema_defines(prim, _ACCUMULATE_OUTPUTS):
        accumulator_class = SensorRotationAccumulator
    else:
        accumulator_class = BatchRotationAccumulator
    accumulator_class.configure_prim(prim)
    return accumulator_class(width, height, max_returns)


class RotationAccumulator:
    """Turns a stream of GMO frames into complete rotations.

    After add() returns True, `range_m` and `intensity` hold the newest rotation as (H, W) float32
    images (0 = no return) and `coverage` the fraction of its columns that data reached.
    """

    def __init__(self, width, height, max_returns):
        self.width = int(width)
        self.height = int(height)
        self.max_returns = max(1, int(max_returns))
        self.range_m = None
        self.intensity = None
        self.coverage = 1.0
        self._last_key = None

    @staticmethod
    def configure_prim(prim):
        """Author the sensor attributes this strategy relies on (before the render product exists)."""
        raise NotImplementedError

    def reset(self):
        """Forget any partial rotation, e.g. when streaming restarts or sim time jumps back."""
        self._last_key = None

    def add(self, gmo):
        """Feed one parsed GMO frame; return True if a new rotation was published."""
        # The annotator keeps returning its latest frame until the sensor produces a new one.
        key = (int(gmo.frameId), int(gmo.timestampNs))
        if key == self._last_key:
            return False
        if self._last_key is not None and key[1] < self._last_key[1]:
            self.reset()  # timeline stopped/rewound
        self._last_key = key
        return self._add_frame(gmo)

    def _add_frame(self, gmo):
        raise NotImplementedError

    def _publish(self, range_m, intensity, coverage):
        self.range_m = range_m
        self.intensity = intensity
        self.coverage = float(coverage)

    @staticmethod
    def _mask_invalid(distance, intensity, flags):
        """Zero range and intensity for returns the model flags invalid (dropped/no hit)."""
        valid = ((flags & _FLAG_VALID) != 0) & np.isfinite(distance) & (distance > 0)
        return (np.where(valid, distance, 0).astype(np.float32),
                np.where(valid, np.nan_to_num(intensity), 0).astype(np.float32))


class SensorRotationAccumulator(RotationAccumulator):
    """Isaac Sim 6.0+: the sensor model accumulates each rotation (accumulateOutputs=True).

    Measured headless on 6.0 (Lidar_Reflective_Room-minimal.usd, OS0 1024x10, 2 returns): one fresh
    GMO per 100 ms of sim time, each exactly one rotation -- ticks 0..W-1, timeOffSetNs rising
    monotonically from ~1 us to 99.9 ms, including the first rotation after play -- repeated by the
    annotator for ~6 node reads. A superseded frame therefore no longer costs columns: the
    batch path lost 18-49 frames per 600 updates in the same scene, this path none.

    The buffer is dense (tick, emitter, return) and is decoded by POSITION, not by tickId: for
    ticks 0..170 the (emitter 0, slot 0) element carries tickId 853+k (the start of the rotation's
    last render frame) while its data belongs at tick k. Placed by position every valid return lands
    within 0.041 deg of the model azimuth; placed by the reported tickId those 171 returns were
    60 deg off. The layout check below tolerates exactly that mislabel and nothing else.
    """

    @staticmethod
    def configure_prim(prim):
        prim.GetAttribute(_ACCUMULATE_OUTPUTS).Set(True)

    def _add_frame(self, gmo):
        shape = (self.width, self.height, self.max_returns)
        if gmo.numElements != self.width * self.height * self.max_returns:
            raise ValueError(f"Accumulated GMO has {gmo.numElements} elements, expected a full rotation "
                             f"of {shape[0]}x{shape[1]}x{shape[2]} (ticks x emitters x returns)")
        emitters = np.asarray(gmo.emitterId).reshape(shape)
        ticks = (np.asarray(gmo.tickId) % self.width).reshape(shape)
        if not np.array_equal(emitters[:, :, 0], np.broadcast_to(np.arange(self.height), shape[:2])):
            raise ValueError("Accumulated GMO is not in (tick, emitter, return) order")
        mislabelled = ticks != np.arange(self.width)[:, None, None]
        mislabelled[:, 0, 0] = False  # known 6.0 mislabel, see class docstring
        if np.any(mislabelled):
            raise ValueError("Accumulated GMO tick labels do not match their buffer positions")
        distance = np.asarray(gmo.z).reshape(shape)[:, :, 0]
        intensity = np.asarray(gmo.scalar).reshape(shape)[:, :, 0]
        flags = np.asarray(gmo.flags).reshape(shape)[:, :, 0]
        range_m, intensity = self._mask_invalid(distance, intensity, flags)
        self._publish(range_m.T.copy(), intensity.T.copy(), 1.0)
        return True


class BatchRotationAccumulator(RotationAccumulator):
    """Isaac Sim 5.1 (and 6.0 prims without accumulateOutputs): stitch per-frame tick batches.

    Each GMO holds the ticks fired during one render frame, dense in (tick, emitter, return) order.
    Returns are placed at [emitterId, tickId % W]. A rotation is published when tick W-1 arrives
    or, if the batch carrying it was lost, when the next rotation starts, provided MIN_COVERAGE of
    its columns were filled; columns no batch reached go out as zeros.

    Row key is emitterId: it indexes the emitterState azimuth/elevation tables the Ouster beam
    angles are built from (GMO channelId equals it, 0-based, but channelId is the detector mapping
    and 6.0's schema default table is 1-based). The first return is taken by position within each
    (tick, emitter) run, not by echoId: 6.0 BASIC reports echoId 0 for both return slots, so an
    echoId == 0 filter kept the always-invalid second slot, which then overwrote the first.
    Position rank equals echoId on 100% of a 5.1 capture.
    """

    # The annotator only exposes its latest frame, so when two sensor frames land between two node
    # reads the older one is lost upstream (GMO frameId steps by 2, 33.3 ms of sim time, ~171 of
    # 1024 columns): measured in Lidar_Reflective_Room-minimal.usd over 600 updates, 18-49 lost
    # frames on 6.0 and 0-19 on 5.1, uncorrelated with slow updates. Requiring full coverage
    # dropped every such rotation (6.0 streamed ~7 Hz instead of 10). 0.5 tolerates one or two
    # lost 60 Hz frames per rotation while still rejecting the partial first rotation after play.
    MIN_COVERAGE = 0.5

    def __init__(self, width, height, max_returns):
        super().__init__(width, height, max_returns)
        self._range = np.zeros((self.height, self.width), dtype=np.float32)
        self._intensity = np.zeros((self.height, self.width), dtype=np.float32)
        self._columns = np.zeros(self.width, dtype=bool)
        self._last_tick = None
        self._published = False

    @staticmethod
    def configure_prim(prim):
        # 6.0 Ouster assets author accumulateOutputs=True, which this strategy cannot consume.
        if schema_defines(prim, _ACCUMULATE_OUTPUTS):
            prim.GetAttribute(_ACCUMULATE_OUTPUTS).Set(False)

    def reset(self):
        super().reset()
        self._clear_rotation()
        self._last_tick = None

    def _clear_rotation(self):
        self._range.fill(0)
        self._intensity.fill(0)
        self._columns.fill(False)
        self._published = False

    def _publish_rotation(self):
        self._publish(self._range.copy(), self._intensity.copy(), np.mean(self._columns))
        self._published = True

    def _add_frame(self, gmo):
        emitters = np.asarray(gmo.emitterId)
        ticks = np.asarray(gmo.tickId)
        distance = np.asarray(gmo.z)
        intensity = np.asarray(gmo.scalar)
        flags = np.asarray(gmo.flags)
        if any(values.size != gmo.numElements for values in (emitters, ticks, distance, intensity, flags)):
            raise ValueError("GMO requires BASIC auxiliary data with one firing ID per element")
        if np.any(emitters >= self.height):
            raise ValueError("GMO firing IDs do not match the advertised sensor dimensions")
        ticks = ticks % self.width
        run_key = ticks.astype(np.int64) * self.height + emitters
        run_start = np.r_[True, run_key[1:] != run_key[:-1]]
        position = np.arange(run_key.size)
        first = position == np.maximum.accumulate(np.where(run_start, position, 0))
        emitters, ticks = emitters[first], ticks[first]
        distance, intensity = self._mask_invalid(distance[first], intensity[first], flags[first])

        published = False
        for indices in np.split(np.arange(ticks.size), np.flatnonzero(ticks[1:] < ticks[:-1]) + 1):
            if not indices.size:
                continue
            segment_ticks = ticks[indices]
            if self._last_tick is not None and segment_ticks[0] < self._last_tick:
                # Wrapped without seeing tick W-1: the batch carrying the end of the previous
                # rotation was lost, so publish that rotation here instead.
                if not self._published and np.mean(self._columns) >= self.MIN_COVERAGE:
                    self._publish_rotation()
                    published = True
                self._clear_rotation()
            self._range[emitters[indices], segment_ticks] = distance[indices]
            self._intensity[emitters[indices], segment_ticks] = intensity[indices]
            self._columns[segment_ticks] = True
            self._last_tick = int(segment_ticks[-1])
            if (self._last_tick == self.width - 1 and not self._published and
                    np.mean(self._columns) >= self.MIN_COVERAGE):
                self._publish_rotation()
                published = True
        return published
