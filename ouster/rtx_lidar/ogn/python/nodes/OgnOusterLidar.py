import numpy as np
import omni.graph.core as og
import omni.kit.app
import omni.timeline
import omni.replicator.core as rep

import socket
import json
import queue
import threading
import time

from ouster.sdk.core import (SensorInfo, LidarScan, LidarMode, HeaderType,
                             OperatingMode, UDPProfileLidar, UDPProfileIMU, TimestampMode)
from ouster.sdk._bindings.client import PacketType
from ouster.sdk.util.parsing import scan_to_packets

from ouster.rtx_lidar.extension import HttpServer


_gmo_parser = None


def _parse_gmo(data):
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


def _schema_defines(prim, name):
    """True if the prim's applied schemas define the attribute `name`.

    HasAttribute() is also true for an opinion authored against another schema
    version, e.g. a 5.1-authored asset (or a stage this node saved on 5.1, which
    carries auxOutputType=BASIC) opened in 6.0 -- exactly the case the version
    branches in _setup_pipeline have to tell apart.
    """
    return bool(prim.GetPrimDefinition().GetAttributeDefinition(name))


class OgnOusterLidarInternalState:
    """Per-node internal state for the Ouster Lidar OGN node."""

    def __init__(self):
        self.initialized = False
        self.sensor_info = None
        self.render_product = None
        self.render_product_path = None
        self.annotator = None
        self.frame_id = 0
        self.pkt_count = 0
        self.scan = None
        self.scan_range_m = None
        self.scan_intensity = None
        self.http_server = None
        self.http_server_error = ""
        self.sock_lidar = None
        self.sock_imu = None
        self.packet_queue = None
        self.packet_sender_stop = None
        self.packet_sender_thread = None
        self.accum_range_m = None
        self.accum_intensity = None
        self.accum_columns = None
        self.last_tick = None
        self.last_gmo_key = None
        self.rotation_published = False
        self.last_publish_coverage = 1.0
        self.partial_rotations = 0
        self.warned_partial_rotation = False
        self.max_returns = 1
        # Track last config to detect changes
        self._last_sensor_path = None
        self._last_sensor_variant = None
        self._last_udp_dest = None
        self._last_udp_port_lidar = None
        self._last_udp_port_imu = None
        self._last_http_addr = None
        self._last_http_port = None
        self._last_enabled = None

    def needs_reinit(self, sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled):
        return (self._last_sensor_path != sensor_path or
                self._last_sensor_variant != sensor_variant or
                self._last_udp_port_lidar != udp_port_lidar or
                self._last_udp_port_imu != udp_port_imu or
                self._last_http_addr != http_addr or
                self._last_http_port != http_port)

    def config_changed_runtime(self, udp_dest, enabled):
        """Check if runtime-changeable config has changed."""
        return (self._last_udp_dest != udp_dest or
                self._last_enabled != enabled)

    def save_config(self, sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled):
        self._last_sensor_path = sensor_path
        self._last_sensor_variant = sensor_variant
        self._last_udp_dest = udp_dest
        self._last_udp_port_lidar = udp_port_lidar
        self._last_udp_port_imu = udp_port_imu
        self._last_http_addr = http_addr
        self._last_http_port = http_port
        self._last_enabled = enabled


class OgnOusterLidar:
    """Ouster RTX Lidar streaming node for OmniGraph ActionGraph."""

    # Minimum fraction of a rotation's columns that must have been filled for it
    # to be published; unfilled columns go out as zeros (no return). The GMO
    # annotator only exposes its latest frame, so when two sensor frames land
    # between two node reads the older one is lost upstream: measured on the
    # Lidar_Reflective_Room-minimal.usd scene over 600 updates, 49 lost frames on
    # 6.0 and 0-19 on 5.1, each a GMO frameId step of 2 / 33.3 ms of sim time
    # and ~171 of 1024 columns, with no correlation to slow updates. Requiring
    # full coverage dropped every such rotation (6.0 streamed ~7 Hz instead of
    # 10). 0.5 tolerates one or two lost 60 Hz frames per rotation while still
    # rejecting the partial first rotation after play or a sim reset.
    SCAN_MIN_COVERAGE = 0.5

    @staticmethod
    def internal_state():
        return OgnOusterLidarInternalState()

    @staticmethod
    def _read_sensor_variant_from_prim(sensor_path):
        """Read the sensorModelConfig attribute from the sensor prim on the USD stage."""
        import omni.usd
        stage = omni.usd.get_context().get_stage()
        if stage is None:
            print("No USD stage found, cannot read sensor variant")
            return None
        prim = stage.GetPrimAtPath(sensor_path)
        if prim.IsValid():
            variant_sets = prim.GetVariantSets()
            if variant_sets.HasVariantSet("sensor"):
                sensor_variant_set = variant_sets.GetVariantSet("sensor")
                return sensor_variant_set.GetVariantSelection()

        print("Variant set 'sensor' not found on this prim.")
        return None

    @staticmethod
    def _compose_sensor_info(lidar_sensor_variant, udp_port_lidar, udp_port_imu, udp_dest, sensor_path=None):
        """Build metadata from JSON, overridden by the live OmniLidar prim (Isaac Sim 5.1 / 6.0)."""
        import omni.usd
        ext_manager = omni.kit.app.get_app_interface().get_extension_manager()
        ext_id = ext_manager.get_enabled_extension_id("isaacsim.sensors.rtx")
        isaac_sim_ext_path = ext_manager.get_extension_path(ext_id)
        # Derive sensor family (OS0, OS1, OS2) from variant name prefix
        sensor_family = "OS0"  # default
        for prefix in ("OS0", "OS1", "OS2"):
            if lidar_sensor_variant.startswith(prefix):
                sensor_family = prefix
                break
        lidar_config_path = f"{isaac_sim_ext_path}/data/lidar_configs/Ouster/{sensor_family}/{lidar_sensor_variant}.json"
        print(f"Loading lidar config from {lidar_config_path}")
        with open(lidar_config_path, "r") as f:
            lidar_cfg = json.load(f)
        lidar_cfg = lidar_cfg["profile"]
        if sensor_path:
            stage = omni.usd.get_context().get_stage()
            prim = stage.GetPrimAtPath(f"{sensor_path}/sensor")
            if not prim.IsValid() or prim.GetTypeName() != "OmniLidar":
                raise ValueError("Indexed streaming requires an OmniLidar sensor prim")
            # JSON key -> prim attribute names, newest first. Isaac Sim 6.0
            # renamed reportRateBaseHz to patternFiringRateHz and
            # start/endAzimuthDeg to validStart/validEndAzimuthDeg (5.1 already
            # carries the valid* names), and dropped emitterStateCount (counted
            # below instead). The first name with an *authored* value wins, then
            # the first with any value (schema fallback), then the JSON.
            # Authored-first matters on 6.0 with a 5.1-authored asset such as the
            # OS0 in Lidar_Reflective_Room-minimal.usd: it authors
            # reportRateBaseHz=10240 while patternFiringRateHz resolves to the 6.0
            # schema fallback 36000, which advertised 3600 columns.
            for key, names in (("numberOfEmitters", ("numberOfEmitters",)),
                               ("maxReturns", ("maxReturns",)),
                               ("scanRateBaseHz", ("scanRateBaseHz",)),
                               ("reportRateBaseHz", ("patternFiringRateHz", "reportRateBaseHz")),
                               ("rotationDirection", ("rotationDirection",)),
                               ("scanType", ("scanType",)),
                               ("startAzimuthDeg", ("validStartAzimuthDeg", "startAzimuthDeg")),
                               ("endAzimuthDeg", ("validEndAzimuthDeg", "endAzimuthDeg")),
                               ("emitterStateCount", ("emitterStateCount",))):
                attrs = [prim.GetAttribute(f"omni:sensor:Core:{name}") for name in names]
                attrs = [attr for attr in attrs if attr.IsValid() and attr.Get() is not None]
                authored = [attr for attr in attrs if attr.HasAuthoredValue()]
                if authored or attrs:
                    lidar_cfg[key] = (authored or attrs)[0].Get()
            # 6.0: one OmniSensorGenericLidarCoreEmitterStateAPI:s### instance per state.
            states = [s for s in prim.GetAppliedSchemas()
                      if s.startswith("OmniSensorGenericLidarCoreEmitterStateAPI:")]
            if states:
                lidar_cfg["emitterStateCount"] = len(states)
            emitter = dict(lidar_cfg["emitterStates"][0])
            for name in ("azimuthDeg", "elevationDeg"):
                value = prim.GetAttribute(f"omni:sensor:Core:emitterState:s001:{name}").Get()
                if value is not None:
                    emitter[name] = list(value)
            lidar_cfg["emitterStates"] = [emitter]
        if (lidar_cfg["scanType"].upper() != "ROTARY" or
                lidar_cfg.get("rotationDirection", "CW") != "CW" or
                lidar_cfg.get("startAzimuthDeg", 0) != 0 or
                lidar_cfg.get("endAzimuthDeg", 360) != 360 or
                lidar_cfg.get("emitterStateCount", 1) != 1):
            raise ValueError("Indexed streaming requires a full-circle CW lidar with one emitter state")
        num_channels = lidar_cfg["numberOfEmitters"]
        max_returns = lidar_cfg.get("maxReturns", 1)
        azimuth_deg = lidar_cfg["emitterStates"][0]["azimuthDeg"]
        elevation_deg = lidar_cfg["emitterStates"][0]["elevationDeg"]
        scan_rate_hz = int(lidar_cfg["scanRateBaseHz"])
        report_rate_hz = int(lidar_cfg["reportRateBaseHz"])
        columns_per_frame = int(report_rate_hz / scan_rate_hz)

        sensor_info = SensorInfo()
        sensor_info.sn = 1234567890
        sensor_info.config.azimuth_window = [0, 360000]
        sensor_info.config.columns_per_packet = 16
        sensor_info.config.lidar_mode = LidarMode.from_string(
            f"{columns_per_frame}x{scan_rate_hz}")
        sensor_info.config.udp_port_lidar = udp_port_lidar
        sensor_info.config.udp_port_imu = udp_port_imu
        sensor_info.config.udp_port_zm = 0
        sensor_info.config.udp_dest = udp_dest
        sensor_info.config.udp_dest_zm = ""
        sensor_info.config.operating_mode = OperatingMode.OPERATING_NORMAL
        sensor_info.config.udp_profile_lidar = UDPProfileLidar.RNG19_RFL8_SIG16_NIR16
        sensor_info.config.udp_profile_imu = UDPProfileIMU.LEGACY
        sensor_info.config.timestamp_mode = TimestampMode.TIME_FROM_INTERNAL_OSC
        sensor_info.beam_altitude_angles = np.array(elevation_deg, dtype=np.float64)
        sensor_info.beam_azimuth_angles = np.array(azimuth_deg, dtype=np.float64)
        sensor_info.beam_to_lidar_transform = np.eye(4, dtype=np.float64)
        sensor_info.build_date = "2022-12-23T22:27:12Z"
        sensor_info.fw_rev = "v3.0.0-beta.2-23-gdbfc6371c"
        sensor_info.image_rev = "ousteros-image-prod-bootes-v3.0.0-beta.2+20221223222646"
        sensor_info.init_id = 7777777
        sensor_info.prod_line = f"OS-0-{num_channels}"
        sensor_info.prod_pn = "XXX-XXXXXXX-XX"
        sensor_info.status = "RUNNING"
        sensor_info.user_data = ""
        sensor_info.imu_to_sensor_transform = np.array(
            [[1.0, 0.0, 0.0, -2.441],
             [0.0, 1.0, 0.0, -9.725],
             [0.0, 0.0, 1.0, 7.533],
             [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)
        sensor_info.lidar_to_sensor_transform = np.eye(4, dtype=np.float64)
        sensor_info.lidar_origin_to_beam_origin_mm = 0.0
        sensor_info.format.column_window = [0, columns_per_frame - 1]
        sensor_info.format.columns_per_frame = columns_per_frame
        sensor_info.format.columns_per_packet = 16
        sensor_info.format.fps = scan_rate_hz
        sensor_info.format.header_type = HeaderType.STANDARD
        sensor_info.format.pixel_shift_by_row = np.rint(
            np.asarray(azimuth_deg) * columns_per_frame / 360.0).astype(int).tolist()
        sensor_info.format.imu_measurements_per_packet = 0
        sensor_info.format.imu_packets_per_frame = 0
        sensor_info.format.pixels_per_column = num_channels
        sensor_info.format.udp_profile_imu = UDPProfileIMU.LEGACY
        sensor_info.format.udp_profile_lidar = UDPProfileLidar.RNG19_RFL8_SIG16_NIR16
        sensor_info.format.zone_monitoring_enabled = False
        return sensor_info, max_returns

    @staticmethod
    def _setup_pipeline(state, sensor_path):
        """Read spherical GMO before ScanBuffer discards invalid rays and firing IDs."""
        import omni.usd
        from pxr import Sdf
        prim = omni.usd.get_context().get_stage().GetPrimAtPath(f"{sensor_path}/sensor")
        for name, value in (("elementsCoordsType", "SPHERICAL"),
                            ("outputFrameOfReference", "SENSOR"),
                            ("outputMotionCompensationState", "NONCOMPENSATED")):
            prim.CreateAttribute(f"omni:sensor:Core:{name}", Sdf.ValueTypeNames.Token).Set(value)
        prim.CreateAttribute("omni:sensor:Core:skipDroppingInvalidPoints", Sdf.ValueTypeNames.Bool).Set(True)
        # Aux level (tickId/channelId/emitterId): 5.1 reads Core:auxOutputType;
        # 6.0 removed it from the schema and reads the RenderVar channels
        # attribute (what isaacsim.sensors.experimental.rtx authors). Branch on
        # the schema rather than writing both, so neither version gets an
        # unknown attribute. Verified headless: header auxType == BASIC on both.
        if _schema_defines(prim, "omni:sensor:Core:auxOutputType"):
            prim.GetAttribute("omni:sensor:Core:auxOutputType").Set("BASIC")
        else:
            prim.CreateAttribute("_replicator:rendervar:GenericModelOutput:channels",
                                 Sdf.ValueTypeNames.StringArray, True).Set(["BASIC"])
        # 6.0 Ouster assets author accumulateOutputs=True: every GMO is then an
        # overlapping full-rotation buffer whose aux arrays are not in
        # (tick, channel, echo) order (each 256-element run of ticks 0..170
        # starts with one stray element from tick 853+k), which
        # _accumulate_gmo would read as ~170 wraps per buffer. Per-frame
        # batches restore the 5.1 layout: 171 ticks x 128 ch x 2 slots.
        if _schema_defines(prim, "omni:sensor:Core:accumulateOutputs"):
            prim.GetAttribute("omni:sensor:Core:accumulateOutputs").Set(False)
        # 6.0 fires at Core:patternFiringRateHz, the renamed reportRateBaseHz. A
        # 5.1-authored asset (the OS0 referenced by
        # Lidar_Reflective_Room-minimal.usd) authors only reportRateBaseHz, so
        # 6.0 falls back to the schema's 36000 Hz and, for that OS0, produced no
        # GMO elements at all (measured: 0 of 200 frames; 43776 per frame, ticks
        # 0..1023, once set to 10240). Carry the authored rate over. On 5.1 the
        # schema has no patternFiringRateHz, so this is a no-op there.
        pattern_rate = prim.GetAttribute("omni:sensor:Core:patternFiringRateHz")
        legacy_rate = prim.GetAttribute("omni:sensor:Core:reportRateBaseHz")
        if (_schema_defines(prim, "omni:sensor:Core:patternFiringRateHz") and not pattern_rate.HasAuthoredValue()
                and legacy_rate.IsValid() and legacy_rate.HasAuthoredValue()):
            pattern_rate.Set(int(legacy_rate.Get()))
        state.render_product = rep.create.render_product(
            f"{sensor_path}/sensor", [1, 1],
            name="OgnOusterLidarRenderProduct",
            render_vars=["GenericModelOutput", "RtxSensorMetadata"],
        )
        state.render_product_path = state.render_product.path

        state.annotator = rep.AnnotatorRegistry.get_annotator("GenericModelOutput", device="cpu")
        state.annotator.attach([state.render_product_path])

    @staticmethod
    def _start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port):
        """Initialize UDP sockets and packet sender thread.

        The sender thread reads udp_dest/udp_port_lidar/udp_port_imu LIVE from
        state.sensor_info.config on every send, not from the arguments passed
        here. This is required because the HTTP API (configure_sensor, called
        by Detect or ouster-cli) mutates state.sensor_info.config.udp_dest
        directly at runtime; a value captured once at thread spawn would go
        stale the moment an external client reconfigures the destination.
        """
        state.scan = LidarScan(state.sensor_info)
        state.frame_id = 0
        state.pkt_count = 0
        state.scan_range_m = None
        state.scan_intensity = None
        state.accum_range_m = None
        state.accum_intensity = None
        state.accum_columns = None
        state.last_tick = None
        state.last_gmo_key = None
        state.rotation_published = False
        state.last_publish_coverage = 1.0
        state.partial_rotations = 0
        state.packet_queue = queue.Queue()
        state.packet_sender_stop = threading.Event()

        class _SensorHolder:
            def __init__(self, si):
                self._si = si
            @property
            def sensor_info(self):
                return self._si

        holder = _SensorHolder(state.sensor_info)

        try:
            state.http_server = HttpServer(holder, http_addr, http_port, autostart=True)
            state.http_server_error = ""
        except Exception as e:
            state.http_server = None
            state.http_server_error = f"{type(e).__name__}: {e}"
            # HTTP bind failed. UDP sockets are intentionally not created.
            # Ensure we don't enqueue packets while streaming isn't running.
            state.packet_queue = None
            state.packet_sender_stop = None
            return

        state.sock_lidar = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        state.sock_imu = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        def _sender_loop(pkt_queue, stop_event, sock_l, sock_i, state_ref):
            last_ts = None
            while True:
                item = pkt_queue.get()
                if item is None or stop_event.is_set():
                    break
                pkt_type, payload, host_ts = item
                if last_ts is not None:
                    delta_ns = max(0, host_ts - last_ts)
                    deadline = time.perf_counter() + delta_ns * 1e-9
                    while not stop_event.is_set():
                        if deadline - time.perf_counter() <= 0:
                            break
                        time.sleep(min(deadline - time.perf_counter(), 0.01))
                    if stop_event.is_set():
                        break
                try:
                    cfg = state_ref.sensor_info.config
                    dest = cfg.udp_dest
                    if pkt_type == PacketType.Lidar:
                        sock_l.sendto(payload, (dest, cfg.udp_port_lidar))
                    elif pkt_type == PacketType.Imu:
                        sock_i.sendto(payload, (dest, cfg.udp_port_imu))
                    state_ref.pkt_count += 1
                except OSError:
                    break
                last_ts = host_ts

        state.packet_sender_thread = threading.Thread(
            target=_sender_loop,
            args=(state.packet_queue, state.packet_sender_stop,
                  state.sock_lidar, state.sock_imu, state),
            daemon=True,
        )
        state.packet_sender_thread.start()

    @staticmethod
    def _stop_streaming(state):
        """Tear down streaming resources."""
        if state.packet_sender_stop is not None:
            state.packet_sender_stop.set()
        if state.packet_queue is not None:
            state.packet_queue.put(None)
        if state.packet_sender_thread is not None:
            state.packet_sender_thread.join(timeout=1.0)
            state.packet_sender_thread = None
        state.packet_queue = None
        state.packet_sender_stop = None

        if state.sock_lidar is not None:
            state.sock_lidar.close()
            state.sock_lidar = None
        if state.sock_imu is not None:
            state.sock_imu.close()
            state.sock_imu = None

        if state.http_server is not None:
            state.http_server.stop()
            state.http_server = None

        state.scan = None
        state.scan_range_m = None
        state.scan_intensity = None
        state.frame_id = 0
        state.pkt_count = 0

    @staticmethod
    def _teardown_pipeline(state):
        """Detach annotator and destroy render product."""
        if state.annotator and state.render_product_path:
            try:
                state.annotator.detach([state.render_product_path])
            except Exception:
                pass
        state.annotator = None
        if state.render_product:
            state.render_product.destroy()
        state.render_product = None
        state.render_product_path = None

    @staticmethod
    def _accumulate_gmo(state, gmo):
        """Copy first echoes by firing ID; publish each rotation once it ends.

        A rotation is published when tick W-1 arrives or, if the batch carrying
        it was lost, when the next rotation starts, provided at least
        SCAN_MIN_COVERAGE of its columns were filled; missing columns are zeros.

        Dense GMO order is (tick, channel, echo), whereas Ouster fields are
        (channel, tick). Neither angle lookup nor destaggering belongs here.
        Raw spherical ranges share the virtual sensor origin, so metadata must
        not add physical Ouster beam-origin translations.

        Row key is emitterId: it indexes the emitterState azimuth/elevation
        tables that beam_azimuth/altitude_angles are built from. (GMO channelId
        equals it, 0-based, on 5.1 and 6.0, but channelId is the detector
        mapping and 6.0's schema default table is 1-based.)

        Echo rank is taken from position within each (tick, emitter) run, not
        from echoId: 6.0 BASIC reports echoId 0 for both return slots, so an
        echoId == 0 filter kept the always-invalid second slot, which then
        overwrote the first. Position rank equals echoId on 100% of a 5.1
        capture.
        """
        width = int(state.sensor_info.format.columns_per_frame)
        height = int(state.sensor_info.format.pixels_per_column)
        key = (int(gmo.frameId), int(gmo.timestampNs))
        if key == state.last_gmo_key:
            return False
        if state.accum_range_m is None or (state.last_gmo_key is not None and key[1] < state.last_gmo_key[1]):
            state.accum_range_m = np.zeros((height, width), dtype=np.float32)
            state.accum_intensity = np.zeros((height, width), dtype=np.float32)
            state.accum_columns = np.zeros(width, dtype=bool)
            state.last_tick = None
            state.rotation_published = False
        state.last_gmo_key = key
        channels = np.asarray(gmo.emitterId)
        ticks = np.asarray(gmo.tickId)
        distance = np.asarray(gmo.z)
        intensity = np.asarray(gmo.scalar)
        flags = np.asarray(gmo.flags)
        if any(values.size != gmo.numElements for values in (channels, ticks, distance, intensity, flags)):
            raise ValueError("GMO requires BASIC auxiliary data with one firing ID per element")
        if np.any(channels >= height):
            raise ValueError("GMO firing IDs do not match the advertised sensor dimensions")
        ticks = ticks % width
        run_key = ticks.astype(np.int64) * height + channels
        run_start = np.r_[True, run_key[1:] != run_key[:-1]]
        position = np.arange(run_key.size)
        first_echo = position == np.maximum.accumulate(np.where(run_start, position, 0))
        channels, ticks = channels[first_echo], ticks[first_echo]
        valid = ((flags[first_echo] & 64) != 0) & np.isfinite(distance[first_echo]) & (distance[first_echo] > 0)
        distance = np.where(valid, distance[first_echo], 0)
        intensity = np.where(valid, np.nan_to_num(intensity[first_echo]), 0)
        boundaries = np.flatnonzero(ticks[1:] < ticks[:-1]) + 1
        completed = False

        def publish():
            # Columns no batch reached this rotation are still 0 (no return), since
            # the accumulator is cleared at every rotation boundary.
            state.scan_range_m = state.accum_range_m.copy()
            state.scan_intensity = state.accum_intensity.copy()
            state.last_publish_coverage = float(np.mean(state.accum_columns))
            state.rotation_published = True

        for indices in np.split(np.arange(ticks.size), boundaries):
            if not indices.size:
                continue
            segment_ticks = ticks[indices]
            if state.last_tick is not None and segment_ticks[0] < state.last_tick:
                # Wrapped without seeing tick W-1: the batch carrying the end of
                # the previous rotation was lost, so publish it here instead.
                if not state.rotation_published and np.mean(state.accum_columns) >= OgnOusterLidar.SCAN_MIN_COVERAGE:
                    publish()
                    completed = True
                state.accum_range_m.fill(0)
                state.accum_intensity.fill(0)
                state.accum_columns.fill(False)
                state.rotation_published = False
            segment_channels = channels[indices]
            dense = (indices.size % height == 0 and
                     np.all(segment_channels.reshape(-1, height) == np.arange(height)) and
                     np.all(segment_ticks.reshape(-1, height) == segment_ticks[::height, None]))
            if dense:
                columns = segment_ticks[::height]
                state.accum_range_m[:, columns] = distance[indices].reshape(-1, height).T
                state.accum_intensity[:, columns] = intensity[indices].reshape(-1, height).T
            else:
                columns = np.unique(segment_ticks)
                state.accum_range_m[segment_channels, segment_ticks] = distance[indices]
                state.accum_intensity[segment_channels, segment_ticks] = intensity[indices]
            state.accum_columns[columns] = True
            state.last_tick = int(segment_ticks[-1])
            if (state.last_tick == width - 1 and not state.rotation_published and
                    np.mean(state.accum_columns) >= OgnOusterLidar.SCAN_MIN_COVERAGE):
                publish()
                completed = True
        return completed

    @staticmethod
    def compute(db) -> bool:
        from ouster.rtx_lidar.ogn.OgnOusterLidarDatabase import OgnOusterLidarDatabase
        per_node_data = OgnOusterLidarDatabase.PER_NODE_DATA.get(db.node.node_id(), {})
        state = per_node_data.get("state")
        if state is None:
            state = OgnOusterLidarInternalState()
            per_node_data["state"] = state

        sensor_path = db.inputs.sensorPath
        sensor_variant = db.inputs.sensorVariant
        udp_dest = db.inputs.udpDest
        udp_port_lidar = db.inputs.udpPortLidar
        udp_port_imu = db.inputs.udpPortImu
        http_addr = db.inputs.httpAddr
        http_port = db.inputs.httpPort
        enabled = db.inputs.enabled

        if not sensor_path:
            db.outputs.execOut = og.ExecutionAttributeState.DISABLED
            db.log_warn("OgnOusterLidar: no sensor prim path set, disabling execution")
            return False

        if not sensor_variant:
            sensor_variant = OgnOusterLidar._read_sensor_variant_from_prim(sensor_path)
            if not sensor_variant:
                db.outputs.execOut = og.ExecutionAttributeState.DISABLED
                db.log_warn(f"OgnOusterLidar: could not read sensorModelConfig from prim '{sensor_path}'")
                return False

        if not state.initialized or state.needs_reinit(
                sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled):
            if state.initialized:
                OgnOusterLidar._stop_streaming(state)
                OgnOusterLidar._teardown_pipeline(state)

            try:
                state.sensor_info, state.max_returns = OgnOusterLidar._compose_sensor_info(
                    sensor_variant, udp_port_lidar, udp_port_imu, udp_dest, sensor_path)
            except Exception as error:
                db.log_error(f"Failed to load lidar config: {error}")
                db.outputs.execOut = og.ExecutionAttributeState.DISABLED
                return False

            OgnOusterLidar._setup_pipeline(state, sensor_path)

            if enabled:
                OgnOusterLidar._start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port)
                if state.http_server_error:
                    db.log_error(f"OgnOusterLidar: HTTP bind failed on {http_addr}:{http_port} -> {state.http_server_error}")

            state.initialized = True
            state.save_config(sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled)

        if state.config_changed_runtime(udp_dest, enabled):
            if not enabled and state.sock_lidar is not None:
                OgnOusterLidar._stop_streaming(state)
            elif enabled and state.sock_lidar is None:
                OgnOusterLidar._start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port)
                if state.http_server_error:
                    db.log_error(f"OgnOusterLidar: HTTP bind failed on {http_addr}:{http_port} -> {state.http_server_error}")
            if state.sensor_info is not None:
                state.sensor_info.config.udp_dest = udp_dest
            state.save_config(sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled)

        # --- Debug outputs, populated every tick ---
        db.outputs.httpServerError = state.http_server_error or ""
        if state.http_server is not None:
            db.outputs.httpBoundAddr = f"{http_addr}:{http_port}"
            db.outputs.lastClientAddr = state.http_server.last_client_addr
        else:
            db.outputs.httpBoundAddr = ""
            db.outputs.lastClientAddr = ""
        db.outputs.packetsSent = state.pkt_count

        if not enabled or state.annotator is None or state.scan is None:
            db.outputs.execOut = og.ExecutionAttributeState.DISABLED
            return True

        scan_data = state.annotator.get_data()
        if scan_data is None or len(scan_data) == 0:
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True

        gmo = _parse_gmo(scan_data)
        if gmo.magicNumber != 0x4E474D4F or gmo.numElements == 0:
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True
        if not OgnOusterLidar._accumulate_gmo(state, gmo):
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True
        if state.last_publish_coverage < 1.0:
            state.partial_rotations += 1
            if not state.warned_partial_rotation:
                state.warned_partial_rotation = True
                db.log_warn(
                    f"OgnOusterLidar: published a rotation with {100 * (1 - state.last_publish_coverage):.0f}% of "
                    "columns zero-filled (a GMO frame was superseded before this node read it). Further partial "
                    "rotations are counted in state.partial_rotations and not logged.")

        width = int(state.sensor_info.format.columns_per_frame)
        scan_range_m, scan_intensity = state.scan_range_m, state.scan_intensity

        state.frame_id += 1
        state.scan.frame_id = state.frame_id
        state.scan.field("RANGE")[:] = np.where(scan_range_m > 0, scan_range_m * 1e+3, 0).astype(np.uint32)
        state.scan.field("REFLECTIVITY")[:] = np.clip(scan_intensity * 255.0, 0, 255).astype(np.uint8)

        fps = state.sensor_info.format.fps
        timestamps = 1e9 * (state.frame_id / fps + np.arange(width) / (width * fps))
        state.scan.timestamp[:] = timestamps
        state.scan.status[:] = 0x1
        packet_count = len(state.scan.packet_timestamp)
        state.scan.packet_timestamp[:] = timestamps[np.arange(packet_count) * 16]

        packets = scan_to_packets(state.scan, state.sensor_info)
        if state.packet_queue is not None:
            for packet in packets:
                if packet.type in (PacketType.Lidar, PacketType.Imu):
                    state.packet_queue.put((
                        packet.type,
                        bytes(packet.buf),
                        packet.host_timestamp,
                    ))

        db.outputs.frameId = state.frame_id
        db.outputs.execOut = og.ExecutionAttributeState.ENABLED
        return True

    @staticmethod
    def release(node):
        try:
            from ouster.rtx_lidar.ogn.OgnOusterLidarDatabase import OgnOusterLidarDatabase
            per_node_data = OgnOusterLidarDatabase.PER_NODE_DATA.get(node.node_id())
            if per_node_data:
                state = per_node_data.get("state")
                if state and state.initialized:
                    OgnOusterLidar._stop_streaming(state)
                    OgnOusterLidar._teardown_pipeline(state)
                    state.initialized = False
        except Exception:
            pass
