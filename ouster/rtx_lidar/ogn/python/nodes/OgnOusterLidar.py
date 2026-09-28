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
from ouster.rtx_lidar.rotation_accumulator import (GMO_MAGIC, create_rotation_accumulator, parse_gmo,
                                                    schema_defines)


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
        self.accumulator = None
        self.http_server = None
        self.http_server_error = ""
        self.sock_lidar = None
        self.sock_imu = None
        self.packet_queue = None
        self.packet_sender_stop = None
        self.packet_sender_thread = None
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
        if schema_defines(prim, "omni:sensor:Core:auxOutputType"):
            prim.GetAttribute("omni:sensor:Core:auxOutputType").Set("BASIC")
        else:
            prim.CreateAttribute("_replicator:rendervar:GenericModelOutput:channels",
                                 Sdf.ValueTypeNames.StringArray, True).Set(["BASIC"])
        # Rotation assembly: sensor-side accumulation where the schema offers it
        # (6.0), per-frame batch stitching otherwise (5.1). Sets accumulateOutputs.
        state.accumulator = create_rotation_accumulator(
            prim, state.sensor_info.format.columns_per_frame, state.sensor_info.format.pixels_per_column,
            state.max_returns)
        # 6.0 fires at Core:patternFiringRateHz, the renamed reportRateBaseHz. A
        # 5.1-authored asset (the OS0 referenced by
        # Lidar_Reflective_Room-minimal.usd) authors only reportRateBaseHz, so
        # 6.0 falls back to the schema's 36000 Hz and, for that OS0, produced no
        # GMO elements at all (measured: 0 of 200 frames; 43776 per frame, ticks
        # 0..1023, once set to 10240). Carry the authored rate over. On 5.1 the
        # schema has no patternFiringRateHz, so this is a no-op there.
        pattern_rate = prim.GetAttribute("omni:sensor:Core:patternFiringRateHz")
        legacy_rate = prim.GetAttribute("omni:sensor:Core:reportRateBaseHz")
        if (schema_defines(prim, "omni:sensor:Core:patternFiringRateHz") and not pattern_rate.HasAuthoredValue()
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
        if state.accumulator is not None:
            state.accumulator.reset()
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
        state.accumulator = None
        if state.render_product:
            state.render_product.destroy()
        state.render_product = None
        state.render_product_path = None

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

        gmo = parse_gmo(scan_data)
        if gmo.magicNumber != GMO_MAGIC or gmo.numElements == 0:
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True
        accumulator = state.accumulator
        if not accumulator.add(gmo):
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True
        if accumulator.coverage < 1.0:
            state.partial_rotations += 1
            if not state.warned_partial_rotation:
                state.warned_partial_rotation = True
                db.log_warn(
                    f"OgnOusterLidar: published a rotation with {100 * (1 - accumulator.coverage):.0f}% of "
                    "columns zero-filled (a GMO frame was superseded before this node read it). Further partial "
                    "rotations are counted in state.partial_rotations and not logged.")

        width = int(state.sensor_info.format.columns_per_frame)
        scan_range_m, scan_intensity = accumulator.range_m, accumulator.intensity

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
