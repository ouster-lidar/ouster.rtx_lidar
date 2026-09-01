# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Jorge Octavio Martinez Tovar, Ussama Naal
#
# ouster.rtx_lidar -- OgnOusterLidar node
# =============================================================================
# Virtual Ouster lidar sensor for NVIDIA Isaac Sim / OmniGraph.
#
# Reads the RTX lidar annotator output for a sensor prim, rebuilds it into an
# Ouster-native scan image, packetizes it with the Ouster SDK, and streams UDP
# lidar/IMU packets while serving the Ouster sensor HTTP API, so that Ouster
# clients (Detect / Gemini, ouster-cli, Ouster Studio) treat it as hardware.
#
# Authors:
#   Jorge Octavio Martinez Tovar (Digital Twin Engineering, Ternium
#     Pesqueria) -- point cloud generation conventions (C3-C5), synthetic
#     IMU (C6), HTTP lifecycle patch, multi-sensor support (Checkpoint A),
#     fixed-rate emitter (Checkpoint B), production integration and
#     validation against Ouster Detect.
#   Ussama Naal (Ouster, Inc.) -- original ouster.rtx_lidar extension:
#     base OGN node, sensor HTTP API emulation, and pcap replay tooling.
#
# -----------------------------------------------------------------------------
# REVISION HISTORY (identifiers referenced throughout the code)
# -----------------------------------------------------------------------------
# HTTP-LIFECYCLE PATCH: HttpServer persists across sensor reinits via a
#   mutable _SensorHolder; destroyed only on bind addr/port change or node
#   release. (See earlier revisions for the full root-cause analysis.)
# C3: clockwise scan direction -- encoder column inversion; without it the
#   cloud is mirrored across Y (verified: 19.54 m error vs ground truth,
#   0.0745 m vs mirrored ground truth using the SDK XYZLut).
# C4: full-rotation accumulation -- render ticks deliver partial azimuth
#   batches; the scan image accumulates and completes at
#   SCAN_COMPLETE_COVERAGE column coverage.
# C5: real per-beam azimuth offsets published; the encoder column carries
#   the offset ADDED so client reconstruction encoder(v) - offset returns
#   the true azimuth (sign verified empirically; subtracting doubled the
#   4-group misalignment).
# C6: synthetic IMU (LEGACY 48 B) carrying the gravity vector from the
#   sensor prim's USD world orientation, so Detect's "IMU Pose" recovers
#   the bracket pitch/roll.
# CHECKPOINT A: per-instance Flask app (B1, in extension.py); sensorSn
#   identity input with derived init_id; warning latches.
#
# CHECKPOINT B (this revision) -- publication decoupled from simulation FPS:
#   B-EMIT: the queue + host-timestamp-paced sender is REPLACED by a
#     fixed-rate wall-clock emitter thread. compute() now only accumulates
#     the rotation and, when complete, swaps it into a double buffer. The
#     emitter wakes every 1/fps seconds (fps read from
#     sensor_info.format.fps -- removes the previous 10 Hz hardcode),
#     packetizes whatever complete rotation is in the buffer, stamps
#     WALL-CLOCK timestamps, and sends. If the simulation stalls, the LAST
#     complete rotation is re-emitted with fresh timestamps and a fresh
#     frame_id: Detect sees a steady, correctly-timed sensor observing a
#     momentarily frozen world (exactly what a physical sensor pointed at
#     a paused scene produces). This removes the "Timed out waiting for
#     sensor data" reconnect cycle when render FPS drops below the sensor
#     rate. Side effect: the sensor stays alive in Detect while the
#     timeline is paused.
#   B-CFG: client-pushed configuration (udp_dest, udp ports,
#     operating_mode) now SURVIVES node reinits. Previously a reinit
#     rebuilt SensorInfo from input defaults, silently discarding what
#     Detect had pushed via the config API and desynchronizing the node
#     from the client (observed in production as a data timeout with
#     Detect listening on reassigned ports). Rule: a value is carried over
#     from the previous SensorInfo unless the corresponding node INPUT was
#     the thing that changed.
#   B-SEND: sending is resilient. An OSError no longer kills the emitter
#     (logged with a 5 s throttle, then continue); an empty/unset
#     udp_dest skips sending instead of raising.
#   Gravity (C6) is sampled in compute() -- the main thread, where USD
#     access is safe -- and cached for the emitter thread.
# =============================================================================

import numpy as np
import omni.graph.core as og
import omni.kit.app
import omni.timeline
import omni.replicator.core as rep

import socket
import json
import struct
import threading
import time

from ouster.sdk.core import (SensorInfo, LidarScan, LidarMode, HeaderType,
                             OperatingMode, UDPProfileLidar, UDPProfileIMU, TimestampMode)
from ouster.sdk._bindings.client import PacketType
from ouster.sdk.util.parsing import scan_to_packets

from ouster.rtx_lidar.extension import HttpServer


class _SensorHolder:
    """Mutable indirection between the HTTP server and the active SensorInfo.

    HTTP-LIFECYCLE PATCH: the HttpServer keeps this holder across sensor
    reinits; set_sensor_info() re-points the served metadata without ever
    reconstructing the Flask app or the server.
    """

    def __init__(self, si):
        self._si = si

    def set_sensor_info(self, si):
        self._si = si

    @property
    def sensor_info(self):
        return self._si


class OgnOusterLidarInternalState:
    """Per-node internal state. One instance per node via PER_NODE_DATA, so
    multiple nodes in one session are fully isolated."""

    def __init__(self):
        self.initialized = False
        self.sensor_info = None
        self.render_product = None
        self.render_product_path = None
        self.annotator = None
        self.frame_id = 0          # owned by the emitter thread (B-EMIT)
        self.pkt_count = 0
        self.scan_range_m = None
        self.scan_intensity = None
        self.http_server = None
        self.http_holder = None    # HTTP-LIFECYCLE PATCH: persistent holder
        self.http_server_error = ""
        self.sock_lidar = None
        self.sock_imu = None
        # B-EMIT: double buffer (compute() writes, emitter reads) + thread.
        self.buffer_lock = threading.Lock()
        self.latest_range = None      # last COMPLETE rotation (float32 m)
        self.latest_intensity = None
        self.latest_gravity = np.array([0.0, 0.0, 1.0])
        self.emitter_stop = None
        self.emitter_thread = None
        self.elevation_lut_order = None
        self.elevation_lut_deg_sorted = None
        self.max_returns = 1
        self.beam_azimuth_lut = None   # C5: per-row azimuth offset (deg)
        self.covered_cols = None      # C4: columns swept this rotation
        self.render_frames_per_scan = 0  # C4: diagnostics
        # C5w warning latches (log once; re-arm when the condition clears)
        self.warned_no_path = False
        self.warned_no_variant = False
        # Last-seen input snapshot used to detect configuration changes.
        self._last_sensor_path = None
        self._last_sensor_sn = None
        self._last_sensor_variant = None
        self._last_udp_dest = None
        self._last_udp_port_lidar = None
        self._last_udp_port_imu = None
        self._last_http_addr = None
        self._last_http_port = None
        self._last_enabled = None

    def needs_reinit(self, sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled, sensor_sn):
        """True when an input changed that requires a full pipeline rebuild.
        udp_dest and enabled are runtime-changeable and deliberately
        excluded (see config_changed_runtime)."""
        return (self._last_sensor_path != sensor_path or
                self._last_sensor_variant != sensor_variant or
                self._last_udp_port_lidar != udp_port_lidar or
                self._last_udp_port_imu != udp_port_imu or
                self._last_http_addr != http_addr or
                self._last_http_port != http_port or
                self._last_sensor_sn != sensor_sn)

    def http_config_changed(self, http_addr, http_port):
        """True only when the change requires rebuilding the HTTP server
        (HTTP-LIFECYCLE PATCH)."""
        return (self._last_http_addr is not None and
                (self._last_http_addr != http_addr or
                 self._last_http_port != http_port))

    def config_changed_runtime(self, udp_dest, enabled):
        """Check if runtime-changeable config has changed."""
        return (self._last_udp_dest != udp_dest or
                self._last_enabled != enabled)

    def save_config(self, sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled, sensor_sn=None):
        """Persist the last-applied input snapshot. sensor_sn=None preserves
        the previously stored value."""
        self._last_sensor_sn = sensor_sn if sensor_sn is not None else self._last_sensor_sn
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

    # C4: fraction of columns required before a rotation counts as complete.
    SCAN_COMPLETE_COVERAGE = 0.98
    # B-EMIT: fraction of the frame period across which lidar packets are
    # spread (the remainder is slack for scheduling jitter).
    PACKET_SPREAD = 0.85
    # B-SEND: minimum seconds between repeated send-error log lines.
    SEND_ERROR_LOG_PERIOD = 5.0
    # Sensor identity convention (SENSOR_CONVENTION.md): Sensor #1 is the
    # licensed identity; init_id tracks the SN delta.
    BASE_SN = 1234567890
    BASE_INIT_ID = 7777777

    # OS1 SUPPORT: lidar_origin_to_beam_origin_mm IS standardized per
    # product line -- confirmed straight from the Ouster SDK's own source
    # (ouster_core/src/sensor_info.cpp, default_lidar_origin_to_beam_origin).
    # beam_to_lidar_transform is identity with this distance as the X
    # translation, same shape already used below. Safe to trust without a
    # physical unit.
    BEAM_ORIGIN_MM_BY_FAMILY = {"OS0": 27.67, "OS1": 15.806, "OS2": 13.762}

    # OS1 SUPPORT: imu_to_sensor_transform / lidar_to_sensor_transform are
    # NOT parameterized by product line in the Ouster SDK -- it ships one
    # generic "design values" constant for both. The OS0 numbers below are
    # NOT that generic constant: they were pulled from the real calibrated
    # metadata of our physical OS0 unit (they don't match the SDK default).
    # We don't have a physical OS1 yet, so OS1 uses the SDK's generic
    # default as the best available placeholder -- replace with the real
    # OS1's metadata (GET /api/v1/sensor/metadata/{imu,lidar}_intrinsics)
    # once it's connected.
    IMU_TO_SENSOR_TRANSFORM_BY_FAMILY = {
        "OS0": np.array([[1.0, 0.0, 0.0, -2.441],
                         [0.0, 1.0, 0.0, -9.725],
                         [0.0, 0.0, 1.0, 7.533],
                         [0.0, 0.0, 0.0, 1.0]], dtype=np.float64),
    }
    DEFAULT_IMU_TO_SENSOR_TRANSFORM = np.array(
        [[1.0, 0.0, 0.0, 6.253],
         [0.0, 1.0, 0.0, -11.775],
         [0.0, 0.0, 1.0, 7.645],
         [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)
    LIDAR_TO_SENSOR_TRANSFORM_BY_FAMILY = {
        "OS0": np.array([[1.0, 0.0, 0.0, 27.67],
                         [0.0, 1.0, 0.0, 0.0],
                         [0.0, 0.0, 1.0, 0.0],
                         [0.0, 0.0, 0.0, 1.0]], dtype=np.float64),
    }
    # DIAGNOSED IN PRODUCTION (2026-09-01): the SDK's generic default here
    # carries a 180-deg rotation about Z (diag(-1,-1,1)). Detect derives
    # its point-cloud leveling/orientation from the synthetic IMU gravity
    # vector (C6) through this transform, so that 180-deg flip mirrored the
    # pitch/roll direction for OS1 -- tilting the physical bracket down
    # (rotateXYZ Y on the parent Xform) displayed as tilting up in Detect.
    # OS0's real (calibrated) lidar_to_sensor_transform has an IDENTITY
    # rotation, which is why OS0 never showed this. Using identity here
    # instead of the SDK default until we have OS1's real calibrated value
    # -- rotation matters for this bug, translation doesn't, so this is a
    # safe placeholder (unlike the SDK default, which actively broke it).
    DEFAULT_LIDAR_TO_SENSOR_TRANSFORM = np.array(
        [[1.0, 0.0, 0.0, 0.0],
         [0.0, 1.0, 0.0, 0.0],
         [0.0, 0.0, 1.0, 36.18],
         [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)

    @staticmethod
    def internal_state():
        return OgnOusterLidarInternalState()

    @staticmethod
    def _read_sensor_variant_from_prim(sensor_path):
        """Read the selected variant from the 'sensor' USD variant set on the
        sensor prim (e.g. 'OS0_REV7_128ch10hz1024res'). Reads a VARIANT SET
        selection, not an attribute; confirmed working in production with
        the sensorVariant input left empty."""
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
    def _compose_sensor_info(lidar_sensor_variant, udp_port_lidar, udp_port_imu, udp_dest, sensor_sn=1234567890):
        """Build a SensorInfo from the Isaac Sim RTX lidar config JSON file.

        OS1 SUPPORT: prod_line and beam origin now follow sensor_family
        (see BEAM_ORIGIN_MM_BY_FAMILY). imu/lidar_to_sensor_transform fall
        back to the Ouster SDK's generic default for any family we don't
        have real calibrated metadata for yet (see
        IMU_TO_SENSOR_TRANSFORM_BY_FAMILY / DEFAULT_IMU_TO_SENSOR_TRANSFORM
        above) -- revisit once a physical unit of that family is connected.
        """
        ext_manager = omni.kit.app.get_app_interface().get_extension_manager()
        ext_id = ext_manager.get_enabled_extension_id("isaacsim.sensors.rtx")
        isaac_sim_ext_path = ext_manager.get_extension_path(ext_id)
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
        num_channels = lidar_cfg["numberOfEmitters"]
        max_returns = lidar_cfg.get("maxReturns", 1)
        azimuth_deg = lidar_cfg["emitterStates"][0]["azimuthDeg"]
        elevation_deg = lidar_cfg["emitterStates"][0]["elevationDeg"]
        scan_rate_hz = int(lidar_cfg["scanRateBaseHz"])
        report_rate_hz = int(lidar_cfg["reportRateBaseHz"])
        columns_per_frame = int(report_rate_hz / scan_rate_hz)

        sensor_info = SensorInfo()
        sensor_info.sn = int(sensor_sn)  # CHECKPOINT A: identity from input
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
        # C5: publish the sensor's REAL azimuth offsets; the encoder column
        # folds this offset in (_map_distance_to_u_v) so client
        # reconstruction encoder(v) - beam_azimuth[row] is the true azimuth.
        sensor_info.beam_azimuth_angles = np.array(azimuth_deg, dtype=np.float64)
        # OS1 SUPPORT: beam origin keyed by real family (falls back to OS0's
        # value only if an unknown family slips through).
        beam_origin_mm = OgnOusterLidar.BEAM_ORIGIN_MM_BY_FAMILY.get(sensor_family, 27.67)
        sensor_info.beam_to_lidar_transform = np.array(
            [[1.0, 0.0, 0.0, beam_origin_mm],
             [0.0, 1.0, 0.0, 0.0],
             [0.0, 0.0, 1.0, 0.0],
             [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)
        sensor_info.build_date = "2022-12-23T22:27:12Z"
        sensor_info.fw_rev = "v3.0.0-beta.2-23-gdbfc6371c"
        sensor_info.image_rev = "ousteros-image-prod-bootes-v3.0.0-beta.2+20221223222646"
        # CHECKPOINT A: init_id derived from SN (SENSOR_CONVENTION.md).
        sensor_info.init_id = OgnOusterLidar.BASE_INIT_ID + (int(sensor_sn) - OgnOusterLidar.BASE_SN)
        # OS1 SUPPORT: prod_line now reflects the real family (was a
        # literal "OS-0-" regardless of variant).
        sensor_info.prod_line = f"OS-{sensor_family[2:]}-{num_channels}"
        sensor_info.prod_pn = "XXX-XXXXXXX-XX"
        sensor_info.status = "RUNNING"
        sensor_info.user_data = ""
        sensor_info.imu_to_sensor_transform = OgnOusterLidar.IMU_TO_SENSOR_TRANSFORM_BY_FAMILY.get(
            sensor_family, OgnOusterLidar.DEFAULT_IMU_TO_SENSOR_TRANSFORM)
        sensor_info.lidar_to_sensor_transform = OgnOusterLidar.LIDAR_TO_SENSOR_TRANSFORM_BY_FAMILY.get(
            sensor_family, OgnOusterLidar.DEFAULT_LIDAR_TO_SENSOR_TRANSFORM)
        sensor_info.lidar_origin_to_beam_origin_mm = beam_origin_mm
        sensor_info.format.column_window = [0, columns_per_frame - 1]
        sensor_info.format.columns_per_frame = columns_per_frame
        sensor_info.format.columns_per_packet = 16
        sensor_info.format.fps = scan_rate_hz
        sensor_info.format.header_type = HeaderType.STANDARD
        # C5: true per-row column stagger for client destaggering.
        _col_per_deg = columns_per_frame / 360.0
        sensor_info.format.pixel_shift_by_row = [
            int(round(a * _col_per_deg)) for a in azimuth_deg
        ]
        sensor_info.format.imu_measurements_per_packet = 0
        sensor_info.format.imu_packets_per_frame = 0
        sensor_info.format.pixels_per_column = num_channels
        sensor_info.format.udp_profile_imu = UDPProfileIMU.LEGACY
        sensor_info.format.udp_profile_lidar = UDPProfileLidar.RNG19_RFL8_SIG16_NIR16
        sensor_info.format.zone_monitoring_enabled = False
        return sensor_info, max_returns

    @staticmethod
    def _preserve_client_config(new_info, old_info, state, udp_port_lidar, udp_port_imu):
        """B-CFG: carry client-pushed configuration across a reinit.

        Ouster clients (Detect) push udp_dest / udp ports / operating_mode
        through the HTTP config API at connect time and then assume the
        sensor retains them, like hardware does. A reinit that rebuilds
        SensorInfo from input defaults silently discards that push and
        desynchronizes the node from the client (observed in production:
        Detect listening on reassigned ports while the node streamed to
        defaults -> data timeout).

        Rule: preserve the old value UNLESS the corresponding node INPUT is
        what changed (then the operator's input wins). udp_dest has no
        say in needs_reinit, so it is always preserved here; the runtime
        branch in compute() still applies an operator's udp_dest change.
        """
        if old_info is None:
            return
        new_info.config.udp_dest = old_info.config.udp_dest
        new_info.config.operating_mode = old_info.config.operating_mode
        if state._last_udp_port_lidar == udp_port_lidar:
            new_info.config.udp_port_lidar = old_info.config.udp_port_lidar
        if state._last_udp_port_imu == udp_port_imu:
            new_info.config.udp_port_imu = old_info.config.udp_port_imu

    @staticmethod
    def _setup_pipeline(state, sensor_path):
        """Create Render Product and annotator for the given sensor path."""
        state.render_product = rep.create.render_product(
            f"{sensor_path}/sensor", [1, 1],
            name="OgnOusterLidarRenderProduct",
            render_vars=["GenericModelOutput", "RtxSensorMetadata"],
        )
        state.render_product_path = state.render_product.path

        state.annotator = rep.AnnotatorRegistry.get_annotator("IsaacCreateRTXLidarScanBuffer")
        state.annotator.initialize(
            outputIntensity=True,
            outputDistance=True,
            keepOnlyPositiveDistance=False,
            transformPoints=False,
            outputTimestamp=True,
            outputAzimuth=True,
            outputElevation=True
        )
        state.annotator.attach([state.render_product_path])

    @staticmethod
    def _start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port):
        """Create UDP sockets and start the fixed-rate emitter thread (B-EMIT).

        The emitter reads udp_dest / udp ports LIVE from
        state.sensor_info.config on every send: the HTTP API
        (configure_sensor, driven by Detect or ouster-cli) mutates that
        config at runtime, so a value captured at thread spawn would go
        stale on the first client reconfiguration.
        """
        state.frame_id = 0
        state.pkt_count = 0
        state.scan_range_m = None
        state.scan_intensity = None
        state.covered_cols = None        # C4
        state.render_frames_per_scan = 0  # C4
        with state.buffer_lock:
            state.latest_range = None
            state.latest_intensity = None
        state.emitter_stop = threading.Event()

        # HTTP-LIFECYCLE PATCH: persistent holder; server created ONCE per
        # node, surviving sensor changes.
        if state.http_holder is None:
            state.http_holder = _SensorHolder(state.sensor_info)
        else:
            state.http_holder.set_sensor_info(state.sensor_info)

        if state.http_server is None:
            try:
                state.http_server = HttpServer(state.http_holder, http_addr,
                                               http_port, autostart=True)
                state.http_server_error = ""
            except Exception as e:
                state.http_server = None
                state.http_server_error = f"{type(e).__name__}: {e}"
                # HTTP bind failed: UDP intentionally not started; the node
                # retries on the next reinit-triggering input change.
                return
        else:
            state.http_server_error = ""

        state.sock_lidar = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        state.sock_imu = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        def _emitter_loop(stop_event, sock_l, sock_i, state_ref):
            """B-EMIT: fixed-rate wall-clock frame emitter.

            Every 1/fps seconds (fps from sensor_info.format.fps), take the
            latest COMPLETE rotation from the double buffer, stamp
            wall-clock timestamps and a fresh frame_id, packetize, and send
            with intra-frame pacing (packets spread across PACKET_SPREAD of
            the period). If the simulation has not produced a new rotation
            since the last emission, the previous one is re-sent: Detect
            sees a steady sensor watching a momentarily frozen world.
            Absolute deadlines (next_deadline += period) prevent cumulative
            drift regardless of OS sleep granularity.

            B-SEND: an OSError is logged (throttled) and skipped, never
            fatal; an empty udp_dest skips the frame entirely.
            """
            sensor_info = state_ref.sensor_info
            fps = int(sensor_info.format.fps) or 10
            period = 1.0 / fps
            width = int(sensor_info.format.columns_per_frame)
            scan = LidarScan(sensor_info)
            n_imu = 10  # IMU packets per frame (~100 Hz at 10 fps)
            last_err_log = 0.0

            next_deadline = time.perf_counter() + period
            while not stop_event.is_set():
                # Sleep to the frame deadline in short chunks so stop is
                # responsive; absolute deadline -> no drift.
                while not stop_event.is_set():
                    remaining = next_deadline - time.perf_counter()
                    if remaining <= 0:
                        break
                    time.sleep(min(remaining, 0.01))
                if stop_event.is_set():
                    break
                next_deadline += period

                with state_ref.buffer_lock:
                    rng = state_ref.latest_range
                    refl = state_ref.latest_intensity
                    grav = state_ref.latest_gravity
                if rng is None:
                    continue  # no complete rotation yet

                cfg = sensor_info.config
                dest = cfg.udp_dest
                if not dest:
                    continue  # B-SEND: unconfigured destination, skip

                # Fresh identity + wall-clock time base for this emission.
                state_ref.frame_id += 1
                scan.frame_id = state_ref.frame_id
                scan.field("RANGE")[:] = np.where(rng > 0, rng * 1e+3, 0).astype(np.uint32)
                scan.field("REFLECTIVITY")[:] = np.clip(refl * 255.0, 0, 255).astype(np.uint8)
                now_ns = time.time_ns()
                period_ns = int(period * 1e9)
                # Columns span the rotation that just "finished": from
                # now - period to now, like a physical sensor.
                timestamps = (now_ns - period_ns
                              + np.linspace(0, period_ns, width)).astype(np.uint64)
                scan.timestamp[:] = timestamps
                scan.status[:] = 0x1
                packet_count = len(scan.packet_timestamp)
                scan.packet_timestamp[:] = timestamps[np.arange(packet_count) * 16]

                try:
                    packets = scan_to_packets(scan, sensor_info)
                except Exception as e:
                    now = time.monotonic()
                    if now - last_err_log > OgnOusterLidar.SEND_ERROR_LOG_PERIOD:
                        print(f"OgnOusterLidar emitter: packetization failed: {e}")
                        last_err_log = now
                    continue
                lidar_payloads = [bytes(p.buf) for p in packets
                                  if p.type == PacketType.Lidar]
                n_lidar = len(lidar_payloads)
                if n_lidar == 0:
                    continue

                # C6: synthetic IMU from the cached gravity vector (sampled
                # in compute(), the USD-safe thread). LEGACY 48 B: 3x uint64
                # ts + accel XYZ (g) + gyro XYZ (deg/s); static bracket =>
                # gyro 0.
                imu_ts = (now_ns - period_ns
                          + np.linspace(0, period_ns, n_imu)).astype(np.uint64)
                imu_payloads = [
                    struct.pack('<QQQffffff', int(t), int(t), int(t),
                                float(grav[0]), float(grav[1]),
                                float(grav[2]), 0.0, 0.0, 0.0)
                    for t in imu_ts
                ]

                # Intra-frame pacing: lidar packets spread across
                # PACKET_SPREAD of the period (Detect's ingest and OS UDP
                # buffers behave better without a single burst); one IMU
                # packet interleaved every n_lidar/n_imu lidar packets.
                frame_start = time.perf_counter()
                spread = period * OgnOusterLidar.PACKET_SPREAD
                imu_every = max(1, n_lidar // n_imu)
                imu_idx = 0
                for i, payload in enumerate(lidar_payloads):
                    pkt_deadline = frame_start + spread * (i / n_lidar)
                    while not stop_event.is_set():
                        rem = pkt_deadline - time.perf_counter()
                        if rem <= 0:
                            break
                        time.sleep(min(rem, 0.005))
                    if stop_event.is_set():
                        break
                    try:
                        sock_l.sendto(payload, (dest, cfg.udp_port_lidar))
                        state_ref.pkt_count += 1
                        if i % imu_every == 0 and imu_idx < n_imu:
                            sock_i.sendto(imu_payloads[imu_idx],
                                          (dest, cfg.udp_port_imu))
                            imu_idx += 1
                            state_ref.pkt_count += 1
                    except OSError as e:
                        # B-SEND: transient network error; log throttled,
                        # keep the emitter alive.
                        now = time.monotonic()
                        if now - last_err_log > OgnOusterLidar.SEND_ERROR_LOG_PERIOD:
                            print(f"OgnOusterLidar emitter: send failed to "
                                  f"{dest}: {e}")
                            last_err_log = now

        state.emitter_thread = threading.Thread(
            target=_emitter_loop,
            args=(state.emitter_stop, state.sock_lidar, state.sock_imu, state),
            daemon=True,
        )
        state.emitter_thread.start()

    @staticmethod
    def _stop_streaming(state, stop_http=False):
        """Tear down streaming resources. The HTTP server is stopped ONLY
        with stop_http=True (node release, or HTTP addr/port change)."""
        if state.emitter_stop is not None:
            state.emitter_stop.set()
        if state.emitter_thread is not None:
            state.emitter_thread.join(timeout=1.0)
            state.emitter_thread = None
        state.emitter_stop = None

        if state.sock_lidar is not None:
            state.sock_lidar.close()
            state.sock_lidar = None
        if state.sock_imu is not None:
            state.sock_imu.close()
            state.sock_imu = None

        if stop_http and state.http_server is not None:  # HTTP-LIFECYCLE PATCH
            state.http_server.stop()
            state.http_server = None
            state.http_holder = None

        with state.buffer_lock:
            state.latest_range = None
            state.latest_intensity = None
        state.scan_range_m = None
        state.scan_intensity = None
        state.covered_cols = None        # C4
        state.render_frames_per_scan = 0  # C4
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
    def _gravity_in_sensor_frame(rot_local_to_world_rowmajor):
        """C6 (pure, unit-testable): given the 3x3 rotation block of USD's
        ComputeLocalToWorldTransform (row-vector convention: world =
        local * R), return the vector a resting accelerometer measures in
        the sensor frame: +1g toward the world zenith in local axes.
        local = R @ world for orthonormal R in that convention."""
        R = np.asarray(rot_local_to_world_rowmajor, dtype=np.float64).reshape(3, 3)
        a = R @ np.array([0.0, 0.0, 1.0])
        n = np.linalg.norm(a)
        return a / n if n > 1e-9 else np.array([0.0, 0.0, 1.0])

    @staticmethod
    def _sensor_gravity_accel(sensor_path):
        """C6: WORLD orientation of the sensor prim via USD.
        ComputeLocalToWorldTransform composes the ENTIRE parent chain --
        the same transform the raytracer uses, so IMU and cloud are always
        coherent. MUST be called from the main thread (USD access is not
        thread-safe); compute() caches the result for the emitter.
        Fallback on any failure: a level sensor."""
        try:
            import omni.usd
            from pxr import Usd, UsdGeom
            stage = omni.usd.get_context().get_stage()
            prim = stage.GetPrimAtPath(sensor_path)
            xf = UsdGeom.Xformable(prim).ComputeLocalToWorldTransform(
                Usd.TimeCode.Default())
            rot = xf.ExtractRotationMatrix()
            R = [[rot[i][j] for j in range(3)] for i in range(3)]
            return OgnOusterLidar._gravity_in_sensor_frame(R)
        except Exception:
            return np.array([0.0, 0.0, 1.0])

    @staticmethod
    def _map_distance_to_u_v(state, width, height, distance, intensity, azimuth, elevation):
        """Map flat distance/intensity arrays into the 2D scan image.
        u (row): nearest beam elevation. v (column): encoder angle per C5,
        inverted per C3 (clockwise scan). Accumulates across ticks (C4)."""
        # C4: allocate only; clearing happens at buffer-swap time.
        if (state.scan_range_m is None or state.scan_range_m.shape != (height, width)):
            state.scan_range_m = np.zeros((height, width), dtype=np.float32)
            state.scan_intensity = np.zeros((height, width), dtype=np.float32)
            state.covered_cols = np.zeros(width, dtype=bool)

        elevation_lut = state.elevation_lut_deg_sorted
        elevation_order = state.elevation_lut_order

        insert_idx = np.searchsorted(elevation_lut, elevation, side="left")
        right_idx = np.clip(insert_idx, 0, height - 1)
        left_idx = np.clip(insert_idx - 1, 0, height - 1)
        use_right = np.abs(elevation_lut[right_idx] - elevation) < np.abs(elevation - elevation_lut[left_idx])
        u = np.where(use_right, elevation_order[right_idx], elevation_order[left_idx]).astype(np.intp, copy=False)
        np.clip(u, 0, height - 1, out=u)

        # C5 v2: column = ENCODER angle; the client subtracts the offset on
        # reconstruction, so the encoder carries it ADDED (sign verified
        # empirically; see revision history).
        encoder_deg = np.remainder(azimuth + state.beam_azimuth_lut[u], 360.0)
        v = np.floor(encoder_deg * (width / 360.0)).astype(np.intp)
        np.clip(v, 0, width - 1, out=v)
        # C3: clockwise scan -> column inversion; without it the cloud is
        # mirrored across Y (see revision history for the measurements).
        v = (width - v) % width

        # C4: accumulate; compute() decides when the rotation is complete.
        state.covered_cols[v] = True
        state.scan_range_m[u, v] = distance
        state.scan_intensity[u, v] = intensity
        return state.scan_range_m, state.scan_intensity

    @staticmethod
    def compute(db) -> bool:
        """Per-tick entry point: config handling and rotation accumulation.
        B-EMIT: emission moved to the fixed-rate emitter thread; this
        function's only output duty is swapping complete rotations into the
        double buffer."""
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
        sensor_sn = db.inputs.sensorSn  # CHECKPOINT A

        if not sensor_path:
            db.outputs.execOut = og.ExecutionAttributeState.DISABLED
            if not state.warned_no_path:  # C5w: warn once, not per tick
                db.log_warn("OgnOusterLidar: no sensor prim path set, disabling execution")
                state.warned_no_path = True
            return False
        state.warned_no_path = False

        if not sensor_variant:
            sensor_variant = OgnOusterLidar._read_sensor_variant_from_prim(sensor_path)
            if not sensor_variant:
                db.outputs.execOut = og.ExecutionAttributeState.DISABLED
                if not state.warned_no_variant:  # C5w
                    db.log_warn(f"OgnOusterLidar: could not read 'sensor' variant set from prim '{sensor_path}'")
                    state.warned_no_variant = True
                return False
        state.warned_no_variant = False

        if not state.initialized or state.needs_reinit(
                sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled, sensor_sn):
            old_sensor_info = state.sensor_info  # B-CFG: for config carry-over
            if state.initialized:
                # HTTP-LIFECYCLE PATCH: HTTP server destroyed only on bind
                # addr/port change.
                stop_http = state.http_config_changed(http_addr, http_port)
                OgnOusterLidar._stop_streaming(state, stop_http=stop_http)
                OgnOusterLidar._teardown_pipeline(state)

            try:
                state.sensor_info, state.max_returns = OgnOusterLidar._compose_sensor_info(
                    sensor_variant, udp_port_lidar, udp_port_imu, udp_dest, sensor_sn)
            except Exception as error:
                db.log_error(f"Failed to load lidar config: {error}")
                db.outputs.execOut = og.ExecutionAttributeState.DISABLED
                return False

            # B-CFG: client-pushed config survives the reinit.
            OgnOusterLidar._preserve_client_config(
                state.sensor_info, old_sensor_info, state, udp_port_lidar, udp_port_imu)

            elevation_lut_deg = np.array(state.sensor_info.beam_altitude_angles, dtype=np.float64)
            state.elevation_lut_order = np.argsort(elevation_lut_deg)
            state.elevation_lut_deg_sorted = elevation_lut_deg[state.elevation_lut_order]
            # C5: per-row azimuth offset, folded into the encoder column.
            state.beam_azimuth_lut = np.array(
                state.sensor_info.beam_azimuth_angles, dtype=np.float64)

            OgnOusterLidar._setup_pipeline(state, sensor_path)

            if enabled:
                OgnOusterLidar._start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port)
                if state.http_server_error:
                    db.log_error(f"OgnOusterLidar: HTTP bind failed on {http_addr}:{http_port} -> {state.http_server_error}")

            state.initialized = True
            state.save_config(sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled, sensor_sn)

        if state.config_changed_runtime(udp_dest, enabled):
            if not enabled and state.sock_lidar is not None:
                # HTTP-LIFECYCLE PATCH: HTTP server survives disable.
                OgnOusterLidar._stop_streaming(state)
            elif enabled and state.sock_lidar is None:
                OgnOusterLidar._start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port)
                if state.http_server_error:
                    db.log_error(f"OgnOusterLidar: HTTP bind failed on {http_addr}:{http_port} -> {state.http_server_error}")
            if state.sensor_info is not None and state._last_udp_dest != udp_dest:
                # Operator changed the input: it wins over any client push.
                state.sensor_info.config.udp_dest = udp_dest
            state.save_config(sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled, sensor_sn)

        # --- Debug outputs, populated every tick ---
        db.outputs.httpServerError = state.http_server_error or ""
        if state.http_server is not None:
            db.outputs.httpBoundAddr = f"{http_addr}:{http_port}"
            db.outputs.lastClientAddr = state.http_server.last_client_addr
        else:
            db.outputs.httpBoundAddr = ""
            db.outputs.lastClientAddr = ""
        db.outputs.packetsSent = state.pkt_count
        db.outputs.frameId = state.frame_id

        if not enabled or state.annotator is None:
            db.outputs.execOut = og.ExecutionAttributeState.DISABLED
            return True

        scan_data = state.annotator.get_data()
        if scan_data is None or "distance" not in scan_data or "intensity" not in scan_data:
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True

        distance = np.asarray(scan_data["distance"])
        intensity = np.asarray(scan_data["intensity"])
        azimuth = np.asarray(scan_data["azimuth"])
        elevation = np.asarray(scan_data["elevation"])

        if distance.size == 0 or intensity.size == 0 or azimuth.size == 0 or elevation.size == 0:
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True

        width = int(state.sensor_info.format.columns_per_frame)
        height = int(state.sensor_info.format.pixels_per_column)

        scan_range_m, scan_intensity = OgnOusterLidar._map_distance_to_u_v(
            state, width, height, distance, intensity, azimuth, elevation)

        # C4: a rotation is complete at coverage threshold; partial ticks
        # keep accumulating.
        state.render_frames_per_scan += 1
        coverage = float(state.covered_cols.mean())
        if coverage < OgnOusterLidar.SCAN_COMPLETE_COVERAGE:
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True

        # B-EMIT: swap the completed rotation into the double buffer for
        # the emitter thread. Copies (~0.5 MB for 128x1024 float32) keep
        # the accumulators free to be cleared immediately. Gravity (C6) is
        # sampled here, in the USD-safe main thread, and cached.
        gravity = OgnOusterLidar._sensor_gravity_accel(state._last_sensor_path)
        with state.buffer_lock:
            state.latest_range = scan_range_m.copy()
            state.latest_intensity = scan_intensity.copy()
            state.latest_gravity = gravity

        # C4: rotation handed off -> reset the accumulator.
        state.scan_range_m.fill(0)
        state.scan_intensity.fill(0)
        state.covered_cols.fill(False)
        state.render_frames_per_scan = 0

        db.outputs.execOut = og.ExecutionAttributeState.ENABLED
        return True

    @staticmethod
    def release(node):
        """Node release: full teardown including the HTTP server."""
        try:
            from ouster.rtx_lidar.ogn.OgnOusterLidarDatabase import OgnOusterLidarDatabase
            per_node_data = OgnOusterLidarDatabase.PER_NODE_DATA.get(node.node_id())
            if per_node_data:
                state = per_node_data.get("state")
                if state and state.initialized:
                    OgnOusterLidar._stop_streaming(state, stop_http=True)  # HTTP-LIFECYCLE PATCH
                    OgnOusterLidar._teardown_pipeline(state)
                    state.initialized = False
        except Exception:
            pass
