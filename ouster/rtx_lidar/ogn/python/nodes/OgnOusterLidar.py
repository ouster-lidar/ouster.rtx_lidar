# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Jorge Octavio Martinez Tovar, Ussama Naal
#
# ouster.rtx_lidar -- OgnOusterLidar node
# =============================================================================
# Virtual Ouster lidar sensor for NVIDIA Isaac Sim / OmniGraph.
#
# This node reads the RTX lidar annotator output for a sensor prim, rebuilds
# it into an Ouster-native scan image, packetizes it with the Ouster SDK, and
# streams UDP lidar/IMU packets while serving the Ouster sensor HTTP API.
# The result is a virtual sensor that Ouster clients (Ouster Detect / Gemini,
# ouster-cli, Ouster Studio) discover, configure, and consume exactly as if
# it were physical hardware.
#
# Authors:
#   Jorge Octavio Martinez Tovar (Digital Twin Engineering, Ternium
#     Pesqueria) -- point cloud generation conventions (C3-C5), synthetic
#     IMU (C6), HTTP lifecycle patch, multi-sensor support (Checkpoint A),
#     production integration and validation against Ouster Detect.
#   Ussama Naal (Ouster, Inc.) -- original ouster.rtx_lidar extension:
#     base OGN node, sensor HTTP API emulation, and pcap replay tooling.
#
# -----------------------------------------------------------------------------
# REVISION HISTORY (patch identifiers referenced throughout the code)
# -----------------------------------------------------------------------------
# HTTP-LIFECYCLE PATCH
#   Root cause of the original crash when changing inputs:sensorPath at
#   runtime: HttpServer.__init__ registered @app.route handlers on a
#   module-global Flask app on every construction. Flask forbids route
#   registration after the first request has been served (Detect polls the
#   API), and the HTTP port stayed owned by the previous server instance,
#   producing AssertionError + bind failure and killing the pipeline for the
#   whole session. Fix strategy: (1) a mutable module-level _SensorHolder,
#   (2) the HttpServer and its holder PERSIST across sensor reinits -- on a
#   sensorPath change only UDP/thread/pipeline are torn down and the holder
#   is re-pointed at the new sensor_info, (3) the HTTP server is destroyed
#   only when httpAddr/httpPort change or on node release().
#
# C3 (scan direction)
#   Ouster sensors scan CLOCKWISE: as the encoder count (column) increases,
#   the azimuth angle about +Z decreases. Clients reconstruct
#   theta(v) = 360 * (1 - v/W). Without the column inversion,
#   sin(360 - a) = -sin(a) and the reconstructed cloud comes out MIRRORED
#   across Y. Empirically verified with the SDK XYZLut: 19.54 m RMS error
#   vs ground truth without the fix, 0.0745 m vs mirrored ground truth.
#
# C4 (full-rotation emission gate)
#   One Ouster frame corresponds to one full rotation. Render ticks deliver
#   partial azimuth batches, so the node ACCUMULATES into the scan image and
#   emits only when column coverage reaches SCAN_COMPLETE_COVERAGE.
#   Empirically verified: emitting every tick reintroduces blur/jumps; the
#   coverage gate also filters ticks where the annotator returns a partial
#   batch.
#
# C5 (azimuth calibration / encoder convention, v2)
#   The sensor's REAL per-beam azimuth offsets are published in metadata
#   (zeroing them erased calibration and made the output depend on the
#   client honoring zeros instead of the internal calibration of the
#   declared model, prod_line "OS-0-128"; with internal calibration the 4
#   emitter groups, measured offsets [-4.220, +4.250] deg with ~2.81 deg
#   separation, fan out into 4 ghost copies -- the autocorrelation peak at
#   lag=8 columns = 2.81 deg). The image column represents the ENCODER
#   angle. The Ouster manual defines theta_azimuth = -2*pi*(beam_azimuth/
#   360): the client SUBTRACTS the offset during reconstruction
#   (theta = encoder(v) - offset), therefore the encoder must carry the
#   offset ADDED: theta = (a + off) - off = a. Sign verified empirically:
#   subtracting here (v1) DOUBLED the 4-group misalignment (a - 2*off);
#   adding cancels it. Works identically whether the client uses the
#   published metadata or its internal model calibration.
#
# C6 (synthetic IMU)
#   Detect derives sensor pitch/roll from the accelerometer ("IMU Pose"
#   button in perception setup); without IMU packets it assumes a level
#   sensor and the cloud inherits the bracket tilt with no way to correct
#   it in the UI (which only exposes X/Y/heading). The node synthesizes
#   LEGACY-format 48-byte IMU packets (3x uint64 timestamps + accel XYZ in
#   g + gyro XYZ in deg/s) carrying the gravity vector computed from the
#   sensor prim's world orientation via USD ComputeLocalToWorldTransform --
#   the same transform chain the raytracer uses, so IMU and point cloud are
#   always coherent. Static bracket => gyro = 0. 10 packets per frame
#   (~100 Hz nominal).
#
# CHECKPOINT A (multi-sensor enabler)
#   B1: extension.py now creates one Flask app PER HttpServer instance,
#       removing the residual limitation of the HTTP-lifecycle patch:
#       hot-reload and multiple Ouster nodes in one session no longer
#       collide (each node binds its own NIC IP on port 80; see
#       SENSOR_CONVENTION.md).
#   ID: new input sensorSn (sensor identity). Default 1234567890 is
#       Sensor #1, the identity the Ouster Detect license was activated
#       against. init_id is derived: BASE_INIT_ID + (sn - BASE_SN).
#   C3q: the packet sender queue is bounded with whole-frame drop-oldest
#       semantics (MAX_QUEUED_FRAMES).
#   C5w: missing-path / missing-variant warnings are latched (logged once,
#       reset when the condition clears) instead of firing at tick rate.
# =============================================================================

import numpy as np
import omni.graph.core as og
import omni.kit.app
import omni.timeline
import omni.replicator.core as rep

import socket
import json
import queue
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

    HTTP-LIFECYCLE PATCH: the HttpServer keeps a reference to this holder
    across sensor reinits; set_sensor_info() re-points the metadata served
    by the API without ever reconstructing the Flask app or the server.
    """

    def __init__(self, si):
        self._si = si

    def set_sensor_info(self, si):
        """Re-point the holder at a new SensorInfo (called on sensor reinit)."""
        self._si = si

    @property
    def sensor_info(self):
        return self._si


class OgnOusterLidarInternalState:
    """Per-node internal state for the Ouster Lidar OGN node.

    One instance lives in OgnOusterLidarDatabase.PER_NODE_DATA per node, so
    multiple nodes in one session are fully isolated from each other.
    """

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
        self.http_holder = None  # HTTP-LIFECYCLE PATCH: persistent holder
        self.http_server_error = ""
        self.sock_lidar = None
        self.sock_imu = None
        self.packet_queue = None
        self.packet_sender_stop = None
        self.packet_sender_thread = None
        self.elevation_lut_order = None
        self.elevation_lut_deg_sorted = None
        self.max_returns = 1
        self.beam_azimuth_lut = None   # C5: per-row azimuth offset (deg)
        self.covered_cols = None      # C4: columns swept in the current rotation
        self.render_frames_per_scan = 0  # C4: diagnostics
        # C5w (CHECKPOINT A): one-shot warning latches (reset when the
        # condition clears) so missing-path/variant warnings don't fire at
        # tick rate (~60/s).
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

        Note: udp_dest and enabled are deliberately excluded -- they are
        runtime-changeable (see config_changed_runtime) and must not tear
        down the render product or the HTTP server.
        """
        return (self._last_sensor_path != sensor_path or
                self._last_sensor_variant != sensor_variant or
                self._last_udp_port_lidar != udp_port_lidar or
                self._last_udp_port_imu != udp_port_imu or
                self._last_http_addr != http_addr or
                self._last_http_port != http_port or
                self._last_sensor_sn != sensor_sn)

    def http_config_changed(self, http_addr, http_port):
        """True only when the change requires rebuilding the HTTP server.

        HTTP-LIFECYCLE PATCH: on a plain sensor change the server survives;
        it is destroyed exclusively for a bind address/port change (or node
        release).
        """
        return (self._last_http_addr is not None and
                (self._last_http_addr != http_addr or
                 self._last_http_port != http_port))

    def config_changed_runtime(self, udp_dest, enabled):
        """Check if runtime-changeable config has changed."""
        return (self._last_udp_dest != udp_dest or
                self._last_enabled != enabled)

    def save_config(self, sensor_path, sensor_variant, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port, enabled, sensor_sn=None):
        """Persist the last-applied input snapshot.

        CHECKPOINT A: sensor_sn is keyword-with-default so every call site
        can pass it explicitly; None preserves the previously stored value.
        """
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

    # C4: fraction of columns that must be covered before a rotation is
    # emitted as a frame.
    SCAN_COMPLETE_COVERAGE = 0.98
    # C3q (CHECKPOINT A): maximum whole frames the sender queue may hold
    # before drop-oldest kicks in.
    MAX_QUEUED_FRAMES = 3
    # CHECKPOINT A -- sensor identity convention (SENSOR_CONVENTION.md):
    # Sensor #1 is the licensed identity. init_id is derived from the SN
    # delta so both identities stay unique in lockstep.
    BASE_SN = 1234567890
    BASE_INIT_ID = 7777777

    @staticmethod
    def internal_state():
        return OgnOusterLidarInternalState()

    @staticmethod
    def _read_sensor_variant_from_prim(sensor_path):
        """Read the selected variant from the 'sensor' USD variant set on the
        sensor prim (e.g. 'OS0_REV7_128ch10hz1024res'). Note: this reads a
        VARIANT SET selection, not an attribute; confirmed working in
        production with the sensorVariant input left empty."""
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

        The variant JSON (shipped with isaacsim.sensors.rtx) provides beam
        intrinsics, channel count and scan/report rates; everything else is
        populated to look like a plausible physical OS-series sensor.

        CHECKPOINT A: sensor_sn parametrizes the virtual sensor identity;
        the default is Sensor #1 (the licensed identity, == BASE_SN).
        init_id is derived from the SN delta.

        Known limitation (accepted, out of current scope): prod_line, the
        beam/IMU transforms and lidar_origin_to_beam_origin_mm are OS0
        values regardless of variant family. Irrelevant while all deployed
        variants are OS0; must be revisited before mixing OS1/OS2.
        """
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
        # C5: publish the sensor's REAL azimuth offsets (see revision
        # history for the full rationale and the empirical evidence of the
        # "4 ghost fans" failure mode when these are zeroed).
        # _map_distance_to_u_v folds this offset into the encoder column so
        # that the client reconstruction encoder(v) - beam_azimuth[row]
        # returns the true azimuth.
        sensor_info.beam_azimuth_angles = np.array(azimuth_deg, dtype=np.float64)
        sensor_info.beam_to_lidar_transform = np.array(
            [[1.0, 0.0, 0.0, 27.67],
             [0.0, 1.0, 0.0, 0.0],
             [0.0, 0.0, 1.0, 0.0],
             [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)
        sensor_info.build_date = "2022-12-23T22:27:12Z"
        sensor_info.fw_rev = "v3.0.0-beta.2-23-gdbfc6371c"
        sensor_info.image_rev = "ousteros-image-prod-bootes-v3.0.0-beta.2+20221223222646"
        # CHECKPOINT A: init_id derived from SN so both identities stay
        # unique in lockstep across sensors (SENSOR_CONVENTION.md).
        sensor_info.init_id = OgnOusterLidar.BASE_INIT_ID + (int(sensor_sn) - OgnOusterLidar.BASE_SN)
        sensor_info.prod_line = f"OS-0-{num_channels}"
        sensor_info.prod_pn = "XXX-XXXXXXX-XX"
        sensor_info.status = "RUNNING"
        sensor_info.user_data = ""
        sensor_info.imu_to_sensor_transform = np.array(
            [[1.0, 0.0, 0.0, -2.441],
             [0.0, 1.0, 0.0, -9.725],
             [0.0, 0.0, 1.0, 7.533],
             [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)
        sensor_info.lidar_to_sensor_transform = np.array(
            [[1.0, 0.0, 0.0, 27.67],
             [0.0, 1.0, 0.0, 0.0],
             [0.0, 0.0, 1.0, 0.0],
             [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)
        sensor_info.lidar_origin_to_beam_origin_mm = 27.67
        sensor_info.format.column_window = [0, columns_per_frame - 1]
        sensor_info.format.columns_per_frame = columns_per_frame
        sensor_info.format.columns_per_packet = 16
        sensor_info.format.fps = scan_rate_hz
        sensor_info.format.header_type = HeaderType.STANDARD
        # C5: the true per-row column stagger introduced by each azimuth
        # offset, so clients can destagger the 2D image correctly.
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
        state.covered_cols = None        # C4
        state.render_frames_per_scan = 0  # C4
        state.packet_queue = queue.Queue()
        state.packet_sender_stop = threading.Event()

        # HTTP-LIFECYCLE PATCH: persistent, mutable holder; the HttpServer
        # is created ONCE per node and survives sensor changes. On a change,
        # only the holder is re-pointed at the new sensor_info.
        if state.http_holder is None:
            state.http_holder = _SensorHolder(state.sensor_info)
        else:
            state.http_holder.set_sensor_info(state.sensor_info)

        if state.http_server is None:  # HTTP-LIFECYCLE PATCH: only if absent
            try:
                state.http_server = HttpServer(state.http_holder, http_addr,
                                               http_port, autostart=True)
                state.http_server_error = ""
            except Exception as e:
                state.http_server = None
                state.http_server_error = f"{type(e).__name__}: {e}"
                # HTTP bind failed. UDP sockets are intentionally not created:
                # the node stays enabled and will retry on the next compute()
                # if the input config changes (needs_reinit picks it up).
                return
        else:
            # HTTP-LIFECYCLE PATCH: previous server remains valid.
            state.http_server_error = ""

        state.sock_lidar = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        state.sock_imu = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        def _sender_loop(pkt_queue, stop_event, sock_l, sock_i, state_ref):
            """Paced sender: replays inter-packet gaps from host timestamps.

            Known limitation (fix scheduled for Checkpoint B): a sendto()
            OSError terminates the loop silently; toggling the node's
            'enabled' input off/on restarts it without losing sensor config.
            """
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
    def _stop_streaming(state, stop_http=False):
        """Tear down streaming resources.

        HTTP-LIFECYCLE PATCH: the HTTP server is stopped ONLY with
        stop_http=True (node release, or an HTTP address/port change).
        """
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

        if stop_http and state.http_server is not None:  # HTTP-LIFECYCLE PATCH
            state.http_server.stop()
            state.http_server = None
            state.http_holder = None

        state.scan = None
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
        the sensor frame: +1g toward the world zenith, expressed in local
        axes. local = R @ world for orthonormal R in that convention."""
        R = np.asarray(rot_local_to_world_rowmajor, dtype=np.float64).reshape(3, 3)
        a = R @ np.array([0.0, 0.0, 1.0])
        n = np.linalg.norm(a)
        return a / n if n > 1e-9 else np.array([0.0, 0.0, 1.0])

    @staticmethod
    def _sensor_gravity_accel(sensor_path):
        """C6: WORLD orientation of the sensor prim via USD.
        ComputeLocalToWorldTransform composes the ENTIRE parent chain
        (bracket, OS0 housing, etc.) -- the same transform the raytracer
        uses, so IMU and point cloud are always coherent. Fallback on any
        failure: a level sensor."""
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
        """Map flat distance/intensity arrays into a 2D scan image.

        u (row) is selected by nearest beam elevation; v (column) is the
        encoder angle per the C5 convention, inverted per C3 to match the
        clockwise scan direction. Data ACCUMULATES across render ticks
        (C4); compute() decides when to emit and clear.
        """
        # C4: allocate only; clearing happens at EMISSION time (empirically
        # verified: per-tick emission reintroduces blur/jumps; the coverage
        # gate also filters ticks with a partial annotator batch).
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

        # C5 v2: the column represents the ENCODER angle. The Ouster manual
        # defines theta_azimuth = -2*pi*(beam_azimuth/360): the client
        # SUBTRACTS the offset during reconstruction (theta = encoder(v) -
        # off), therefore the encoder must carry the offset ADDED:
        # theta = (a + off) - off = a. Sign verified empirically:
        # subtracting here (v1) DOUBLED the 4-group misalignment (a -
        # 2*off); adding cancels it. Works identically with the published
        # metadata or the client's internal model calibration.
        encoder_deg = np.remainder(azimuth + state.beam_azimuth_lut[u], 360.0)
        v = np.floor(encoder_deg * (width / 360.0)).astype(np.intp)
        np.clip(v, 0, width - 1, out=v)
        # C3: Ouster sensors scan CLOCKWISE: as the encoder (column) grows,
        # the angle about Z decreases. The client reconstructs
        # theta(v) = 360*(1 - v/W). Without this inversion,
        # sin(360-a) = -sin(a) and the cloud comes out MIRRORED across Y
        # (verified with the SDK XYZLut: 19.54 m error vs ground truth,
        # 0.0745 m vs mirrored ground truth).
        v = (width - v) % width

        # C4: accumulate; compute() decides when to emit and clear.
        state.covered_cols[v] = True
        state.scan_range_m[u, v] = distance
        state.scan_intensity[u, v] = intensity
        return state.scan_range_m, state.scan_intensity

    @staticmethod
    def compute(db) -> bool:
        """Per-tick entry point: config-change handling, accumulation,
        full-rotation emission (C4), packetization and synthetic IMU (C6)."""
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
            if state.initialized:
                # HTTP-LIFECYCLE PATCH: the HTTP server is destroyed only
                # if the bind address/port changed.
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

            elevation_lut_deg = np.array(state.sensor_info.beam_altitude_angles, dtype=np.float64)
            state.elevation_lut_order = np.argsort(elevation_lut_deg)
            state.elevation_lut_deg_sorted = elevation_lut_deg[state.elevation_lut_order]
            # C5: per-row azimuth offset (same ordering as
            # beam_altitude_angles), folded into the encoder column in
            # _map_distance_to_u_v.
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
                # HTTP-LIFECYCLE PATCH: the HTTP server survives disable.
                OgnOusterLidar._stop_streaming(state)
            elif enabled and state.sock_lidar is None:
                OgnOusterLidar._start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_addr, http_port)
                if state.http_server_error:
                    db.log_error(f"OgnOusterLidar: HTTP bind failed on {http_addr}:{http_port} -> {state.http_server_error}")
            if state.sensor_info is not None:
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

        # C4: emit ONLY on a complete sweep (one Ouster frame = one full
        # rotation). Ticks with partial coverage keep accumulating.
        state.render_frames_per_scan += 1
        coverage = float(state.covered_cols.mean())
        if coverage < OgnOusterLidar.SCAN_COMPLETE_COVERAGE:
            db.outputs.execOut = og.ExecutionAttributeState.ENABLED
            return True

        state.frame_id += 1
        state.scan.frame_id = state.frame_id
        state.scan.field("RANGE")[:] = np.where(scan_range_m > 0, scan_range_m * 1e+3, 0).astype(np.uint32)
        state.scan.field("REFLECTIVITY")[:] = np.clip(scan_intensity * 255.0, 0, 255).astype(np.uint8)

        # Known limitation (fix scheduled for Checkpoint B): synthetic
        # timestamps assume a fixed 10 Hz frame rate instead of reading
        # sensor_info.format.fps, and advance with the frame counter rather
        # than wall clock, so the time base decouples from reality when the
        # simulation runs slower than real time.
        ts = np.linspace(0, 1 / 10.0, width)
        timestamps = 1e9 * (state.frame_id / 10.0 + ts)
        state.scan.timestamp[:] = timestamps
        state.scan.status[:] = 0x1
        packet_count = len(state.scan.packet_timestamp)
        state.scan.packet_timestamp[:] = timestamps[np.arange(packet_count) * 16]

        packets = scan_to_packets(state.scan, state.sensor_info)
        if state.packet_queue is not None:
            # C3q (CHECKPOINT A): bounded backlog with drop-oldest, whole-
            # frame granularity. If the paced sender has fallen more than
            # MAX_QUEUED_FRAMES behind (sim producing rotations faster than
            # nominal rate, or a post-stall burst), drain everything and
            # keep only this newest frame. Dropping whole frames preserves
            # frame integrity; a maxsize queue would truncate frames mid-
            # packet. NOTE: provisional -- Checkpoint B replaces this queue
            # with a double-buffered fixed-rate emitter and this gate
            # disappears.
            frame_pkt_estimate = len(packets) + 10  # lidar pkts + 10 IMU
            if state.packet_queue.qsize() > OgnOusterLidar.MAX_QUEUED_FRAMES * frame_pkt_estimate:
                try:
                    while True:
                        state.packet_queue.get_nowait()
                except queue.Empty:
                    pass
            for packet in packets:
                if packet.type in (PacketType.Lidar, PacketType.Imu):
                    state.packet_queue.put((
                        packet.type,
                        bytes(packet.buf),
                        packet.host_timestamp,
                    ))

            # C6: synthetic IMU. Detect derives pitch/roll from the
            # accelerometer ("IMU Pose" button); without IMU it assumes a
            # level sensor and the cloud inherits the bracket tilt with no
            # way to correct it in the UI (which only exposes X/Y/heading).
            # LEGACY 48 B format: 3x uint64 timestamps + accel XYZ (g) +
            # gyro XYZ (deg/s). Static bracket => gyro = 0. 10 packets per
            # frame (~100 Hz nominal), timestamps within the frame window.
            accel = OgnOusterLidar._sensor_gravity_accel(
                state._last_sensor_path)
            imu_ts = (1e9 * (state.frame_id / 10.0
                             + np.linspace(0.0, 0.09, 10))).astype(np.uint64)
            for t_ns in imu_ts:
                t = int(t_ns)
                buf = struct.pack('<QQQffffff', t, t, t,
                                  float(accel[0]), float(accel[1]),
                                  float(accel[2]), 0.0, 0.0, 0.0)
                state.packet_queue.put((PacketType.Imu, buf, t))

        # C4: rotation emitted -> reset the accumulator.
        state.scan_range_m.fill(0)
        state.scan_intensity.fill(0)
        state.covered_cols.fill(False)
        state.render_frames_per_scan = 0

        db.outputs.frameId = state.frame_id
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
