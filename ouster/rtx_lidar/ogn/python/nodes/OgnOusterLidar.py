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
        self.sock_lidar = None
        self.sock_imu = None
        self.packet_queue = None
        self.packet_sender_stop = None
        self.packet_sender_thread = None
        self.elevation_lut_order = None
        self.elevation_lut_deg_sorted = None
        self.max_returns = 1
        # Track last config to detect changes
        self._last_sensor_path = None
        self._last_config_path = None
        self._last_udp_dest = None
        self._last_udp_port_lidar = None
        self._last_udp_port_imu = None
        self._last_http_port = None
        self._last_enabled = None

    def needs_reinit(self, sensor_path, config_path, udp_dest, udp_port_lidar, udp_port_imu, http_port, enabled):
        """Check if any configuration changed that requires reinitialization."""
        return (self._last_sensor_path != sensor_path or
                self._last_config_path != config_path or
                self._last_udp_port_lidar != udp_port_lidar or
                self._last_udp_port_imu != udp_port_imu or
                self._last_http_port != http_port)

    def config_changed_runtime(self, udp_dest, enabled):
        """Check if runtime-changeable config has changed."""
        return (self._last_udp_dest != udp_dest or
                self._last_enabled != enabled)

    def save_config(self, sensor_path, config_path, udp_dest, udp_port_lidar, udp_port_imu, http_port, enabled):
        self._last_sensor_path = sensor_path
        self._last_config_path = config_path
        self._last_udp_dest = udp_dest
        self._last_udp_port_lidar = udp_port_lidar
        self._last_udp_port_imu = udp_port_imu
        self._last_http_port = http_port
        self._last_enabled = enabled


class OgnOusterLidar:
    """Ouster RTX Lidar streaming node for OmniGraph ActionGraph."""

    @staticmethod
    def internal_state():
        return OgnOusterLidarInternalState()

    @staticmethod
    def _compose_sensor_info(lidar_config_path, udp_port_lidar, udp_port_imu, udp_dest):
        """Build a SensorInfo from an RTX lidar config JSON file."""
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
        sensor_info.beam_to_lidar_transform = np.array(
            [[1.0, 0.0, 0.0, 27.67],
             [0.0, 1.0, 0.0, 0.0],
             [0.0, 0.0, 1.0, 0.0],
             [0.0, 0.0, 0.0, 1.0]], dtype=np.float64)
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
        sensor_info.format.pixel_shift_by_row = [0] * num_channels
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
            sensor_path,
            [1, 1],
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
    def _start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_port):
        """Initialize UDP sockets and packet sender thread."""
        state.scan = LidarScan(state.sensor_info)
        state.frame_id = 0
        state.pkt_count = 0
        state.scan_range_m = None
        state.scan_intensity = None
        state.packet_queue = queue.Queue()
        state.packet_sender_stop = threading.Event()

        # HTTP server
        class _SensorHolder:
            def __init__(self, si):
                self._si = si
            @property
            def sensor_info(self):
                return self._si

        holder = _SensorHolder(state.sensor_info)
        state.http_server = HttpServer(holder, "127.0.0.1", http_port, autostart=True)

        # UDP sockets
        state.sock_lidar = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        state.sock_imu = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Packet sender thread
        def _sender_loop(pkt_queue, stop_event, sock_l, sock_i, dest, port_l, port_i, state_ref):
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
                    if pkt_type == PacketType.Lidar:
                        sock_l.sendto(payload, (dest, port_l))
                    elif pkt_type == PacketType.Imu:
                        sock_i.sendto(payload, (dest, port_i))
                    state_ref.pkt_count += 1
                except OSError:
                    break
                last_ts = host_ts

        state.packet_sender_thread = threading.Thread(
            target=_sender_loop,
            args=(state.packet_queue, state.packet_sender_stop,
                  state.sock_lidar, state.sock_imu,
                  udp_dest, udp_port_lidar, udp_port_imu, state),
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
    def _map_distance_to_u_v(state, width, height, distance, intensity, azimuth, elevation):
        """Map flat distance/intensity arrays into a 2D scan image."""
        if (state.scan_range_m is None or state.scan_range_m.shape != (height, width)):
            state.scan_range_m = np.zeros((height, width), dtype=np.float32)
            state.scan_intensity = np.zeros((height, width), dtype=np.float32)
        else:
            state.scan_range_m.fill(0)
            state.scan_intensity.fill(0)

        elevation_lut = state.elevation_lut_deg_sorted
        elevation_order = state.elevation_lut_order

        insert_idx = np.searchsorted(elevation_lut, elevation, side="left")
        right_idx = np.clip(insert_idx, 0, height - 1)
        left_idx = np.clip(insert_idx - 1, 0, height - 1)
        use_right = np.abs(elevation_lut[right_idx] - elevation) < np.abs(elevation - elevation_lut[left_idx])
        u = np.where(use_right, elevation_order[right_idx], elevation_order[left_idx]).astype(np.intp, copy=False)
        np.clip(u, 0, height - 1, out=u)

        v = np.floor(np.remainder(azimuth, 360.0) * (width / 360.0)).astype(np.intp)
        np.clip(v, 0, width - 1, out=v)

        state.scan_range_m[u, v] = distance
        state.scan_intensity[u, v] = intensity
        return state.scan_range_m, state.scan_intensity

    @staticmethod
    def compute(db) -> bool:
        from ouster.rtx_lidar.ogn.OgnOusterLidarDatabase import OgnOusterLidarDatabase
        per_node_data = OgnOusterLidarDatabase.PER_NODE_DATA.get(db.node.node_id(), {})
        state = per_node_data.get("state")
        if state is None:
            state = OgnOusterLidarInternalState()
            per_node_data["state"] = state

        sensor_path = db.inputs.sensorPath
        config_path = db.inputs.lidarConfigPath
        udp_dest = db.inputs.udpDest
        udp_port_lidar = db.inputs.udpPortLidar
        udp_port_imu = db.inputs.udpPortImu
        http_port = db.inputs.httpPort
        enabled = db.inputs.enabled

        if not sensor_path or not config_path:
            db.outputs.execOut = og.ExecutionAttributeState.DISABLED
            return False

        # Initialize or reinitialize if config changed
        if not state.initialized or state.needs_reinit(
                sensor_path, config_path, udp_dest, udp_port_lidar, udp_port_imu, http_port, enabled):
            # Tear down existing
            if state.initialized:
                OgnOusterLidar._stop_streaming(state)
                OgnOusterLidar._teardown_pipeline(state)

            try:
                state.sensor_info, state.max_returns = OgnOusterLidar._compose_sensor_info(
                    config_path, udp_port_lidar, udp_port_imu, udp_dest)
            except Exception as error:
                db.log_error(f"Failed to load lidar config: {error}")
                db.outputs.execOut = og.ExecutionAttributeState.DISABLED
                return False

            elevation_lut_deg = np.array(state.sensor_info.beam_altitude_angles, dtype=np.float64)
            state.elevation_lut_order = np.argsort(elevation_lut_deg)
            state.elevation_lut_deg_sorted = elevation_lut_deg[state.elevation_lut_order]

            OgnOusterLidar._setup_pipeline(state, sensor_path)

            if enabled:
                OgnOusterLidar._start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_port)

            state.initialized = True
            state.save_config(sensor_path, config_path, udp_dest, udp_port_lidar, udp_port_imu, http_port, enabled)

        # Handle runtime changes (enable/disable, udp_dest change)
        if state.config_changed_runtime(udp_dest, enabled):
            if not enabled and state.sock_lidar is not None:
                OgnOusterLidar._stop_streaming(state)
            elif enabled and state.sock_lidar is None:
                OgnOusterLidar._start_streaming(state, udp_dest, udp_port_lidar, udp_port_imu, http_port)
            if state.sensor_info is not None:
                state.sensor_info.config.udp_dest = udp_dest
            state.save_config(sensor_path, config_path, udp_dest, udp_port_lidar, udp_port_imu, http_port, enabled)

        # Process frame if enabled
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

        state.frame_id += 1
        state.scan.frame_id = state.frame_id
        state.scan.field("RANGE")[:] = np.where(scan_range_m > 0, scan_range_m * 1e+3, 0).astype(np.uint32)
        state.scan.field("REFLECTIVITY")[:] = np.clip(scan_intensity * 255.0, 0, 255).astype(np.uint8)

        ts = np.linspace(0, 1 / 10.0, width)
        timestamps = 1e9 * (state.frame_id / 10.0 + ts)
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
        db.outputs.packetsSent = state.pkt_count
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
