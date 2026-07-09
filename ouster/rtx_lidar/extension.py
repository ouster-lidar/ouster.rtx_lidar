import socket
import psutil
import ipaddress
import threading
import time
import json
import click
from datetime import datetime, timezone
from ouster.sdk.core import (SensorInfo, SensorConfig, DataFormat, LidarMode, HeaderType,
                             OperatingMode, UDPProfileLidar, UDPProfileIMU, TimestampMode)
from ouster.sdk.zone_monitor import ZoneSetOutputFilter
from ouster.sdk._bindings.client import PacketType
from zeroconf import ServiceInfo, Zeroconf
from flask import Flask, request, jsonify, render_template


# TODO: fix
SENSOR_IDX = 0


def ts_format(ts):
    return datetime.fromtimestamp(ts * 1e-9, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f %Z')


app = Flask(__name__)

class PacketSourcePacer():

    def __init__(self, packet_source_factory, rate, loop=False):
        self._packet_source_factory = packet_source_factory
        self._packet_source = packet_source_factory()
        self._rate = rate
        self._loop = loop
        self._restart_timestamp = None

    @property
    def sensor_info(self):
        return self._packet_source.sensor_info

    def _restart_source(self):
        """Restart the packet source and reset timing."""
        self._packet_source = self._packet_source_factory()
        self._restart_timestamp = time.monotonic()

    def check_and_clear_restart(self):
        """Check if a restart occurred recently and clear the flag."""
        if self._restart_timestamp is not None:
            restart_time = self._restart_timestamp
            self._restart_timestamp = None
            return restart_time
        return None

    def __iter__(self):
        while True:
            started = False
            pcap_start_time = None
            real_start_time = None

            try:
                for _, packet in self._packet_source:
                    if not started:
                        started = True
                        pcap_start_time = packet.host_timestamp
                        real_start_time = time.perf_counter()
                        # Yield the first packet immediately without delay
                        yield packet
                        continue

                    # Calculate the arrival time based on packet timestamp and rate
                    packet_time_offset = (packet.host_timestamp - pcap_start_time) * 1e-9
                    arrival_time = real_start_time + (packet_time_offset / self._rate)

                    # Sleep until real-time catches up with the packet time
                    while True:
                        current_time = time.perf_counter()
                        sleep_duration = arrival_time - current_time
                        if sleep_duration <= 0:
                            break
                        time.sleep(sleep_duration)

                    yield packet

                # If we reach here, the source has ended naturally
                if self._loop:
                    self._restart_source()
                else:
                    break

            except StopIteration:
                # Handle explicit StopIteration from the source
                if self._loop:
                    self._restart_source()
                else:
                    break


class mDNSService:

    def __init__(self, sensor_info: SensorInfo):
        self._sensor_info = sensor_info
        service_type = "_roger._tcp.local."
        service_name = f"Ouster Sensor {sensor_info.sn}"
        ipv4_address = socket.inet_aton("127.0.0.1")
        # TODO: enable ipv6 interfaces
        # ipv6_address = socket.inet_pton(socket.AF_INET6, "::1")
        properties = {
            "sn": sensor_info.sn,
            "pn": sensor_info.prod_pn,
            "fw": sensor_info.image_rev
        }
        self._info = ServiceInfo(
            service_type,
            f"{service_name}.{service_type}",
            addresses=[ipv4_address],
            port=sensor_info.config.udp_port_lidar,
            properties=properties,
            server="localhost"
        )
        self._zeroconf = Zeroconf(interfaces=self._active_addresses())
        self.register()

    def _active_addresses(self):
        addresses = psutil.net_if_addrs()
        active_addresses = []
        for interface_name, interface_addresses in addresses.items():
            # Check if the interface is logically 'up'
            stats = psutil.net_if_stats().get(interface_name)
            if not stats or not stats.isup:
                continue
            for snicaddr in interface_addresses:
                address, family = snicaddr.address, snicaddr.family
                if family == socket.AF_INET:
                    # IPv4: Exclude loopback
                    if address != '127.0.0.1':
                        active_addresses.append(address)
                elif family == socket.AF_INET6:
                    # IPv6: Exclude loopback (::1)
                    if address == '::1':
                        continue
                    # Check if is a valid ipaddress
                    try:
                        _ = ipaddress.ip_address(address)
                        active_addresses.append(address)
                    except ValueError:
                        continue
        return active_addresses

    def register(self):
        self._zeroconf.register_service(self._info)
        print("mDNS started.")

    def unregister(self):
        self._zeroconf.unregister_service(self._info)
        self._zeroconf.close()
        print("mDNS stopped.")

    def __del__(self):
        self.unregister()


class HttpServer():

    def __init__(self, sensor, http_addr, http_port, autostart=True):
        self._sensor = sensor
        self._http_addr = http_addr
        self._http_port = http_port
        self._flask_thread = None
        self._waitress_server = None
        self._autostart = autostart
        self.last_client_addr = ""
        self.bind_error = ""

        def _sensor_info_as_json(sensor_info):
            sensor_info_json = json.loads(sensor_info.to_json_string())
            if "zone_set" in sensor_info_json:
                del sensor_info_json["zone_set"]
            if "ouster-sdk" in sensor_info_json:
                del sensor_info_json["ouster-sdk"]
            return sensor_info_json

        @app.route('/api/v1/sensor/metadata', methods=['GET'])
        def get_all_metadata():
            result = _sensor_info_as_json(self._sensor.sensor_info)
            return jsonify(result)

        @app.route('/api/v1/zone_monitor/active/zip', methods=['GET'])
        def handle_zones():
            zone_set = self._sensor.sensor_info.zone_set
            if zone_set is None:
                return "{}", 404
            blob = zone_set.to_zip_blob(ZoneSetOutputFilter.STL_AND_ZRB)
            return blob, 200

        @app.route('/api/v1/sensor/metadata/<metadata_type>', methods=['GET'])
        def get_sensor_metadata(metadata_type):
            sensor_info_json = _sensor_info_as_json(self._sensor.sensor_info)
            if metadata_type in sensor_info_json:
                return jsonify(sensor_info_json[metadata_type])
            return jsonify({"error": "Metadata not found"}), 404

        @app.route('/api/v1/sensor/config', methods=['GET', 'POST'])
        def sensor_config_handler():
            sensor_info_json = _sensor_info_as_json(self._sensor.sensor_info)
            if request.method == 'GET':
                result = sensor_info_json["config_params"]
                return jsonify(result)
            elif request.method == 'POST':
                sensor_config = {}
                try:
                    sensor_config = json.loads(request.data)
                except Exception:
                    return jsonify({"error": {
                        "title": "500 Internal Server Error",
                        "description": "Command \"set_config_param\" failed: error: Expected 1-2 arguments"}})
                self.configure_sensor(sensor_config, request.remote_addr)
                return jsonify({"message": "Configuration updated successfully"})
            elif request.method == 'DELETE':
                # sensor_config.clear()
                return jsonify({"message": "Configuration deleted successfully"})

        @app.route('/api/v1/sensor/cmd/get_config_param', methods=['GET'])
        def get_config_param():
            sensor_info_json = _sensor_info_as_json(self._sensor.sensor_info)
            config_parameters = {
                "active": sensor_info_json["config_params"],
                "staged": sensor_info_json["config_params"]
            }
            args = request.args.get('args')
            if args in config_parameters:
                return jsonify(config_parameters[args])
            else:
                return jsonify({"error": "Config parameter not found"}), 404

        @app.route('/api/v1/sensor/cmd/set_udp_dest_auto', methods=['GET'])
        def set_udp_dest_auto():
            self._sensor.sensor_info.config.udp_dest = request.remote_addr
            self.last_client_addr = request.remote_addr
            return "{}"

        @app.route('/api/v1/sensor/cmd/set_config_param', methods=['GET', 'POST', 'DELETE'])
        def set_config_param():
            # parse the lidar udp and imu udp from the request
            args = request.args.get('args')
            sensor_config = {}
            try:
                args = args[args.index('{'):]
                sensor_config = json.loads(args)
            except Exception:
                return jsonify({"error": {
                    "title": "500 Internal Server Error",
                    "description": "Command \"set_config_param\" failed: error: Expected 1-2 arguments"}})
            self.configure_sensor(sensor_config, request.remote_addr)
            self.last_client_addr = request.remote_addr
            return "\"set_config_param\""

        @app.route('/api/v1/user/data', methods=['GET', 'PUT', 'DELETE'])
        def user_data():
            if request.method == 'GET':
                user_data = self._sensor.sensor_info.user_data if self._sensor.sensor_info.user_data else "\"\""
                return user_data
            elif request.method == 'PUT':
                new_data = request.json
                self._sensor.sensor_info.user_data = new_data
                return new_data if new_data else "\"\""
            elif request.method == 'DELETE':
                user_data = self._sensor.sensor_info.user_data
                self._sensor.sensor_info.user_data = ""
                return user_data if user_data else "\"\""

        @app.route('/api/v1/sensor/cmd/reinitialize', methods=['GET'])
        def reinitialize():
            return "{}"

        @app.route('/api/v1/system/firmware', methods=['GET'])
        def get_firmware_version():
            sensor_info_json = _sensor_info_as_json(self._sensor.sensor_info)
            return jsonify({"fw": sensor_info_json["sensor_info"]["image_rev"]})

        # TODO: properly populate info from metadata
        @app.route('/api/v1/system/network', methods=['GET'])
        def get_network_info():
            sensor_info = self._sensor.sensor_info
            return jsonify({"duplex": "full",
                            "ipv6": {
                                "link_local": "fe80::be0f:a7ff:fe00:4521/64"},  # hardcoded for now
                            "speed": 1000, "ethaddr": "bc:0f:a7:00:45:21",      # hardcoded for now
                            "speed_override": "null",
                            "hostname": f"os-{sensor_info.sn}",
                            "carrier": "true",
                            "ipv4": {
                                "override": "null",
                                "link_local": f"{sensor_info.config.udp_dest}/16"}
                            })

        # TODO: Use packets from the source to populate timing values instead of realtime
        @app.route('/api/v1/time/system', methods=['GET'])
        def get_system_time():
            response = jsonify({
                "monotonic": time.monotonic(),
                "realtime": datetime.now().timestamp(),
            })
            return response

        @app.route('/api/v1/sensor/cmd/save_config_params', methods=['GET'])
        def save_config_params():
            print("Ignoring the save_config_params operation")
            # Note: does this activate the sensor?
            return "{}"

        @app.route('/api/v1/sensor/telemetry', methods=['GET'])
        def get_sensor_telemetry():
            response = jsonify({
                "phase_lock_status": "DISABLED",
                "input_voltage_mv": "23879",
                "input_current_ma": "693",
                "timestamp_ns": str(int(time.time() * 1e9)),
                "internal_temperature_deg_c": "60.0"
            })
            return response

        self.active_alerts = []
        self.alert_cursor = 0

        @app.route('/api/v1/sensor/alerts', methods=['GET'])
        def get_sensor_alerts():
            return jsonify({
                "active": self.active_alerts,
                "log": [],
                "next_cursor": self.alert_cursor
            })

        @app.route('/api/v1/dev/inject_alert', methods=['POST'])
        def inject_alert():
            data = request.json or {}
            new_alert = {
                "active": True,
                "category": data.get("category", "ENVIRONMENT"),
                "cursor": self.alert_cursor,
                "id": data.get("id", "0x01000001"),
                "level": data.get("level", "WARNING"),
                "msg": data.get("msg", "Alerte générée par le replay"),
                "msg_verbose": data.get("msg_verbose", "Détails de l'alerte virtuelle"),
                "realtime": str(int(time.time() * 1e9))
            }
            self.active_alerts.append(new_alert)
            self.alert_cursor += 1
            return jsonify({"status": "success", "alert": new_alert}), 201

        @app.route('/api/v1/dev/clear_alerts', methods=['POST'])
        def clear_alerts():
            self.active_alerts = []
            return jsonify({"status": "cleared"}), 200

        def tab_title(tab):
            return f"os-{self._sensor.sensor_info.sn} :: {tab} :: Ouster"

        @app.route('/')
        def dashboard():
            system_info = {
                'Ethernet Address': '00:00:00:00:00:00',
                'IPv4 (Link-Local)': '127.0.0.1/16',
                'IPv6 (Link-Local)': '[::1]/64',
                'Hostname': f"os-{self._sensor.sensor_info.sn}",
                'Serial Number': self._sensor.sensor_info.sn,
                'Part Number': self._sensor.sensor_info.prod_pn,
                'Model': self._sensor.sensor_info.prod_line
            }

            version = self._sensor.sensor_info.get_version()
            firmware_update = {
                'Current Image': self._sensor.sensor_info.image_rev,
                'Current Version': f'v{version.major}.{version.minor}.{version.patch}'
            }

            system_status = {
                'State': self._sensor.sensor_info.config.operating_mode.name,
                'Active Alerts': str(len(self.active_alerts))
            }

            configuration = {
                'Lidar Mode': self._sensor.sensor_info.config.lidar_mode.name,
                'Signal Multiplier': self._sensor.sensor_info.config.signal_multiplier,
                'Active Azimuth Window': self._sensor.sensor_info.config.azimuth_window,
                'UDP Profile Lidar': self._sensor.sensor_info.config.udp_profile_lidar
            }

            return render_template('dashboard.html',
                                   title=tab_title("Dashboard"),
                                   system_info=system_info,
                                   firmware_update=firmware_update,
                                   system_status=system_status,
                                   configuration=configuration)

        @app.route('/diag')
        def diagnostics():
            return render_template('diagnostics.html',
                                   title=tab_title("Diagnostics"),
                                   active_alerts=self.active_alerts,
                                   logged_alerts=[])

        @app.route('/config')
        def configuration():
            config = self._sensor.sensor_info.config
            config_data = {
                'network': [
                    {
                        'name': 'udp_dest',
                        'label': 'UDP Destination Address',
                        'active': config.udp_dest,
                        'staged': config.udp_dest,
                        'type': 'text',
                        'button': 'Set Local'
                    },
                    {
                        'name': 'udp_dest_zm',
                        'label': 'UDP Destination Address ZM',
                        'active': config.udp_dest_zm,
                        'staged': config.udp_dest_zm,
                        'type': 'text',
                        'button': 'Set Local'
                    },
                    {
                        'name': 'udp_port_lidar',
                        'label': 'UDP Port Lidar',
                        'active': config.udp_port_lidar,
                        'staged': config.udp_port_lidar,
                        'type': 'text',
                        'button': 'Reset to default'
                    },
                    {
                        'name': 'udp_port_imu',
                        'label': 'UDP Port IMU',
                        'active': config.udp_port_imu,
                        'staged': config.udp_port_imu,
                        'type': 'text',
                        'button': 'Reset to default'
                    },
                    {
                        'name': 'udp_port_zm',
                        'label': 'UDP Port ZM',
                        'active': config.udp_port_zm,
                        'staged': config.udp_port_zm,
                        'type': 'text',
                        'button': 'Reset to default'
                    }
                ],
                'mode': [
                    {
                        'name': 'lidar_mode',
                        'immutable': True,
                        'label': 'Lidar Mode',
                        'active': config.lidar_mode.name,
                        'staged': config.lidar_mode.name,
                        'type': 'dropdown',
                        'options': ['512x10', '512x20', '1024x10', '1024x20', '2048x10']
                    },
                    {
                        'name': 'operating_mode',
                        'immutable': False,
                        'label': 'Operating Mode',
                        'active': config.operating_mode.name,
                        'staged': config.operating_mode.name,
                        'type': 'dropdown',
                        'options': ['NORMAL', 'STANDBY']
                    },
                    {
                        'name': 'azimuth_window',
                        'immutable': True,
                        'label': 'Azimuth Window',
                        'active_start': config.azimuth_window[0],
                        'active_end': config.azimuth_window[1],
                        'staged_start': config.azimuth_window[0],
                        'staged_end': config.azimuth_window[1],
                        'type': 'range'
                    },
                    {
                        'name': 'signal_multiplier',
                        'immutable': True,
                        'label': 'Signal Multiplier',
                        'active': config.signal_multiplier,
                        'staged': config.signal_multiplier,
                        'type': 'dropdown',
                        'options': ['1', '2', '3']
                    },
                    {
                        'name': 'udp_profile_lidar',
                        'immutable': True,
                        'label': 'UDP Profile Lidar',
                        'active': config.udp_profile_lidar.name,
                        'staged': config.udp_profile_lidar.name,
                        'type': 'dropdown',
                        'options': [profile.name for profile in UDPProfileLidar.values()]
                    }
                ],
                'timing': [
                    {
                        'immutable': True,
                        'label': 'Timestamp Mode',
                        'active': config.timestamp_mode.name,
                        'staged': config.timestamp_mode.name,
                        'type': 'dropdown',
                        'options': [time_mode.name for time_mode in TimestampMode.values()]
                    },
                    {
                        'immutable': True,
                        'label': 'Multipurpose IO Mode',
                        'active': config.multipurpose_io_mode,
                        'staged': config.multipurpose_io_mode,
                        'type': 'dropdown',
                        'options': ['OFF', 'INPUT_NMEA_UART', 'OUTPUT_*']
                    },
                    {
                        'immutable': True,
                        'label': 'NMEA In Polarity',
                        'active': config.nmea_in_polarity,
                        'staged': config.nmea_in_polarity,
                        'type': 'dropdown',
                        'options': ['ACTIVE_HIGH', 'ACTIVE_LOW']
                    },
                    {
                        'immutable': True,
                        'label': 'NMEA Ignore Valid Char',
                        'active': config.nmea_ignore_valid_char,
                        'staged': config.nmea_ignore_valid_char,
                        'type': 'checkbox'
                    },
                    {
                        'immutable': True,
                        'label': 'NMEA Baud Rate',
                        'active': config.nmea_baud_rate,
                        'staged': config.nmea_baud_rate,
                        'type': 'dropdown',
                        'options': ['BAUD_9600', 'BAUD_115200']
                    },
                    {
                        'immutable': True,
                        'label': 'NMEA Leap Seconds',
                        'active': config.nmea_leap_seconds,
                        'staged': config.nmea_leap_seconds,
                        'type': 'text'
                    },
                    {
                        'immutable': True,
                        'label': 'Sync Pulse In Polarity',
                        'active': config.sync_pulse_in_polarity,
                        'staged': config.sync_pulse_in_polarity,
                        'type': 'dropdown',
                        'options': ['ACTIVE_HIGH', 'ACTIVE_LOW']
                    },
                    {
                        'immutable': True,
                        'label': 'Sync Pulse Out Polarity',
                        'active': config.sync_pulse_out_polarity,
                        'staged': config.sync_pulse_out_polarity,
                        'type': 'dropdown',
                        'options': ['ACTIVE_HIGH', 'ACTIVE_LOW']
                    },
                    {
                        'immutable': True,
                        'label': 'Sync Pulse Out Frequency',
                        'active': config.sync_pulse_out_frequency,
                        'staged': config.sync_pulse_out_frequency,
                        'type': 'text'
                    },
                    {
                        'immutable': True,
                        'label': 'Sync Pulse Out Angle',
                        'active': config.sync_pulse_out_angle,
                        'staged': config.sync_pulse_out_angle,
                        'type': 'text'
                    },
                    {
                        'immutable': True,
                        'label': 'Sync Pulse Out Pulse Width',
                        'active': config.sync_pulse_out_pulse_width,
                        'staged': config.sync_pulse_out_pulse_width,
                        'type': 'text'
                    },
                    {
                        'immutable': True,
                        'label': 'Phase Lock Enable',
                        'active': config.phase_lock_enable,
                        'staged': config.phase_lock_enable,
                        'type': 'checkbox'
                    },
                    {
                        'immutable': True,
                        'label': 'Phase Lock Offset',
                        'active': config.phase_lock_offset,
                        'staged': config.phase_lock_offset,
                        'type': 'text'
                    }
                ]
            }

            return render_template('configuration.html',
                                   title=tab_title("Configuration"),
                                   config=config_data)

        @app.route('/apply_config', methods=['POST'])
        def apply_config():
            udp_dest = request.form.get('udp_dest')
            if udp_dest:
                self._sensor.sensor_info.config.udp_dest = udp_dest
            lidar_port = request.form.get('udp_port_lidar')
            if lidar_port:
                self._sensor.sensor_info.config.udp_port_lidar = int(lidar_port)
            imu_port = request.form.get('udp_port_imu')
            if imu_port:
                self._sensor.sensor_info.config.udp_port_imu = int(imu_port)
            udp_dest_zm = request.form.get('udp_dest_zm')
            if udp_dest_zm:
                self._sensor.sensor_info.config.udp_dest_zm = udp_dest_zm
            zm_port = request.form.get('udp_port_zm')
            if zm_port:
                self._sensor.sensor_info.config.udp_port_zm = int(zm_port)
            operating_mode = request.form.get('operating_mode')
            if operating_mode:
                self._sensor.sensor_info.config.operating_mode = OperatingMode.from_string(operating_mode)
            return configuration()

        @app.route('/disclaimer')
        def disclaimer():
            return render_template('disclaimer.html',
                                   title=tab_title("Disclaimer"))

        if self._autostart:
            self.start()

    def start(self):
        self._service_status = {"status": 0}

        def serve_flask_app(waitress_server, service_status):
            try:
                waitress_server.run()
            except PermissionError:
                click.secho("Permission error!",
                            fg='red')
                service_status['status'] = -1
            except Exception as e:
                if service_status['status'] == 0:
                    print("Unknown error: " + str(e))
                    service_status['status'] = -2

        try:
            from waitress import create_server
            self._waitress_server = create_server(app, host=self._http_addr, port=self._http_port)
        except PermissionError as e:
            self.bind_error = f"PermissionError: {e}"
            click.secho("Permission error!", fg='red')
            raise RuntimeError("http server failed to start") from e
        except Exception as e:
            self.bind_error = f"{type(e).__name__}: {e}"
            print("Unknown error: " + str(e))
            raise RuntimeError("http server failed to start") from e

        self._flask_thread = threading.Thread(target=serve_flask_app,
                                              args=(self._waitress_server, self._service_status,),
                                              daemon=True)

        self._flask_thread.start()
        time.sleep(0.5)
        if "status" in self._service_status and self._service_status["status"] != 0:
            raise RuntimeError("http server failed to start")

        print("http server started")

    def configure_sensor(self, sensor_config, remote_addr):
        if 'udp_dest' in sensor_config:
            udp_dest = remote_addr if sensor_config['udp_dest'] == "@auto" else sensor_config['udp_dest']
            self._sensor.sensor_info.config.udp_dest = udp_dest
        if 'udp_port_lidar' in sensor_config:
            self._sensor.sensor_info.config.udp_port_lidar = sensor_config['udp_port_lidar']
        if 'udp_port_imu' in sensor_config:
            self._sensor.sensor_info.config.udp_port_imu = sensor_config['udp_port_imu']
        if 'operating_mode' in sensor_config:
            self._sensor.sensor_info.config.operating_mode = OperatingMode.from_string(sensor_config['operating_mode'])
        if 'user_data' in sensor_config:
            self._sensor.sensor_info.user_data = sensor_config['user_data']

    def stop(self):
        if self._waitress_server is not None:
            try:
                self._waitress_server.close()
            except Exception as e:
                print("Failed to stop http server cleanly: " + str(e))

        if self._flask_thread is not None and self._flask_thread.is_alive():
            self._flask_thread.join(timeout=2.0)
            if self._flask_thread.is_alive():
                print("http server thread did not stop before timeout")
                return

        self._waitress_server = None
        self._flask_thread = None
        print("http server stopped")


class ScanSourceUdpReplay():

    def __init__(self, packet_source_factory, source_url, http_addr, http_port, loop, rate, soft_id_check, lidar_port=-1,
                 imu_port=-1, udp_dest="127.0.0.1", operating_mode="NORMAL", sensor_sn="",
                 hide_diagnostics=False, zm_port=-1):

        self._packet_source = PacketSourcePacer(
                                packet_source_factory,
                                rate=rate,
                                loop=loop)

        self._sensor_info = self._packet_source.sensor_info[SENSOR_IDX]
        # override ports before starting the HTTP server & DNS service
        if lidar_port != -1:
            self._sensor_info.config.udp_port_lidar = lidar_port
        if imu_port != -1:
            self._sensor_info.config.udp_port_imu = imu_port
        if zm_port != -1:
            self._sensor_info.config.udp_port_zm = zm_port
        # should we override the current udp_dest? by default ?
        self._sensor_info.config.udp_dest = udp_dest
        self._sensor_info.config.udp_dest_zm = udp_dest
        self._sensor_info.sn = int(sensor_sn) if sensor_sn != "" else self._sensor_info.sn
        self._sensor_info.config.operating_mode = OperatingMode.from_string(operating_mode)

        self._show_diagnostics = not hide_diagnostics
        self._first_diagnostics_message = True

        self._http_server = HttpServer(self, http_addr, http_port)
        # self._mdns = mDNSService(self._sensor_info)
        # self.start()

    @property
    def sensor_info(self) -> SensorInfo:
        return self._sensor_info

    def display_diagnostics(self, lines):
        def clear_diagnostics(line_count):
            for _ in range(line_count):
                print('\033[1A\033[2K', end='')

        if self._first_diagnostics_message:
            self._first_diagnostics_message = False
        else:
            clear_diagnostics(len(lines) + 2)
        print("===== Sensor Replay Diagnostics =====")
        diagnostics_msg = '\n* '.join(lines)
        print(f"* {diagnostics_msg}")
        print("=====================================")

    def start(self):
        print("starting udp streaming")
        initialized = False
        sock_lidar = 0
        sock_imu = 0
        while not initialized:
            try:
                sock_lidar = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock_imu = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                print("sockets initialized")
                initialized = True
            except ConnectionRefusedError:
                print("connection refused")
                time.sleep(0.1)

        # diagnostics
        pkts_count = 0
        last_time = time.monotonic()

        for packet in self._packet_source:
            # Check if a restart occurred and reset timing measurements if so
            restart_time = self._packet_source.check_and_clear_restart()
            if restart_time is not None:
                # Reset the timing measurements to account for restart
                pkts_count = 0
                last_time = time.monotonic()

            # grab current config in case it was changed via HTTP
            operating_mode = self._sensor_info.config.operating_mode
            udp_dest = self._sensor_info.config.udp_dest
            udp_dest_zm = self._sensor_info.config.udp_dest_zm
            udp_port_lidar = self._sensor_info.config.udp_port_lidar
            udp_port_imu = self._sensor_info.config.udp_port_imu
            udp_port_zm = self._sensor_info.config.udp_port_zm

            if operating_mode == OperatingMode.OPERATING_NORMAL:
                if packet.type == PacketType.Lidar:
                    sock_lidar.sendto(
                        packet.buf, (udp_dest, udp_port_lidar))
                elif packet.type == PacketType.Imu:
                    sock_imu.sendto(
                        packet.buf, (udp_dest, udp_port_imu))
                elif packet.type == PacketType.Zone:
                    sock_imu.sendto(
                        packet.buf, (udp_dest_zm, udp_port_zm))
                else:
                    print(f"Unknown packet type {packet.type}")

            # diagnostics
            if self._show_diagnostics:
                pkts_count += 1
                curr_time = time.monotonic()
                if curr_time - last_time > 1:
                    self.display_diagnostics(
                        [f"Operating Mode: {operating_mode}",
                        f"UDP Destination: {udp_dest}",
                        f"UDP Port (Lidar): {udp_port_lidar}",
                        f"UDP Port (IMU): {udp_port_imu}",
                        f"UDP Port (ZM): {udp_port_zm}",
                        f"Host Time: {ts_format(packet.host_timestamp)}",
                        f"{pkts_count} packets/seconds"])
                    pkts_count = 0
                    last_time = curr_time

        sock_lidar.close()
        sock_imu.close()

