import socket
import psutil
import ipaddress
import threading
import time
import json
import queue
import click
from datetime import datetime, timezone
from ouster.sdk.core import (SensorInfo, SensorConfig, DataFormat, LidarMode, HeaderType,
                             OperatingMode, UDPProfileLidar, UDPProfileIMU, TimestampMode)
from ouster.sdk.zone_monitor import ZoneSetOutputFilter
from ouster.sdk._bindings.client import PacketType
from ouster.sdk.util.parsing import scan_to_packets
from zeroconf import ServiceInfo, Zeroconf
from flask import Flask, request, jsonify, render_template
import numpy as np


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

        # --- DÉBUT DES MODIFICATIONS : GESTION DES ALERTES VIRTUELLES ---
        self.active_alerts = []
        self.alert_cursor = 0

        @app.route('/api/v1/sensor/alerts', methods=['GET'])
        def get_sensor_alerts():
            """L'endpoint officiel que le SDK ou ROS interroge"""
            return jsonify({
                "active": self.active_alerts,
                "log": [],
                "next_cursor": self.alert_cursor
            })

        @app.route('/api/v1/dev/inject_alert', methods=['POST'])
        def inject_alert():
            """Notre backdoor pour créer une alerte sur demande"""
            data = request.json or {}
            new_alert = {
                "active": True,
                "category": data.get("category", "ENVIRONMENT"),
                "cursor": self.alert_cursor,
                "id": data.get("id", "0x01000001"),
                "level": data.get("level", "WARNING"),
                "msg": data.get("msg", "Alerte générée par le replay"),
                "msg_verbose": data.get("msg_verbose", "Détails de l'alerte virtuelle"),
                "realtime": str(int(time.time() * 1e9)) # Temps en nanosecondes
            }
            self.active_alerts.append(new_alert)
            self.alert_cursor += 1
            return jsonify({"status": "success", "alert": new_alert}), 201

        @app.route('/api/v1/dev/clear_alerts', methods=['POST'])
        def clear_alerts():
            """Pour simuler un retour à la normale"""
            self.active_alerts = []
            return jsonify({"status": "cleared"}), 200
        # --- FIN DES MODIFICATIONS ---

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
                'Active Alerts': str(len(self.active_alerts)) # MODIFIÉ ICI
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
            # MODIFIÉ ICI
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
            # read udp dest, lidar port, imu port from the form and apply them
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
            # TODO: display the confirmation message
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
        except PermissionError:
            click.secho("Permission error!",
                        fg='red')
            raise RuntimeError("http server failed to start")
        except Exception as e:
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


import omni.ext
import omni.usd
import omni.kit.app
import omni.timeline
import omni.replicator.core as rep
import numpy as np
import asyncio
from pathlib import Path
import time

try:
    from PIL import Image
except ImportError:
    Image = None

try:
    from ouster.sdk import open_source
    print("Ouster SDK was imported successfully.")
    from ouster.sdk.osf import Writer
    from ouster.sdk.core import LidarScan, FieldType, ChanField, FieldClass
    from ouster.sdk.core import destagger
except ImportError:
    print("Ouster SDK is not available. Ensure it is installed in the Python environment.")

class ExistingLidarReader(omni.ext.IExt):

    # Path to the RTXLidar sensor configuration that Isaac Sim uses.
    LIDAR_CONFIG_PATH = "/home/unaal/nvidia/isaac-sim/exts/isaacsim.sensors.rtx/data/lidar_configs/Ouster/OS0/OS0_REV7_128ch10hz1024res.json"

    def __init__(self):
        super().__init__()

    def compose_sensor_info_from_lidar_config(self, lidar_config_path):
        """
        'accel_fsr', 'bloom_reduction_optimization', 'extra_options',
        'gyro_fsr', 'imu_packets_per_frame', 'lidar_frame_azimuth_offset',
        ''min_range_threshold_cm'
        'phase_lock_enable', 'phase_lock_offset',
        'return_order', 'signal_multiplier',
        'sync_pulse_in_polarity', 'sync_pulse_out_angle',
        'sync_pulse_out_frequency', 'sync_pulse_out_polarity',
        'sync_pulse_out_pulse_width', 'timestamp_mode',
        'udp_multicast_ttl', 'udp_multicast_ttl_zm',
        """
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
        self._max_returns = max_returns

        sensor_info = SensorInfo()
        sensor_info.sn = 1234567890
        sensor_info.config.azimuth_window = [0, 360000]
        sensor_info.config.columns_per_packet = 16
        sensor_info.config.lidar_mode = LidarMode.from_string(
            f"{columns_per_frame}x{scan_rate_hz}")
        # sensor_info.config.multipurpose_io_mode = "OFF"
        # sensor_info.config.nmea_baud_rate = "BAUD_9600"
        # sensor_info.config.nmea_ignore_valid_char = 0
        # sensor_info.config.nmea_in_polarity = "ACTIVE_HIGH"
        # sensor_info.config.nmea_leap_seconds = 0
        sensor_info.config.udp_port_lidar = 7502
        sensor_info.config.udp_port_imu = 7503
        sensor_info.config.udp_port_zm = 0
        sensor_info.config.udp_dest = "127.0.0.1"
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
        sensor_info.image_rev = "ousteros-image-prod-bootes-v3.0.0-beta.2+20221223222646.staging-2022.ci-git.aj-test-encoder-stuff.dbfc6371c6.MR-2619"
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
        # format
        sensor_info.format.column_window = [0, columns_per_frame - 1]
        sensor_info.format.columns_per_frame = columns_per_frame
        sensor_info.format.columns_per_packet = 16
        sensor_info.format.fps = 10
        sensor_info.format.header_type = HeaderType.STANDARD
        sensor_info.format.pixel_shift_by_row = [0] * num_channels
        sensor_info.format.imu_measurements_per_packet = 0
        sensor_info.format.imu_packets_per_frame = 0
        sensor_info.format.pixels_per_column = num_channels
        sensor_info.format.udp_profile_imu = UDPProfileIMU.LEGACY
        sensor_info.format.udp_profile_lidar = UDPProfileLidar.RNG19_RFL8_SIG16_NIR16
        sensor_info.format.zone_monitoring_enabled = False
        return sensor_info

    def dump_sensor_info(self, sensor_info):
        lidar_profile = sensor_info.format.udp_profile_lidar
        imu_profile = sensor_info.format.udp_profile_imu
        lidar_mode = sensor_info.config.lidar_mode
        print("Ouster IsaacSim sensor info:")
        print("----------------------------")
        print(f"product: {sensor_info.prod_line}, sn: {sensor_info.sn}")
        print(f"lidar mode: {lidar_mode}")
        print(f"lidar udp profile: {lidar_profile}, imu udp profile: {imu_profile}")


    def on_startup(self, ext_id):
        self.sensor_path = "/OS0_REV7_128_10hz___1024_resolution/sensor"
        self._timeline = omni.timeline.get_timeline_interface()
        self.render_product = None
        self.render_product_path = None
        self.annotator = None
        self._update_sub = None
        self._setup_task = None
        self._timeline_sub = None
        self._is_shutting_down = False
        self._session_active = False
        self._frame_id = 0
        self._pkt_count = 0
        self._writer = None
        self._scan = None
        self._scan_range_m = None
        self._scan_intensity = None
        self._http_server = None
        self._sock_lidar = None
        self._sock_imu = None
        self._packet_queue = None
        self._packet_sender_stop = None
        self._packet_sender_thread = None

        # Load RTXLidar config to drive scan dimensions.
        self._sensor_info = self.compose_sensor_info_from_lidar_config(self.LIDAR_CONFIG_PATH)
        self.dump_sensor_info(self._sensor_info)

        elevation_lut_deg = np.array(self._sensor_info.beam_altitude_angles, dtype=np.float64)
        self._elevation_lut_order = np.argsort(elevation_lut_deg)
        self._elevation_lut_deg_sorted = elevation_lut_deg[self._elevation_lut_order]

        self._timeline_sub = self._timeline.get_timeline_event_stream().create_subscription_to_pop(
            self._on_timeline_event, name="existing_lidar_reader_timeline"
        )

        # 2. Start the async pipeline to hook into the data stream
        self._setup_task = asyncio.ensure_future(self._setup_pipeline_async())

    @property
    def sensor_info(self):
        return self._sensor_info

    def _on_timeline_event(self, event):
        if event.type == int(omni.timeline.TimelineEventType.PLAY):
            self._start_session()
        elif event.type == int(omni.timeline.TimelineEventType.STOP):
            self._stop_session()

    def _start_session(self):
        if self._session_active or self._is_shutting_down:
            return

        self._writer = Writer(
            "/home/unaal/nvidia/isaac-sim-ext/lidar_capture.osf",
            self._sensor_info)

        field_types = [
            FieldType(ChanField.RANGE, np.uint32, (), FieldClass.PIXEL_FIELD),
            FieldType(ChanField.REFLECTIVITY, np.uint8, (), FieldClass.PIXEL_FIELD)
        ]
        self._scan = LidarScan(self._sensor_info)
        self._frame_id = 0
        self._pkt_count = 0
        self._scan_range_m = None
        self._scan_intensity = None
        self._packet_queue = queue.Queue()
        self._packet_sender_stop = threading.Event()

        if self._http_server is None:
            self._http_server = HttpServer(self, "127.0.0.1", 8080, autostart=False)
        self._http_server.start()

        initialized = False
        while not initialized:
            try:
                self._sock_lidar = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self._sock_imu = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                print("[ExistingLidarReader] UDP sockets initialized")
                initialized = True
            except ConnectionRefusedError:
                print("[ExistingLidarReader] socket connection refused, retrying")
                time.sleep(0.1)

        self._packet_sender_thread = threading.Thread(
            target=self._packet_sender_loop,
            name="existing_lidar_packet_sender",
            daemon=True,
        )
        self._packet_sender_thread.start()

        self._session_active = True
        print("[ExistingLidarReader] Streaming session started")

    def _stop_session(self):
        if not self._session_active and self._writer is None and self._sock_lidar is None:
            return

        if self._writer is not None:
            self._writer.close()
            self._writer = None

        self._scan = None
        self._scan_range_m = None
        self._scan_intensity = None
        self._frame_id = 0
        self._pkt_count = 0

        if self._packet_sender_stop is not None:
            self._packet_sender_stop.set()
        if self._packet_queue is not None:
            self._packet_queue.put(None)
        if self._packet_sender_thread is not None:
            self._packet_sender_thread.join(timeout=1.0)
            self._packet_sender_thread = None
        self._packet_queue = None
        self._packet_sender_stop = None

        if self._sock_lidar is not None:
            self._sock_lidar.close()
            self._sock_lidar = None

        if self._sock_imu is not None:
            self._sock_imu.close()
            self._sock_imu = None

        if self._http_server is not None:
            self._http_server.stop()

        self._session_active = False
        print("[ExistingLidarReader] Streaming session stopped")

    def _configure_sensor_outputs(self):
        stage = omni.usd.get_context().get_stage()
        sensor_prim = stage.GetPrimAtPath(self.sensor_path) if stage else None
        if sensor_prim is None or not sensor_prim.IsValid():
            print(f"[ExistingLidarReader] WARNING: sensor prim {self.sensor_path} is not valid")
            return

        sensor_attrs = {
            "omni:sensor:Core:outputFrameOfReference": "SENSOR",
            "omni:sensor:Core:auxOutputType": "FULL",
        }
        for attr_name, attr_value in sensor_attrs.items():
            attr = sensor_prim.GetAttribute(attr_name)
            if not attr or not attr.IsValid():
                print(f"[ExistingLidarReader] WARNING: sensor prim is missing attribute {attr_name}")
                continue
            attr.Set(attr_value)

    async def _setup_pipeline_async(self):
        """Safely attaches the annotator to the existing sensor."""
        await omni.kit.app.get_app().next_update_async()

        if self._is_shutting_down:
            return

        self._configure_sensor_outputs()

        # 1. Create the Render Product pointing at the existing sensor path
        self.render_product = rep.create.render_product(
            self.sensor_path,
            [1, 1],
            name="RtxSensorRenderProduct",
            render_vars=["GenericModelOutput", "RtxSensorMetadata"],
        )
        self.render_product_path = self.render_product.path

        # 2. Initialize the safe ScanBuffer annotator
        self.annotator = rep.AnnotatorRegistry.get_annotator("IsaacCreateRTXLidarScanBuffer")
        self.annotator.initialize(
            outputIntensity=True,
            outputDistance=True,
            keepOnlyPositiveDistance=False,
            transformPoints=False,
            outputTimestamp=True,
            outputAzimuth=True,
            outputElevation=True
        )

        # 3. Attach and let Replicator build the graph
        self.annotator.attach([self.render_product_path])
        await rep.orchestrator.step_async()

        if self._is_shutting_down:
            return

        # 4. Subscribe to the update loop
        self._update_sub = omni.kit.app.get_app().get_update_event_stream().create_subscription_to_pop(
            self._on_update, name="read_existing_lidar_stream"
        )
        print(f"[ExistingLidarReader] Pipeline attached to {self.sensor_path}. Press PLAY to stream.")

        if self._timeline.is_playing():
            self._start_session()


    def _reshape_scan_field(self, field_data, width, height, echoes):
        native_shape = (width, height, echoes)
        reshaped = np.asarray(field_data).reshape(native_shape)
        return np.transpose(reshaped, (1, 0, 2))

    def _project_distance_to_3d(self, scan_data):
        distances = np.copy(scan_data["distance"])
        azimuths = np.copy(scan_data["azimuth"])
        elevations = np.copy(scan_data["elevation"])
        x = distances * np.cos(azimuths) * np.cos(elevations)
        y = distances * np.sin(azimuths) * np.cos(elevations)
        z = distances * np.sin(elevations)
        return x, y, z

    def _map_distance_to_u_v(self, width, height, distance, intensity, azimuth, elevation):
        if (
            self._scan_range_m is None
            or self._scan_range_m.shape != (height, width)
        ):
            self._scan_range_m = np.zeros((height, width), dtype=np.float32)
            self._scan_intensity = np.zeros((height, width), dtype=np.float32)
        else:
            self._scan_range_m.fill(0)
            self._scan_intensity.fill(0)

        elevation_lut = self._elevation_lut_deg_sorted
        elevation_order = self._elevation_lut_order

        # u: nearest elevation row. Searchsorted avoids the old height x samples
        # broadcast allocation while still recomputing indices every frame.
        insert_idx = np.searchsorted(elevation_lut, elevation, side="left")
        right_idx = np.clip(insert_idx, 0, height - 1)
        left_idx = np.clip(insert_idx - 1, 0, height - 1)
        use_right = np.abs(elevation_lut[right_idx] - elevation) < np.abs(elevation - elevation_lut[left_idx])
        u = np.where(use_right, elevation_order[right_idx], elevation_order[left_idx]).astype(np.intp, copy=False)
        np.clip(u, 0, height - 1, out=u)

        # v: azimuth is already degrees. Wrap negative angles into [0, 360)
        # and bin directly into [0, width), avoiding deg/rad conversions.
        v = np.floor(np.remainder(azimuth, 360.0) * (width / 360.0)).astype(np.intp)
        np.clip(v, 0, width - 1, out=v)

        self._scan_range_m[u, v] = distance
        self._scan_intensity[u, v] = intensity

        return self._scan_range_m, self._scan_intensity

    def _enqueue_packets(self, packets):
        if self._packet_queue is None:
            return

        for packet in packets:
            if packet.type not in (PacketType.Lidar, PacketType.Imu):
                print(f"Unknown packet type {packet.type}")
                continue

            self._packet_queue.put((
                packet.type,
                bytes(packet.buf),
                packet.host_timestamp,
            ))

    def _packet_sender_loop(self):
        last_host_timestamp = None

        while True:
            item = self._packet_queue.get()
            if item is None or self._packet_sender_stop.is_set():
                break

            packet_type, payload, host_timestamp = item

            if last_host_timestamp is not None:
                delta_ns = max(0, host_timestamp - last_host_timestamp)
                deadline = time.perf_counter() + delta_ns * 1e-9

                while not self._packet_sender_stop.is_set():
                    sleep_seconds = deadline - time.perf_counter()
                    if sleep_seconds <= 0:
                        break

                    time.sleep(min(sleep_seconds, 0.01))

                if self._packet_sender_stop.is_set():
                    break

            try:
                if packet_type == PacketType.Lidar:
                    self._sock_lidar.sendto(payload, ("127.0.0.1", 7502))
                elif packet_type == PacketType.Imu:
                    self._sock_imu.sendto(payload, ("127.0.0.1", 7503))
                self._pkt_count += 1
            except OSError:
                break

            last_host_timestamp = host_timestamp

    def _on_update(self, e):
        """Fires every frame to extract the buffered data."""
        if not self._session_active or not self._timeline.is_playing():
            return

        scan_data = self.annotator.get_data()

        if scan_data is None or "distance" not in scan_data or "intensity" not in scan_data:
            return

        points3d = scan_data.get("data", None)
        distance = np.asarray(scan_data["distance"])
        intensity = np.asarray(scan_data["intensity"])
        azimuth = np.asarray(scan_data["azimuth"])
        elevation = np.asarray(scan_data["elevation"])

        if distance.size == 0 or intensity.size == 0 or azimuth.size == 0 or elevation.size == 0:
            return

        # Isaac Sim 5.1 still exposes these fields on info, but they are deprecated and
        # may stay zero even when the scan buffer contains valid returns.
        width = int(self._sensor_info.format.columns_per_frame)
        height = int(self._sensor_info.format.pixels_per_column)

        scan_range_m, scan_intensity = self._map_distance_to_u_v(
            width, height, distance, intensity, azimuth, elevation)

        # Build the Ouster LidarScan from the Isaac Sim data.
        # Distance: meters (float32) -> clamp negatives (missed rays) -> mm -> uint32
        # Intensity: [0, 1] float -> [0, 255] -> uint8
        self._frame_id += 1
        self._scan.frame_id = self._frame_id
        self._scan.field("RANGE")[:] = np.where(scan_range_m > 0, scan_range_m * 1e+3, 0).astype(np.uint32)
        self._scan.field("REFLECTIVITY")[:] = np.clip(scan_intensity * 255.0, 0, 255).astype(np.uint8)

        # TODO[UN]: get the timestamps directly from the scan data
        ts = np.linspace(0, 1 / 10.0, width)  # Simulate timestamps across the scan duration
        timestamps = 1e9 * (self._frame_id / 10.0 + ts)
        self._scan.timestamp[:] = timestamps
        self._scan.status[:] = 0x1
        packet_count = len(self._scan.packet_timestamp)
        self._scan.packet_timestamp[:] = timestamps[np.arange(packet_count) * 16]

        # self._writer.save(0, self._scan, int(time.time() * 1e9))  # Save with current timestamp in nanoseconds

        packets = scan_to_packets(self._scan, self.sensor_info)
        self._enqueue_packets(packets)
        queued = self._packet_queue.qsize() if self._packet_queue is not None else 0
        print(
            f"[ExistingLidarReader] Queued {len(packets)} packets for frame "
            f"{self._frame_id} (queued: {queued}, sent: {self._pkt_count})"
        )

    def on_shutdown(self):
        """Cleans up the pipeline to avoid memory leaks."""
        self._is_shutting_down = True

        self._stop_session()

        if self._setup_task and not self._setup_task.done():
            self._setup_task.cancel()
        self._setup_task = None

        self._timeline_sub = None
        self._update_sub = None
        if self.annotator and self.render_product_path:
            try:
                self.annotator.detach([self.render_product_path])
            except Exception:
                pass
        self.annotator = None
        if self.render_product:
            self.render_product.destroy()
        self.render_product = None
        self.render_product_path = None
        self._http_server = None
