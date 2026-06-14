"""Support for simplified access to data on nodes of type ouster.rtx_lidar.OgnOusterLidar

Ouster RTX Lidar streaming node. Reads the RTX lidar sensor output, converts it
to Ouster SDK format, and streams UDP packets to an Ouster client.
"""

import sys
import traceback

import omni.graph.core as og
import omni.graph.core._omni_graph_core as _og
import omni.graph.tools.ogn as ogn


class OgnOusterLidarDatabase(og.Database):
    """Helper class providing simplified access to data on nodes of type ouster.rtx_lidar.OgnOusterLidar

    Class Members:
        node: Node being evaluated

    Attribute Value Properties:
        Inputs:
            inputs.execIn
            inputs.sensorPath
            inputs.lidarConfigPath
            inputs.udpDest
            inputs.udpPortLidar
            inputs.udpPortImu
            inputs.httpPort
            inputs.enabled
        Outputs:
            outputs.execOut
            outputs.frameId
    """

    GENERATOR_VERSION = (1, 79, 2)
    TARGET_VERSION = (2, 184, 5)

    PER_NODE_DATA = {}

    INTERFACE = og.Database._get_interface([
        ('inputs:execIn', 'execution', 0, None, 'Execution input - connect to OnPlaybackTick or OnTick', {ogn.MetadataKeys.DEFAULT: '0'}, True, 0, False, ''),
        ('inputs:sensorPath', 'string', 0, 'Sensor Prim Path', 'USD path to the RTX Lidar sensor prim in the stage', {ogn.MetadataKeys.DEFAULT: '"/World/OS0_REV7_128_10hz___1024_resolution/sensor"'}, True, "/World/OS0_REV7_128_10hz___1024_resolution/sensor", False, ''),
        ('inputs:lidarConfigPath', 'string', 0, 'Lidar Config File', 'Path to the RTX Lidar JSON configuration file', {ogn.MetadataKeys.DEFAULT: '""'}, True, "", False, ''),
        ('inputs:udpDest', 'string', 0, 'UDP Destination', 'UDP destination address for lidar/IMU packets', {ogn.MetadataKeys.DEFAULT: '"127.0.0.1"'}, True, "127.0.0.1", False, ''),
        ('inputs:udpPortLidar', 'int', 0, 'UDP Port (Lidar)', 'UDP port for lidar data packets', {ogn.MetadataKeys.DEFAULT: '7502'}, True, 7502, False, ''),
        ('inputs:udpPortImu', 'int', 0, 'UDP Port (IMU)', 'UDP port for IMU data packets', {ogn.MetadataKeys.DEFAULT: '7503'}, True, 7503, False, ''),
        ('inputs:httpPort', 'int', 0, 'HTTP Port', 'HTTP server port for the sensor metadata API', {ogn.MetadataKeys.DEFAULT: '8080'}, True, 8080, False, ''),
        ('inputs:enabled', 'bool', 0, 'Enabled', 'Enable or disable UDP streaming', {ogn.MetadataKeys.DEFAULT: 'true'}, True, True, False, ''),
        ('outputs:execOut', 'execution', 0, None, 'Execution output', {}, True, None, False, ''),
        ('outputs:frameId', 'int64', 0, 'Frame ID', 'Current frame ID being streamed', {ogn.MetadataKeys.DEFAULT: '0'}, True, 0, False, ''),
    ])

    @classmethod
    def _populate_role_data(cls):
        """Populate a role structure with the non-default roles on this node type"""
        role_data = super()._populate_role_data()
        role_data.inputs.execIn = og.AttributeRole.EXECUTION
        role_data.outputs.execOut = og.AttributeRole.EXECUTION
        return role_data

    class ValuesForInputs(og.DynamicAttributeAccess):
        LOCAL_PROPERTY_NAMES = {
            "execIn", "sensorPath", "lidarConfigPath", "udpDest",
            "udpPortLidar", "udpPortImu", "httpPort", "enabled",
            "_setting_locked", "_batchedReadAttributes", "_batchedReadValues"
        }

        def __init__(self, node: og.Node, attributes, dynamic_attributes: og.DynamicAttributeInterface):
            context = node.get_graph().get_default_graph_context()
            super().__init__(context, node, attributes, dynamic_attributes)
            self._batchedReadAttributes = [
                self._attributes.execIn,
                self._attributes.sensorPath,
                self._attributes.lidarConfigPath,
                self._attributes.udpDest,
                self._attributes.udpPortLidar,
                self._attributes.udpPortImu,
                self._attributes.httpPort,
                self._attributes.enabled,
            ]
            self._batchedReadValues = [
                0,
                "/World/OS0_REV7_128_10hz___1024_resolution/sensor",
                "",
                "127.0.0.1",
                7502,
                7503,
                8080,
                True,
            ]

        @property
        def execIn(self):
            return self._batchedReadValues[0]

        @execIn.setter
        def execIn(self, value):
            self._batchedReadValues[0] = value

        @property
        def sensorPath(self):
            return self._batchedReadValues[1]

        @sensorPath.setter
        def sensorPath(self, value):
            self._batchedReadValues[1] = value

        @property
        def lidarConfigPath(self):
            return self._batchedReadValues[2]

        @lidarConfigPath.setter
        def lidarConfigPath(self, value):
            self._batchedReadValues[2] = value

        @property
        def udpDest(self):
            return self._batchedReadValues[3]

        @udpDest.setter
        def udpDest(self, value):
            self._batchedReadValues[3] = value

        @property
        def udpPortLidar(self):
            return self._batchedReadValues[4]

        @udpPortLidar.setter
        def udpPortLidar(self, value):
            self._batchedReadValues[4] = value

        @property
        def udpPortImu(self):
            return self._batchedReadValues[5]

        @udpPortImu.setter
        def udpPortImu(self, value):
            self._batchedReadValues[5] = value

        @property
        def httpPort(self):
            return self._batchedReadValues[6]

        @httpPort.setter
        def httpPort(self, value):
            self._batchedReadValues[6] = value

        @property
        def enabled(self):
            return self._batchedReadValues[7]

        @enabled.setter
        def enabled(self, value):
            self._batchedReadValues[7] = value

        def __getattr__(self, item: str):
            if item in self.LOCAL_PROPERTY_NAMES:
                return object.__getattribute__(self, item)
            else:
                return super().__getattr__(item)

        def __setattr__(self, item: str, new_value):
            if item in self.LOCAL_PROPERTY_NAMES:
                object.__setattr__(self, item, new_value)
            else:
                super().__setattr__(item, new_value)

        def _prefetch(self):
            readAttributes = self._batchedReadAttributes
            newValues = _og._prefetch_input_attributes_data(readAttributes)
            if len(readAttributes) == len(newValues):
                self._batchedReadValues = newValues

    class ValuesForOutputs(og.DynamicAttributeAccess):
        LOCAL_PROPERTY_NAMES = {"execOut", "frameId", "_batchedWriteValues"}

        def __init__(self, node: og.Node, attributes, dynamic_attributes: og.DynamicAttributeInterface):
            context = node.get_graph().get_default_graph_context()
            super().__init__(context, node, attributes, dynamic_attributes)
            self._batchedWriteValues = {}

        @property
        def execOut(self):
            value = self._batchedWriteValues.get(self._attributes.execOut)
            if value:
                return value
            else:
                data_view = og.AttributeValueHelper(self._attributes.execOut)
                return data_view.get()

        @execOut.setter
        def execOut(self, value):
            self._batchedWriteValues[self._attributes.execOut] = value

        @property
        def frameId(self):
            value = self._batchedWriteValues.get(self._attributes.frameId)
            if value:
                return value
            else:
                data_view = og.AttributeValueHelper(self._attributes.frameId)
                return data_view.get()

        @frameId.setter
        def frameId(self, value):
            self._batchedWriteValues[self._attributes.frameId] = value

        def __getattr__(self, item: str):
            if item in self.LOCAL_PROPERTY_NAMES:
                return object.__getattribute__(self, item)
            else:
                return super().__getattr__(item)

        def __setattr__(self, item: str, new_value):
            if item in self.LOCAL_PROPERTY_NAMES:
                object.__setattr__(self, item, new_value)
            else:
                super().__setattr__(item, new_value)

        def _commit(self):
            _og._commit_output_attributes_data(self._batchedWriteValues)
            self._batchedWriteValues = {}

    class ValuesForState(og.DynamicAttributeAccess):
        def __init__(self, node: og.Node, attributes, dynamic_attributes: og.DynamicAttributeInterface):
            context = node.get_graph().get_default_graph_context()
            super().__init__(context, node, attributes, dynamic_attributes)

    def __init__(self, node):
        super().__init__(node)
        dynamic_attributes = self.dynamic_attribute_data(node, og.AttributePortType.ATTRIBUTE_PORT_TYPE_INPUT)
        self.inputs = OgnOusterLidarDatabase.ValuesForInputs(node, self.attributes.inputs, dynamic_attributes)
        dynamic_attributes = self.dynamic_attribute_data(node, og.AttributePortType.ATTRIBUTE_PORT_TYPE_OUTPUT)
        self.outputs = OgnOusterLidarDatabase.ValuesForOutputs(node, self.attributes.outputs, dynamic_attributes)
        dynamic_attributes = self.dynamic_attribute_data(node, og.AttributePortType.ATTRIBUTE_PORT_TYPE_STATE)
        self.state = OgnOusterLidarDatabase.ValuesForState(node, self.attributes.state, dynamic_attributes)

    class abi:
        @staticmethod
        def get_node_type():
            get_node_type_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'get_node_type', None)
            if callable(get_node_type_function):
                return get_node_type_function()
            return 'ouster.rtx_lidar.OgnOusterLidar'

        @staticmethod
        def compute(context, node):
            def database_valid():
                return True
            try:
                per_node_data = OgnOusterLidarDatabase.PER_NODE_DATA[node.node_id()]
                db = per_node_data.get('_db')
                if db is None:
                    db = OgnOusterLidarDatabase(node)
                    per_node_data['_db'] = db
                if not database_valid():
                    per_node_data['_db'] = None
                    return False
            except Exception:
                db = OgnOusterLidarDatabase(node)

            try:
                compute_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'compute', None)
                if callable(compute_function) and compute_function.__code__.co_argcount > 1:
                    return compute_function(context, node)

                db.inputs._prefetch()
                db.inputs._setting_locked = True
                with og.in_compute():
                    return OgnOusterLidarDatabase.NODE_TYPE_CLASS.compute(db)
            except Exception as error:
                stack_trace = "".join(traceback.format_tb(sys.exc_info()[2].tb_next))
                db.log_error(f'Assertion raised in compute - {error}\n{stack_trace}', add_context=False)
            finally:
                db.inputs._setting_locked = False
                db.outputs._commit()
            return False

        @staticmethod
        def initialize(context, node):
            OgnOusterLidarDatabase._initialize_per_node_data(node)
            initialize_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'initialize', None)
            if callable(initialize_function):
                initialize_function(context, node)

            per_node_data = OgnOusterLidarDatabase.PER_NODE_DATA[node.node_id()]

            def on_connection_or_disconnection(*args):
                per_node_data['_db'] = None

            node.register_on_connected_callback(on_connection_or_disconnection)
            node.register_on_disconnected_callback(on_connection_or_disconnection)

        @staticmethod
        def initialize_nodes(context, nodes):
            for n in nodes:
                OgnOusterLidarDatabase.abi.initialize(context, n)

        @staticmethod
        def release(node):
            release_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'release', None)
            if callable(release_function):
                release_function(node)
            OgnOusterLidarDatabase._release_per_node_data(node)

        @staticmethod
        def init_instance(node, graph_instance_id):
            init_instance_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'init_instance', None)
            if callable(init_instance_function):
                init_instance_function(node, graph_instance_id)

        @staticmethod
        def release_instance(node, graph_instance_id):
            release_instance_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'release_instance', None)
            if callable(release_instance_function):
                release_instance_function(node, graph_instance_id)
            OgnOusterLidarDatabase._release_per_node_instance_data(node, graph_instance_id)

        @staticmethod
        def update_node_version(context, node, old_version, new_version):
            update_node_version_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'update_node_version', None)
            if callable(update_node_version_function):
                return update_node_version_function(context, node, old_version, new_version)
            return False

        @staticmethod
        def initialize_type(node_type):
            initialize_type_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'initialize_type', None)
            needs_initializing = True
            if callable(initialize_type_function):
                needs_initializing = initialize_type_function(node_type)
            if needs_initializing:
                node_type.set_metadata(ogn.MetadataKeys.EXTENSION, "ouster.rtx_lidar")
                node_type.set_metadata(ogn.MetadataKeys.UI_NAME, "Ouster RTX Lidar")
                node_type.set_metadata(ogn.MetadataKeys.CATEGORIES, "isaacSensor")
                node_type.set_metadata(ogn.MetadataKeys.DESCRIPTION, "Ouster RTX Lidar streaming node. Reads the RTX lidar sensor output, converts it to Ouster SDK format, and streams UDP packets to an Ouster client.")
                node_type.set_metadata(ogn.MetadataKeys.LANGUAGE, "Python")
                OgnOusterLidarDatabase.INTERFACE.add_to_node_type(node_type)

        @staticmethod
        def on_connection_type_resolve(node):
            on_connection_type_resolve_function = getattr(OgnOusterLidarDatabase.NODE_TYPE_CLASS, 'on_connection_type_resolve', None)
            if callable(on_connection_type_resolve_function):
                on_connection_type_resolve_function(node)

    NODE_TYPE_CLASS = None

    @staticmethod
    def register(node_type_class):
        OgnOusterLidarDatabase.NODE_TYPE_CLASS = node_type_class
        og.register_node_type(OgnOusterLidarDatabase.abi, 1)

    @staticmethod
    def deregister():
        og.deregister_node_type("ouster.rtx_lidar.OgnOusterLidar")
