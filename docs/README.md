# Ouster RTX Lidar [ouster.rtx_lidar]

A virtual Ouster using IsaacSim as the backend

## Setup

- Download the extension to a known location
- Open the extensions window and modify the search paths to include where the extension has been installed
- Open the THIRD PARTY tab and you should find the **OUSTER LIDAR RTX** extension under Users
- Enable the extension and also check the **AUTOLOAD** so the plugin is always ready.
![enable the extension](ext-enable.png)

## Usage

1. Add an ouster sensor from the **Creat** menu as shown below
![adding an ouster sensor](add-sensor.png)

2. Next enable **Action Graphs** window from **Window -> Graph Editors** sub menu.

3. Create a new action graph if there wasn't one already.

4. In the Action Graph find the **Search nodes** tab and type `Ouster` the **Ouster RTX Lidar** node
should show as one of the options as shown below
![Search for extension](ext-search.png) 

5. Drag and drop the Ouster RTX Lidar into the Action Graph
![add ouster rtx lidar to the graph](add-ouster-rtx-lidar.png)

6. In the Action Graph window search again for the `On Playback Tick` node and add the node to the graph

7. Connect the **On Playback Tick** node to the **Ouster RTX Lidar** node as shown below:
![connect ouster rtx lidar](connect-ouster-rtx-lidar.png)

Finaly, we need to configure the **Ouster RTX Lidar** node to know which sensor it is used for. Select the
**Ouster RTX Lidar** node from the Action Graph window (or through the **Stage** window). This should show
the node properties in the **Property** window. At minimum the `Sensr Prim Path` should be updated to point
to the Ouster sensor that we added earlier in first step. Your configuration should look like this:
![configure-sensor](configure-sensor.png)

Now you may click on IsaacSim play.

## Motion BVH (optional)

Motion BVH lets the lidar rays see objects at the position they have when each ray fires, so things
moving during a scan are captured with motion effects. Without it Isaac Sim logs
`MotionBVH for lidar model not enabled. This will result in an incorrect point cloud without motion effects!`
when the sensor starts; the lidar still streams normally.

Motion BVH is a renderer setting for the whole application: it applies to every sensor in the scene
and cannot be set per sensor. It must be turned on **when Isaac Sim is launched**, by adding these
flags to the command line:

```bash
./isaac-sim.sh \
  --/renderer/raytracingMotion/enabled=true \
  --/renderer/raytracingMotion/enableHydraEngineMasking=true \
  --/renderer/raytracingMotion/enabledForHydraEngines=0
```

For standalone Python scripts, pass `enable_motion_bvh` when creating the app, which sets the same
flags:

```python
from isaacsim import SimulationApp
simulation_app = SimulationApp({"headless": True, "enable_motion_bvh": True})
```

Do not change these settings while Isaac Sim is running (for example from the Python console or a
script after startup): on both Isaac Sim 5.1 and 6.0 the renderer then reports Motion BVH as on but
the lidar stops producing data. To switch Motion BVH on or off, restart Isaac Sim with or without the
flags. When it is on, the `MotionBVH for lidar model not enabled` warning no longer appears.