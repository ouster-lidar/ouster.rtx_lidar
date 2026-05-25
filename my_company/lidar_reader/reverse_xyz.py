import numpy as np
import copy

def binary_search(arr, x):
    low, high = 0, len(arr) - 1
    while low <= high:
        mid = (low + high) // 2
        if x < arr[mid]:
            low = mid + 1
        elif x > arr[mid]:
            high = mid - 1
        else: # x == arr[mid]
            return mid
    if high < 0: return 0
    d1 = abs(arr[low] - x)
    d2 = abs(arr[high] - x)
    return low if d1 < d2 else high


class ReverseXYZLut:
    def __init__(self, metadata, height=None, width=None) -> None:
        self._sensor_info = metadata

        # For DEBUGGING
        self._register = {}

    def _generate_metadata(self, height, width):
        si = self._sensor_info
        factor = si.format.pixels_per_column // height
        meta = copy.deepcopy(si)
        pi = si.get_product_info()
        form_factor = pi.form_factor if not pi.form_factor[-1].isdigit() else \
            F"{pi.form_factor[:-1]}-{pi.form_factor[-1]}"
        si.prod_line = F"{form_factor}-{height}"
        si.format.pixels_per_column = height
        si.format.pixel_shift_by_row = si.format.pixel_shift_by_row[::factor]
        si.beam_azimuth_angles = si.beam_azimuth_angles[::factor]
        si.beam_altitude_angles = si.beam_altitude_angles[::factor]
        return meta

    @staticmethod
    def phi_to_u(beams, phi_deg):
        if phi_deg > beams[0]:
            u = 0
        elif phi_deg < beams[-1]:
            u = len(beams) - 1
        else:
            u = binary_search(beams, phi_deg)
        return u

    @staticmethod
    def point_2_u_v(pt, r0, beams, azimuth_angles_deg, W):
        """
        r0 is the distance from the beam origin to the lidar origin
        """
        x, y, z = pt
        r = np.linalg.norm([x, y, z])
        sin_beta = z / r
        cos_beta = np.sqrt(1 -  np.power(sin_beta, 2))
        phi = np.arctan2(z, r * cos_beta - r0)

        r_prime = np.rint(z / np.sin(phi) + r0)

        # now azimuth
        u = ReverseXYZLut.phi_to_u(beams, np.rad2deg(phi))
        a = np.deg2rad(-azimuth_angles_deg[u])

        A = r_prime * np.cos(phi)
        K = A * np.cos(a) + r0
        L = A * np.sin(a)

        # KL2 = np.sqrt(K**2 + L**2)     # the denominator optimized
        # sin_e = (K * py - L * px) / KL2
        # cos_e = (K * px + L * py) / KL2

        sin_e = K * y - L * x
        cos_e = K * x + L * y

        theta_e = np.arctan2(sin_e, cos_e)
        e = 2.0 * np.pi - theta_e * W / (2 * np.pi)
        v = np.int32(np.rint(e)) - 6    # TODO: still need to figure out why `v-6`
        return r_prime, u, v, np.rad2deg(phi), np.rad2deg(theta_e)

    def transform_point(self, p, transform):
        hp = np.append(p, 1)
        tp = np.dot(transform, hp)
        return tp[:-1]

    def project_point(self, pt):
        si = self._sensor_info
        return ReverseXYZLut.point_2_u_v(pt,
                            r0=si.lidar_origin_to_beam_origin_mm,
                            beams=si.beam_altitude_angles,
                            azimuth_angles_deg=si.beam_azimuth_angles,
                            W=si.format.columns_per_frame)
    
    def check_for_duplicates(self, u, v, p):
        if (u, v) not in self._register:
            self._register[(u, v)] = []
            self._register[(u, v)].append(p)
        else:
            self._register[(u, v)].append(p)

    def report_duplicates(self):
        duplicates = 0
        for k, v in self._register.items():
            if len(v) > 1:
                print(k, v)
                duplicates += 1
        print("total duplicates:", duplicates)
   

    def project_frame(self, points, use_sensor_2_lidar=True):
        si = self._sensor_info
        sensor_2_lidar = np.linalg.inv(si.lidar_to_sensor_transform)

        H = si.format.pixels_per_column
        W = si.format.columns_per_frame

        output = np.zeros((H, W), dtype=np.int32)
        elevation = np.zeros((H, W), dtype=np.float64)
        azimuth = np.zeros((H, W), dtype=np.float64)
        self._register = {}

        r = 0

        for i, p in enumerate(points):
            R = np.linalg.norm(p)
            if np.any(np.isnan(p)) or R == 0:
                continue

            try:
                if use_sensor_2_lidar:
                    p = self.transform_point(p, sensor_2_lidar)
                if np.linalg.norm(p) == 0:
                    continue
                r, u, v, phi, azi = self.project_point(p)
                output[u, v] = r
                elevation[u, v] = phi
                azimuth[u, v] = azi
                self.check_for_duplicates(u, v, p)
            except Exception as e:
                print(f"error: {e}")
                import traceback
                traceback.print_exc()
                print(r, u, v)
                exit(1)

        self.report_duplicates()

        return output, elevation, azimuth
