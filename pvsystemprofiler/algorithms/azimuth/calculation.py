import numpy as np
from scipy.optimize import curve_fit

def run_curve_fit(func, delta, omega, costheta, boolean_daytime_range, init_values):
        costheta_fit = costheta[boolean_daytime_range]
        x = np.array([delta, omega])
        popt, pcov = curve_fit(func, x, costheta_fit, p0=np.deg2rad(init_values),
                               bounds=([0, -3.14], [1.57, 3.14]))
        tilt_estimate, azimuth_estimate = np.degrees(popt)
        return tilt_estimate, azimuth_estimate
