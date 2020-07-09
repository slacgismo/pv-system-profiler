from scipy.optimize import curve_fit
import numpy as np
import cvxpy as cvx


def run_curve_fit(func, delta, omega, costheta, boolean_daytime_range, init_values, fit_bounds):
    costheta_fit = costheta[boolean_daytime_range]
    x = np.array([delta, omega])
    popt, pcov = curve_fit(func, x, costheta_fit, p0=np.deg2rad(init_values),
                           bounds=fit_bounds)
    estimates = np.degrees(popt)
    return estimates


def find_fit_costheta(data_matrix, clear_index):
    data = np.max(data_matrix, axis=0)
    s1 = cvx.Variable(len(data))
    s2 = cvx.Variable(len(data))
    cost = 1e1 * cvx.norm(cvx.diff(s1, k=2), p=2) + cvx.norm(s2[clear_index])
    objective = cvx.Minimize(cost)
    constraints = [
        data == s1 + s2,
        s1[365:] == s1[:-365]
    ]
    problem = cvx.Problem(objective, constraints)
    problem.solve(solver='MOSEK')
    scale_factor_costheta = s1.value
    costheta_fit = data_matrix / np.max(s1.value)
    return scale_factor_costheta, costheta_fit


def calculate_costheta(func, delta_sys, omega_sys, lat, tilt, azim, est_dict=None, ground_truth=False):
    if ground_truth:
        if lat is None or tilt is None or azim is None:
            return None
        else:
            latitude_sys = lat
            tilt_sys = tilt
            azimuth_sys = azim
    else:
        if lat is None:
            latitude_sys = est_dict['latitude_estimate']
        else:
            latitude_sys = lat
        if tilt is None:
            tilt_sys = est_dict['tilt_estimate']
        else:
            tilt_sys = tilt
        if azim is None:
            azimuth_sys = est_dict['azimuth_estimate']
        else:
            azimuth_sys = azim

    x = np.array([delta_sys, omega_sys])
    phi = np.deg2rad(latitude_sys)
    beta = np.deg2rad(tilt_sys)
    gamma = np.deg2rad(azimuth_sys)
    costheta = func(x, phi, beta, gamma)
    return costheta