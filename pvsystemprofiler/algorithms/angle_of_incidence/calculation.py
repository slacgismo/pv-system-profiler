from scipy.optimize import curve_fit
import numpy as np
import cvxpy as cvx
""" Angle of incidence Module
This module contains the function for the calculation of the system's latitude, tilt and azimuth when some of them are
given as precalculates and others are left as unknowns. Unknowns are calculated via fit to the angle of incidence 
cos(theta) equation (1.6.2) in:
Duffie, John A., and William A. Beckman. Solar engineering of thermal processes. New York: Wiley, 1991.
"""
def run_curve_fit(func, delta, omega, costheta, boolean_daytime_range, init_values, fit_bounds):
    """
    :param func: Angle of incidence model function.
    :param delta: System's declination (array).
    :param omega: System's hour angle (array).
    :param costheta: The dependent data. Angle of incidence array used to fit parameters.
    :param boolean_daytime_range: boolean array specifying days to be used in fitting.
    :param init_values: Initial guess for the parameters.
    :param fit_bounds: Lower and upper bounds on parameters.
    :return: Optimal values for the parameters.
    """
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


def calculate_costheta(func, delta_sys, omega_sys, lat=None, tilt=None, azim=None, est_dict=None, ground_truth=False):
    """
    :param func: angle of incidence model function.
    :param delta: System's declination (array).
    :param omega: System's hour angle (array).
    :param lat: (optional) System's latitude.
    :param tilt: (optional) System's tilt.
    :param azim: (optional)System's azimuth.
    :param est_dict: directory containig estimated parametes
    :param ground_truth:
    :return: angle of incidence array
    """
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