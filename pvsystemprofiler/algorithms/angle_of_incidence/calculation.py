import numpy as np

"""
Calculates the angle incidence using the cos(theta) equation (1.6.2) in:
Duffie, John A., and William A. Beckman. Solar engineering of thermal processes. New York: Wiley, 1991.
"""


def calculate_costheta(func, delta, omega, lat=None, tilt=None, azim=None, est_dict=None):
    """
    :param func: angle of incidence model function.
    :param delta: System's declination (array).
    :param omega: System's hour angle (array).
    :param lat: (optional) System's latitude.
    :param tilt: (optional) System's tilt.
    :param azim: (optional)System's azimuth.
    :param est_dict: directory containing estimated parameters
    :return: angle of incidence array
    """

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

    x = np.array([delta, np.deg2rad(omega)])
    phi = np.deg2rad(latitude_sys)
    beta = np.deg2rad(tilt_sys)
    gamma = np.deg2rad(azimuth_sys)
    costheta = func(x, phi, beta, gamma)
    return costheta
