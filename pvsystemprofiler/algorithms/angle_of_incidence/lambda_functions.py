"""
This module is used to set the hour_angle_equation in terms of the unknowns. The hour equation is a function of the
declination (delta), the hour angle (omega) , latitude (phi), tilt (beta) and azimuth (gamma). The declination and the
hour angle are treated as input parameters for all cases. Latitude, tilt and azimuth can be given as input parameters
(precalculates) or left as unknowns. In total, seven different combinations arise from having these three parameters
as an inputs or as a unknowns. The seven conditionals below correspond to those combinations. The output function "func"
is used as one of the inputs to run_curve_fit which in turn is used to fit the unknowns. The function other outputs are
the 'init_values' array containing the initial values for the fit and the 'bounds' tuple containing the bounds for the
variables. Bounds for latitude are -90 to 90. Bounds for tilt are 0 to 90. Bounds for azimuth  are -180 to 180. It is
noted that theoretically, Bounds for tilt are 0 to 180 (Duffie, John A., and William A. Beckman. Solar engineering of
thermal processes. New York: Wiley, 1991.). However a value of tilt >90  would mean that that the surface has a
downward-facing component, which is not the case of the current application.
"""
from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta
import numpy as np

def select_function(lat_precalc=None, tilt_precalc=None, azim_precalc=None):
    '''
    :param lat_precalc: (optional) Latitude precalculate in Degrees.
    :param tilt_precalc: (optional) Tilt precalculate in Degrees.
    :param azim_precalc: (optional) Azimuth precalculate in Degrees.
    :return: Customized function 'func', 'bounds' tuple and 'init_values' array used by run_curve_fit as
             input. Array dict_keys containing the parameters left as variables.
    '''

    if lat_precalc is None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, phi, beta, gamma: func_costheta(x, phi, beta, gamma)

    if lat_precalc is not None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, beta, gamma: func_costheta(x, np.deg2rad(lat_precalc), beta, gamma)

    if lat_precalc is None and tilt_precalc is not None and azim_precalc is None:
        func = lambda x, phi, gamma: func_costheta(x, phi, np.deg2rad(tilt_precalc), gamma)

    if lat_precalc is None and tilt_precalc is None and azim_precalc is not None:
        func = lambda x, phi, beta: func_costheta(x, phi, beta, np.deg2rad(azim_precalc))

    if lat_precalc is None and tilt_precalc is not None and azim_precalc is not None:
        func = lambda x, phi: func_costheta(x, phi, np.deg2rad(tilt_precalc), np.deg2rad(azim_precalc))

    if lat_precalc is not None and tilt_precalc is None and azim_precalc is not None:
        func = lambda x, beta: func_costheta(x, np.deg2rad(lat_precalc), beta, np.deg2rad(azim_precalc))

    if lat_precalc is not None and tilt_precalc is not None and azim_precalc is None:
        func = lambda x, gamma: func_costheta(x, np.deg2rad(lat_precalc), np.deg2rad(tilt_precalc), gamma)

    bounds_dict = {'latitude': [-np.pi/2, np.pi/2], 'tilt': [0, np.pi/2], 'azimuth': [-np.pi, np.pi]}
    bounds = []

    if lat_precalc is None:
        bounds.append(bounds_dict['latitude'])
    if tilt_precalc is None:
        bounds.append(bounds_dict['tilt'])
    if azim_precalc is None:
        bounds.append(bounds_dict['azimuth'])

    bounds = tuple(np.transpose(bounds).tolist())
    init_values = 10 * np.ones(len(bounds[0]))

    return func, bounds, init_values

