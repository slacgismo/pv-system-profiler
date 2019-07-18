"""
This module contains code for longitude estimation using fitting algorithms.
The estimation is based only on PV power output. All output is longitude
in degrees West.
"""

import cvxpy as cvx
from sys import path
path.append('..')
from pvsystemprofiler.utilities.tools import *
#from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from pvsystemprofiler.algorithms.longitude.abstract import Parameters

class FitEstimator(Parameters):
  def __init__(self, power_signals, solarnoon_approach, GMT_offset, EOT):
    Parameters.__init__(self, power_signals, solarnoon_approach)
    self.GMT_offset = GMT_offset
    self.EOT = EOT

  def fit_norm1(self):
    solarnoon = Parameters.extract_solarnoon(self)
    lon = cvx.Variable()
    sn_m = 4*(15*self.GMT_offset - lon)-self.EOT+720
    sn_h = sn_m / 60
    cost = cvx.norm1(sn_h - solarnoon)
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value


    print("Welcome", self.firstname, self.lastname, "to the class of", self.graduationyear)

# class LongFit():
#     """ The fitting approach applied here for longitude estimation uses convex
#     optimization. The longitude is estimated by minimizing the misfit between
#     measured data and expected data.
#
#     Parameters
#     ----------
#     GMT_offset : Time difference between local standard clock time and Greenwich Mean Time (GMT).
#     This value is given in hours absolute value.
#
#     EOT : Equation of Time as list of numberic values with length same as power signals day length.
#           Difference in time between solar time and mean solar time in minutes.
#
#     solarnoon : Solarnoon as measured from power signal data. Solarnoon should be given in hour values.
#
#     Returns
#     ----------
#     longitude : Numeric (with sign)
#     """
#     def fit_norm1(self, GMT_offset, EOT, solarnoon):
#         lon = cvx.Variable()
#         sn_m = 4*(15*GMT_offset - lon)-EOT+720
#         sn_h = sn_m / 60
#         cost = cvx.norm1(sn_h - solarnoon)
#         objective = cvx.Minimize(cost)
#         problem = cvx.Problem(objective)
#         problem.solve()
#         lon_value = -lon.value
#         return lon_value
#
#     def fit_norm(self, GMT_offset, EOT, power_signal, sn_method):
#         lon = cvx.Variable()
#         sn_m = 4*(15*GMT_offset - lon)-EOT+720
#         sn_h = sn_m / 60
#         cost = cvx.norm(sn_h - solarnoon)
#         objective = cvx.Minimize(cost)
#         problem = cvx.Problem(objective)
#         problem.solve()
#         lon_value = -lon.value
#         return lon_value
#
#     def fit_huber(self, GMT_offset, EOT, power_signal, sn_method):
#         lon = cvx.Variable()
#         sn_m = 4*(15*GMT_offset - lon)-EOT+720
#         sn_h = sn_m / 60
#         cost = cvx.sum(cvx.huber(sn_h - solarnoon))
#         objective = cvx.Minimize(cost)
#         problem = cvx.Problem(objective)
#         problem.solve()
#         lon_value = -lon.value
#         return lon_value
#
# class LongCal():
#     """ The fitting approach applied here for longitude estimation uses methods from
#     Haghdadi et al. and Duffie & Beckman. The longitude is estimated by a series of equations.
#
#     Parameters
#     ----------
#     GMT_offset : Time difference between local standard clock time and Greenwich Mean Time (GMT).
#     This value is given in hours absolute value.
#
#     EOT : Equation of Time as list of numberic values with length same as power signals day length.
#           Difference in time between solar time and mean solar time in minutes.
#           The EOT is calculated either based on Haghdadi et al. or on Duffie & Beckman, modules imported
#           by tools.
#
#     solarnoon : Solarnoon as measured from power signal data. Solarnoon should be given in hour values.
#
#     Returns
#     ----------
#     Longitude : Numeric (with sign)
#
#     References
#     ----------
#     [1] Haghdadi, Navid, et al. "A method to estimate the location and orientation of
#     distributed photovoltaic systems from their generation output data." Renewable Energy 108 (2017): 390-400.
#     [2] Duffie, John A., and William A. Beckman. Solar engineering of thermal processes. John Wiley & Sons, 2013.
#     """
#     def cal_haghdadi(self, GMT_offset, day_of_year, power_signal, sn_method):
#         if sn_method == 'avg_sunrise_sunset':
#             solarnoon = avg_sunrise_sunset(power_signal)
#         elif sn_method == 'energy_com':
#             solarnoon = energy_com(power_signal)
#             print(solarnoon)
#         else:
#             print("wrong parameter for solarnoon method - should be 'avg_sunrise_sunset' or 'energy_com'")
#         B_h = calculate_simple_day_angle_Haghdadi(day_of_year, offset=81)
#         E_h = equation_of_time_Haghdadi(B_h)
#         lon_value = np.median((720-solarnoon*60)/4-(E_h/4)) - 15*GMT_offset
#         return lon_value
#
#     def cal_duffie(self, GMT_offset, day_of_year, power_signal, sn_method):
#         if sn_method == 'avg_sunrise_sunset':
#             solarnoon = avg_sunrise_sunset(power_signal)
#         elif sn_method == 'energy_com':
#             solarnoon = energy_com(power_signal)
#         else:
#             print("wrong parameter for solarnoon method - should be 'avg_sunrise_sunset' or 'energy_com'")
#         B_d = calculate_simple_day_angle_Duffie(day_of_year, offset=81)
#         E_d = equation_of_time_Duffie(B_d)
#         lon_value_signal = (solarnoon*60 + E_d -720 - 4*15*GMT_offset)/4
#         lon_value = np.nanmedian(lon_value_signal)
#         return lon_value
#
# if __name__ == "__main__":
#     main()
