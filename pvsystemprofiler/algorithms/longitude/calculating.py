"""
This module contains code for longitude estimation using fitting algorithms.
The estimation is based only on PV power output. All output is longitude
in degrees West.The fitting approach applied here for longitude estimation uses convex
optimization. The longitude is estimated by minimizing the misfit between
measured data and expected data.

Parameters
----------
GMT_offset : Time difference between local standard clock time and Greenwich Mean Time (GMT).
This value is given in hours absolute value.

EOT : Equation of Time as list of numberic values with length same as power signals day length.
          Difference in time between solar time and mean solar time in minutes.

solarnoon : Solarnoon as measured from power signal data. Solarnoon should be given in hour values.

Returns
----------
longitude : Numeric (with sign)
"""

import cvxpy as cvx
from sys import path
path.append('..')
from pvsystemprofiler.utilities.tools import *
from pvsystemprofiler.algorithms.longitude.config import Config


class CalEstimator():
    """ The fitting approach applied here for longitude estimation uses methods from
    Haghdadi et al. and Duffie & Beckman. The longitude is estimated by a series of equations.

    Parameters
    ----------
    GMT_offset : Time difference between local standard clock time and Greenwich Mean Time (GMT).
    This value is given in hours absolute value.

    EOT : Equation of Time as list of numberic values with length same as power signals day length.
          Difference in time between solar time and mean solar time in minutes.
          The EOT is calculated either based on Haghdadi et al. or on Duffie & Beckman, modules imported
          by tools.

    solarnoon : Solarnoon as measured from power signal data. Solarnoon should be given in hour values.

    Returns
    ----------
    Longitude : Numeric (with sign)

    References
    ----------
    [1] Haghdadi, Navid, et al. "A method to estimate the location and orientation of
    distributed photovoltaic systems from their generation output data." Renewable Energy 108 (2017): 390-400.
    [2] Duffie, John A., and William A. Beckman. Solar engineering of thermal processes. John Wiley & Sons, 2013.
    """
    def __init__(self, power_signals, index, days_approach, solarnoon_approach, scsf_flag, GMT_offset):
        self.power_signals = power_signals
        self.index = index
        self.solarnoon_approach = solarnoon_approach
        self.days_approach = days_approach
        self.scsf_flag = scsf_flag
        self.GMT_offset = GMT_offset
        self.config = Config(power_signals, index, days_approach, solarnoon_approach, scsf_flag, GMT_offset)

    def cal_haghdadi(self):
        solarnoon = Config.config_solarnoon(self)
        day_of_year = day_of_year_finder(self.index)[1:-1]
        days = Config.config_days(self)
        B_h = calculate_simple_day_angle_Haghdadi(day_of_year[days], offset=81)
        E_h = equation_of_time_Haghdadi(B_h)
        lon_value = np.nanmedian((720-solarnoon[days]*60)/4-(E_h/4)) - 15*self.GMT_offset
        return lon_value

    def cal_duffie(self):
        solarnoon = Config.config_solarnoon(self)
        day_of_year = day_of_year_finder(self.index)[1:-1]
        days = Config.config_days(self)
        B_d = calculate_simple_day_angle_Duffie(day_of_year[days], offset=81)
        E_d = equation_of_time_Duffie(B_d)
        lon_value_signal = (solarnoon[days]*60 + E_d -720 - 4*15*self.GMT_offset)/4
        lon_value = np.nanmedian(lon_value_signal)
        return lon_value

if __name__ == "__main__":
    main()
