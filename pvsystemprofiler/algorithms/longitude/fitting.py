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

class FitEstimator():
  def __init__(self, power_signals, index, days_approach, solarnoon_approach, scsf_flag, GMT_offset, EOT):
    self.power_signals = power_signals
    self.index = index
    self.solarnoon_approach = solarnoon_approach
    self.days_approach = days_approach
    self.scsf_flag = scsf_flag
    self.GMT_offset = GMT_offset
    self.EOT = EOT
    self.config = Config(power_signals, index, days_approach, solarnoon_approach, scsf_flag, GMT_offset)

  def fit_norm1(self):
    solarnoon = Config.config_solarnoon(self)
    temp = np.isnan(solarnoon)
    days = Config.config_days(self)
    lon = cvx.Variable()
    sn_m = 4*(15*self.GMT_offset - lon)-self.EOT+720
    sn_h = sn_m / 60
    cost = cvx.norm1(sn_h[days] - solarnoon[days])
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

  def fit_norm(self):
    solarnoon = Config.config_solarnoon(self)
    days = Config.config_days(self)
    lon = cvx.Variable()
    sn_m = 4*(15*self.GMT_offset - lon)-self.EOT+720
    sn_h = sn_m / 60
    cost = cvx.norm(sn_h[days] - solarnoon[days])
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

  def fit_huber(self):
    solarnoon = Config.config_solarnoon(self)
    days = Config.config_days(self)
    lon = cvx.Variable()
    sn_m = 4*(15*self.GMT_offset - lon)-self.EOT+720
    sn_h = sn_m / 60
    cost = cvx.sum(cvx.huber(sn_h[days] - solarnoon[days]))
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

if __name__ == "__main__":
    main()
