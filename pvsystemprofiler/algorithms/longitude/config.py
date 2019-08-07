"""  Parameters class for defines common functionality in terms of parameters for longitude estimators.
The method include solarnoon calculation based on users approach (average avg_sunrise_sunset versus energy_com),
days choice ("all", "clear", "cloudy"), and SCSF fitting ("True or False).
Note that if SCSF applied ("True"), the days_approach should be "all".

Parameters
----------

solarnoon : Solarnoon as measured from power signal data. Solarnoon should be given in hour values.

Returns
----------
"""
import cvxpy as cvx
from sys import path
path.append('..')
import numpy as np
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from solardatatools.clear_day_detection import find_clear_days
from statistical_clear_sky.algorithm.iterative_fitting import IterativeFitting

def run_scsf(power_signals):
    scsf = IterativeFitting(power_signals, rank_k=6, solver_type='MOSEK')
    scsf.execute(mu_l=5e2, mu_r=1e3, tau=0.85, max_iteration=10)
    return(scsf.clear_sky_signals())

class Config():
  def __init__(self, power_signals, index, days_approach, solarnoon_approach, scsf_flag, GMT_offset):
    self.power_signals = power_signals
    self.index = index
    self.solarnoon_approach = solarnoon_approach
    self.days_approach = days_approach
    self.scsf_flag = scsf_flag
    self.GMT_offset = GMT_offset

  def config_solarnoon(self):
    if self.scsf_flag == "False":
        solarnoon = self.solarnoon_approach(self.power_signals)
    if self.scsf_flag == "True":
        solarnoon = self.solarnoon_approach(run_scsf(self.power_signals))
    return(solarnoon)

  def config_days(self):
    if self.days_approach == 'all':
        days = np.array([True] * len(self.power_signals[0]))
    if self.days_approach == 'clear days':
        days = find_clear_days(self.power_signals)
    if self.days_approach == 'cloudy days':
        days = ~find_clear_days(self.power_signals)
    return(days)


if __name__ == "__main__":
    main()
