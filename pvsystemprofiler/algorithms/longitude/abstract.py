"""
This module defines common functionality of longitude estimators.
Since there is common code used in different longitude estimators, the common
parts are placed in the abstract class.
"""

import cvxpy as cvx
from sys import path
path.append('..')
#from pvsystemprofiler.utilities.tools import *
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset

# class Abstract():
#     """  Abstract class for estimators include methods are the same for all longitude estimators.
#     these methods include solarnoon calculation based on users approach
#     (average avg_sunrise_sunset versus energy_com), and days choice (all, clear, cloudy, after SCSF fitting)
#
#     Parameters
#     ----------
#
#     solarnoon : Solarnoon as measured from power signal data. Solarnoon should be given in hour values.
#
#     Returns
#     ----------
#     """
#     def extract_solarnoon(self, approach, power_signal):
#         if approach == 'avg_sunrise_sunset':
#             self.solarnoon = avg_sunrise_sunset(power_signal)
#         elif approach == 'energy_com':
#             self.solarnoon = energy_com(power_signal)
#         else:
#             print("wrong parameter for solarnoon method - should be 'avg_sunrise_sunset' or 'energy_com'")
#         return self.solarnoon
#
# if __name__ == "__main__":
#     main()
#


class Parameters:
  def __init__(self, power_signals, solarnoon_approach):
    self.power_signals = power_signals
    self.solarnoon_approach = solarnoon_approach
    #self.daysdata = d

  def extract_solarnoon(self):
    if self.solarnoon_approach == 'avg_sunrise_sunset':
        solarnoon = avg_sunrise_sunset(self.power_signals)
    return(solarnoon)

#Use the Person class to create an object, and then execute the printname method:
