''' Longitude Study Module
This module contains a class for running a "longitude study". The class accepts
a time-series data set from a PV system, typically multiple years of sub-daily
power data. It then runs all possible methods for estimating the longitude of
the system:
- Forward calculation, Hagdhadi
- Forward calculation, Duffie
- Curve fitting, L1 norm cost function
- Curve fitting, L2 norm cost function
- Curve fitting, Huber cost function (https://en.wikipedia.org/wiki/Huber_loss
'''
import numpy as np
import cvxpy as cvx
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from pvsystemprofiler.utilities.equation_of_time import haghdadi, duffie

class LongitudeStudy():
    def __init__(self, data_handler, day_selection="cloudy days",
                 solarnoon_approach='sunrise_sunset_average', GMT_offset=8):
        self.data_handler = data_handler
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.data_matrix = self.data_handler.filled_data_matrix
        self.day_selection = day_selection
        if solarnoon_approach == 'sunrise_sunset_average':
            self.solarnoon_function = avg_sunrise_sunset
        elif solarnoon_approach == 'energy_center_of_mass':
            self.solarnoon_function = energy_com
        self.GMT_offset = GMT_offset
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.lon_value_haghdadi = None
        self.lon_value_duffie = None
        self.eot_duffie = duffie(self.day_of_year)
        self.eot_hag = haghdadi(self.day_of_year)
        self.lon_value_fit_norm = None
        self.lon_value_fit_norm1 = None
        self.solarnoon = self.solarnoon_function(self.data_matrix)
        if self.day_selection == 'clear days':
            self.days = self.data_handler.daily_flags.clear
        if self.day_selection == 'cloudy days':
            self.days = self.data_handler.daily_flags.cloudy
        else:
            self.days = np.ones(self.data_matrix.shape[1], dtype=np.bool)

    def run(self):
        self.lon_value_haghdadi = np.nanmedian((720-self.solarnoon[self.days]*60)/4-(self.eot_hag[self.days]/4)) - 15*self.GMT_offset
        self.lon_value_duffie = np.nanmedian(self.solarnoon[self.days]*60 + self.eot_duffie[self.days] -720 - 4*15*self.GMT_offset)/4
        self.fit_norm1()
        self.fit_norm()
        self.fit_huber()
        return

    def fit_norm1(self):
        lon = cvx.Variable()
        sn_m = 4*(15*self.GMT_offset - lon)-self.eot_duffie+720
        sn_h = sn_m / 60
        cost = cvx.norm1(sn_h[self.days] - self.solarnoon[self.days])
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        self.lon_value_fit_norm1 = -lon.value
        return

    def fit_norm(self):
        lon = cvx.Variable()
        sn_m = 4*(15*self.GMT_offset - lon)-self.eot_duffie+720
        sn_h = sn_m / 60
        cost = cvx.norm(sn_h[self.days] - self.solarnoon[self.days])
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        self.lon_value_fit_norm = -lon.value
        return

    def fit_huber(self):
        lon = cvx.Variable()
        sn_m = 4*(15*self.GMT_offset - lon)-self.eot_duffie+720
        sn_h = sn_m / 60
        cost = cvx.sum(cvx.huber(sn_h[self.days] - self.solarnoon[self.days]))
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        self.lon_value_fit_huber = -lon.value
        return
