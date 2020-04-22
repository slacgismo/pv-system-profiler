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
import pandas as pd
import cvxpy as cvx
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon
from pvsystemprofiler.utilities.equation_of_time import eot_haghdadi, eot_duffie
from pvsystemprofiler.utilities.progress import progress

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
        self.gmt_offset = GMT_offset
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.lon_value_haghdadi = None
        self.lon_value_duffie = None
        self.eot_duffie = eot_duffie(self.day_of_year)
        self.eot_hag = eot_haghdadi(self.day_of_year)
        self.lon_value_fit_norm = None
        self.lon_value_fit_norm1 = None
        self.solarnoon = self.solarnoon_function(self.data_matrix)
        if self.day_selection == 'clear days':
            self.days = self.data_handler.daily_flags.clear
        if self.day_selection == 'cloudy days':
            self.days = self.data_handler.daily_flags.cloudy
        else:
            self.days = np.ones(self.data_matrix.shape[1], dtype=np.bool)

    def run(self, estimator=('calculated', 'fit_l1', 'fit_l2', 'fit_huber'),
            eot_calculation=('duffie', 'haghdadi'),
            solar_noon_method=('rise_set_average', 'energy_com'),
            day_selection_method=('all', 'clear', 'cloudy')):
        results = pd.DataFrame(columns=[
            'longitude', 'estimator', 'eot_calculation', 'solar_noon_method',
            'day_selection_method'
        ])
        estimator = np.atleast_1d(estimator)
        eot_calculation = np.atleast_1d(eot_calculation)
        solar_noon_method = np.atleast_1d(solar_noon_method)
        day_selection_method = np.atleast_1d(day_selection_method)
        total = (len(estimator) * len(eot_calculation) * len(solar_noon_method)
                 * len(day_selection_method))
        counter = 0
        for est in estimator:
            for eot in eot_calculation:
                pass
        for sn in solar_noon_method:
            if sn == 'rise_set_average':
                self.solarnoon = avg_sunrise_sunset(self.data_matrix)
            elif sn == 'energy_com':
                self.solarnoon = energy_com(self.data_matrix)
            for ds in day_selection_method:
                if ds == 'all':
                    self.days = np.ones(self.data_matrix.shape[1],
                                        dtype=np.bool)
                elif ds == 'clear':
                    self.days = self.data_handler.daily_flags.clear
                elif ds == 'cloudy':
                    self.days = self.data_handler.daily_flags.cloudy
                for est in estimator:
                    for eot in eot_calculation:
                        progress(counter, total)
                        lon = self.estimate_longitude(est, eot)
                        results.loc[counter] = [
                            lon, est, eot, sn, ds
                        ]
                        counter += 1
        progress(counter, total)
        return results

    def estimate_longitude(self, estimator, eot_calculation):
        if estimator == 'calculated':
            return self.calculate_longitude(eot_ref=eot_calculation)
        else:
            loss = estimator.split('_')[-1]
            return self.fit_longitude(loss=loss, eot_ref=eot_calculation)

    def calculate_longitude(self, eot_ref='duffie'):
        sn = 60 * self.solarnoon[self.days]  # convert hours to minutes
        if eot_ref in ('duffie', 'd', 'duf'):
            eot = self.eot_duffie[self.days]
        elif eot_ref in ('haghdadi', 'h', 'hag'):
            eot = self.eot_hag[self.days]
        gmt = self.gmt_offset
        estimates = calc_lon(sn, eot, gmt)
        return np.nanmedian(estimates)


    def fit_longitude(self, loss='l2', eot_ref='duffie'):
        lon = cvx.Variable()
        if loss == 'l2':
            cost_func = cvx.norm
        elif loss == 'l1':
            cost_func = cvx.norm1
        elif loss == 'huber':
            cost_func = lambda x: cvx.sum(cvx.huber(x))
        if eot_ref in ('duffie', 'd', 'duf'):
            eot = self.eot_duffie
        elif eot_ref in ('haghdadi', 'h', 'hag'):
            eot = self.eot_hag
        sn_m = 720 - eot + 4 * (15 * self.gmt_offset - lon)
        sn_h = sn_m / 60
        nan_mask = np.isnan(self.solarnoon)
        use_days = np.logical_and(self.days, ~nan_mask)
        cost = cost_func(sn_h[use_days] - self.solarnoon[use_days])
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        return lon.value.item()

