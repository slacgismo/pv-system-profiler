"""

"""

# Standard Imports
import numpy as np
import cvxpy as cvx
# Solar Data Tools Imports
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from solardatatools.algorithms import SunriseSunset
# Module Imports
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon
from pvsystemprofiler.utilities.equation_of_time import eot_haghdadi, eot_duffie
from pvsystemprofiler.utilities.progress import progress


class ConfigurationEstimator():
    def __init__(self, data_handler):
        if not data_handler._ran_pipeline:
            data_handler.run_pipeline()
        self.data_handler = data_handler

        # Parameters to be estimated
        self.longitude = None
        self.latitude = None
        self.tilt = None
        self.azimuth = None
        # Attributes used for all calculations
        self.gmt_offset = gmt_offset
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.eot_duffie = eot_duffie(self.day_of_year)
        self.eot_hag = eot_haghdadi(self.day_of_year)

    def estimate_longitude(self, estimator='calculated',
                             eot_calculation='duffie',
                             solar_noon_method='optimized_filled',
                             day_selection_method='all'):
        dh = self.data_handler
        if solar_noon_method == 'rise_set_average':
            self.solarnoon = avg_sunrise_sunset(dh.filled_data_matrix)
        elif solar_noon_method == 'energy_com':
            self.solarnoon = energy_com(dh.filled_data_matrix)
        elif solar_noon_method == 'optimized_raw':
            ss = SunriseSunset()
            ss.run_optimizer(data=dh.raw_data_matrix)
            self.solarnoon = np.nanmean(
                [ss.sunrise_estimates, ss.sunset_estimates], axis=0)
        elif solar_noon_method == 'optimized_filled':
            ss = SunriseSunset()
            ss.run_optimizer(data=dh.filled_data_matrix)
            self.solarnoon = np.nanmean(
                [ss.sunrise_estimates, ss.sunset_estimates], axis=0)
        if day_selection_method == 'all':
            self.days = np.ones(self.data_matrix.shape[1],
                                dtype=np.bool)
        elif day_selection_method == 'clear':
            self.days = self.data_handler.daily_flags.clear
        elif day_selection_method == 'cloudy':
            self.days = self.data_handler.daily_flags.cloudy
        if estimator == 'calculated':
            self.longitude = self._cal_lon_helper(eot_ref=eot_calculation)
        else:
            loss = estimator.split('_')[-1]
            self.longitude = self._fit_lon_helper(loss=loss, eot_ref=eot_calculation)

    def _cal_lon_helper(self, eot_ref='duffie'):
        sn = 60 * self.solarnoon[self.days]  # convert hours to minutes
        if eot_ref in ('duffie', 'd', 'duf'):
            eot = self.eot_duffie[self.days]
        elif eot_ref in ('haghdadi', 'h', 'hag'):
            eot = self.eot_hag[self.days]
        gmt = self.gmt_offset
        estimates = calc_lon(sn, eot, gmt)
        return np.nanmedian(estimates)

    def _fit_lon_helper(self, loss='l2', eot_ref='duffie'):
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

    def latitude_estimation(self):
        pass

    def orientation_estimation(self):
        pass