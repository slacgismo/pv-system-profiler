"""

"""

# Standard Imports
import numpy as np
import cvxpy as cvx
# Solar Data Tools Imports
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset

# Module Imports
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon
from pvsystemprofiler.utilities.equation_of_time import eot_da_rosa, eot_duffie
from pvsystemprofiler.utilities.declination_equation import delta_cooper
from pvsystemprofiler.algorithms.latitude.direct_calculation import calc_lat
from solardatatools.algorithms import SunriseSunset
from pvsystemprofiler.algorithms.latitude.hours_daylight import calculate_hours_daylight
from pvsystemprofiler.utilities.progress import progress


class ConfigurationEstimator():
    def __init__(self, data_handler, gmt_offset):
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
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.eot_duffie = eot_duffie(self.day_of_year)
        self.eot_da_rosa = eot_da_rosa(self.day_of_year)
        self.delta = None
        self.hours_daylight = None
        self.days = None

    def estimate_longitude(self, estimator='calculated',
                             eot_calculation='duffie',
                             solar_noon_method='optimized_estimates',
                             data_matrix ='filled',
                             day_selection_method='all'):
        dh = self.data_handler

        if data_matrix == 'raw':
            data_in = dh.raw_data_matrix
        elif data_matrix == 'filled':
            data_in = dh.filled_data_matrix
        if solar_noon_method == 'rise_set_average':
            self.solarnoon = avg_sunrise_sunset(data_in)
        elif solar_noon_method == 'energy_com':
            self.solarnoon = energy_com(data_in)
        elif solar_noon_method == 'optimized_estimates':
            ss = SunriseSunset()
            ss.run_optimizer(data=data_in)
            self.solarnoon = np.nanmean(
                [ss.sunrise_estimates, ss.sunset_estimates], axis=0)
        if day_selection_method == 'all':
            self.days = dh.daily_flags.no_errors
        elif day_selection_method == 'clear':
            self.days = dh.daily_flags.clear
        elif day_selection_method == 'cloudy':
            self.days = dh.daily_flags.cloudy
        if estimator == 'calculated':
            self.longitude = self._cal_lon_helper(eot_ref=eot_calculation)
        else:
            loss = estimator.split('_')[-1]
            self.longitude = self._fit_lon_helper(loss=loss, eot_ref=eot_calculation)

    def _cal_lon_helper(self, eot_ref='duffie'):
        sn = 60 * self.solarnoon[self.days]  # convert hours to minutes
        if eot_ref in ('duffie', 'd', 'duf'):
            eot = self.eot_duffie[self.days]
        elif eot_ref in ('da_rosa', 'dr', 'rosa'):
            eot = self.eot_da_rosa[self.days]
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
        elif eot_ref in ('da_rosa', 'dr', 'rosa'):
            eot = self.eot_da_rosa
        sn_m = 720 - eot + 4 * (15 * self.gmt_offset - lon)
        sn_h = sn_m / 60
        nan_mask = np.isnan(self.solarnoon)
        use_days = np.logical_and(self.days, ~nan_mask)
        cost = cost_func(sn_h[use_days] - self.solarnoon[use_days])
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        return lon.value.item()

    def estimate_latitude(self, daytime_threshold=0.001,  data_matrix='filled', daylight_method='optimized_estimates',
                          day_selection_method='all'):
        dh = self.data_handler
        self.delta = delta_cooper(self.day_of_year, self.daily_meas)
        if data_matrix == 'raw':
            data_in = self.data_handler.raw_data_matrix
        elif data_matrix == 'filled':
            data_in = self.data_handler.filled_data_matrix
        if daylight_method in ('sunrise-sunset', 'sunrise sunset'):
            hours_daylight_all = calculate_hours_daylight(data_in, daytime_threshold)
        elif daylight_method in ('optimized_estimates', 'Optimized_Estimates'):
            ss = SunriseSunset()
            ss.run_optimizer(data=data_in)
            hours_daylight_all = ss.sunset_estimates - ss.sunrise_estimates
        if day_selection_method == 'all':
            self.days = self.data_handler.daily_flags.no_errors
        elif day_selection_method == 'clear':
            self.days = self.data_handler.daily_flags.clear
        elif day_selection_method == 'cloudy':
            self.days = self.data_handler.daily_flags.cloudy

        if np.any(np.isnan(hours_daylight_all)):
            hours_mask = np.isnan(hours_daylight_all)
            full_mask = ~hours_mask & self.days
            self.hours_daylight = hours_daylight_all[full_mask]
            self.delta = self.delta[:, full_mask]
        else:
            self.hours_daylight = hours_daylight_all[self.days]
            self.delta = self.delta[:, self.days]

        self.latitude = self._cal_lat_helper()

    def _cal_lat_helper(self):
        latitude_estimate = calc_lat(self.hours_daylight, self.delta)
        return np.nanmedian(latitude_estimate)

    def orientation_estimation(self):
        pass