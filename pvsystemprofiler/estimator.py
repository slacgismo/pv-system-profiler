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
from pvsystemprofiler.utilities.declination_equation import delta_cooper
from solardatatools.algorithms import SunriseSunset
from pvsystemprofiler.algorithms.latitude.direct_calculation import calc_lat
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
        self.eot_hag = eot_haghdadi(self.day_of_year)
        self.delta = delta_cooper(self.day_of_year, self.daily_meas)
        self.hours_daylight = None

    def estimate_longitude(self, estimator='calculated',
                             eot_calculation='duffie',
                             solar_noon_method='optimized',
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
        elif solar_noon_method == 'optimized':
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

    def estimate_latitude(self, daytime_threshold=0.001,  data_matrix='filled', daylight_method='optimized'):
        dh = self.data_handler
        if data_matrix == 'raw':
            data_in = self.data_handler.raw_data_matrix
        elif data_matrix == 'filled':
            data_in = self.data_handler.filled_data_matrix
        if daylight_method in ('sunrise-sunset', 'sunrise sunset'):
            self.hours_daylight = self.calculate_hours_daylight(data_in, daytime_threshold)
        elif daylight_method in ('optimized', 'Optimized'):
            ss = SunriseSunset()
            ss.run_optimizer(data=data_in)
            self.hours_daylight = ss.sunset_estimates - ss.sunrise_estimates
        self.latitude = self._cal_lat_helper()

    def _cal_lat_helper(self):
        latitude_estimate = calc_lat(self.hours_daylight, self.delta)
        return np.median(latitude_estimate)

    def calculate_hours_daylight(self, data_in, threshold=0.001):
        data = np.copy(data_in).astype(np.float)
        num_meas_per_hour = data.shape[0] / 24
        x = np.arange(0, 24, 1. / num_meas_per_hour)
        night_msk = ~find_daytime(data_in, threshold=threshold)
        data[night_msk] = np.nan
        good_vals = (~np.isnan(data)).astype(int)
        sunrise_idxs = np.argmax(good_vals, axis=0)
        sunset_idxs = data.shape[0] - np.argmax(np.flip(good_vals, 0), axis=0)
        sunset_idxs[sunset_idxs == data.shape[0]] = data.shape[0] - 1
        hour_of_day = x
        sunset_times = hour_of_day[sunset_idxs]
        sunrise_times = hour_of_day[sunrise_idxs]
        return sunset_times - sunrise_times

    def orientation_estimation(self):
        pass