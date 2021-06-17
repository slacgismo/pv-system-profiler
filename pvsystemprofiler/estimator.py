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
from pvsystemprofiler.algorithms.latitude.direct_calculation import calc_lat
from solardatatools.algorithms import SunriseSunset
from pvsystemprofiler.algorithms.latitude.hours_daylight import calculate_hours_daylight
from pvsystemprofiler.utilities.hour_angle_equation import calculate_omega
from pvsystemprofiler.utilities.declination_equation import delta_cooper
from pvsystemprofiler.algorithms.angle_of_incidence.curve_fitting import run_curve_fit
from pvsystemprofiler.algorithms.performance_model_estimation import find_fit_costheta
from pvsystemprofiler.algorithms.angle_of_incidence.lambda_functions import select_function
from pvsystemprofiler.algorithms.angle_of_incidence.dynamic_value_functions import determine_keys
from pvsystemprofiler.algorithms.angle_of_incidence.dynamic_value_functions import select_init_values
from pvsystemprofiler.algorithms.tilt_azimuth.daytime_threshold_quantile import find_boolean_daytime
from pvsystemprofiler.utilities.tools import random_initial_values

class ConfigurationEstimator():
    def __init__(self, data_handler, gmt_offset):
        if not data_handler._ran_pipeline:
            data_handler.run_pipeline()
        self.data_handler = data_handler
        self.data_matrix = None
        # Parameters to be estimated
        self.longitude_estimate = None
        self.latitude_estimate = None
        self.tilt_estimate = None
        self.azimuth_estimate = None
        self.longitude_precalculate = None
        self.latitude_precalculate = None
        self.tilt_precalculate = None
        self.azimuth_precalculate = None
        # Attributes used for all calculations
        self.gmt_offset = gmt_offset
        self.hours_daylight = None
        self.days = None
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.daytime_threshold = None
        self.day_interval = None
        self.daytime_threshold_fit = None
        self.x1 = None
        self.x2 = None
        self.data_sampling = self.data_handler.data_sampling
        self.num_days = self.data_handler.num_days
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.eot_duffie = eot_duffie(self.day_of_year)
        self.eot_da_rosa = eot_da_rosa(self.day_of_year)
        self.delta = None
        self.omega = None

    def estimate_longitude(self, estimator='fit_l1',
                           eot_calculation='duffie',
                           solar_noon_method='optimized_estimates',
                           data_matrix='filled',
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
            self.longitude_estimate = self._cal_lon_helper(eot_ref=eot_calculation)
        else:
            loss = estimator.split('_')[-1]
            self.longitude_estimate = self._fit_lon_helper(loss=loss, eot_ref=eot_calculation)

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

    def estimate_latitude(self, daytime_threshold=0.001, data_matrix='filled', daylight_method='optimized_estimates',
                          day_selection_method='all'):

        dh = self.data_handler
        self.delta = delta_cooper(self.day_of_year, self.daily_meas)
        if data_matrix == 'raw':
            data_in = dh.raw_data_matrix
        elif data_matrix == 'filled':
            data_in = dh.filled_data_matrix
        if daylight_method in ('sunrise-sunset', 'sunrise sunset'):
            hours_daylight_all = calculate_hours_daylight(data_in, daytime_threshold)
        elif daylight_method in ('optimized_estimates', 'Optimized_Estimates'):
            ss = SunriseSunset()
            ss.run_optimizer(data=data_in)
            hours_daylight_all = ss.sunset_estimates - ss.sunrise_estimates
        if day_selection_method == 'all':
            self.days = dh.daily_flags.no_errors
        elif day_selection_method == 'clear':
            self.days = dh.daily_flags.clear
        elif day_selection_method == 'cloudy':
            self.days = dh.daily_flags.cloudy

        if np.any(np.isnan(hours_daylight_all)):
            hours_mask = np.isnan(hours_daylight_all)
            full_mask = ~hours_mask & self.days
            self.hours_daylight = hours_daylight_all[full_mask]
            self.delta = self.delta[:, full_mask]
        else:
            self.hours_daylight = hours_daylight_all[self.days]
            self.delta = self.delta[:, self.days]

        self.latitude_estimate = self._cal_lat_helper()

    def _cal_lat_helper(self):
        latitude_estimate = calc_lat(self.hours_daylight, self.delta)
        return np.nanmedian(latitude_estimate)

    def estimate_orientation(self, lon_precalculate=None, lat_precalculate=None, tilt_precalculate=None,
                             azimuth_precalculate=None, day_interval=None, x1=0.9, x2=0.9):

        self.longitude_precalculate = lon_precalculate
        self.latitude_precalculate = lat_precalculate
        self.tilt_precalculate = tilt_precalculate
        self.azimuth_precalculate = azimuth_precalculate
        self.day_interval = day_interval
        self.x1 = x1
        self.x2 = x2
        dh = self.data_handler
        self.data_matrix = dh.filled_data_matrix
        self.days = dh.daily_flags.clear
        self.num_days = dh.num_days
        self.delta = delta_cooper(self.day_of_year, self.daily_meas)
        self.omega = calculate_omega(self.data_sampling, self.num_days, self.longitude_precalculate, self.day_of_year,
                                     self.gmt_offset)

        self.latitude_estimate, self.tilt_estimate, self.azimuth_estimate = self._cal_orientation_helper()

    def _cal_orientation_helper(self):
        if self.day_interval is not None:
            day_range = (self.day_of_year > self.day_interval[0]) & (self.day_of_year < self.day_interval[1])
        else:
            day_range = np.ones(self.day_of_year.shape, dtype=bool)

        scale_factor_costheta, costheta_fit = find_fit_costheta(self.data_matrix, self.days)

        boolean_daytime = find_boolean_daytime(self.data_matrix, self.daytime_threshold, self.x1, self.x2)

        boolean_daytime_range = boolean_daytime * self.days * day_range

        delta_f = self.delta[boolean_daytime_range]
        omega_f = self.omega[boolean_daytime_range]
        if ~np.any(boolean_daytime_range):
            print('No data made it through filters')

        lat_initial, tilt_initial, azim_initial = random_initial_values(1)

        func_customized, bounds = select_function(self.latitude_precalculate, self.tilt_precalculate,
                                                  self.azimuth_precalculate)
        dict_keys = determine_keys(latitude=self.latitude_precalculate, tilt=self.tilt_precalculate,
                                   azimuth=self.azimuth_precalculate)

        init_values_dict = {'latitude': lat_initial[0], 'tilt': tilt_initial[0], 'azimuth': azim_initial[0]}
        init_values, ivr = select_init_values(init_values_dict, dict_keys)

        estimates = run_curve_fit(func=func_customized, keys=dict_keys, delta=delta_f, omega=omega_f,
                                  costheta=costheta_fit, boolean_daytime_range=boolean_daytime_range,
                                  init_values=init_values, fit_bounds=bounds)

        for i, estimate in enumerate(dict_keys):
            if estimate == 'latitude_estimate':
                lat_estimate = estimates[i]
            if estimate == 'tilt_estimate':
                tilt_estimate = estimates[i]
            if estimate == 'azimuth_estimate':
                azimuth_estimate = estimates[i]

        if 'latitude_estimate' not in dict_keys:
            lat_estimate = None
        if 'tilt_estimate' not in dict_keys:
            tilt_estimate = None
        if 'azimuth_estimate' not in dict_keys:
            azimuth_estimate = None
        return lat_estimate, tilt_estimate, azimuth_estimate