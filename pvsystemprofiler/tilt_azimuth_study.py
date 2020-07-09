''' Tilt and Azimuth Study Module
This module contains a class for conducting a study
to estimating Tilt and Azimuth from solar power data. This code accepts solar power
data in the form of a `solar-data-tools` `DataHandler` object, which is used
to standardize and pre-process the data. The provided class will then estimate
the Tilt and Azimuth of the site that produced the data, using the `run` method.
Tilt and Azimuth are estimated via numerical fit using equation (1.6.2) in:
    Duffie, John A., and William A. Beckman. Solar engineering of thermal
    processes. New York: Wiley, 1991.
'''
import numpy as np
import pandas as pd
import cvxpy as cvx
from solardatatools.daytime import find_daytime
from pvsystemprofiler.utilities.hour_angle_equation import find_omega
from pvsystemprofiler.utilities.declination_equation import delta_spencer
from pvsystemprofiler.utilities.declination_equation import delta_cooper
from pvsystemprofiler.algorithms.angle_of_incidence.calculation import run_curve_fit
from pvsystemprofiler.algorithms.angle_of_incidence.calculation import find_fit_costheta
from pvsystemprofiler.algorithms.angle_of_incidence.calculation import calculate_costheta
from pvsystemprofiler.algorithms.angle_of_incidence.lambda_functions import select_function
from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta


class TiltAzimuthStudy():
    def __init__(self, data_handler, day_range=None, init_values=None, daytime_threshold=None, lon_precalculate=None,
                 lat_precalculate=None, tilt_precalculate=None, azimuth_precalculate=None, lat_true_value=None,
                 tilt_true_value=None, azimuth_true_value=None, gmt_offset=-8):
        """
        :param data_handler: `DataHandler` class instance loaded with a solar power data set
        :param day_range: (optional) the desired day range to run the study. An array of the form
                              [first day, last day]
        :param init_values: (optional) Tilt and Azimuth guess values for numerical fit.
                                Default values are [10, 10]. (Degrees).
                                are used otherwise
        :param daytime_threshold: (optional) daytime threshold
        :param lon_precalculate: longitude estimate as obtained from the Longitude Study module. (Degrees).
        :param lat_precalculate: precalculated latitude value. (Degrees).
        :param tilt_precalculate: precalculated tilt value. (Degrees).
        :param azimuth_precalculate: precalculated azimuth value. (Degrees).
        :param lat_true_value: (optional) ground truth value for the system's Latitude. (Degrees).
        :param tilt_true_value: (optional) ground truth value for the system's Tilt. (Degrees).
        :param azimuth_true_value: (optional) ground truth value for the system's Azimuth. (Degrees).
        :param gmt_offset: The offset in hours between the local timezone and GMT/UTC
        """

        self.data_handler = data_handler

        if day_range is None:
            self.day_range_dict = {}
            self.day_range_dict = {'summer': [152, 245], 'no winter': [60, 335], 'spring': [60, 153], 'Full Year': None}
        else:
            self.day_range_dict = {'manual': day_range}
        self.data_matrix = self.data_handler.filled_data_matrix
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.init_values = init_values
        self.daytime_threshold = daytime_threshold
        self.lon_precalc = lon_precalculate
        self.lat_precalc = lat_precalculate
        self.tilt_precalc = tilt_precalculate
        self.azim_precalc = azimuth_precalculate
        self.phi_true_value = lat_true_value
        self.beta_true_value = tilt_true_value
        self.gamma_true_value = azimuth_true_value
        self.gmt_offset = gmt_offset
        self.daytime_threshold_fit = None
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.data_sampling = self.data_handler.data_sampling
        self.clear_index = data_handler.daily_flags.clear
        self.boolean_daytime = None
        self.delta_cooper = None
        self.delta_spencer = None
        self.omega = None
        self.scale_factor_costheta = None
        self.costheta_estimated = None
        self.costheta_ground_truth = None
        self.costheta_fit = None
        self.boolean_daytime_range = None
        self.results = None

    def run(self, delta_method=('cooper', 'spencer')):

        delta_method = np.atleast_1d(delta_method)
        self.find_boolean_daytime()
        self.omega = find_omega(self.data_sampling, self.num_days, self.lon_precalc, self.day_of_year,
                                self.gmt_offset)

        self.scale_factor_costheta, self.costheta_fit = find_fit_costheta(self.data_matrix, self.clear_index)
        self.delta_cooper = delta_cooper(self.day_of_year, self.daily_meas)
        self.delta_spencer = delta_spencer(self.day_of_year, self.daily_meas)

        counter = 0
        cols = ['declination method']
        if self.lat_precalc is None:
            cols.append('latitude')
        if self.tilt_precalc is None:
            cols.append('tilt')
        if self.azim_precalc is None:
            cols.append('azimuth')
        self.results = pd.DataFrame(columns=cols)

        for delta_id in delta_method:
            if delta_id in ('Cooper', 'cooper'):
                delta = self.delta_cooper
            if delta_id in ('Spencer', 'spencer'):
                delta = self.delta_spencer
            for day_range_id in self.day_range_dict:
                day_interval = self.day_range_dict[day_range_id]
                self.get_day_range(day_interval)
                delta_f = delta[self.boolean_daytime_range]
                omega_f = self.omega[self.boolean_daytime_range]

                if ~np.any(self.boolean_daytime_range):
                    print('No data made it through selected day_range filter')

                self.delta_f = delta_f
                self.omega_f = omega_f
                self.delta = delta

                func_customized, bounds, init_values, dict_keys = select_function(self.lat_precalc, self.tilt_precalc,
                                                                                  self.azim_precalc)
                if self.init_values is not None:
                    init_values = self.init_values

                estimates = run_curve_fit(func=func_customized, delta=delta_f, omega=omega_f,
                                          costheta=self.costheta_fit, boolean_daytime_range=self.boolean_daytime_range,
                                          init_values=init_values, fit_bounds=bounds)

                estimates_dict = dict(zip(dict_keys, estimates))
                print(estimates_dict)

                self.costheta_estimated = calculate_costheta(func=func_costheta, delta_sys=delta, omega_sys=self.omega,
                                                             lat=self.lat_precalc,
                                                             tilt=self.tilt_precalc,
                                                             azim=self.azim_precalc, est_dict=estimates_dict,
                                                             ground_truth=False)

                self.costheta_ground_truth = calculate_costheta(func=func_costheta, delta_sys=delta,
                                                                omega_sys=self.omega, lat=self.phi_true_value,
                                                                tilt=self.beta_true_value, azim=self.gamma_true_value,
                                                                ground_truth=True)

                self.results.loc[counter] = [delta_id] + list(estimates)
                counter += 1
        return

    def get_day_range(self, interval):
        if interval is not None:
            day_range = (self.day_of_year > interval[0]) & (self.day_of_year < interval[1])
        else:
            day_range = np.ones(self.day_of_year.shape, dtype=bool)
        self.boolean_daytime_range = self.boolean_daytime * self.clear_index * day_range
        return

    def find_boolean_daytime(self):
        if self.daytime_threshold is None:
            self.daytime_threshold_fit = self.find_daytime_threshold_quantile_seasonality()
            self.boolean_daytime = self.data_matrix > self.daytime_threshold_fit
        else:
            self.boolean_daytime = find_daytime(self.data_matrix, self.daytime_threshold)
        return

    def find_daytime_threshold_quantile_seasonality(self):
        m = cvx.Parameter(nonneg=True, value=10 ** 6)
        t = cvx.Parameter(nonneg=True, value=0.9)
        y = np.quantile(self.data_matrix, .9, axis=0)
        x1 = cvx.Variable(len(y))
        x2 = cvx.Variable(len(y))
        if self.data_matrix.shape[1] > 365:
            constraints = [
                x2[365:] == x2[:-365], x1 + x2 == y
            ]
        else:
            constraints = [x1 + x2 == y]
        c1 = cvx.sum(1 / 2 * cvx.abs(x1) + (t - 1 / 2) * x1)
        c2 = cvx.sum_squares(cvx.diff(x2, 2))
        objective = cvx.Minimize(c1 + m * c2)
        prob = cvx.Problem(objective, constraints=constraints)
        prob.solve(solver='MOSEK')
        return x2.value

