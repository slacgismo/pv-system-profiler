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
from scipy.optimize import curve_fit
from solardatatools.daytime import find_daytime
class TiltAzimuthStudy():
    def __init__(self, data_handler, set_day_range=None, day_range=None, init_values=[10, 10],
                 daytime_threshold=None, lat_estimate=None,
                 lat_true_value=None, tilt_true_value=None,
                 azimuth_true_value=None):
        '''
        :param data_handler: `DataHandler` class instance loaded with a solar power data set
        :param set_day_range: (optional) True if running the study over a day range
        :param day_range: (optional) the desired day range to run the study. An array of the form
                              [first day, last day]
        :param init_values: (optional) Initial values for numerical fit. Default values of [10, 10]
                                are used otherwise
        :param daytime_threshold: daytime threshold
        :param lat_estimate: latitude estimate as obtained from the Latitude Study module
        :param lat_true_value: (optional) the ground truth value for the system's latitude
        :param tilt_true_value: (optional) the ground truth value for the system's tilt
        :param azimuth_true_value: (optional) the ground truth value for the system's azimuth
        '''

        self.data_handler = data_handler
        self.set_day_range = set_day_range
        self.day_range = day_range
        self.data_matrix = self.data_handler.filled_data_matrix
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.init_values = init_values
        self.daytime_threshold = daytime_threshold
        self.daytime_threshold_fit = None
        self.latitude_estimate = lat_estimate
        self.phi_true_value = None
        self.beta_true_value = None
        self.gamma_true_value = None
        if lat_true_value is not None:
            self.phi_true_value = np.deg2rad(lat_true_value)
        if tilt_true_value is not None:
            self.beta_true_value = np.deg2rad(tilt_true_value)
        if azimuth_true_value is not None:
            self.gamma_true_value = np.deg2rad(azimuth_true_value)
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.data_sampling = self.data_handler.data_sampling
        self.scsf = data_handler.scsf
        self.clear_index = data_handler.daily_flags.clear
        self.boolean_daytime = None
        self.delta = None
        self.omega = None
        self.tilt_estimate = None
        self.azimuth_estimate = None
        self.scale_factor_costheta = None
        self.costheta_estimated = None
        self.costheta_ground_truth_calculate = None
        self.costheta_fit = None
        self.costheta_fit_f = None
        self.boolean_daytime_range = None
        self.omega_f = None
        self.delta_f = None
        self.results = None
        if self.set_day_range:
            try:
                self.day_range = (self.day_of_year > self.day_range[0]) & \
                                 (self.day_of_year < self.day_range[1])
            except TypeError:
                print('select_day_range flag is set to True but no day range was provided.\n'
                      'Please provide day range or set select_day_range flag=False')
        else:
            self.day_range = np.ones(self.day_of_year.shape, dtype=bool)

    def run(self):
        self.find_boolean_daytime()
        self.make_delta()
        self.make_omega()
        self.find_fit_costheta()

        self.select_days()
        self.run_curve_fit_1()
        self.estimate_costheta()
        if self.phi_true_value is not None:
            if self.beta_true_value is not None:
                if self.gamma_true_value is not None:
                    self.ground_truth_costheta()
                    self.results = pd.DataFrame(columns=['Latitude Residual',
                                                         'Tilt Residual',
                                                         'Azimuth Residual'])
                    r1 = np.rad2deg(self.phi_true_value) - self.latitude_estimate
                    r2 = np.rad2deg(self.beta_true_value) - self.tilt_estimate
                    r3 = np.rad2deg(self.gamma_true_value) - self.azimuth_estimate
                    self.results.loc[0] = [r1, r2, r3]
        return

    def find_boolean_daytime(self):
        if self.daytime_threshold is None:
            self.boolean_daytime = np.zeros([self.daily_meas, self.num_days], dtype=bool)
            self.daytime_threshold_fit = self.find_daytime_threshold_quantile_seasonality()
            for d in range(0, self.num_days - 1):
                self.boolean_daytime[:, d] = self.data_matrix[:, d] > 1 * self.daytime_threshold_fit[d]
        else:
            self.boolean_daytime = find_daytime(self.data_matrix, self.daytime_threshold)

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.day_of_year) / 365)))
        self.delta = np.tile(delta_1, (self.daily_meas, 1))
        return

    def make_omega(self):
        hour = np.arange(0, 24, self.data_sampling / 60)
        omega_1 = np.deg2rad(15 * (hour - 12))
        self.omega = np.tile(omega_1.reshape(-1, 1), (1, self.num_days))
        return

    def find_fit_costheta(self):
        data = np.max(self.data_matrix, axis=0)
        s1 = cvx.Variable(len(data))
        s2 = cvx.Variable(len(data))
        cost = 1e1 * cvx.norm(cvx.diff(s1, k=2), p=2) + cvx.norm(s2[self.clear_index])
        objective = cvx.Minimize(cost)
        constraints = [
            data == s1 + s2,
            s1[365:] == s1[:-365]
        ]
        problem = cvx.Problem(objective, constraints)
        problem.solve(solver='MOSEK');
        self.scale_factor_costheta = s1.value
        self.costheta_fit = self.data_matrix / np.max(s1.value)
        return

    def ground_truth_costheta(self):
        X = np.array([self.omega, self.delta])
        phi_true_value_2d = np.tile(self.phi_true_value,
                              (self.daily_meas, self.num_days))
        beta_true_value_2d = np.tile(self.beta_true_value,
                                 (self.daily_meas, self.num_days))
        gamma_true_value_2d = np.tile(self.gamma_true_value,
                                  (self.daily_meas, self.num_days))
        self.costheta_ground_truth_calculate = \
            self.func2(X, phi_true_value_2d, beta_true_value_2d, gamma_true_value_2d)
        return

    def estimate_costheta(self):
        X = np.array([self.omega, self.delta])
        phi_estimate_2d = np.tile(np.deg2rad(self.latitude_estimate), (self.daily_meas, self.num_days))
        beta_estimate_2d = np.tile(np.deg2rad(self.tilt_estimate), (self.daily_meas, self.num_days))
        gamma_estimate_2d = np.tile(np.deg2rad(self.azimuth_estimate), (self.daily_meas, self.num_days))
        self.costheta_estimated = self.func2(X, phi_estimate_2d, beta_estimate_2d, gamma_estimate_2d)
        return

    def func2(self, x, phi, beta, gamma):
        w = x[0]
        d = x[1]
        A = np.sin(d) * np.sin(phi) * np.cos(beta)
        B = np.sin(d) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        C = np.cos(d) * np.cos(phi) * np.cos(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E

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
            constraints = []
        c1 = cvx.sum(1 / 2 * cvx.abs(x1) + (t - 1 / 2) * x1)
        c2 = cvx.sum_squares(cvx.diff(x2, 2))
        objective = cvx.Minimize(c1 + m * c2)
        prob = cvx.Problem(objective, constraints=constraints)
        prob.solve(solver='MOSEK')
        return x2.value

    def select_days(self):
        if self.scsf:
            self.boolean_daytime_range = self.boolean_daytime * self.day_range
        else:
            self.boolean_daytime_range = self.boolean_daytime * self.clear_index * self.day_range
            self.delta_f = self.delta[self.boolean_daytime_range]
            self.omega_f = self.omega[self.boolean_daytime_range]

    def run_curve_fit_1(self, bootstrap_iterations=None):
        self.costheta_fit_f = self.costheta_fit[self.boolean_daytime_range]
        X = np.array([self.omega_f, self.delta_f])
        popt, pcov = curve_fit(self.func, X, self.costheta_fit_f, p0=np.deg2rad(self.init_values),
                               bounds=([0, -3.14], [1.57, 3.14]))
        self.tilt_estimate, self.azimuth_estimate = np.degrees(popt)
        return
    def func(self, x, beta, gamma):
        w = x[0]
        d = x[1]
        phi = np.deg2rad(self.latitude_estimate)
        A = np.sin(d) * np.sin(phi) * np.cos(beta)
        B = np.sin(d) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        C = np.cos(d) * np.cos(phi) * np.cos(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E
