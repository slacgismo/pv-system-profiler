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
    def __init__(self, data_handler, day_range=None, init_values=None, daytime_threshold=None, lat_estimate=None,
                 lat_true_value=None, tilt_true_value=None, azimuth_true_value=None):
        """
        :param data_handler: `DataHandler` class instance loaded with a solar power data set
        :param day_range: (optional) the desired day range to run the study. An array of the form
                              [first day, last day]
        :param init_values: (optional) Tilt and Azimuth guess values for numerical fit.
                                Default values are [10, 10]. (Degrees).
                                are used otherwise
        :param daytime_threshold: (optional) daytime threshold
        :param lat_estimate: latitude estimate as obtained from the Latitude Study module. (Degrees).
        :param lat_true_value: (optional) ground truth value for the system's Latitude. (Degrees).
        :param tilt_true_value: (optional) ground truth value for the system's Tilt. (Degrees).
        :param azimuth_true_value: (optional) ground truth value for the system's Azimuth. (Degrees)
        """

        self.data_handler = data_handler
        self.day_range = day_range
        self.data_matrix = self.data_handler.filled_data_matrix
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.init_values = init_values
        if init_values is None:
            self.init_values = [10, 10]
        else:
            self.init_values = init_values


        self.daytime_threshold = daytime_threshold
        self.daytime_threshold_fit = None
        self.latitude_estimate = lat_estimate
        self.phi_true_value = lat_true_value
        self.beta_true_value = tilt_true_value
        self.gamma_true_value = azimuth_true_value
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
        self.costheta_ground_truth = None
        self.costheta_fit = None
        self.costheta_fit_f = None
        self.boolean_daytime_range = None
        self.omega_f = None
        self.delta_f = None
        if self.day_range:
            self.day_range = (self.day_of_year > self.day_range[0]) & \
                             (self.day_of_year < self.day_range[1])
        else:
            self.day_range = np.ones(self.day_of_year.shape, dtype=bool)
        self.results = None
        self.results_uncoupled = None
        self.tilt_estimate_uncoupled = None
        self.azimuth_estimate_uncoupled = None

    def run(self):
        self.find_boolean_daytime()
        self.make_delta()
        self.make_omega()
        self.find_fit_costheta()
        self.select_days()
        if ~np.any(self.boolean_daytime_range):
            print('Data in selected day_range does not meet requirements for find tilt and azimuth estimation.\n'
                  'Please increase or shift the day range')
            return
        self.run_curve_fit()
        try:
            self.run_curve_fit_tilt_only()
        except RuntimeError:
            self.tilt_estimate_uncoupled = np.nan
        try:
            self.run_curve_fit_azimuth_only()
        except RuntimeError:
           self.azimuth_estimate_uncoupled = np.nan

        self.estimate_costheta()
        if self.phi_true_value is not None:
            if self.beta_true_value is not None:
                if self.gamma_true_value is not None:
                    self.ground_truth_costheta()
                    self.results = pd.DataFrame(columns=['Latitude Residual', 'Tilt Residual',
                                                 'Azimuth Residual'])
                    r1 = self.phi_true_value - self.latitude_estimate
                    r2 = self.beta_true_value - self.tilt_estimate
                    r3 = self.gamma_true_value - self.azimuth_estimate
                    self.results.loc[0] = [r1, r2, r3]

                    # uncoupled tilt and azimuth results
                    self.results_uncoupled = pd.DataFrame(columns=['Latitude Residual', 'Tilt Residual',
                                                 'Azimuth Residual'])
                    r1 = self.phi_true_value - self.latitude_estimate
                    r2 = self.beta_true_value - self.tilt_estimate_uncoupled
                    r3 = self.gamma_true_value - self.azimuth_estimate_uncoupled
                    self.results_uncoupled.loc[0] = [r1, r2, r3]

        return

    def find_boolean_daytime(self):
        if self.daytime_threshold is None:
            self.daytime_threshold_fit = self.find_daytime_threshold_quantile_seasonality()
            self.boolean_daytime = self.data_matrix > self.daytime_threshold_fit
        else:
            self.boolean_daytime = find_daytime(self.data_matrix, self.daytime_threshold)

    def make_delta(self):
        """Delta is estimated  using equation (1.6.1a) in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991."""
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.day_of_year) / 365)))
        self.delta = np.tile(delta_1, (self.daily_meas, 1))
        return

    def make_omega(self):
        """Omega is estimated  as in example (1.6.1) in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991."""
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
        problem.solve(solver='MOSEK')
        self.scale_factor_costheta = s1.value
        self.costheta_fit = self.data_matrix / np.max(s1.value)
        return

    def ground_truth_costheta(self):
        phi_true_value_2d = np.tile(np.deg2rad(self.phi_true_value),
                                    (self.daily_meas, self.num_days))
        beta_true_value_2d = np.tile(np.deg2rad(self.beta_true_value),
                                     (self.daily_meas, self.num_days))
        gamma_true_value_2d = np.tile(np.deg2rad(self.gamma_true_value),
                                      (self.daily_meas, self.num_days))
        X = np.array([self.omega, self.delta, phi_true_value_2d])
        self.costheta_ground_truth = \
            self.func(X, beta_true_value_2d, gamma_true_value_2d)
        return

    def estimate_costheta(self):
        phi_estimate_2d = np.tile(np.deg2rad(self.latitude_estimate), (self.daily_meas, self.num_days))
        beta_estimate_2d = np.tile(np.deg2rad(self.tilt_estimate), (self.daily_meas, self.num_days))
        gamma_estimate_2d = np.tile(np.deg2rad(self.azimuth_estimate), (self.daily_meas, self.num_days))
        X = np.array([self.omega, self.delta, phi_estimate_2d])
        self.costheta_estimated = self.func(X, beta_estimate_2d, gamma_estimate_2d)
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

    def select_days(self):
        if self.scsf:
            self.boolean_daytime_range = self.boolean_daytime * self.day_range
        else:
            self.boolean_daytime_range = self.boolean_daytime * self.clear_index * self.day_range
            self.delta_f = self.delta[self.boolean_daytime_range]
            self.omega_f = self.omega[self.boolean_daytime_range]

    def run_curve_fit(self, bootstrap_iterations=None):
        self.costheta_fit_f = self.costheta_fit[self.boolean_daytime_range]
        phi = np.deg2rad(self.latitude_estimate)
        phi_estimate_f = np.tile(phi, len(self.omega_f))
        x = np.array([self.omega_f, self.delta_f, phi_estimate_f])
        popt, pcov = curve_fit(self.func, x, self.costheta_fit_f, p0=np.deg2rad(self.init_values),
                               bounds=([0, -3.14], [1.57, 3.14]))
        self.tilt_estimate, self.azimuth_estimate = np.degrees(popt)
        return

    def run_curve_fit_tilt_only(self, bootstrap_iterations=None):
        self.costheta_fit_f = self.costheta_fit[self.boolean_daytime_range]
        phi = np.deg2rad(self.latitude_estimate)
        phi_estimate_f = np.tile(phi, len(self.omega_f))
        x = np.array([self.omega_f, self.delta_f, phi_estimate_f])
        popt, pcov = curve_fit(self.func_tilt, x, self.costheta_fit_f, p0=np.deg2rad(self.init_values[0]),
                               bounds=([0, 1.57]))
        self.tilt_estimate_uncoupled = np.degrees(popt)[0]
        return

    def run_curve_fit_azimuth_only(self, bootstrap_iterations=None):
        self.costheta_fit_f = self.costheta_fit[self.boolean_daytime_range]
        phi = np.deg2rad(self.latitude_estimate)
        phi_estimate_f = np.tile(phi, len(self.omega_f))
        x = np.array([self.omega_f, self.delta_f, phi_estimate_f])
        popt, pcov = curve_fit(self.func_azimuth, x, self.costheta_fit_f, p0=np.deg2rad(self.init_values[1]),
                                bounds=([-3.14, 3.14]))
        self.azimuth_estimate_uncoupled = np.degrees(popt)[0]
        return


    def func(self, x, beta, gamma):
        """The function cos(theta) is  calculated using equation (1.6.2) in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991."""
        omega = x[0]
        delta = x[1]
        phi = x[2]

        a = np.sin(delta) * np.sin(phi) * np.cos(beta)
        b = np.sin(delta) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        c = np.cos(delta) * np.cos(phi) * np.cos(beta) * np.cos(omega)
        d = np.cos(delta) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(omega)
        e = np.cos(delta) * np.sin(beta) * np.sin(gamma) * np.sin(omega)
        return a - b + c + d + e

    def func_tilt(self, x, beta):
        """The function cos(theta) is  calculated using equation (1.6.2) in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991."""
        omega = x[0]
        delta = x[1]
        phi = x[2]
        gamma = np.deg2rad(self.gamma_true_value)
        a = np.sin(delta) * np.sin(phi) * np.cos(beta)
        b = np.sin(delta) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        c = np.cos(delta) * np.cos(phi) * np.cos(beta) * np.cos(omega)
        d = np.cos(delta) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(omega)
        e = np.cos(delta) * np.sin(beta) * np.sin(gamma) * np.sin(omega)
        return a - b + c + d + e

    def func_azimuth(self, x, gamma):
        """The function cos(theta) is  calculated using equation (1.6.2) in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991."""
        omega = x[0]
        delta = x[1]
        phi = x[2]
        beta = np.deg2rad(self.beta_true_value)
        a = np.sin(delta) * np.sin(phi) * np.cos(beta)
        b = np.sin(delta) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        c = np.cos(delta) * np.cos(phi) * np.cos(beta) * np.cos(omega)
        d = np.cos(delta) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(omega)
        e = np.cos(delta) * np.sin(beta) * np.sin(gamma) * np.sin(omega)
        return a - b + c + d + e
