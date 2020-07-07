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
from pvsystemprofiler.utilities.hour_angle_equation import find_omega
from pvsystemprofiler.utilities.declination_equation import delta_spencer
from pvsystemprofiler.utilities.declination_equation import delta_cooper
class TiltAzimuthStudy():
    def __init__(self, data_handler, day_range=None, init_values=None, daytime_threshold=None, lon_estimate=None,
                 lat_estimate=None, lat_true_value=None, tilt_true_value=None, azimuth_true_value=None, gmt_offset=-8):
        """
        :param data_handler: `DataHandler` class instance loaded with a solar power data set
        :param day_range: (optional) the desired day range to run the study. An array of the form
                              [first day, last day]
        :param init_values: (optional) Tilt and Azimuth guess values for numerical fit.
                                Default values are [10, 10]. (Degrees).
                                are used otherwise
        :param daytime_threshold: (optional) daytime threshold
        :param lat_estimate: latitude estimate as obtained from the Latitude Study module. (Degrees).
        :param lon_estimate: latitude estimate as obtained from the Latitude Study module. (Degrees).
        :param lat_true_value: (optional) ground truth value for the system's Latitude. (Degrees).
        :param tilt_true_value: (optional) ground truth value for the system's Tilt. (Degrees).
        :param azimuth_true_value: (optional) ground truth value for the system's Azimuth. (Degrees)
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
        if init_values is None:
            self.init_values = [10, 10]
        else:
            self.init_values = init_values
        self.daytime_threshold = daytime_threshold
        self.lon_estimate = lon_estimate
        self.daytime_threshold_fit = None
        self.latitude_estimate = lat_estimate
        self.phi_true_value = lat_true_value
        self.beta_true_value = tilt_true_value
        self.gamma_true_value = azimuth_true_value
        self.gmt_offset = gmt_offset
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
        self.omega = find_omega(self.data_sampling, self.num_days, self.lon_estimate, self.day_of_year,
                                self.gmt_offset)

        self.scale_factor_costheta, self.costheta_fit = self.find_fit_costheta(self.data_matrix, self.clear_index)
        self.delta_cooper = delta_cooper(self.day_of_year, self.daily_meas)
        self.delta_spencer = delta_spencer(self.day_of_year, self.daily_meas)

        counter = 0
        self.results = pd.DataFrame(columns=['tilt residual', 'azimuth residual', 'day range', 'declination method'])
        #self.results_uncoupled = pd.DataFrame(columns=['tilt Residual', 'azimuth residual', 'day_range', 'declination method'])
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

                func_cust = lambda x, beta, gamma: self.func(x, np.deg2rad(self.latitude_estimate), beta, gamma)

                tilt_estimate, azimuth_estimate = self.run_curve_fit(func=func_cust, delta=delta_f, omega=omega_f,
                                                                     costheta=self.costheta_fit,
                                                                     boolean_daytime_range=self.boolean_daytime_range,
                                                                     init_values=self.init_values)

                self.costheta_estimated = self.calculate_costheta(func=self.func, delta_sys=delta, omega_sys=self.omega,
                                                                  latitude_sys=self.latitude_estimate,
                                                                  tilt_sys=tilt_estimate, azimuth_sys=azimuth_estimate)

                # if self.phi_true_value is not None:
                #     if self.beta_true_value is not None:
                #         if self.gamma_true_value is not None:
                #             self.costheta_ground_truth = self.calculate_costheta(delta, self.omega, self.phi_true_value,
                #                                                                  self.beta_true_value,
                #                                                                  self.gamma_true_value)
                #             r1 = self.beta_true_value - tilt_estimate
                #             r2 = self.gamma_true_value - azimuth_estimate
                #             r3 = day_range_id
                #             r4 = delta_id
                #             self.results.loc[counter] = [r1, r2, r3, r4]
                #counter += 1
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

    def find_fit_costheta(self, data_matrix, clear_index):
        data = np.max(data_matrix, axis=0)
        s1 = cvx.Variable(len(data))
        s2 = cvx.Variable(len(data))
        cost = 1e1 * cvx.norm(cvx.diff(s1, k=2), p=2) + cvx.norm(s2[clear_index])
        objective = cvx.Minimize(cost)
        constraints = [
            data == s1 + s2,
            s1[365:] == s1[:-365]
        ]
        problem = cvx.Problem(objective, constraints)
        problem.solve(solver='MOSEK')
        scale_factor_costheta = s1.value
        costheta_fit = self.data_matrix / np.max(s1.value)
        return scale_factor_costheta, costheta_fit


    def calculate_costheta(self, func, delta_sys, omega_sys, latitude_sys, tilt_sys, azimuth_sys):
        x = np.array([delta_sys, omega_sys])
        phi = np.deg2rad(latitude_sys)
        beta = np.deg2rad(tilt_sys)
        gamma = np.deg2rad(azimuth_sys)
        costheta = func(x, phi, beta, gamma)
        return costheta

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

    def run_curve_fit(self, func, delta, omega, costheta, boolean_daytime_range, init_values):
        costheta_fit = costheta[boolean_daytime_range]
        x = np.array([delta, omega])
        popt, pcov = curve_fit(func, x, costheta_fit, p0=np.deg2rad(init_values),
                               bounds=([0, -3.14], [1.57, 3.14]))
        tilt_estimate, azimuth_estimate = np.degrees(popt)
        return tilt_estimate, azimuth_estimate

    def func(self, x, phi, beta, gamma):
        """The function cos(theta) is  calculated using equation (1.6.2) in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991."""
        delta = x[0]
        omega = x[1]

        a = np.sin(delta) * np.sin(phi) * np.cos(beta)
        b = np.sin(delta) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        c = np.cos(delta) * np.cos(phi) * np.cos(beta) * np.cos(omega)
        d = np.cos(delta) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(omega)
        e = np.cos(delta) * np.sin(beta) * np.sin(gamma) * np.sin(omega)
        return a - b + c + d + e