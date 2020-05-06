''' Tilt and Azimuth Study Module

'''
import numpy as np
import pandas as pd
import cvxpy as cvx
from scipy.optimize import curve_fit
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon
from pvsystemprofiler.utilities.equation_of_time import eot_haghdadi, eot_duffie
from pvsystemprofiler.utilities.progress import progress
from solardatatools.daytime import find_daytime
class TiltAzimuthStudy():
    def __init__(self, data_handler, gmt_offset=-8, summer_flag=False,
                 init_values=None, daytime_threshold=None, lat_estimate=None, lat_true_value=None, tilt_true_value=None,
                 azim_true_value=None):
        self.data_handler = data_handler
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.data_matrix = self.data_handler.filled_data_matrix
        self.gmt_offset = gmt_offset
        self.summer_flag = summer_flag
        self.init_values = init_values
        self.daytime_threshold = daytime_threshold
        self.latitude_estimate = lat_estimate
        self.phi_real = lat_true_value
        self.ground_beta = tilt_true_value
        self.ground_gamma = azim_true_value
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.solarnoon = None
        self.days = None
        self.scsf = data_handler.scsf

        self.init_values = init_values
        self.power_threshold_fit = None

        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.data_sampling = self.data_handler.data_sampling
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.clear_index_set = self.data_handler.daily_flags.clear

        if self.daytime_threshold is None:
            self.boolean_daytime = np.empty([self.daily_meas, self.num_days], dtype=bool)
        else:
            self.boolean_daytime = find_daytime(self.data_matrix, self.daytime_threshold)


        if self.summer_flag:
            # self.slct_summer = (self.day_of_year>152) & (self.day_of_year<245) #summer only
            # self.slct_summer = (self.day_of_year>60) & (self.day_of_year<335) #no winter
            # self.slct_summer = (self.day_of_year>60) & (self.day_of_year<153) #spring only
            self.slct_summer = (self.day_of_year > 85) & (self.day_of_year < 167)  # manual set only
        else:
            self.slct_summer = np.ones(self.day_of_year.shape, dtype=bool)

        self._calculate = None
        self.hours_daylight = None
        self.delta = None
        self.omega = None
        self.costheta_fit = None
        self.latitude_calculate = None
        self.tilt_estimate = None
        self.azimuth_estimate = None
        self.costheta_ground_truth_calculate = None
        self.scale_factor_costheta = None
        self.hours_daylight = None
        self.costheta_estimated = None
        self.costheta_mean_absolute_error = None
        self.costheta_mean_squared_error = None
        self.latitude_error = None
        self.tilt_error = None
        self.azimuth_error = None
        self.cost = None
        self.costheta_fit_f = None

    def run(self):
        self.make_delta()
        self.make_omega()
        self.find_fit_costheta()
        self.ground_truth_costheta()
        if self.daytime_threshold is None:
           self.power_threshold_fit = self.find_power_threshold_quantile_seasonality()
           for d in range(0, self.num_days - 1):
               self.boolean_daytime[:, d] = self.data_matrix[:, d] > 1 * self.power_threshold_fit[d]
        self.select_days()
        self.run_curve_fit_1()
        self.estimate_costheta()

        return

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.day_of_year) / 365)))
        self.delta = np.tile(delta_1, (self.data_matrix.shape[0], 1))
        return

    def make_omega(self):
        hour = np.arange(0, 24, self.data_sampling / 60)
        omega_1 = np.deg2rad(15 * (hour - 12))
        self.omega = np.tile(omega_1.reshape(-1, 1), (1, self.data_matrix.shape[1]))
        return

    def find_fit_costheta(self):
        data = np.max(self.data_matrix, axis=0)
        s1 = cvx.Variable(len(data))
        s2 = cvx.Variable(len(data))
        cost = 1e1 * cvx.norm(cvx.diff(s1, k=2), p=2) + cvx.norm(s2[self.clear_index_set])
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
        phi_real_2d = np.tile(self.phi_real,
                              (self.daily_meas, self.num_days))
        ground_beta_2d = np.tile(self.ground_beta,
                                 (self.daily_meas, self.num_days))
        ground_gamma_2d = np.tile(self.ground_gamma,
                                  (self.daily_meas, self.num_days))
        self.costheta_ground_truth_calculate = \
            self.func2(X, phi_real_2d, ground_beta_2d, ground_gamma_2d)
        return

    def estimate_costheta(self):
        X = np.array([self.omega, self.delta])
        phi_estimate_2d = np.tile(np.deg2rad(self.latitude_estimate),(self.daily_meas,self.num_days))
        beta_estimate_2d = np.tile(np.deg2rad(self.tilt_estimate),(self.daily_meas,self.num_days))
        gamma_estimate_2d = np.tile(np.deg2rad(self.azimuth_estimate),(self.daily_meas,self.num_days))
        self.costheta_estimated = self.func2(X,phi_estimate_2d, beta_estimate_2d, gamma_estimate_2d)
        return

    def func2(self, X, phi, beta, gamma):
        w = X[0]
        d = X[1]
        A = np.sin(d) * np.sin(phi) * np.cos(beta)
        B = np.sin(d) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        C = np.cos(d) * np.cos(phi) * np.cos(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E

    def find_power_threshold_quantile_seasonality(self):
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
            self.slct_curve_fit = self.boolean_daytime * self.slct_summer
        else:
            self.slct_curve_fit = self.boolean_daytime * self.clear_index_set * self.slct_summer
            self.delta_f = self.delta[self.slct_curve_fit]
            self.omega_f = self.omega[self.slct_curve_fit]


    def run_curve_fit_1(self, bootstrap_iterations=None):
        self.costheta_fit_f = self.costheta_fit[self.slct_curve_fit]
        X = np.array([self.omega_f, self.delta_f])
        popt, pcov = curve_fit(self.func, X, self.costheta_fit_f, p0=np.deg2rad(self.init_values), bounds=([0,-3.14],[1.57,3.14]))
        self.tilt_estimate, self.azimuth_estimate = np.degrees(popt)
        return

    def func(self, X, beta, gamma):
        w = X[0]
        d = X[1]
        phi = np.deg2rad(self.latitude_estimate)
        A = np.sin(d) * np.sin(phi) * np.cos(beta)
        B = np.sin(d) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        C = np.cos(d) * np.cos(phi) * np.cos(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E