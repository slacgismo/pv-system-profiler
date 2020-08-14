''' System Parameter (Lat, tilt and azimuth) Estimation Module
This module contains a class for estimating latitude, tilt and azimuth from power signal outputs
'''
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import cvxpy as cvx
from solardatatools import standardize_time_axis, make_2d, plot_2d, load_pvo_data
from solardatatools import DataHandler
import boto3
import sys
from os.path import expanduser
home = expanduser('~')
from scipy.optimize import curve_fit
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from importlib.machinery import SourceFileLoader

class ModelEstimator():
    def __init__(self, data_matrix=None, doy_list=None, phi_real=None, ground_beta=None, ground_gamma=None, SCSF_flag=None, summer_flag=None, init_values = None, fixed_power_thr = None, fix_param = 0.8):
        self.data_matrix = data_matrix
        self.doy_list = doy_list
        #self.end_date = end_date
        self.phi_real = phi_real
        self.ground_beta = ground_beta
        self.ground_gamma = ground_gamma
        self.SCSF_flag = SCSF_flag
        self.summer_flag = summer_flag
        self.init_values = init_values
        self.fixed_power_thr = fixed_power_thr
        self.fix_param = fix_param
        self.power_threshold_fit = None
        if self.data_matrix is not None:
            self.num_days = self.data_matrix.shape[1]
            self.data_sampling_daily = self.data_matrix.shape[0]
            #self.doy_list = pd.date_range(self.start_date, self.end_date).dayofyear.values[1:-2]
            self.data_sampling = 24 * 60 / self.data_matrix.shape[0]
            self.boolean_daylight_lat = self.data_matrix > 0.001 * np.percentile(self.data_matrix, 95)
            if self.fixed_power_thr == None:
                self.boolean_daylight = np.zeros([self.data_sampling_daily, self.num_days], dtype=bool)
            else:
                self.boolean_daylight = self.data_matrix > self.fix_param * np.percentile(self.data_matrix, 95)
        else:
            self.num_days = None
            self.data_sampling = None
        if self.summer_flag == True:
            self.slct_summer = (self.doy_list>152) & (self.doy_list<245) #summer only
            self.slct_summer = (self.doy_list>60) & (self.doy_list<335) #no winter
            self.slct_summer = (self.doy_list>60) & (self.doy_list<153) #spring only
            self.slct_summer = (self.doy_list>85) & (self.doy_list<167) #manual set only
        else:
            self.slct_summer = np.ones(self.doy_list.shape, dtype=bool)
        self.clear_index_set = None
        self.delta = None
        self.omega = None
        self.costheta_fit = None
        self.latitude_estimate = None
        self.latitude_calculate = None
        self.tilt_estimate = None
        self.azimuth_estimate = None
        self.costheta_ground_truth_calculate = None
        self.scale_factor_costheta = None
        self.hours_daylight = None
        self.costheta_ = None
        self.costheta_mean_absolute_error = None
        self.costheta_mean_squared_error = None
        self.latitude_error = None
        self.tilt_error = None
        self.azimuth_error = None
        self.cost = None
        self.costheta_fit_f = None

    def run_preprocessing(self):
        self.make_delta()
        self.make_omega()
        self.calculate_latitude()
        self.flag_clear_days()
        self.find_fit_costheta()
        self.ground_truth_costheta()
        if self.fixed_power_thr is None:
           self.power_threshold_fit = self.find_power_threshold_quantile_seasonality()
           for d in range(0,self.num_days-1):
               self.boolean_daylight[:,d] = self.data_matrix[:,d] > 1 * self.power_threshold_fit[d]
        self.select_days()
        return

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.doy_list) / 365)))
        self.delta = np.tile(delta_1, (self.data_matrix.shape[0], 1))
        return

    def make_omega(self):
        hour = np.arange(0, 24, self.data_sampling / 60)
        omega_1 = np.deg2rad(15 * (hour - 12))
        self.omega = np.tile(omega_1.reshape(-1, 1), (1, self.data_matrix.shape[1]))
        return

    def calculate_latitude(self):
        self.hours_daylight = (np.sum(self.boolean_daylight_lat, axis=0))*self.data_sampling/60
        self.latitude_calculate = np.degrees(np.arctan(- np.cos(np.radians(15/2*self.hours_daylight))/(np.tan(self.delta[0]))))
        self.latitude_estimate = np.median(self.latitude_calculate)
        return

    def flag_clear_days(self):
        if self.SCSF_flag is not None:
            self.clear_index_set = np.s_[:]
        else:
            dh = DataHandler(raw_data_matrix=self.data_matrix)
            dh.run_pipeline(power_col = 0, verbose=False)
            self.clear_index_set = dh.daily_flags.clear
        return

    def select_days(self):
        if self.SCSF_flag is not None:
            self.slct_curve_fit = self.boolean_daylight * self.slct_summer
        else:
            self.slct_curve_fit = self.boolean_daylight * self.clear_index_set * self.slct_summer
            self.delta_f = self.delta[self.slct_curve_fit]
            self.omega_f = self.omega[self.slct_curve_fit]


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

    def func2(self, X, phi, beta, gamma):
        w = X[0]
        d = X[1]
        A = np.sin(d) * np.sin(phi) * np.cos(beta)
        B = np.sin(d) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        C = np.cos(d) * np.cos(phi) * np.cos(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E

    def jacbeta(self, X, beta, gamma):
        w = X[0]
        d = X[1]
        phi = np.deg2rad(np.median(self.latitude_calculate))
        A = -np.sin(d) * np.sin(phi) * np.sin(beta)
        B = np.sin(d) * np.cos(phi) * np.cos(beta) * np.cos(gamma)
        C = -np.cos(d) * np.cos(phi) * np.sin(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.cos(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.cos(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E

    def jacgamma(self, X, beta, gamma):
        w = X[0]
        d = X[1]
        phi = np.deg2rad(np.median(self.latitude_calculate))
        A = 0
        B = -np.sin(d) * np.cos(phi) * np.sin(beta) * np.sin(gamma)
        C = 0
        D = -np.cos(d) * np.sin(phi) * np.sin(beta) * np.sin(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.cos(gamma) * np.sin(w)
        return A - B + C + D + E

    def jac_matrix(self, X, beta, gamma):
        return np.array([self.jacbeta(X, beta, gamma), self.jacgamma(X, beta, gamma)]).T

    def run_curve_fit_1(self, bootstrap_iterations=None):
        self.costheta_fit_f = self.costheta_fit[self.slct_curve_fit]
        X = np.array([self.omega_f, self.delta_f])
        popt, pcov = curve_fit(self.func, X, self.costheta_fit_f, p0=np.deg2rad(self.init_values), bounds=([0,-3.14],[1.57,3.14]))
        self.tilt_estimate, self.azimuth_estimate = np.degrees(popt)
        return

    def ground_truth_costheta(self):
        X = np.array([self.omega, self.delta])
        phi_real_2d = np.tile(self.phi_real,(self.data_sampling_daily,self.num_days))
        ground_beta_2d = np.tile(self.ground_beta,(self.data_sampling_daily,self.num_days))
        ground_gamma_2d = np.tile(self.ground_gamma,(self.data_sampling_daily,self.num_days))
        self.costheta_ground_truth_calculate = self.func2(X,phi_real_2d, ground_beta_2d, ground_gamma_2d)
        return

    def estimate_costheta(self):
        X = np.array([self.omega, self.delta])
        phi_estimate_2d = np.tile(np.deg2rad(self.latitude_estimate),(self.data_sampling_daily,self.num_days))
        beta_estimate_2d = np.tile(np.deg2rad(self.tilt_estimate),(self.data_sampling_daily,self.num_days))
        gamma_estimate_2d = np.tile(np.deg2rad(self.azimuth_estimate),(self.data_sampling_daily,self.num_days))
        self.costheta_estimated = self.func2(X,phi_estimate_2d, beta_estimate_2d, gamma_estimate_2d)
        return

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
        c1 = cvx.sum(1/2 * cvx.abs(x1) + (t - 1/2) * x1)
        c2 = cvx.sum_squares(cvx.diff(x2,2))
        objective = cvx.Minimize(c1 + m * c2)
        prob = cvx.Problem(objective, constraints=constraints)
        prob.solve(solver='MOSEK')
        return x2.value

    # def calculate_cost(self):
    #     delta_f = self.delta
    #     omega_f = self.omega
    #     costheta_estimated_f = self.costheta_fit
    #     X = np.array([omega_f, delta_f])
    #     phi_predicted_2d = np.tile(np.deg2rad(self.latitude_estimate),(self.data_sampling_daily,self.num_days))
    #     beta_predicted_2d = np.tile(np.deg2rad(self.tilt_estimate),(self.data_sampling_daily,self.num_days))
    #     gamma_predicted_2d = np.tile(np.deg2rad(self.azimuth_estimate),(self.data_sampling_daily,self.num_days))
    #     costheta_predicted_f = self.func(X,phi_predicted_2d,beta_predicted_2d, gamma_predicted_2d)
    #     self.cost = np.sum(np.power((costheta_estimated_f - costheta_predicted_f),2))
    #     self.latitude_error = np.rad2deg(self.phi_real - np.deg2rad(self.latitude_estimate))
    #     self.tilt_error = np.rad2deg(self.ground_beta - np.deg2rad(self.tilt_estimate))
    #     self.azimuth_error = np.rad2deg(self.ground_gamma - np.deg2rad(self.azimuth_estimate))
    #     return
