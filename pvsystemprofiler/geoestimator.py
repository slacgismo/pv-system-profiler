''' System Parameter (Lat, tilt and azimuth) Estimation Module
This module contains a class for estimating latitude, tilt and azimuth from power signal outputs
'''
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import cvxpy as cvx
from solardatatools import standardize_time_axis, make_2d, plot_2d, load_pvo_data, DataHandler
import boto3
import sys
from os.path import expanduser
home = expanduser('~')
from scipy.optimize import curve_fit
from statistical_clear_sky.algorithm.iterative_fitting import IterativeFitting

class GeoEstimator():
    def __init__(self, data_matrix=None, start_date=None, end_date=None, phi_real=None, ground_beta=None, ground_gamma=None, SCSF_flag=None):
        self.data_matrix = data_matrix
        self.start_date = start_date
        self.end_date = end_date
        self.phi_real = phi_real
        self.ground_beta = ground_beta
        self.ground_gamma = ground_gamma
        self.SCSF_flag = SCSF_flag
        if self.data_matrix is not None:
            self.num_days = self.data_matrix.shape[1]
            self.data_sampling_daily = self.data_matrix.shape[0]
            self.doy_list = pd.date_range(self.start_date, self.end_date).dayofyear.values[1:-2]
            self.data_sampling = 24 * 60 / self.data_matrix.shape[0]
            self.boolean_daylight = self.data_matrix > 0.001 * np.percentile(self.data_matrix, 95)
        else:
            self.num_days = None
            self.data_sampling = None
        self.clear_index_set = None
        self.delta = None
        self.omega = None
        self.costheta_estimate = None
        self.latitude_estimate = None
        self.latitude_calculate = None
        self.tilt_estimate = None
        self.azimuth_estimate = None
        self.costheta_ground_truth_calculate = None
        self.scale_factor_costheta = None
        self.hours_daylight = None

    def run_preprocessing(self):
        self.make_delta()
        self.make_omega()
        self.calculate_latitude()
        self.flag_clear_days()
        self.estimate_costheta()
        self.ground_truth_costheta()
        return

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.doy_list) / 365)))
        self.delta = np.tile(delta_1, (self.data_matrix.shape[0], 1))
        return

    def make_omega(self):
        hour = np.arange(0, 24, 1 / (60/self.data_sampling)) # don't hardcode 1/12!!
        omega_1 = np.deg2rad(15 * (hour - 12))
        self.omega = np.tile(omega_1.reshape(-1, 1), (1, self.data_matrix.shape[1]))
        return

    def calculate_latitude(self):
        self.hours_daylight = (np.sum(self.boolean_daylight, axis=0))*self.data_sampling/60
        self.latitude_calculate = np.degrees(np.arctan(- np.cos(np.radians(15/2*self.hours_daylight))/(np.tan(self.delta[0]))))
        #self.latitude_calculate = np.tile(latitude_calculate_1, (self.data_matrix.shape[0], 1))
        return

    def flag_clear_days(self):
        if self.SCSF_flag is not None:
            self.clear_index_set = np.s_[:]
        else:
            dh = DataHandler(raw_data_matrix=self.data_matrix)
            dh.run_pipeline(verbose=False)
            self.clear_index_set = dh.daily_flags.clear
        return

    def estimate_costheta(self):
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
        self.costheta_estimate = self.data_matrix / np.max(s1.value)
        return

    def func(self, X, phi, beta, gamma):
        w = X[0]
        d = X[1]
        A = np.sin(d) * np.sin(phi) * np.cos(beta)
        B = np.sin(d) * np.cos(phi) * np.sin(beta) * np.cos(gamma)
        C = np.cos(d) * np.cos(phi) * np.cos(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.sin(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E

    def jacphi(self, X, phi, beta, gamma):
        w = X[0]
        d = X[1]
        A = np.sin(d) * np.cos(phi) * np.cos(beta)
        B = -np.sin(d) * np.sin(phi) * np.sin(beta) * np.cos(gamma)
        C = -np.cos(d) * np.sin(phi) * np.cos(beta) * np.cos(w)
        D = np.cos(d) * np.cos(phi) * np.sin(beta) * np.cos(gamma) * np.cos(w)
        E = 0
        return A - B + C + D + E

    def jacbeta(self, X, phi, beta, gamma):
        w = X[0]
        d = X[1]
        A = -np.sin(d) * np.sin(phi) * np.sin(beta)
        B = np.sin(d) * np.cos(phi) * np.cos(beta) * np.cos(gamma)
        C = -np.cos(d) * np.cos(phi) * np.sin(beta) * np.cos(w)
        D = np.cos(d) * np.sin(phi) * np.cos(beta) * np.cos(gamma) * np.cos(w)
        E = np.cos(d) * np.cos(beta) * np.sin(gamma) * np.sin(w)
        return A - B + C + D + E

    def jacgamma(self, X, phi, beta, gamma):
        w = X[0]
        d = X[1]
        A = 0
        B = -np.sin(d) * np.cos(phi) * np.sin(beta) * np.sin(gamma)
        C = 0
        D = -np.cos(d) * np.sin(phi) * np.sin(beta) * np.sin(gamma) * np.cos(w)
        E = np.cos(d) * np.sin(beta) * np.cos(gamma) * np.sin(w)
        return A - B + C + D + E

    def jac_matrix(self, X, phi, beta, gamma):
        return np.array([self.jacphi(X, phi, beta, gamma), self.jacbeta(X, phi, beta, gamma), self.jacgamma(X, phi, beta, gamma)]).T

    def run_curve_fit_1(self, bootstrap_iterations=None):
        if self.SCSF_flag is not None:
            slct_curve_fit = self.boolean_daylight
        else:
            slct_curve_fit = self.boolean_daylight * self.clear_index_set
        delta_f = self.delta[slct_curve_fit]
        omega_f = self.omega[slct_curve_fit]
        costheta_estimated_f = self.costheta_estimate[slct_curve_fit]
        X = np.array([omega_f, delta_f])
        popt, pcov = curve_fit(self.func, X, costheta_estimated_f, p0=np.deg2rad([30,10,0]), jac=self.jac_matrix, bounds=([-1.57,0,-3.14],[1.57,1.57,3.14]))
        self.latitude_estimate, self.tilt_estimate, self.azimuth_estimate = np.degrees(popt)
        return

    def run_curve_fit_2(self):
        print("Tilt and azimuth not ready yet")
        #self.latitude_estimate = np.median(self.latitude_calculate[0][[self.clear_index_set]])
        self.latitude_estimate = np.median(self.latitude_calculate)
        return

    def ground_truth_costheta(self):
        X = np.array([self.omega, self.delta])
        phi_real_2d = np.tile(self.phi_real,(self.data_sampling_daily,self.num_days))
        ground_beta_2d = np.tile(self.ground_beta,(self.data_sampling_daily,self.num_days))
        ground_gamma_2d = np.tile(self.ground_gamma,(self.data_sampling_daily,self.num_days))
        self.costheta_ground_truth_calculate = self.func(X,phi_real_2d,ground_beta_2d, ground_gamma_2d)
        return
