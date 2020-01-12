''' System Parameter (Lat, tilt and azimuth) Estimation Module
This module contains a class for estimating latitude, tilt and azimuth from power signal outputs
'''
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import cvxpy as cvx
from sys import path
path.append('..')
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from solardatatools.clear_day_detection import find_clear_days
from solardatatools import standardize_time_axis, make_2d, plot_2d
#, load_pvo_data
#, DataHandler
import boto3
import sys
from os.path import expanduser
home = expanduser('~')
from scipy.optimize import curve_fit
#from statistical_clear_sky.algorithm.iterative_fitting import IterativeFitting
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
sys.path.append('/Users/elpiniki/Documents/github/solar-data-tools/solardatatools')
from importlib.machinery import SourceFileLoader

#I use this line only if DataHandler is not working
dp = SourceFileLoader("solardatatools.data_handler", "/Users/elpiniki/Documents/github/solar-data-tools/solardatatools/data_handler.py").load_module()
#dp.DataHandler()

class ModelEstimator():
    def __init__(self, data_matrix=None, days_approach="cloudy days", solarnoon_approach=avg_sunrise_sunset, scsf_flag=False, GMT_offset=None, day_of_year=None):
        self.data_matrix = data_matrix
        self.days_approach = days_approach
        self.solarnoon_approach = solarnoon_approach
        self.scsf_flag = None
        self.GMT_offset = GMT_offset
        self.day_of_year = day_of_year
        self.lon_value_haghdadi = None
        self.lon_value_duffie = None
        self.eot_duffie = None
        self.eot_hag = None
        self.beta_duffie = None
        self.beta_hag = None
        self.lon_value_fit_norm = None
        self.lon_value_fit_norm1 = None
        self.solarnoon = None
        self.days = None

    def config_solarnoon(self):
        #if self.scsf_flag == "False":
        self.solarnoon = self.solarnoon_approach(self.data_matrix)
        #if self.scsf_flag == "True":
        #    self.solarnoon = self.solarnoon_approach(run_scsf(self.data_matrix))
        return

    def config_days(self):
        if self.days_approach == 'all':
            self.days = np.array([True] * len(self.data_matrix[0]))
        if self.days_approach == 'clear days':
            self.days = find_clear_days(self.data_matrix)
        if self.days_approach == 'cloudy days':
            self.days = ~find_clear_days(self.data_matrix)
        return

    def equation_of_time_Haghdadi(self):
        """
        Equation of time from Haghdadi et al. (2017).The coefficients correspond to
        the journal publication (reference below).

        Paremeters
        -------
        beta: day angle for the Earth's orbit around the Sun, calculated by calculate_simple_day_angle_Haghdadi.

        Returns
        -------
        equation_of_time : list of numberic values with length same as power signals day length.
                           Difference in time between solar time and mean solar time in minutes.

        References
        -------
        Haghdadi, Navid, et al. "A method to estimate the location and orientation of
        distributed photovoltaic systems from their generation output data." Renewable Energy 108 (2017): 390-400.
        """

        #eot = (9.87 * np.sin(2.0 * beta)) - (7.53 * np.cos(beta)) - (1.5* np.sin(beta))
        self.eot_hag = (9.87 * np.sin(2.0 * self.beta_hag * np.pi / 180)) - (7.53 * np.cos(self.beta_hag * np.pi / 180)) - (1.5 * np.sin(self.beta_hag * np.pi / 180))
        return

    def calculate_simple_day_angle_Haghdadi(self, offset=81):
        """
        Calculates the day angle for the Earth's orbit around the Sun.

        Parameters
        ----------
        dayofyear : list of numeric values, calculated by day_of_year_finder.

        Returns
        -------
        day_angle : list of numeric values
        """
        self.beta_hag = (360 / 365.) * (self.day_of_year-offset)
        return

    def calculate_simple_day_angle_Duffie(self, offset=1):
        """
        Calculates the day angle for the Earth's orbit around the Sun.

        Parameters
        ----------
        dayofyear : numeric
        offset : int, default 1
            For the Spencer method, offset=1; for the ASCE method, offset=0

        Returns
        -------
        day_angle : list of numeric values
        """
        self.beta_duffie  = ((2. * np.pi) / 365.) * (self.day_of_year-offset)
        return

    def equation_of_time_Duffie(self):
        """
        Calculates equation of time from Duffie & Beckman and attributed to Spencer
        (1971) and Iqbal (1983).

        Parameters
        ----------
        dayofyear : numeric

        Returns
        -------
        equation_of_time : list of numeric
            Difference in time between solar time and mean solar time in minutes.

        References
        ----------
        [1] Duffie, John A., and William A. Beckman. Solar engineering of thermal processes. John Wiley & Sons, 2013.
        """

        self.eot_duffie = (1440.0 / 2 / np.pi) * (
            0.000075 +
            0.001868 * np.cos(self.beta_duffie) - 0.032077 * np.sin(self.beta_duffie) -
            0.014615 * np.cos(2.0 * self.beta_duffie) - 0.040849 * np.sin(2.0 * self.beta_duffie)
        )
        return

    def fit_norm1(self):
        lon = cvx.Variable()
        sn_m = 4*(15*self.GMT_offset - lon)-self.eot_duffie+720
        sn_h = sn_m / 60
        cost = cvx.norm1(sn_h[self.days] - self.solarnoon[self.days])
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        self.lon_value_fit_norm1 = -lon.value
        return

    def fit_norm(self):
        lon = cvx.Variable()
        sn_m = 4*(15*self.GMT_offset - lon)-self.eot_duffie+720
        sn_h = sn_m / 60
        cost = cvx.norm(sn_h[self.days] - self.solarnoon[self.days])
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        self.lon_value_fit_norm = -lon.value
        return

    def fit_huber(self):
        lon = cvx.Variable()
        sn_m = 4*(15*self.GMT_offset - lon)-self.eot_duffie+720
        sn_h = sn_m / 60
        cost = cvx.sum(cvx.huber(sn_h[self.days] - self.solarnoon[self.days]))
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        self.lon_value_fit_huber = -lon.value
        return

    def run(self):
        self.config_solarnoon()
        self.config_days()
        self.calculate_simple_day_angle_Haghdadi()
        self.equation_of_time_Haghdadi()
        self.calculate_simple_day_angle_Duffie()
        self.equation_of_time_Duffie()
        self.lon_value_haghdadi = np.nanmedian((720-self.solarnoon[self.days]*60)/4-(self.eot_hag[self.days]/4)) - 15*self.GMT_offset
        self.lon_value_duffie = np.nanmedian(self.solarnoon[self.days]*60 + self.eot_duffie[self.days] -720 - 4*15*self.GMT_offset)/4
        self.fit_norm1()
        self.fit_norm()
        self.fit_huber()
        return
