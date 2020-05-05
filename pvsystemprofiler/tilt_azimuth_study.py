''' Tilt and Azimuth Study Module

'''
import numpy as np
import pandas as pd
import cvxpy as cvx
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon
from pvsystemprofiler.utilities.equation_of_time import eot_haghdadi, eot_duffie
from pvsystemprofiler.utilities.progress import progress
from solardatatools.daytime import find_daytime
class TiltAzimuthStudy():
    def __init__(self, data_handler, gmt_offset=-8, summer_flag=False,
                 init_values=None, daytime_threshold=None, tilt_true_value=None,
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
        self.ground_beta = tilt_true_value
        self.ground_gamma = azim_true_value
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.solarnoon = None
        self.days = None

        self.init_values = init_values
        self.power_threshold_fit = None

        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.data_sampling = self.data_handler.data_sampling
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.clear_index_set = self.data_handler.daily_flags.clear

        if self.daytime_threshold is None:
            self.boolean_daylight = np.empty([self.daily_meas, self.num_days], dtype=bool)
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
        self.latitude_estimate = None
        self.latitude_calculate = None
        self.latitude_estimate = None
        self.hours_daylight = None
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

    def run(self):
        self.make_delta()
        self.make_omega()
      # self.find_fit_costheta()
        # self.ground_truth_costheta()
        # dh = DataHandler(raw_data_matrix=self.data_matrix)
        # dh.run_pipeline(verbose=False)
        # if self.fixed_power_thr == None:
        #     self.power_threshold_fit = self.find_power_threshold_quantile_seasonality()
        #     for d in range(0, self.num_days - 1):
        #         self.boolean_daylight[:, d] = self.data_matrix[:, d] > 1 * self.power_threshold_fit[d]
        # self.select_days()
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
