''' Latitude Study Module

'''
import numpy as np
import pandas as pd
from pvsystemprofiler.utilities.progress import progress

class LatitudeStudy():
    def __init__(self, data_handler, summer_flag=None, fixed_power_thr=None,
                 fix_param=0.8, true_value=None):

        self.data_handler = data_handler
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.data_matrix = self.data_handler.filled_data_matrix
        self.true_value = true_value
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.summer_flag = summer_flag
        self.fixed_power_thr = fixed_power_thr  #?
        self.fix_param = fix_param # ?
        #self.power_threshold_fit = None  # ?

        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0] #daily measurements
        self.data_sampling = self.data_handler.data_sampling
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.boolean_daylight_lat = self.data_handler.filled_data_matrix \
                                    > 0.001 * np.percentile(self.data_matrix, 95)
        #self.boolean_daylight_lat = data_handler.boolean_masks.daytime

        if self.fixed_power_thr == None:
            self.boolean_daylight = np.empty([self.daily_meas, self.num_days], dtype=bool)
        else:
            self.boolean_daylight = self.data_matrix > self.fix_param * np.percentile(self.data_matrix, 95)

        if self.summer_flag == True:
            # self.slct_summer = (self.doy_list>152) & (self.doy_list<245) #summer only
            # self.slct_summer = (self.doy_list>60) & (self.doy_list<335) #no winter
            # self.slct_summer = (self.doy_list>60) & (self.doy_list<153) #spring only
            self.slct_summer = (self.day_of_year > 85) & (self.day_of_year < 167)  # manual set only
        else:
            self.slct_summer = np.ones(self.day_of_year.shape, dtype=bool)

        self.latitude_calculate = None
        self.latitude_estimate = None
        self.hours_daylight = None
        self.delta = None

        # Results
        #self.results = None
        #self.best_result = None

    def run(self, verbose=True):
        self.make_delta()
        self.estimate_latitude()


    def estimate_latitude(self):
        self.hours_daylight = (np.sum(self.boolean_daylight_lat, axis=0))*self.data_sampling/60
        self.latitude_calculate = np.degrees(np.arctan(- np.cos(np.radians(15/2*self.hours_daylight))/
                                                       (np.tan(self.delta[0]))))
        self.latitude_estimate = np.median(self.latitude_calculate)
        return

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.day_of_year) / 365)))
        self.delta = np.tile(delta_1, (self.data_matrix.shape[0], 1))
        return
