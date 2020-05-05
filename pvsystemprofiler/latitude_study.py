''' Latitude Study Module

'''
import numpy as np
import pandas as pd
from pvsystemprofiler.utilities.progress import progress
from solardatatools.daytime import find_daytime

class LatitudeStudy():
    def __init__(self, data_handler, daytime_threshold=0.01, true_value=None):
        self.data_handler = data_handler
        self.daytime_threshold = daytime_threshold
        self.true_value = true_value
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.data_matrix = self.data_handler.filled_data_matrix
        self.raw_data_matrix = self.data_handler.raw_data_matrix
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.data_sampling = self.data_handler.data_sampling
        self.boolean_daytime = find_daytime(self.data_matrix,
                                            self.daytime_threshold)
        #self.boolean_daytime = find_daytime(self.raw_data_matrix,
                                            # self.daytime_threshold)
        self.discrete_latitude = None
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
        self.hours_daylight = (np.sum(self.boolean_daytime, axis=0))*self.data_sampling/60
        self.discrete_latitude = np.degrees(np.arctan(- np.cos(np.radians(15/2*self.hours_daylight))/
                                                       (np.tan(self.delta[0]))))
        self.latitude_estimate = np.median(self.discrete_latitude)
        return

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.day_of_year) / 365)))
        self.delta = np.tile(delta_1, (self.data_matrix.shape[0], 1))
        return
