''' Latitude Study Module
This module contains a class for conducting a study
to estimating latitude from solar power data. This code accepts solar power
data in the form of a `solar-data-tools` `DataHandler` object, which is used
to standardize and pre-process the data. The provided class will then estimate
the latitude of the site that produced the data, using the `run` method.
Latitude is estimated from equation (1.6.11) in:
    Duffie, John A., and William A. Beckman. Solar engineering of thermal
    processes. New York: Wiley, 1991.
'''
import numpy as np
import pandas as pd
from pvsystemprofiler.utilities.progress import progress
from solardatatools.daytime import find_daytime


class LatitudeStudy():
    def __init__(self, data_handler, daytime_threshold=None, lat_true_value=None):
        '''
        :param data_handler: `DataHandler` class instance loaded with a solar power data set.
        :param daytime_threshold: (optional) daytime threshold.
        :param lat_true_value: (optional) the ground truth value for the system's latitude. (Degrees).
        '''

        self.data_handler = data_handler
        self.daytime_threshold = daytime_threshold
        if self.daytime_threshold is None:
            self.daytime_threshold = [0.001, 0.001]
        self.phi_true_value = lat_true_value

        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.data_matrix = self.data_handler.filled_data_matrix
        self.raw_data_matrix = self.data_handler.raw_data_matrix
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.data_sampling = self.data_handler.data_sampling
        self.boolean_daytime = None
        self.discrete_latitude = None
        self.hours_daylight = None
        self.delta = None
        self.residual = None
        # Results
        self.methods = ['raw data matrix', 'filled data matrix']
        self.results = None

    def run(self, verbose=True):
        self.make_delta()
        
        results = pd.DataFrame(columns=['latitude', ' threshold', 'threshold method'])
        for matrix_ix, matrix_id in enumerate([self.raw_data_matrix, self.data_matrix]):
            dtt = self.daytime_threshold[matrix_ix]
            met = self.methods[matrix_ix]
            lat_est = self.estimate_latitude(matrix_id, daytime_threshold=dtt)

            results.loc[matrix_ix] = [lat_est, dtt, met]
        if self.phi_true_value is not None:
            results['residual'] = self.phi_true_value - results['latitude']

            self.results = results

    def estimate_latitude(self, data_matrix=None, daytime_threshold=0.001):
        self.hours_daylight = self.calculate_hours_daylight(data_matrix, daytime_threshold)

        self.discrete_latitude = np.degrees(np.arctan(- np.cos(np.radians(15 / 2 * self.hours_daylight)) /
                                                      (np.tan(self.delta[0]))))
        return np.median(self.discrete_latitude)

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.day_of_year) / 365)))
        self.delta = np.tile(delta_1, (self.data_matrix.shape[0], 1))
        return

    def calculate_hours_daylight(self, data_in, threshold=0.01):
        data = np.copy(data_in).astype(np.float)
        num_meas_per_hour = data.shape[0] / 24
        x = np.arange(0, 24, 1. / num_meas_per_hour)
        night_msk = ~find_daytime(data_in, threshold=threshold)
        data[night_msk] = np.nan
        good_vals = (~np.isnan(data)).astype(int)
        sunrise_idxs = np.argmax(good_vals, axis=0)
        sunset_idxs = data.shape[0] - np.argmax(np.flip(good_vals, 0), axis=0)
        sunset_idxs[sunset_idxs == data.shape[0]] = data.shape[0] - 1
        hour_of_day = x
        sunset_times = hour_of_day[sunset_idxs]
        sunrise_times = hour_of_day[sunrise_idxs]
        return sunset_times - sunrise_times
