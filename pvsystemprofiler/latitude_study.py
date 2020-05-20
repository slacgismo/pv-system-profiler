''' Latitude Study Module

'''
import numpy as np
import pandas as pd
from pvsystemprofiler.utilities.progress import progress
from solardatatools.daytime import find_daytime

class LatitudeStudy():
    def __init__(self, data_handler, daytime_threshold=[0.01, 0.01, 0.01, 0.01],
                 true_value=None):
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
        self.boolean_daytime = None
        self.discrete_latitude = None
        self.latitude_estimate = None
        self.hours_daylight = None
        self.delta = None
        self.residual = None

        # Results
        self.results = None

    def run(self, verbose=True):

        self.make_delta()
        rs1 = rs2 = rs3 = rs4 = None
        le1 = le2 = le3 = le4 = None
        self.estimate_latitude(self.raw_data_matrix, daytime_threshold=self.daytime_threshold[0],
                               sunrise_sunset=False)
        rs1 = self.residual
        le1 = self.latitude_estimate
        self.estimate_latitude(self.data_matrix, daytime_threshold=self.daytime_threshold[1],
                               sunrise_sunset=False)
        rs2 = self.residual
        le2 = self.latitude_estimate
        self.estimate_latitude(self.raw_data_matrix, daytime_threshold=self.daytime_threshold[2],
                               sunrise_sunset=True)
        rs3 = self.residual
        le3 = self.latitude_estimate
        self.estimate_latitude(self.data_matrix, daytime_threshold=self.daytime_threshold[3],
                               sunrise_sunset=True)
        rs4 = self.residual
        le4 = self.latitude_estimate
        results = pd.DataFrame(columns=[
             'latitude', 'Residual. raw calc-raw matrix', 'Residual. raw calc-filled matrix',
             'Residual. solarnoon calc-raw matrix', 'Residual. solarnoon calc-filled matrix',
             'Lat. raw calc-raw matrix', 'Lat. raw calc-filled matrix',
             'Lat. solarnoon calc-raw matrix', 'Lat. solarnoon calc-filled matrix'
        ])

        results.loc[0] = [self.true_value,
                          rs1, rs2, rs3, rs4,
                          le1, le2, le3, le4]
        self.results = results

    def estimate_latitude(self, data_matrix=None, daytime_threshold=0.01, sunrise_sunset=False):
        if not sunrise_sunset:
            self.boolean_daytime = find_daytime(data_matrix,
                                                daytime_threshold)
            self.hours_daylight = (np.sum(self.boolean_daytime, axis=0))*self.data_sampling/60
        else:
            self.hours_daylight = self.avg_sunrise_sunset(data_matrix, daytime_threshold)

        self.discrete_latitude = np.degrees(np.arctan(- np.cos(np.radians(15/2*self.hours_daylight))/
                                                       (np.tan(self.delta[0]))))
        self.latitude_estimate = np.median(self.discrete_latitude)
        self.residual = self.true_value - self.latitude_estimate
        return

    def make_delta(self):
        delta_1 = np.deg2rad(23.45 * np.sin(np.deg2rad(360 * (284 + self.day_of_year) / 365)))
        self.delta = np.tile(delta_1, (self.data_matrix.shape[0], 1))
        return

    def avg_sunrise_sunset(self,data_in, threshold=0.01):
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
