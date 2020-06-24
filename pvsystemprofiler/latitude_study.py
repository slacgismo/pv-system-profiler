''' Latitude Study Module
This module contains a class for conducting a study
to estimating latitude from solar power data. This code accepts solar power
data in the form of a `solar-data-tools` `DataHandler` object, which is used
to standardize and pre-process the data. The provided class will then estimate
the latitude of the site that produced the data, using the `run` method.
'''
import numpy as np
import pandas as pd
from pvsystemprofiler.utilities.declination_equations import delta_spencer
from pvsystemprofiler.utilities.declination_equations import delta_cooper
from pvsystemprofiler.algorithms.latitude.direct_calculation import calc_lat
from solardatatools.daytime import find_daytime

class LatitudeStudy():
    def __init__(self, data_handler, lat_true_value=None):
        '''
        :param data_handler: `DataHandler` class instance loaded with a solar power data set.

        :param lat_true_value: (optional) the ground truth value for the system's latitude. (Degrees).
        '''

        self.data_handler = data_handler
        self.phi_true_value = lat_true_value
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
        self.delta_cooper = None
        self.delta_spencer = None
        self.residual = None
        self.daytime_threshold = None
        # Results
        self.results = None

    def run(self, threshold_method=('raw data matrix', 'filled data matrix'),
            daylight_method=('raw daylight', 'sunrise-sunset'),
            delta_method=('cooper', 'spencer'),
            threshold=None):
        '''
        :param threshold_method: 'raw data matrix', 'filled data matrix'
        :param daylight_method: 'raw daylight', 'sunrise-sunset'
        :param threshold: (optional) daylight threshold values, tuple of length one to eight
        :param delta_method: (optional) 'cooper', 'spencer'
        :return:
        '''
        threshold_method = np.atleast_1d(threshold_method)
        daylight_method = np.atleast_1d(daylight_method)

        if threshold is None:
            self.daytime_threshold = 0.001*np.ones(len(threshold_method)*len(daylight_method)*len(delta_method))
        else:
            self.daytime_threshold = threshold

        self.delta_cooper = delta_cooper(self.day_of_year, self.daily_meas)
        self.delta_spencer = delta_spencer(self.day_of_year, self.daily_meas)

        results = pd.DataFrame(columns=['latitude', 'threshold', 'threshold matrix', 'daylight calculation',
                                        'declination method'])
        counter = 0
        for delta_id in delta_method:
            for matrix_ix, matrix_id in enumerate(threshold_method):
                for daylight_method_id in daylight_method:
                    dtt = self.daytime_threshold[counter]
                    dlm = daylight_method_id
                    met = threshold_method[matrix_ix]
                    dcc = daylight_method_id
                    dm = delta_id
                    lat_est = self.estimate_latitude(matrix_id, daytime_threshold=dtt, daylight_method=dlm,
                                                     delta_method=delta_id)

                    results.loc[counter] = [lat_est, dtt, met, dcc, dm]
                    counter += 1
        if self.phi_true_value is not None:
            results['residual'] = self.phi_true_value - results['latitude']

        self.results = results

    def estimate_latitude(self, matrix_id=None, daytime_threshold=0.001,
                          daylight_method=('sunrise-sunset', 'raw daylight'),
                          delta_method=('cooper', 'spencer')):
        """"
        Latitude is estimated from equation (1.6.11) in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991.
        """

        if matrix_id in ('raw data matrix', 'raw_data_matrix', 'raw'):
            data_in = self.raw_data_matrix
        if matrix_id in ('filled data matrix', 'filled_data_matrix', 'filled'):
            data_in = self.data_matrix

        if daylight_method in ('sunrise-sunset', 'sunrise sunset'):
            self.hours_daylight = self.calculate_hours_daylight(data_in, daytime_threshold)
        if daylight_method in ('raw_daylight', 'raw daylight'):
            self.hours_daylight = self.calculate_hours_daylight_raw(data_in, daytime_threshold)
        if delta_method in ('Cooper', 'cooper'):
            delta = self.delta_cooper
        if delta_method in ('Spencer', 'spencer'):
            delta = self.delta_spencer

        #self.discrete_latitude = np.degrees(np.arctan(- np.cos(np.radians(15 / 2 * self.hours_daylight)) /
        #                                              (np.tan(delta[0]))))
        self.discrete_latitude = calc_lat(self.hours_daylight, delta)
        return np.median(self.discrete_latitude)

    def calculate_hours_daylight_raw(self, data_in, threshold=0.001):
        self.boolean_daytime = find_daytime(data_in, threshold)
        return (np.sum(self.boolean_daytime, axis=0)) * self.data_sampling / 60

    def calculate_hours_daylight(self, data_in, threshold=0.001):

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
