''' Tilt and Azimuth Study Module
This module contains a class for conducting a study
to estimating Tilt and Azimuth from solar power data. This code accepts solar power
data in the form of a `solar-data-tools` `DataHandler` object, which is used
to standardize and pre-process the data. The provided class will then estimate
the Tilt and Azimuth of the site that produced the data, using the `run` method.
Tilt and Azimuth are estimated via numerical fit using equation (1.6.2) in:
    Duffie, John A., and William A. Beckman. Solar engineering of thermal
    processes. New York: Wiley, 1991.
'''
import numpy as np
import pandas as pd
import cvxpy as cvx
from solardatatools.daytime import find_daytime
from pvsystemprofiler.utilities.hour_angle_equation import calculate_omega
from pvsystemprofiler.utilities.declination_equation import delta_spencer
from pvsystemprofiler.utilities.declination_equation import delta_cooper
from pvsystemprofiler.algorithms.angle_of_incidence.curve_fitting import run_curve_fit
from pvsystemprofiler.algorithms.angle_of_incidence.calculation import calculate_costheta
from pvsystemprofiler.algorithms.performance_model_estimation import find_fit_costheta
from pvsystemprofiler.algorithms.angle_of_incidence.lambda_functions import select_function
from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta
from pvsystemprofiler.algorithms.angle_of_incidence.dynamic_value_functions import determine_unknowns
from pvsystemprofiler.algorithms.angle_of_incidence.dynamic_value_functions import select_init_values

class TiltAzimuthStudy():
    def __init__(self, data_handler, day_range=None, init_values=None, nrandom_init_values=None, daytime_threshold=None,
                 lon_precalculate=None, lat_precalculate=None, tilt_precalculate=None, azimuth_precalculate=None,
                 lat_true_value=None, tilt_true_value=None, azimuth_true_value=None, gmt_offset=-8):
        """
        :param data_handler: `DataHandler` class instance loaded with a solar power data set.
        :param day_range: (optional) the desired day range to run the study. A list of the form
                              [first day, last day].
        :param init_values: (optional) Latitude, Tilt and Azimuth guess values list for numerical fit. A list of the
                form [[latitude_1,.., latitude_n], [tilt_1,.., tilt_n], [azimuth_1,.., azimuth_n]]. Default value is 10.
                (Degrees).
        :param nrandom_init_values: (optional) number of random initial values to be generated.
        :param daytime_threshold: (optional) daytime threshold
        :param lon_precalculate: longitude estimate as obtained from the Longitude Study module in Degrees.
        :param lat_precalculate: precalculated latitude value in Degrees.
        :param tilt_precalculate: precalculated tilt value in Degrees.
        :param azimuth_precalculate: precalculated azimuth value in Degrees.
        :param lat_true_value: (optional) ground truth value for the system's Latitude in Degrees.
        :param tilt_true_value: (optional) ground truth value for the system's Tilt in Degrees.
        :param azimuth_true_value: (optional) ground truth value for the system's Azimuth in Degrees Degrees.
        :param gmt_offset: The offset in hours between the local timezone and GMT/UTC.
        """

        self.data_handler = data_handler

        if day_range is None:
            self.day_range_dict = {}
            self.day_range_dict = {'summer': [152, 245], 'no winter': [60, 335], 'spring': [60, 153], 'Full Year': None}
        else:
            self.day_range_dict = {'manual': day_range}
        self.data_matrix = self.data_handler.filled_data_matrix
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.init_values = init_values
        self.nrandom = nrandom_init_values
        self.daytime_threshold = daytime_threshold
        self.lon_precalc = lon_precalculate
        self.lat_precalc = lat_precalculate
        self.tilt_precalc = tilt_precalculate
        self.azimuth_precalc = azimuth_precalculate
        self.lat_true_value = lat_true_value
        self.tilt_true_value = tilt_true_value
        self.azimuth_true_value = azimuth_true_value
        self.gmt_offset = gmt_offset
        self.daytime_threshold_fit = None
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.num_days = self.data_handler.num_days
        self.daily_meas = self.data_handler.filled_data_matrix.shape[0]
        self.data_sampling = self.data_handler.data_sampling
        self.clear_index = data_handler.daily_flags.clear
        self.boolean_daytime = None
        self.delta_cooper = None
        self.delta_spencer = None
        self.omega = None
        self.scale_factor_costheta = None
        self.costheta_estimated = None
        self.costheta_ground_truth = None
        self.costheta_fit = None
        self.boolean_daytime_range = None
        self.results = None

    def run(self, delta_method=('cooper', 'spencer')):

        delta_method = np.atleast_1d(delta_method)
        self.find_boolean_daytime()
        self.omega = calculate_omega(self.data_sampling, self.num_days, self.lon_precalc, self.day_of_year,
                                     self.gmt_offset)
        self.scale_factor_costheta, self.costheta_fit = find_fit_costheta(self.data_matrix, self.clear_index)
        self.delta_cooper = delta_cooper(self.day_of_year, self.daily_meas)
        self.delta_spencer = delta_spencer(self.day_of_year, self.daily_meas)

        if self.init_values is not None:
            lat_initial = self.init_values[0]
            tilt_initial = self.init_values[1]
            azim_initial = self.init_values[2]
        else:
            if self.nrandom is None:
                lat_initial = [10]
                tilt_initial = [10]
                azim_initial = [10]
            else:
                lat_initial, tilt_initial, azim_initial = self.random_initial_values()

        counter = 0
        self.create_results_table()

        for delta_id in delta_method:
            if delta_id in ('Cooper', 'cooper'):
                delta = self.delta_cooper
            if delta_id in ('Spencer', 'spencer'):
                delta = self.delta_spencer
            for day_range_id in self.day_range_dict:
                day_interval = self.day_range_dict[day_range_id]
                self.get_day_range(day_interval)
                delta_f = delta[self.boolean_daytime_range]
                omega_f = self.omega[self.boolean_daytime_range]
                if ~np.any(self.boolean_daytime_range):
                    print('No data made it through filters')

                func_customized, bounds = select_function(self.lat_precalc, self.tilt_precalc,
                                                          self.azimuth_precalc)

                dict_keys = determine_unknowns(latitude=self.lat_precalc, tilt=self.tilt_precalc,
                                                    azimuth=self.azimuth_precalc)

                nvalues = len(lat_initial)
                for init_val_ix in np.arange(nvalues):
                    init_values_dict = {'latitude': lat_initial[init_val_ix], 'tilt': tilt_initial[init_val_ix],
                                        'azimuth': azim_initial[init_val_ix]}
                    init_values, ivr = select_init_values(init_values_dict, dict_keys)
                    try:
                        estimates = run_curve_fit(func=func_customized, keys=dict_keys, delta=delta_f, omega=omega_f,
                                                  costheta=self.costheta_fit,
                                                  boolean_daytime_range=self.boolean_daytime_range,
                                                  init_values=init_values,
                                                  fit_bounds=bounds)
                    except RuntimeError:
                        precalc_array = np.array([self.lat_precalc, self.tilt_precalc, self.azimuth_precalc])
                        estimates = np.full(np.sum(precalc_array == None), np.nan)

                    estimates_dict = dict(zip(dict_keys, estimates))

                    lat = estimates_dict[
                        'latitude_estimate'] if 'latitude_estimate' in estimates_dict else self.lat_precalc
                    tilt = estimates_dict['tilt_estimate'] if 'tilt_estimate' in estimates_dict else self.tilt_precalc
                    azim = estimates_dict[
                        'azimuth_estimate'] if 'azimuth_estimate' in estimates_dict else self.azimuth_precalc

                    self.costheta_estimated = calculate_costheta(func=func_costheta, delta=delta, omega=self.omega,
                                                                 lat=lat, tilt=tilt, azim=azim)

                    if None not in (self.lat_true_value, self.tilt_true_value, self.azimuth_true_value):
                        self.costheta_ground_truth = calculate_costheta(func=func_costheta, delta=delta,
                                                                        omega=self.omega,
                                                                        lat=self.lat_true_value,
                                                                        tilt=self.tilt_true_value,
                                                                        azim=self.azimuth_true_value)

                    self.results.loc[counter] = [day_range_id, delta_id] + ivr + list(estimates)
                    counter += 1

        if self.lat_true_value is not None and self.lat_precalc is None:
            self.results['latitude residual'] = self.lat_true_value - self.results['latitude']
        if self.tilt_true_value is not None and self.tilt_precalc is None:
            self.results['tilt residual'] = self.tilt_true_value - self.results['tilt']
        if self.azimuth_true_value is not None and self.azimuth_precalc is None:
            self.results['azimuth residual'] = self.azimuth_true_value - self.results['azimuth']
        return

    def get_day_range(self, interval):
        if interval is not None:
            day_range = (self.day_of_year > interval[0]) & (self.day_of_year < interval[1])
        else:
            day_range = np.ones(self.day_of_year.shape, dtype=bool)
        self.boolean_daytime_range = self.boolean_daytime * self.clear_index * day_range
        return

    def find_boolean_daytime(self):
        if self.daytime_threshold is None:
            self.daytime_threshold_fit = self.find_daytime_threshold_quantile_seasonality()
            self.boolean_daytime = self.data_matrix > self.daytime_threshold_fit
        else:
            self.boolean_daytime = find_daytime(self.data_matrix, self.daytime_threshold)
        return

    def find_daytime_threshold_quantile_seasonality(self):
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
            constraints = [x1 + x2 == y]
        c1 = cvx.sum(1 / 2 * cvx.abs(x1) + (t - 1 / 2) * x1)
        c2 = cvx.sum_squares(cvx.diff(x2, 2))
        objective = cvx.Minimize(c1 + m * c2)
        prob = cvx.Problem(objective, constraints=constraints)
        prob.solve(solver='MOSEK')
        return x2.value

    def create_results_table(self):
        cols = ['day range', 'declination method', 'latitude initial value', 'tilt initial value',
                'azimuth initial value']
        if self.lat_precalc is None:
            cols.append('latitude')
        if self.tilt_precalc is None:
            cols.append('tilt')
        if self.azimuth_precalc is None:
            cols.append('azimuth')

        self.results = pd.DataFrame(columns=cols)

    def random_initial_values(self):
        """ Bounds for latitude are -90 to 90. Bounds for tilt are 0 to 90. Bounds for azimuth  are -180 to 180. It is
        noted that, theoretically, bounds for tilt are 0 to 180 (Duffie, John A., and William A. Beckman. Solar
        engineering of thermal processes. New York: Wiley, 1991.). However a value of tilt >90 would mean that that the
        surface has a downward-facing component, which is not the case of the current application."""

        lat_initial_value = np.random.uniform(low=-90, high=90, size=self.nrandom)
        tilt_initial_value = np.random.uniform(low=0, high=90, size=self.nrandom)
        azim_initial_value = np.random.uniform(low=-180, high=180, size=self.nrandom)
        return lat_initial_value, tilt_initial_value, azim_initial_value
