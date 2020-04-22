''' Longitude Study Module
This module contains a class for conducting a study of different approaches
to estimating longitude from solar power data. This code accepts solar power
data in the form of a `solar-data-tools` `DataHandler` object, which is used
to standardize and pre-process the data. The provided class will then estimate
the longitude of the site that produced the data, using configurations that can
be set in the `run` method. The basic concept is to estimate solar noon for
each day based on the measured data, and then use the relationship between
standard time, solar time, and the equation of time to estimate the longitude.
The following configurations can be run:

 - Equation of time (EoT) estimator: Duffie or Haghdadi
 - Estimation algorithm: calculation from EoT definition, curve fitting with
   L2 loss, curve fitting with L1 loss, or curve fitting with Huber loss
 - Method for solar noon estimation: average of sunrise and sunset or the
   energy center of mass
 - Method for day selection: all days, sunny/clear days, cloudy days

'''
import numpy as np
import pandas as pd
import cvxpy as cvx
from solardatatools.solar_noon import energy_com, avg_sunrise_sunset
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon
from pvsystemprofiler.utilities.equation_of_time import eot_haghdadi, eot_duffie
from pvsystemprofiler.utilities.progress import progress

class LongitudeStudy():
    def __init__(self, data_handler, GMT_offset=8, true_value=None):
        """

        :param data_handler: `DataHandler` class instance loaded with a solar power data set
        :param GMT_offset: The offset in hours between the local timezone and GMT/UTC
        :param true_value: (optional) the ground truth value for the system's longitude
        """
        self.data_handler = data_handler
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.data_matrix = self.data_handler.filled_data_matrix
        self.true_value = true_value
        # Attributes used for all calculations
        self.gmt_offset = GMT_offset
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.eot_duffie = eot_duffie(self.day_of_year)
        self.eot_hag = eot_haghdadi(self.day_of_year)
        # Attributes that change depending on the configuration
        self.solarnoon = None
        self.days = None
        # Results
        self.results = None
        self.best_result = None

    def run(self, estimator=('calculated', 'fit_l1', 'fit_l2', 'fit_huber'),
            eot_calculation=('duffie', 'haghdadi'),
            solar_noon_method=('rise_set_average', 'energy_com'),
            day_selection_method=('all', 'clear', 'cloudy')):
        """
        Run a study with the given configuration of options. Defaults to
        running all available options. Any kwarg can be constrained by
        providing a subset of acceptable keys. For example the default keys
        for the estimator kwarg are:

            ('calculated', 'fit_l1', 'fit_l2', 'fit_huber')

        Additionally, any of the following would be acceptable for this kwarg:

            ('calculated', 'fit_l1', 'fit_l2', 'fit_huber')
            ('fit_l2', 'fit_huber')
            ('fit_l2',)
            'fit_l2'

        This method sets the `results` attribute to be a pandas data frame
        containing the results of the study. If a ground truth value was
        provided to the class constructor, the best result will be assigned
        to the `best_result` attribute.

        :param estimator: 'calculated', 'fit_l1', 'fit_l2', 'fit_huber'
        :param eot_calculation: 'duffie', 'haghdadi'
        :param solar_noon_method: 'rise_set_average', 'energy_com'
        :param day_selection_method: 'all', 'clear', 'cloudy'
        :return: None
        """
        results = pd.DataFrame(columns=[
            'longitude', 'estimator', 'eot_calculation', 'solar_noon_method',
            'day_selection_method'
        ])
        estimator = np.atleast_1d(estimator)
        eot_calculation = np.atleast_1d(eot_calculation)
        solar_noon_method = np.atleast_1d(solar_noon_method)
        day_selection_method = np.atleast_1d(day_selection_method)
        total = (len(estimator) * len(eot_calculation) * len(solar_noon_method)
                 * len(day_selection_method))
        counter = 0
        for sn in solar_noon_method:
            if sn == 'rise_set_average':
                self.solarnoon = avg_sunrise_sunset(self.data_matrix)
            elif sn == 'energy_com':
                self.solarnoon = energy_com(self.data_matrix)
            for ds in day_selection_method:
                if ds == 'all':
                    self.days = np.ones(self.data_matrix.shape[1],
                                        dtype=np.bool)
                elif ds == 'clear':
                    self.days = self.data_handler.daily_flags.clear
                elif ds == 'cloudy':
                    self.days = self.data_handler.daily_flags.cloudy
                for est in estimator:
                    for eot in eot_calculation:
                        progress(counter, total)
                        lon = self.estimate_longitude(est, eot)
                        results.loc[counter] = [
                            lon, est, eot, sn, ds
                        ]
                        counter += 1
        progress(counter, total)
        if self.true_value is not None:
            results['residual'] = self.true_value - results['longitude']
        self.results = results
        if self.true_value is not None:
            best_loc = results['residual'].apply(lambda x: np.abs(x)).argmin()
            self.best_result = results.loc[best_loc]
        return

    def estimate_longitude(self, estimator, eot_calculation):
        if estimator == 'calculated':
            return self.calculate_longitude(eot_ref=eot_calculation)
        else:
            loss = estimator.split('_')[-1]
            return self.fit_longitude(loss=loss, eot_ref=eot_calculation)

    def calculate_longitude(self, eot_ref='duffie'):
        sn = 60 * self.solarnoon[self.days]  # convert hours to minutes
        if eot_ref in ('duffie', 'd', 'duf'):
            eot = self.eot_duffie[self.days]
        elif eot_ref in ('haghdadi', 'h', 'hag'):
            eot = self.eot_hag[self.days]
        gmt = self.gmt_offset
        estimates = calc_lon(sn, eot, gmt)
        return np.nanmedian(estimates)


    def fit_longitude(self, loss='l2', eot_ref='duffie'):
        lon = cvx.Variable()
        if loss == 'l2':
            cost_func = cvx.norm
        elif loss == 'l1':
            cost_func = cvx.norm1
        elif loss == 'huber':
            cost_func = lambda x: cvx.sum(cvx.huber(x))
        if eot_ref in ('duffie', 'd', 'duf'):
            eot = self.eot_duffie
        elif eot_ref in ('haghdadi', 'h', 'hag'):
            eot = self.eot_hag
        sn_m = 720 - eot + 4 * (15 * self.gmt_offset - lon)
        sn_h = sn_m / 60
        nan_mask = np.isnan(self.solarnoon)
        use_days = np.logical_and(self.days, ~nan_mask)
        cost = cost_func(sn_h[use_days] - self.solarnoon[use_days])
        objective = cvx.Minimize(cost)
        problem = cvx.Problem(objective)
        problem.solve()
        return lon.value.item()

