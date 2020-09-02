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
from solardatatools.algorithms import SunriseSunset

class LongitudeStudy():
    def __init__(self, data_handler, gmt_offset=-8, true_value=None):
        """
        Default value for GMT offset is -8 which corresponds to Pacific
        Standard Time, or systems located in California.

        :param data_handler: `DataHandler` class instance loaded with a solar power data set
        :param GMT_offset: The offset in hours between the local timezone and GMT/UTC
        :param true_value: (optional) the ground truth value for the system's longitude
        """
        self.data_handler = data_handler
        if not data_handler._ran_pipeline:
            print('Running DataHandler preprocessing pipeline with defaults')
            self.data_handler.run_pipeline()
        self.data_matrix = self.data_handler.filled_data_matrix
        self.raw_data_matrix = self.data_handler.raw_data_matrix
        self.true_value = true_value
        # Attributes used for all calculations
        self.gmt_offset = gmt_offset
        self.day_of_year = self.data_handler.day_index.dayofyear
        self.eot_duffie = eot_duffie(self.day_of_year)
        self.eot_hag = eot_haghdadi(self.day_of_year)
        # Attributes that change depending on the configuration
        self.solarnoon = None
        self.days = None
        # Results
        self.results = None
        self.best_result = None

    def run(self, data_matrix=('raw', 'filled'),
            estimator=('calculated', 'fit_l1', 'fit_l2', 'fit_huber'),
            eot_calculation=('duffie', 'haghdadi'),
            solar_noon_method=('rise_set_average', 'energy_com', 'optimized', 'measurements'),
            day_selection_method=('all', 'clear', 'cloudy'),
            verbose=True):
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
        :param data_matrix: 'raw', 'filled'
        :param estimator: 'calculated', 'fit_l1', 'fit_l2', 'fit_huber'
        :param eot_calculation: 'duffie', 'haghdadi'
        :param solar_noon_method: 'rise_set_average', 'energy_com', 'optimized', 'measurements'
        :param day_selection_method: 'all', 'clear', 'cloudy'
        :param verbose: show progress bar if True
        :return: None
        """
        results = pd.DataFrame(columns=[
            'longitude', 'estimator', 'eot_calculation', 'solar_noon_method',
            'day_selection_method', 'data matrix'
        ])
        estimator = np.atleast_1d(estimator)
        eot_calculation = np.atleast_1d(eot_calculation)
        solar_noon_method = np.atleast_1d(solar_noon_method)
        day_selection_method = np.atleast_1d(day_selection_method)
        data_matrix = np.atleast_1d(data_matrix)
        total = (len(estimator) * len(eot_calculation) * len(solar_noon_method)
                 * len(day_selection_method) * len(data_matrix))
        counter = 0
        for dm in data_matrix:
            if dm == 'raw':
                data_in = self.raw_data_matrix
            elif dm == 'filled':
                data_in = self.data_matrix
            for sn in solar_noon_method:
                if sn == 'rise_set_average':
                    self.solarnoon = avg_sunrise_sunset(data_in)
                elif sn == 'energy_com':
                    self.solarnoon = energy_com(data_in)
                elif sn == 'optimized':
                    ss = SunriseSunset()
                    ss.run_optimizer(data=data_in)
                    self.solarnoon = np.nanmean([ss.sunrise_estimates, ss.sunset_estimates], axis=0)
                elif sn == 'measurements':
                    ss = SunriseSunset()
                    ss.run_optimizer(data=data_in)
                    self.solarnoon = np.nanmean([ss.sunrise_measurements, ss.sunset_measurements], axis=0)

                for ds in day_selection_method:
                    if ds == 'all':
                        self.days = self.data_handler.daily_flags.no_errors
                    elif ds == 'clear':
                        self.days = self.data_handler.daily_flags.clear
                    elif ds == 'cloudy':
                        self.days = self.data_handler.daily_flags.cloudy
                    for est in estimator:
                        for eot in eot_calculation:
                            if verbose:
                                progress(counter, total)

                            try:
                                lon = self.estimate_longitude(est, eot)
                            except ValueError:
                                lon = np.nan

                            results.loc[counter] = [
                                lon, est, eot, sn, ds, dm
                            ]
                            counter += 1
        if verbose:
            progress(counter, total)
        if self.true_value is not None:
            results['residual'] = self.true_value - results['longitude']
        self.results = results
        if self.true_value is not None:
            best_loc = results['residual'].apply(lambda x: np.abs(x)).argmin()
            self.best_result = results.loc[best_loc]
            self.results = results.loc[np.argsort(np.abs(results['residual']).values)]
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

