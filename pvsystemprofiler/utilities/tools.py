import numpy as np
import pandas as pd


def day_of_year_finder(index):
    doy = pd.DatetimeIndex(np.unique(index.date)).dayofyear.values
    return (doy)


def equation_of_time_da_rosa(beta):
    """
    Equation of time from Haghdadi et al. (2017).The coefficients correspond to
    the journal publication (reference below).

    Paremeters
    -------
    beta: day angle for the Earth's orbit around the Sun, calculated by calculate_simple_day_angle_da_rosa.

    Returns
    -------
    equation_of_time : list of numberic values with length same as power signals day length.
                       Difference in time between solar time and mean solar time in minutes.

    References
    -------
    Haghdadi, Navid, et al. "A method to estimate the location and orientation of
    distributed photovoltaic systems from their generation output data." Renewable Energy 108 (2017): 390-400.
    """

    # eot = (9.87 * np.sin(2.0 * beta)) - (7.53 * np.cos(beta)) - (1.5* np.sin(beta))
    eot = (9.87 * np.sin(2.0 * beta * np.pi / 180)) - (7.53 * np.cos(beta * np.pi / 180)) - (
                1.5 * np.sin(beta * np.pi / 180))
    return eot


def calculate_simple_day_angle_da_rosa(dayofyear, offset=81):
    """
    Calculates the day angle for the Earth's orbit around the Sun.

    Parameters
    ----------
    dayofyear : list of numeric values, calculated by day_of_year_finder.

    Returns
    -------
    day_angle : list of numeric values
    """
    return (360 / 365.) * (dayofyear - offset)


def equation_of_time_Duffie(beta):
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

    eot = (1440.0 / 2 / np.pi) * (
            0.000075 +
            0.001868 * np.cos(beta) - 0.032077 * np.sin(beta) -
            0.014615 * np.cos(2.0 * beta) - 0.040849 * np.sin(2.0 * beta)
    )
    return eot


def calculate_simple_day_angle_Duffie(dayofyear, offset=1):
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
    return ((2. * np.pi) / 365.) * (dayofyear - offset)


def random_initial_values(nrandom):
    lat_initial_value = np.random.uniform(low=-90, high=90, size=nrandom)
    tilt_initial_value = np.random.uniform(low=0, high=90, size=nrandom)
    azim_initial_value = np.random.uniform(low=-180, high=180, size=nrandom)
    return lat_initial_value, tilt_initial_value, azim_initial_value
