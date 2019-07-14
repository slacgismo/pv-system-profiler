import numpy as np
from solardatatools.data_transforms import make_2d
import pandas as pd
import cvxpy as cvx

def day_of_year_finder(index):
    doy = pd.DatetimeIndex(np.unique(index.date)).dayofyear.values
    return(doy)

def equation_of_time_Duffie(beta):
    """
    Equation of time from Duffie & Beckman and attributed to Spencer
    (1971) and Iqbal (1983).

    The coefficients correspond to the online copy of the `Fourier
    paper`_ [1]_ in the Sundial Mailing list that was posted in 1998 by
    Mac Oglesby from his correspondence with Macquarie University Prof.
    John Pickard who added the following note.

        In the early 1970s, I contacted Dr Spencer about this method because I
        was trying to use a hand calculator for calculating solar positions,
        etc. He was extremely helpful and gave me a reprint of this paper. He
        also pointed out an error in the original: in the series for E, the
        constant was printed as 0.000075 rather than 0.0000075. I have
        corrected the error in this version.

    There appears to be another error in formula as printed in both
    Duffie & Beckman's [2]_ and Frank Vignola's [3]_ books in which the
    coefficient 0.04089 is printed instead of 0.040849, corresponding to
    the value used in the Bird Clear Sky model implemented by Daryl
    Myers [4]_ and printed in both the Fourier paper from the Sundial
    Mailing List and R. Hulstrom's [5]_ book.

    .. _Fourier paper: http://www.mail-archive.com/sundial@uni-koeln.de/msg01050.html

    Parameters
    ----------
    dayofyear : numeric

    Returns
    -------
    equation_of_time : numeric
        Difference in time between solar time and mean solar time in minutes.

    References
    ----------
    .. [1] J. W. Spencer, "Fourier series representation of the position of the
       sun" in Search 2 (5), p. 172 (1971)

    .. [2] J. A. Duffie and W. A. Beckman,  "Solar Engineering of Thermal
       Processes, 3rd Edition" pp. 9-11, J. Wiley and Sons, New York (2006)

    .. [3] Frank Vignola et al., "Solar And Infrared Radiation Measurements",
       p. 13, CRC Press (2012)

    .. [5] Daryl R. Myers, "Solar Radiation: Practical Modeling for Renewable
       Energy Applications", p. 5 CRC Press (2013)

    .. [4] Roland Hulstrom, "Solar Resources" p. 66, MIT Press (1989)

Elpiniki comment: 0.000075 --> 1 zero different from Duffie !!!!! 0.0000075
    See Also
    --------
    equation_of_time_pvcdrom
    """
    #day_angle = _calculate_simple_day_angle(dayofyear)
    # convert from radians to minutes per day = 24[h/day] * 60[min/h] / 2 / pi
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
    day_angle : numeric
    """
    return ((2. * np.pi) / 365.) * (dayofyear-offset)

def calculate_simple_day_angle_Haghdadi(dayofyear, offset=81):
    """
    Calculates the day angle for the Earth's orbit around the Sun.

    Parameters
    ----------
    dayofyear : numeric
    offset : int, default 1
        For the Spencer method, offset=1; for the ASCE method, offset=0

    Returns
    -------
    day_angle : numeric
    """
    return (360 / 365.) * (dayofyear-offset)

def equation_of_time_Haghdadi(beta):
    #day_angle = _calculate_simple_day_angle(dayofyear)
    # convert from radians to minutes per day = 24[h/day] * 60[min/h] / 2 / pi
    eot = (9.87 * np.sin(2.0 * beta)) - (7.53 * np.cos(beta)) - (1.5* np.sin(beta))
    return eot

def local_median_filter(signal, c1=1e3, solver="MOSEK"):
    x = cvx.Variable(len(signal))
    cost = cvx.norm1(signal - x) + c1 * cvx.norm(cvx.diff(x, k=2))
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve(solver=solver)
    return x.value
