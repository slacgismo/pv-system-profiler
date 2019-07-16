'''
This module contains code for different methods of longitude estimation based only on
PV power output. All modules resurn longitude in degrees West.
'''

import numpy as np
import cvxpy as cvx

def haghdadi(day_of_year, sn_sunpower, com1_sunpower, timezone_GMTdiff):
    B_h = calculate_simple_day_angle_Haghdadi(day_of_year, offset=81)
    E_h = equation_of_time_Haghdadi(B_h)
    sn_longitude_Haghdadi = np.median((sn_sunpower*60 - 720)/4-(E_h/4)) - 15*timezone_GMTdiff
    com_longitude_Haghdadi = np.nanmedian((com1_sunpower*60 - 720)/4-(E_h/4)) - 15*timezone_GMTdiff
    return sn_longitude_Haghdadi, com_longitude_Haghdadi

def duffie(day_of_year, sn_sunpower, com1_sunpower, timezone_GMTdiff):
    sn_longitude_Duffie = (sn_sunpower*60 +E -720 - 4*15*timezone_GMTdiff)/4
    com_longitude_Duffie = (com1_sunpower*60 +E -720 - 4*15*timezone_GMTdiff)/4
    mediam_sn_longitude_Duffie = np.nanmedian(sn_longitude_Duffie)
    mediam_com_longitude_Duffie = np.nanmedian(com_longitude_Duffie)
    return mediam_sn_longitude_Duffie, mediam_com_longitude_Duffie

def fitting_norm1(timezone_GMTdiff):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.norm1(sn_h - sn_sunpower)
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_norm(timezone_GMTdiff):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.norm(sn_h - sn_sunpower)
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_huber(timezone_GMTdiff):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.sum(cvx.huber(sn_h - sn_sunpower))
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_norm1_cl_days(timezone_GMTdiff, days):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.norm1(sn_h[days] - sn_sunpower[days])
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_norm_cl_days(timezone_GMTdiff, days):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.norm(sn_h[days] - sn_sunpower[days])
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_huber_cl_days(timezone_GMTdiff, days):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.sum(cvx.huber(sn_h[days] - sn_sunpower[days]))
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_scsf_norm1(sn_clear):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.norm1(sn_h - sn_clear)
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_scsf_norm(sn_clear):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.norm(sn_h - sn_clear)
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

def fitting_scsf_huber(sn_clear):
    lon = cvx.Variable()
    sn_m = 4*(15*timezone_GMTdiff - lon)-E+720
    sn_h = sn_m / 60
    cost = cvx.sum(cvx.huber(sn_h - sn_clear))
    objective = cvx.Minimize(cost)
    problem = cvx.Problem(objective)
    problem.solve()
    lon_value = -lon.value
    return lon_value

if __name__ == "__main__":
    main()
