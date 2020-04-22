""" Longitude Direct Calculation Module
This module contains the function for the direct calculation of system
longitude based on estimated local solar noon and timezone offset from UTC.
The same exact equation is used for "Hadghdadi" and "Duffie" approaches.
"""


def calc_lon(solar_noon, eot, gmt_offset):
    sn = solar_noon
    tc = 720 - sn
    lon = (tc - eot) / 4 + 15 * gmt_offset
    return lon