import unittest
import os
from pathlib import Path
import numpy as np
path = Path.cwd().parent.parent
os.chdir(path)
from pvsystemprofiler.algorithms.longitude.calculation import calculate_longitude


class TestCalculateLongitude(unittest.TestCase):

    def test_calculate_longitude(self):
        # INPUTS

        # eot_duffie
        data_file_path = Path(__file__).parent.parent.joinpath("fixtures/longitude_calculation/eot_duffie_output.csv")
        with open(data_file_path) as file:
                eot_duffie = np.genfromtxt(file, delimiter=',')
        # solarnoon
        data_file_path = Path(__file__).parent.parent.joinpath("fixtures/longitude_calculation/solarnoon.csv")
        with open(data_file_path) as file:
                solarnoon = np.genfromtxt(file, delimiter=',')
        # days
        data_file_path = Path(__file__).parent.parent.joinpath("fixtures/longitude_calculation/days.csv")
        with open(data_file_path) as file:
                days = np.genfromtxt(file, delimiter=',')
        # gmt_offset
        gmt_offset = -5
        # loss
        loss = 'l2'

        # Expected Longitude Output is downloaded directly from https://maps.nrel.gov/pvdaq/ PVDAQ Contributed Sites
        expected_output =  -76.6636

        actual_output = calculate_longitude(eot_duffie, solarnoon, days, gmt_offset)
        np.testing.assert_almost_equal(actual_output, expected_output)


if __name__ == '__main__':
    unittest.main()
