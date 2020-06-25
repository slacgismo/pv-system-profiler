import numpy as np

def find_omega(data_sampling, num_days):
        """Omega, the hour angle is estimated as defined on p. 13 in:
        Duffie, John A., and William A. Beckman. Solar engineering of thermal
        processes. New York: Wiley, 1991."""
        hour = np.arange(0, 24, data_sampling / 60)
        omega_1 = np.deg2rad(15 * (hour - 12))
        omega = np.tile(omega_1.reshape(-1, 1), (1, num_days))
        return omega
