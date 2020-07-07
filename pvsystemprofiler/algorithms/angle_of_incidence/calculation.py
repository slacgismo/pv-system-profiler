from scipy.optimize import curve_fit
import numpy as np
import cvxpy as cvx

def run_curve_fit(func, delta, omega, costheta, boolean_daytime_range, init_values):
        costheta_fit = costheta[boolean_daytime_range]
        x = np.array([delta, omega])
        popt, pcov = curve_fit(func, x, costheta_fit, p0=np.deg2rad(init_values),
                               bounds=([0, -3.14], [1.57, 3.14]))
        tilt_estimate, azimuth_estimate = np.degrees(popt)
        return tilt_estimate, azimuth_estimate

def find_fit_costheta(data_matrix, clear_index):
        data = np.max(data_matrix, axis=0)
        s1 = cvx.Variable(len(data))
        s2 = cvx.Variable(len(data))
        cost = 1e1 * cvx.norm(cvx.diff(s1, k=2), p=2) + cvx.norm(s2[clear_index])
        objective = cvx.Minimize(cost)
        constraints = [
            data == s1 + s2,
            s1[365:] == s1[:-365]
        ]
        problem = cvx.Problem(objective, constraints)
        problem.solve(solver='MOSEK')
        scale_factor_costheta = s1.value
        costheta_fit = data_matrix / np.max(s1.value)
        return scale_factor_costheta, costheta_fit

