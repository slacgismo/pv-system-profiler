# Standard Imports
import numpy as np
import cvxpy as cvx


def filter_data(data_matrix, daytime_threshold, x1=None, x2=None):
    if daytime_threshold is None:
        daytime_threshold_fit = find_daytime_threshold_quantile_seasonality(data_matrix, x1, x2)
        boolean_daytime = data_matrix > daytime_threshold_fit
    else:
        boolean_daytime = data_matrix > daytime_threshold
    return boolean_daytime


def find_daytime_threshold_quantile_seasonality(data_matrix, p1, p2):
    m = cvx.Parameter(nonneg=True, value=10 ** 6)
    # setting local quantile for 10% of the data
    t = cvx.Parameter(nonneg=True, value=p1)
    y = np.quantile(data_matrix, p2, axis=0)
    x1 = cvx.Variable(len(y))
    x2 = cvx.Variable(len(y))
    if data_matrix.shape[1] > 365:
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
