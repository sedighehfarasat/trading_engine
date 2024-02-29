import numpy as np
import pandas as pd


def create_sharpe_ratio(returns, periods=252):
    """
    Create the annualized Sharpe ratio for the strategy, based on a benchmark of zero (no risk-free rate information).
    Parameters:
    returns - A pandas Series representing period percentage returns.
    periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
    """

    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)


def create_drawdown(pnl):
    """
    Calculate the largest peak-to-trough drawdown of the PnL curve as well as the duration of the drawdown.
    Parameters:
    pnl - A pandas Series representing period percentage returns.
    Returns: drawdown, duration - Highest peak-to-trough drawdown and duration.
    """

    hwm = [0]  # the High WaterMark
    idx = pnl.index
    drawdown = pd.Series(index=idx)
    duration = pd.Series(index=idx)

    for t in range(1, len(idx)):
        hwm.append(max(hwm[t - 1], pnl[t]))
        drawdown[t] = (hwm[t] - pnl[t])
        duration[t] = (0 if drawdown[t] == 0 else duration[t - 1] + 1)

    return drawdown, drawdown.max(), duration.max()  # TODO: duration.max() is not drawdown duration.
