import pandas as pd
s_train = pd.read_csv("result.csv", index_col=0, parse_dates=True).squeeze(True)
from adtk.data import validate_series
s_train = validate_series(s_train)
from adtk.visualization import plot
plot(s_train)