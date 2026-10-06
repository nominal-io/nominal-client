from dateutil import parser
import datetime
import pandas as pd
import numpy as np

dataset_name = "1h_5ms"
start = parser.isoparse("2024-10-22T00:00:00Z")
end = parser.isoparse("2024-10-22T01:00:00Z")
resolution = datetime.timedelta(milliseconds=5)

dt = end - start
n = int((dt.total_seconds() * 1e6 + dt.microseconds) / resolution.microseconds)

np.random.seed(2)
cols = ["value"]

# Generate a fake signal
signal = np.random.normal(0, 0.3, size=n).cumsum() + 50

# Generate many noisy samples from the signal
noise = lambda var, bias, n: np.random.normal(bias, var, n)

data = {c: signal + noise(1, 10 * (np.random.random() - 0.5), n) for c in cols}

# Pick a few samples from the first line and really blow them out
locs = np.random.choice(n, 10)
data["value"][locs] *= 2

data["Time"] = [(start + resolution * i).isoformat() for i in range(n)]

df_gen = pd.DataFrame(data)
df_gen
