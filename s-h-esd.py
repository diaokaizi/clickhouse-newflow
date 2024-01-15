
# Synthetic "weekly" seasonality time-series
x = np.array([10, 20, 30, 40, 50, 60, 70, 10, 20, 30, 40, 50, 60, 70, 10, 20, 30, 40, 50, 60, 70, 10, 20, 30, 40, 50, 60, 70, 10, 20, 30, 40, 50, 60, 70])
# Synthetic anomalies
x[[0, 12, 17, 25, 30]] = 0

# Use the algorithm to find anomalies
anomalies = esd_test(x, 7, alpha=0.95, ub=0.499, hybrid=True)

# Plot the anomalies
fig, ax = pyplot.subplots()
ax.plot(pd.Series(x).index, x, color="blue", label = "Original")
ax.scatter(anomalies, x[anomalies], color='red', label='Anomaly')
pyplot.legend(loc="best")
pyplot.show()