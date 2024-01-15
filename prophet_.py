import pandas as pd
import time
from fbprophet import Prophet
import csv
from datetime import datetime
# csv_reader = csv.reader(open("result.csv"))
# data = []
# for row in csv_reader:
#     data.append([row[0][:-6], row[1]])
# with open("file.csv", "a", encoding="utf-8", newline="") as f:
#     csv_writer = csv.writer(f)
#     name=['ds','y']
#     csv_writer.writerow(name)
#     csv_writer.writerows(data)
#     f.close()
# print(len(data[1:]))
# print(len(data))
df = pd.read_csv(r'./file.csv')
# df.rename(columns={'ds':'ds','value':'ds'}, inplace=True)
# df['ds'] = pd.to_datetime(df['ds'],format='%Y-%m-%d %H:%M:%S%z')#修改日期字段格式
# df['ds'].apply(lambda x:time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(str(x),'%Y-%m-%d %H:%M:%S%z')))
# print(str(df['ds'][0]))
print(type(df['ds']))
# df['ds'] = df['ds'].astype(np.float)
m = Prophet(changepoint_range=0.9
		#,seasonality_mode='multiplicative'
			,seasonality_prior_scale=11
			,daily_seasonality=True)
# m.add_seasonality(name='monthly', period=30.5, fourier_order=5)
forecast = m.fit(df).predict()
from fbprophet.plot import add_changepoints_to_plot
fig = m.plot(forecast)
fig.savefig('temp2.png')
# a = add_changepoints_to_plot(fig.gca(), m, forecast)
# print(type(fig))
future = m.make_future_dataframe(periods=1440, freq='min')
forecast = m.predict(future)
fig1 = m.plot(forecast)
fig1.savefig('temp3.png')