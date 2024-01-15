import statsmodels.api as sm
import matplotlib.pyplot as plt
import pandas as pd
#from dateutil import get_gran, format_timestamp
 
data = pd.read_csv('result.csv',usecols=['time', 'value'],index_col='time')#将时间列作为索引
print(data)
rd = sm.tsa.seasonal_decompose(data['value'].values, period=1440)#该时间序列周期为72
rd.plot()
plt.savefig('savefig_example.png')
plt.show()