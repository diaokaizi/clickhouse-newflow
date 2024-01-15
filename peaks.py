from clickhouse import connect_client
import numpy as np
import matplotlib.pyplot as plt
def AMPD(data):
    """
    实现AMPD算法
    :param data: 1-D numpy.ndarray 
    :return: 波峰所在索引值的列表
    """
    p_data = np.zeros_like(data, dtype=np.int32)
    count = data.shape[0]
    arr_rowsum = []
    for k in range(1, count // 2 + 1):
        row_sum = 0
        for i in range(k, count - k):
            if data[i] > data[i - k] and data[i] > data[i + k]:
                row_sum -= 1
        arr_rowsum.append(row_sum)
    min_index = np.argmin(arr_rowsum)
    max_window_length = min_index
    for k in range(1, max_window_length + 1):
        for i in range(k, count - k):
            if data[i] > data[i - k] and data[i] > data[i + k]:
                p_data[i] += 1
    return np.where(p_data == max_window_length)[0]

client = connect_client()
sql = "select toStartOfInterval(start, INTERVAL 1 minute ) as minute,\
    sum(bytes)/60 as bps\
    from netflow\
    where start >= timestamp_sub(hour, 5, now())\
    group by minute\
    order by minute\
    "
result = client.query(sql)
data = np.array(result.result_rows)

y = data[:,1]
plt.plot(range(len(y)), y)
px = AMPD(y)
plt.scatter(px, y[px], color="red")
plt.savefig('example.png')