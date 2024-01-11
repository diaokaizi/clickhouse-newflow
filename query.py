from clickhouse import connect_client
import csv
client = connect_client()
# sql = "select toStartOfInterval(start, INTERVAL 1 minute ) as minute,\
#     sum(bytes)/60 as bps\
#     from netflow\
#     where start >= '2024-01-04 12:00:00' and start < '2024-01-04 13:00:00'\
#     group by minute\
#     order by minute"

sql = "SELECT\
            toStartOfInterval(start, toIntervalMinute(1)) AS minute,\
            sum(bytes) / 60 AS bps\
        FROM netflow\
        GROUP BY minute\
        ORDER BY minute ASC"
print(sql)
result = client.query(sql)
print(type(result.result_rows[0]))
with open('result.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(result.result_rows)
