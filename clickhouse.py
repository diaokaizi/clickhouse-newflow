import clickhouse_connect

def connect_client():
    CLICKHOUSE_CLOUD_HOSTNAME = '127.0.0.1'
    CLICKHOUSE_CLOUD_USER = 'default'
    CLICKHOUSE_CLOUD_PASSWORD = '123456'
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_CLOUD_HOSTNAME, port=8123, username=CLICKHOUSE_CLOUD_USER, password=CLICKHOUSE_CLOUD_PASSWORD)
    return client