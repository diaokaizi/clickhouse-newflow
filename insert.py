import json
import requests
import json
import datetime
from urllib import parse
import schedule
import time
from clickhouse import connect_client

client = connect_client()

class NetflowObj:
    def __init__(self, start, end, srcIP, dstIP, srcTransportPort, dstTransportPort, transport, bgpSrcAsNumber, bgpDstAsNumber, bytes, packets, flowDirection):
        self.start = start
        self.end = end
        self.srcIP = srcIP
        self.dstIP = dstIP
        self.srcTransportPort = srcTransportPort
        self.dstTransportPort = dstTransportPort
        self.transport = transport
        self.bgpSrcAsNumber = bgpSrcAsNumber
        self.bgpDstAsNumber = bgpDstAsNumber
        self.bytes = bytes
        self.packets = packets
        self.flowDirection = flowDirection
    def __str__(self) -> str:
        return json.dumps(self.__dict__)
    
    def clickhouse_data(self) -> str:
        return list(self.__dict__.values())

def lokiapi(host: str, start: str, end: str, limit: int = None) -> list[NetflowObj]:
    try:
        startdt = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        enddt = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        query_data = {
            'start': startdt.timestamp(),
            'end': enddt.timestamp()
        }
        if limit is not None:
            query_data['limit'] = limit
        query = parse.urlencode(query=query_data)
        base_url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}"
        resp = requests.get(f'{base_url}&{query}')
        # url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}&limit={limit}&start={startdt.timestamp()}&end={enddt.timestamp()}"
        if resp.status_code != 200:
            raise Exception(f"{resp.text}")
        return parse_lokiapi_data(resp.text)
    except:
        return []


def convert_utc_to_local(utc_string):
    # 将UTC字符串转换为local字符串
    utcTime = datetime.datetime.strptime(utc_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    localtime = utcTime + datetime.timedelta(hours=8)
    return localtime

def parse_ipv4_data(newflow) -> NetflowObj:
    return NetflowObj(
        start=convert_utc_to_local(newflow['event']['start']),
        end=convert_utc_to_local(newflow['event']['end']),
        srcIP=newflow['source']['ip'], 
        dstIP=newflow['destination']['ip'], 
        srcTransportPort=newflow['source']['port'],
        dstTransportPort=newflow['destination']['port'],
        transport=newflow['network']['transport'],
        bgpSrcAsNumber=newflow['bgpSrcAsNumber'], 
        bgpDstAsNumber=newflow['bgpDstAsNumber'], 
        bytes=newflow['network']['bytes'], 
        packets=newflow['network']['packets'],
        flowDirection=newflow['flowDirection']
        )

def parse_ipv6_data(newflow) -> NetflowObj:
    return NetflowObj(
        start=convert_utc_to_local(newflow['event']['start']),
        end=convert_utc_to_local(newflow['event']['end']),
        srcIP=newflow['source']['ip'], 
        dstIP=newflow['destination']['ip'], 
        srcTransportPort=newflow['source']['port'],
        dstTransportPort=newflow['destination']['port'],
        transport=newflow['network']['transport'],
        bgpSrcAsNumber=newflow['netflow']['bgp_source_as_number'], 
        bgpDstAsNumber=newflow['netflow']['bgp_destination_as_number'], 
        bytes=newflow['network']['bytes'], 
        packets=newflow['network']['packets'],
        flowDirection=newflow['netflow']['flow_direction']
        )

def parse_lokiapi_data(data) -> list[NetflowObj]:
    try:
        source = json.loads(data)['data']['result'][0]['values']
        newflows = [json.loads(record[1]) for record in source]
        data = []
        for newflow in newflows:
            try:
                if('bgpSrcAsNumber' in newflow.keys()):
                    #ipv4
                    data.append(parse_ipv4_data(newflow))
                else:
                    data.append(parse_ipv6_data(newflow))
            except:
                pass
        return data
    except:
        return []

def get_data_by_5m(host, start) -> list[NetflowObj]:
    end = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    return lokiapi(host=host, start=start, end=end, limit=1000000)

def get_latest_5m_start_datetime():
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    return datetime.datetime.fromtimestamp(timestamp - timestamp % 300 - 300)

def insert_data_clickhouse(host:str):
    start_datetime=get_latest_5m_start_datetime()
    netflowObjs = get_data_by_5m(host=host, start=start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    data = [obj.clickhouse_data() for obj in netflowObjs]
    print('insert start')
    client.insert('netflow', data, column_names=['start', 'end', 'srcIP', 'dstIP', 'srcTransportPort', 'dstTransportPort', 
                                                 'transport', 'bgpSrcAsNumber', 'bgpDstAsNumber', 'bytes', 'packets', 'flowDirection'])
    print('insert finish', start_datetime, len(data))

def creat_table():
    client.command(
        "CREATE TABLE IF NOT EXISTS netflow (\
            start DateTime('Asia/Shanghai'), \
            end DateTime('Asia/Shanghai'), \
            srcIP String, \
            dstIP String, \
            srcTransportPort UInt16, \
            dstTransportPort UInt16, \
            transport String, \
            bgpSrcAsNumber UInt16, \
            bgpDstAsNumber UInt16, \
            bytes UInt32, \
            packets UInt16, \
            flowDirection UInt8, \
            ) ENGINE MergeTree ORDER BY start")

if __name__ == '__main__':
    host = "223.193.36.79:7140"
    creat_table()
    schedule.every(5).minutes.at(":10").do(insert_data_clickhouse, host)
    while True:
        schedule.run_pending()
        time.sleep(1)
# print("table new_table created or exists already!\n")

# row1 = [1000, 'String Value 1000', 5.233]
# row2 = [2000, 'String Value 2000', -107.04]
# data = [row1, row2]
# client.insert('new_table', data, column_names=['key', 'value', 'metric'])

# print("written 2 rows to table new_table\n")

# QUERY = "SELECT max(key), avg(metric) FROM new_table"

# result = client.query(QUERY)

# sys.stdout.write("query: ["+QUERY + "] returns:\n\n")
# print(result.result_rows)