import os
import urllib
from dotenv import load_dotenv
import requests
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

endpoint_url = 'https://api.bitflyer.com/'
product_code = 'FX_BTC_JPY'
org = 'takakisan.com'
bucket = 'bf'
load_dotenv(verbose=True)

def fetch_executions():
    payload = {
        'product_code': product_code,
        'count': 100
    }
    r = requests.get(
        urllib.parse.urljoin(endpoint_url, '/v1/getexecutions'),
        params=payload
    )
    executions = r.json()
    client = get_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for e in executions:
        print(e)
        record = {
            'measurement': 'executions',
            'time': e['exec_date'],
            'tags': {
                'product': product_code,
                'side': e['side']
            },
            'fields': {
                'size': e['size'],
                'price': e['price']
            }
        }
        write_api.write(bucket, org, record=record)
    client.close()

def get_client():
    return InfluxDBClient(
        url=os.environ.get('INFLUXDB_URL'),
        token=os.environ.get('INFLUXDB_TOKEN')
    )

if __name__ == '__main__':
    fetch_executions()
