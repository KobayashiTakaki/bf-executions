import os
import time
import urllib
from multiprocessing import Process, Queue
from dotenv import load_dotenv
import requests
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

endpoint_url = 'https://api.bitflyer.com/'
product_code = 'FX_BTC_JPY'
org = 'takakisan.com'
bucket = 'bf'
load_dotenv(verbose=True)

def fetch_executions(q):
    payload = {
        'product_code': product_code,
        'count': 1000
    }
    last_id = 0
    while True:
        if last_id != 0:
            payload['after'] = last_id
        try:
            r = requests.get(
                urllib.parse.urljoin(endpoint_url, '/v1/getexecutions'),
                params=payload
            )
        except requests.exceptions.RequestException as e:
            print('Error in requests: ', e)
            time.sleep(6)
            continue
        if r.status_code != 200:
            time.sleep(6)
            continue
        executions = r.json()
        q.put(executions)
        ids = [e['id'] for e in executions]
        if len(ids) > 0:
            last_id = max(ids)
        time.sleep(6)

def get_client():
    return InfluxDBClient(
        url=os.environ.get('INFLUXDB_URL'),
        token=os.environ.get('INFLUXDB_TOKEN')
    )

if __name__ == '__main__':
    q = Queue()
    p = Process(target=fetch_executions, args=(q,))
    client = get_client()
    p.start()
    with client.write_api(write_options=SYNCHRONOUS) as write_api:
        while True:
            executions = q.get()
            for e in executions:
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
                try:
                    write_api.write(bucket, org, record=record)
                except Exception as e:
                    print(e)
                    continue
            time.sleep(1)
