import influxdb_client
from influxdb_client import InfluxDBClient
from influxdb_client import WriteApi, QueryApi
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS


# utils

# Clases

class InfluxService():
    """Represents a influxdb connection"""
    def __init__(self, url: str, token: str, org: str) -> None:
        self.client: InfluxDBClient = InfluxDBClient(url=url, token=token, org=org)
        self.org: str = org
    
    def write_client(self, name: str) -> 'InfluxWrite':
        write_api: WriteApi = self.client.write_api(write_options=SYNCHRONOUS)
        return InfluxWrite(write_api=write_api, bucket=name, org=self.org)
    
    def query_client(self, name: str) -> 'InfluxQuery':
        query_api: QueryApi = self.client.query_api()
        return InfluxQuery(query_api=query_api, bucket=name, org=self.org)

class InfluxWrite():
    def __init__(self, write_api: WriteApi, bucket: str, org: str) -> None:
        self.write_api: WriteApi = write_api
        self.bucket: str = bucket
        self.org: str = org
    
    def write_point(self, point: Point) -> None:
        self.write_api.write(bucket=self.bucket, org=self.org, record=point)

class InfluxQuery():
    def __init__(self, query_api: QueryApi, bucket: str, org: str) -> None:
        self.query_api: QueryApi = query_api
        self.bucket: str = bucket
        self.org: str = org

        self.pipeline: list = []
    
    def _query(self) -> str:
        query = ""
    
    def _reset(self) -> None:
        self.pipeline = []

    def aggregate(self):
        query = None
        results = self.query_api.query(org=self.org, query=query)
        self._reset()
        return results

    def get(self) -> list:
        return list(self.aggregate())
        

        



