from influxdb_client import InfluxDBClient
from influxdb_client import WriteApi, QueryApi
from influxdb_client import Point
from influxdb_client.client.flux_table import TableList
from influxdb_client.client.write_api import SYNCHRONOUS


# utils

# Clases

class InfluxService():
    """Represents a influxdb connection"""
    def __init__(self, url: str, token: str, org: str) -> None:
        self.client: InfluxDBClient = InfluxDBClient(url=url, token=token, org=org)
        self.org: str = org
    
    def write_client(self, bucket: str) -> 'InfluxWrite':
        """Get a influxdb write client"""
        write_api: WriteApi = self.client.write_api(write_options=SYNCHRONOUS)
        return InfluxWrite(write_api=write_api, bucket=bucket, org=self.org)
    
    def query_client(self, bucket: str) -> 'InfluxQuery':
        """Get a influxdb query client"""
        query_api: QueryApi = self.client.query_api()
        return InfluxQuery(query_api=query_api, bucket=bucket, org=self.org)

class InfluxWrite():
    """Represents a influxdb client that write records"""
    def __init__(self, write_api: WriteApi, bucket: str, org: str) -> None:
        self.write_api: WriteApi = write_api
        self.bucket: str = bucket
        self.org: str = org
    
    def write_record(self, point: str | Point) -> None:
        """Write a point in the database"""
        self.write_api.write(bucket=self.bucket, org=self.org, record=point)

class InfluxQuery():
    """Represents a influxdb client that query records"""
    def __init__(self, query_api: QueryApi, bucket: str, org: str) -> None:
        self.query_api: QueryApi = query_api
        self.bucket: str = bucket
        self.org: str = org

        self.pipeline: list[str] = []
        self.filters: list[str] = []
        self._reset()
    
    def _reset(self) -> None:
        """Reset the agregation pipeline"""
        self.pipeline = [f'from(bucket: "{self.bucket}")\n']
        self.filters = []

    def _query(self, query: str) -> TableList:
        """Make the influxdb query with a query string"""
        return self.query_api.query(query=query, org=self.org)
    
    def get(self) -> TableList:
        """Get a TableList with all filtered measurements"""
        return self.aggregate()

    # Aggregation
    def aggregate(self) -> TableList:
        """Get a TableList by applying a agregation pipeline"""
        query = "".join(self.pipeline)
        results = self._query(query=query)
        self._reset()
        return results
    
    def range(self, range: str) -> 'InfluxQuery':
        """Filter measurements in a time range"""
        self.pipeline += [f'|> range(start: {range})\n']
        return self
    
    def measurement(self, measurement: str) -> 'InfluxQuery':
        """Filter measurements by type"""
        self.filters += [f'|> filter(fn: (r) => r._measurement == "{measurement}")\n']
        return self
    
    def tag(self, tag: str, value:str) -> 'InfluxQuery':
        """Filter measurements by tag value pair"""
        self.filters += [f'|> filter(fn:(r) => r.{tag} == "{value}")\n']
        return self
    
    def field(self, field: str) -> 'InfluxQuery':
        """Filter measurements by field"""
        self.filters += [f'|> filter(fn:(r) => r._field == "{field}")\n']
        return self
