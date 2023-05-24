from influxdb_client import InfluxDBClient
from influxdb_client import WriteApi, QueryApi
from influxdb_client import Point
from influxdb_client.client.flux_table import TableList
from influxdb_client.client.write_api import SYNCHRONOUS


# utils

def pp_instr(instr: list):
    match instr:
        case ["bucket", const]: return f'from(bucket: "{const}")'
        case ["range", const]: return f'|> range(start: {const})'
        case ["filter", bexpr, mode]: return f'|> filter(fn: (r) => {pp_bexpr(bexpr)}, onEmpty: "{mode}")'
        case ["group", cols, mode]: return f'|> group(columns: {pp_cols(cols)}, mode: "{mode}")'
        case ["sort", cols]: return f'|> sort(columns: {pp_cols(cols)})'
        case ["limit", num]: return f'|> limit(n: {num})'
        case ["map", aexpr]: return f'|> map(fn: (r) => ({{r with {pp_aexpr(aexpr)}}}))'

def pp_bexpr(bexpr: list) -> str:
    match bexpr:
        case ["tag", const]: return f'r.{const}'
        case [e1, "==", e2]: return f'{pp_bexpr(e1)} == {pp_bexpr(e2)}'
        case [e1, "<", e2]: return f'{pp_bexpr(e1)} < {pp_bexpr(e2)}'
        case [e1, ">", e2]: return f'{pp_bexpr(e1)} > {pp_bexpr(e2)}'
        case [e1, "and", e2]: return f'{pp_bexpr(e1)} and {pp_bexpr(e2)}'
        case ["if", c, t, e]: return 
        case _: return f'"{bexpr}"'

def pp_aexpr(aexpr: tuple) -> str:
    raise Exception("Not implemented")

def pp_cols(cols: list) -> str:
    return '[' + f'"{", ".join(cols)}"' + ']'


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
        self._reset()
    
    def _reset(self) -> None:
        """Reset the agregation pipeline"""
        self.pipeline = [("bucket", self.bucket)]
        self.filters = []

    def _parse(self):
        return "\n".join(pp_instr(instr) for instr in self.pipeline)

    def _query(self, query: str) -> TableList:
        """Make the influxdb query with a query string"""
        return self.query_api.query(query=query, org=self.org)
    
    def get(self) -> TableList:
        """Get a TableList with all filtered measurements"""
        return self.aggregate()

    # Aggregation
    def aggregate(self) -> TableList:
        """Get a TableList by applying a agregation pipeline"""
        results = self._query(query=self._parse())
        self._reset()
        return results
    
    def range(self, range: str) -> 'InfluxQuery':
        """Filter measurements in a time range"""
        self.pipeline += [("range", range)]
        return self
    
    def group(self, columns: list[str], mode: str="by"):
        """"""
        self.pipeline += [("group", columns, mode)]
        return self

    def sort(self, columns: list[str]):
        """"""
        self.pipeline += [("sort", columns)]
        return self

    def limit(self, limit: int) -> 'InfluxQuery':
        """"""
        self.pipeline += [("limit", limit)]
        return self
    
    def filter(self, tag: str, value:str, mode: str="drop") -> 'InfluxQuery':
        """Filter records by tag value pair"""
        self.pipeline += [("filter", (("tag", tag), "==", value), mode)]
        return self
    
    def measurement(self, measurement: str) -> 'InfluxQuery':
        """Filter records by measurements"""
        return self.filter("_measurement", measurement)
    
    def field(self, field: str) -> 'InfluxQuery':
        """Filter records by field"""
        return self.filter("_field", field)
