from influxdb_client import InfluxDBClient
from influxdb_client import WriteApi, QueryApi
from influxdb_client import Point
from influxdb_client.client.flux_table import TableList
from influxdb_client.client.write_api import SYNCHRONOUS


# utils

def pp_instr(instr: list) -> str:
    """Parse instructions to string"""
    match instr:
        case ["bucket", const]: return f'from(bucket: "{const}")'
        case ["range", const]: return f'|> range(start: {const})'
        case ["filter", bexpr, mode]: return f'|> filter(fn: (r) => {pp_bexpr(bexpr)}, onEmpty: "{mode}")'
        case ["group", cols, mode]: return f'|> group(columns: {pp_cols(cols)}, mode: "{mode}")'
        case ["sort", cols]: return f'|> sort(columns: {pp_cols(cols)})'
        case ["keep", cols]: return f'|> keep(columns: {pp_cols(cols)})'
        case ["fill", col, value]: return f'|> fill(column: "{(col)}", value: {pp_bexpr(value)})'
        case ["unique", col]: return f'|> unique(column: "{(col)}")'
        case ["count", col]: return f'|> count(column: "{(col)}")'
        case ["sum", col]: return f'|> sum(column: "{(col)}")'
        case ["first", col]: return f'|> first(column: "{(col)}")'
        case ["last", col]: return f'|> last(column: "{(col)}")'
        case ["min", col]: return f'|> min(column: "{(col)}")'
        case ["max", col]: return f'|> max(column: "{(col)}")'
        case ["mode", col]: return f'|> mode(column: "{(col)}")'
        case ["mean", col]: return f'|> mean(column: "{(col)}")'
        case ["median", col]: return f'|> median(column: "{(col)}")'
        case ["window", const, fn]: return f'|> aggregateWindow(every: {const}, fn: {fn})'
        case ["map", aexpr]: return f'|> map(fn: (r) => ({{r with {pp_aexpr(aexpr)}}}))'
        case ["limit", num]: return f'|> limit(n: {num})'
        case ["top", num]: return f'|> top(n: {num})'

def pp_bexpr(bexpr: list) -> str:
    """Parse boolean expresions to string"""
    match bexpr:
        # case 2
        case ["flux", const]: return f'{const}'
        case ["tag", const]: return f'r.{const}'
        # case 3
        case [e1, "==", e2]: return f'{pp_bexpr(e1)} == {pp_bexpr(e2)}'
        case [e1, "!=", e2]: return f'{pp_bexpr(e1)} != {pp_bexpr(e2)}'
        case [e1, "<", e2]: return f'{pp_bexpr(e1)} < {pp_bexpr(e2)}'
        case [e1, ">", e2]: return f'{pp_bexpr(e1)} > {pp_bexpr(e2)}'
        case [e1, "<=", e2]: return f'{pp_bexpr(e1)} <= {pp_bexpr(e2)}'
        case [e1, ">=", e2]: return f'{pp_bexpr(e1)} >= {pp_bexpr(e2)}'
        case [e1, "and", e2]: return f'{pp_bexpr(e1)} and {pp_bexpr(e2)}'
        case [e1, "or", e2]: return f'{pp_bexpr(e1)} or {pp_bexpr(e2)}'
        # case 4
        case ["if", c, t, e]: return f'if {pp_bexpr(c)} then {pp_bexpr(t)} else {pp_bexpr(e)}'
        # case _
        case _: return bexpr if isinstance(bexpr, int) else f'"{bexpr}"'

def pp_aexpr(aexpr: tuple) -> str:
    """Parse arithmetic expresions to string"""
    raise Exception("Not implemented")

def pp_cols(cols: list) -> str:
    """Parse columns to string"""
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

    def _parse(self) -> str:
        """Parse instructions from aggregation pipeline"""
        return "\n".join(pp_instr(instr) for instr in self.pipeline)
    
    def _append(self, instr: tuple) -> 'InfluxQuery':
        """Append a instruction to aggregation pipeline"""
        self.pipeline += [instr]
        return self

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
    
    # Filters
    def range(self, range: str) -> 'InfluxQuery':
        """Filter records in a time range"""
        return self._append(("range", range))
    
    def filter(self, bexpr: tuple, mode: str="drop") -> 'InfluxQuery':
        """Filter records using a boolean expresion"""
        return self._append(("filter", bexpr, mode))
    
    # Order data
    def group(self, columns: list[str], mode: str="by") -> 'InfluxQuery':
        """Group records based on values in specified columns"""
        return self._append(("group", columns, mode))

    def sort(self, columns: list[str]) -> 'InfluxQuery':
        """Sort records based on values in specified columns"""
        return self._append(("sort", columns))
    
    def keep(self, columns: list[str]) -> 'InfluxQuery':
        """Returns records only with the specified columns"""
        return self._append(("keep", columns))
    
    # Operators
    def fill(self, column: str="_value", value: str=0) -> 'InfluxQuery':
        """Replaces all null values with a non-null value"""
        return self._append(("fill", column, value))

    def unique(self, column: str="_value") -> 'InfluxQuery':
        """Find unique values from a specified column"""
        return self._append(("unique", column))
    
    def count(self, column: str="_value") -> 'InfluxQuery':
        """Count values from a specified column"""
        return self._append(("count", column))
    
    def sum(self, column: str="_value") -> 'InfluxQuery':
        """Sum values from a specified column"""
        return self._append(("sum", column))
    
    def first(self, column: str="_value") -> 'InfluxQuery':
        """Returns the first record"""
        return self._append(("first", column))

    def last(self, column: str="_value") -> 'InfluxQuery':
        """Returns the last records"""
        return self._append(("last", column))
    
    def min(self, column: str="_value") -> 'InfluxQuery':
        """Returns the min record"""
        return self._append(("min", column))
    
    def max(self, column: str="_value") -> 'InfluxQuery':
        """Returns the max record"""
        return self._append(("max", column))
    
    def mode(self, column: str="_value") -> 'InfluxQuery':
        """Returns the mode records"""
        return self._append(("mode", column))
    
    def mean(self, column: str="_value") -> 'InfluxQuery':
        """Returns the max record"""
        return self._append(("mean", column))

    def median(self, column: str="_value") -> 'InfluxQuery':
        """Returns the min record"""
        return self._append(("median", column))
    
    # Functions
    def window(self, every: str, fn: str="max") -> 'InfluxQuery':
        """Downsamples records by grouping data into fixed windows"""
        return self._append(("window", every, fn))
    
    def map(self, aexpr: tuple) -> 'InfluxQuery':
        """Iterates over and applies a function to records"""
        return self._append(("map", aexpr))

    # Show data
    def limit(self, num: int) -> 'InfluxQuery':
        """Returns the first n records"""
        return self._append(("limit", num))
    
    def bottom(self, num: int) -> 'InfluxQuery':
        """Returns the bottom n records"""
        return self._append(("bottom", num))
    
    def top(self, num: int) -> 'InfluxQuery':
        """Returns the top n records"""
        return self._append(("top", num))
    
    # Basic filters
    def tag(self, tag: str, value: str, mode: str="drop") -> 'InfluxQuery':
        """Filter records by tag value pair"""
        return self.filter((("tag", tag), "==", value), mode=mode)
    
    def measurement(self, measurement: str) -> 'InfluxQuery':
        """Filter records by measurement"""
        return self.tag("_measurement", measurement)
    
    def field(self, field: str) -> 'InfluxQuery':
        """Filter records by field"""
        return self.tag("_field", field)
