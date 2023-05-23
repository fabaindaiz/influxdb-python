import time
from influxdb_client import Point
from influx import InfluxService
from login import URL, TOKEN, ORG


if __name__=="__main__":
    influx_service = InfluxService(url=URL, token=TOKEN, org=ORG)

    bucket_name = "smart_sector_history"

    # Write client
    write_client = influx_service.write_client(bucket=bucket_name)

    '''
    for value in range(5):
        point = (
            Point("measurement1")
            .tag("tagname1", "tagvalue1")
            .field("field1", value)
        )
        write_client.write_sync(point)
        time.sleep(1) # separate points by 1 second
    '''

    # Query client
    query_client = influx_service.query_client(bucket=bucket_name)

    '''
    query = """from(bucket: "smart_sector_history")
    |> range(start: -60m)
    |> filter(fn: (r) => r._measurement == "measurement1")
    |> filter(fn:(r) => r.tagname1 == "tagvalue1")
    |> filter(fn:(r) => r._field == "field1")"""
    tables = query_client._query(query)
    '''
    
    query_client.range("-60m").measurement("measurement1")
    query_client.tag("tagname1", "tagvalue1").field("field1")
    tables = query_client.get()
    
    for table in tables:
        for record in table.records:
            print(record)