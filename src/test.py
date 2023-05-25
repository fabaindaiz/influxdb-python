import time
from influxdb_client import Point
from influx import InfluxService
from login import URL, TOKEN, ORG


if __name__=="__main__":
    influx_service = InfluxService(url=URL, token=TOKEN, org=ORG)

    bucket_name = "smart_sector_history"

    # Write client
    write_client = influx_service.write_client(bucket=bucket_name)

    if False:
        for value in range(5):
            point = (
                Point("measurement1")
                .tag("tagname1", "tagvalue1")
                .field("field1", value)
            )
            write_client.write_record(point)
            time.sleep(1) # separate points by 1 second


    # Query client
    query_client = influx_service.query_client(bucket=bucket_name)
    
    # Range, must go first
    query_client.range("-30d")
    
    # Basic filters
    query_client.tag("tagname1", "tagvalue1")
    query_client.measurement("measurement1").field("field1")

    # Advanced filters
    query_client.filter((("tag", "tagname1"), "!=", "tagvalue2"))

    # Operators
    #query_client.unique("_value")
    #query_client.count()
    #query_client.sum()

    # Order data
    #query_client.group(["_value"])
    query_client.sort(["_value"])

    # Show data
    query_client.limit(5)
    #query_client.top(5)

    # Print query
    print(query_client._parse())
    
    if True:
        tables = query_client.get()
        for table in tables:
            for record in table.records:
                print(record)
