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
        write_client.write_record(point)
        time.sleep(1) # separate points by 1 second
    '''

    # Query client
    query_client = influx_service.query_client(bucket=bucket_name)
    
    # Range, must go first
    query_client.range("-10d")
    
    # Filters
    query_client.filter("tagname1", "tagvalue1")
    query_client.measurement("measurement1").field("field1")

    # Show
    #query_client.group(["_value"])
    query_client.sort(["_value"])
    query_client.limit(5)

    # Print query
    print(query_client._parse())
    
    if True:
        # Query records
        tables = query_client.get()

        # Print records
        for table in tables:
            for record in table.records:
                print(record)
