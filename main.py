import hashlib
import logging
import sys

from typing import List
from models import Record, RecordList


def ships_in_san_francisco_bay(records: RecordList) -> RecordList:
    """
    This function takes a RecordList as input and returns a filtered list of records with shipType 'SAILING' and
    lastPositionUpdate latitude and longitude data that puts them in the San Francisco Bay Area Waters.
    """
    def is_in_san_francisco_bay(record: Record) -> bool:
        latitude = record.value["lastPositionUpdate"]["latitude"]
        longitude = record.value["lastPositionUpdate"]["longitude"]
        return 37.5 <= latitude <= 38.2 and -123.0 <= longitude <= -122.2

    return RecordList(
        [record for record in records if record.value["staticData"]["shipType"] == "SAILING" and is_in_san_francisco_bay(record)]
    )


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            # To configure your data stores as resources on the Meroxa Platform
            # use the Meroxa Dashboard, CLI, or Meroxa Terraform Provider.
            # For more details refer to: https://docs.meroxa.com/

            # Identify an upstream data store for your data app
            # with the `resources` function.
            # Replace `source_name` with the resource name the
            # data store was configured with on the Meroxa platform.
            source = await turbine.resources("my-spire")

            # Specify which upstream records to pull
            # with the `records` function.
            # Replace `collection_name` with a table, collection,
            # or bucket name in your data store.
            # If you need additional connector configurations, replace '{}'
            # with the key and value, i.e. {"incrementing.field.name": "id"}
            records = await source.records("*")

            # Specify what code to execute against upstream records
            # with the `process` function.
            # Replace `anonymize` with the name of your function code.
            sailing = await turbine.process(records, ships_in_san_francisco_bay)

            # Identify a downstream data store for your data app
            # with the `resources` function.
            # Replace `destination_name` with the resource name the
            # data store was configured with on the Meroxa platform.
            destination_db = await turbine.resources("sailingwh")

            # Specify where to write records downstream
            # using the `write` function.
            # Replace `collection_archive` with a table, collection,
            # or bucket name in your data store.
            # If you need additional connector configurations, replace '{}'
            # with the key and value, i.e. {"behavior.on.null.values": "ignore"}
            await destination_db.write(sailing, "*")
        except Exception as e:
            print(e, file=sys.stderr)
