import logging
import sys

from typing import List
from turbine.runtime import Record, RecordList
from turbine.runtime import Runtime


def ships_in_san_francisco_bay(records: RecordList) -> RecordList:

    # This function takes a RecordList as input and returns a filtered list of records with shipType 'SAILING' and
    # lastPositionUpdate latitude and longitude data that puts them in the San Francisco Bay Area Waters.

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
            source = await turbine.resources("my-spire")
            records = await source.records(" ")
            sailing = await turbine.process(records, ships_in_san_francisco_bay)
            destination_db = await turbine.resources("webhook")
            await destination_db.write(sailing, " ")
        except Exception as e:
            print(e, file=sys.stderr)