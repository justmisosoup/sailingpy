import hashlib
import logging
import sys

from typing import List
from turbine.runtime import Record, RecordList
from turbine.runtime import Runtime

def ships_in_san_francisco_bay(records: RecordList) -> RecordList:

    # This function takes a RecordList as input and returns a filtered list of records with shipType 'SAILING' and
    # lastPositionUpdate latitude and longitude data that puts them in the San Francisco Bay Area Waters.

    def is_in_san_francisco_bay(record: Record) -> bool:
        payload = record.value["payload"]
        if record.value["after"] is not None:
            payload = record.value["after"]
        latitude = payload["lastPositionUpdate"]["latitude"]
        longitude = payload["lastPositionUpdate"]["longitude"]
        return 37.5 <= latitude <= 38.2 and -123.0 <= longitude <= -122.2

    rl = RecordList()
    
    try:
        for record in records:
            
            payload = record.value["payload"]
            if record.value["after"] is not None:
                payload = record.value["after"]
            if payload["staticData"]["shipType"]  == "SAILING" and is_in_san_francisco_bay(record):
                rl.append(record)
    except Exception as e:
        print(e, file=sys.stderr)
    return rl

class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("my-spire")
            records = await source.records("")
            sailing = await turbine.process(records, ships_in_san_francisco_bay)
            destination_db = await turbine.resources("demobucket")
            await destination_db.write(sailing, "spire_archive_processed")
        except Exception as e:
            print(e, file=sys.stderr)        