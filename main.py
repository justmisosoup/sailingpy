import hashlib
import logging
import sys

from typing import List
from turbine.runtime import Record, RecordList
from turbine.runtime import Runtime

class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("my-spire")
            records = await source.records("")
            destination_db = await turbine.resources("demobucket")
            await destination_db.write(records, "sailingpy_archive")
        except Exception as e:
            print(e, file=sys.stderr)