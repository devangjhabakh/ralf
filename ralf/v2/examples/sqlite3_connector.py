from ralf.v2 import LIFO, FIFO, BaseTransform, RalfApplication, RalfConfig, Record, BaseScheduler
from ralf.v2.operator import OperatorConfig, SimpyOperatorConfig, RayOperatorConfig
import time

from typing import List
from collections import defaultdict
from dataclasses import dataclass
import sqlite3


@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float

@dataclass
class SumValue:
    key: str
    value: int  

class FakeSource(BaseTransform):
    def __init__(self, total: int) -> None:
        self.count = 0
        self.total = total
        self.num_keys = 10

    def on_event(self, _: Record) -> List[Record[SourceValue]]:

        if self.count >= self.total: 
            print("completed iteration")
            raise StopIteration()

        self.count += 1
        key = str(self.count % self.num_keys)

        # sleep to slow down send rate
        time.sleep(1)

        # return list of records (wrapped around dataclass)
        return [
            Record(
                entry=SourceValue(
                    key=key,
                    value=self.count, 
                    timestamp=time.time(), 
                ), 
                shard_key=key # key to shard/query 
            )
        ]

class Sum(BaseTransform):

    def __init__(self): 
        self.total = defaultdict(lambda: 0)

    def on_event(self, record: Record) -> Record[SumValue]:
        self.total[record.entry.key] += record.entry.value
        print(f"Record {record.entry.key}, value {self.total[record.entry.key]}")
        return Record(entry=SumValue(key=record.entry.key, value=self.total[record.entry.key]))

class SQLiteConnector(BaseTransform):

    def __init__(self, dbfilename):
        self.dbfilename = dbfilename
        conn = sqlite3.connect(dbfilename) 
        curr = conn.cursor()

        # Creating a test table in `test.db` that holds
        # a key-value pair, for the purposes of our test.
        curr.execute(f"DROP TABLE IF EXISTS test")
        curr.execute(f"CREATE TABLE IF NOT EXISTS test (key INT, value INT)")
        conn.commit()
    
    def on_event(self, record: Record) -> None:
        conn = sqlite3.connect(self.dbfilename) 
        curr = conn.cursor()
        curr.execute(f"INSERT INTO test (key, value) VALUES ({record.entry.key}, {record.entry.value})")
        conn.commit()
        row = curr.execute(f"SELECT value FROM test WHERE key = {record.entry.key}").fetchone()
        print(row)
        return None
        
if __name__ == "__main__":

    deploy_mode = "ray"
    #deploy_mode = "local"
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    source_ff = app.source(
        FakeSource(10000),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    sum_ff = source_ff.transform(
        Sum(),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    # For the below command to work, you will need to have a redis server running
    # on your local machine. To do so, follow the instructions as stated
    # here: https://phoenixnap.com/kb/install-redis-on-mac
    # and stop at the `brew services start redis` step.
    sqlite_ff = sum_ff.transform(
        SQLiteConnector(
            "test.db"
        ),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    app.deploy()
    app.wait()