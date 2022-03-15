from ralf.v2 import LIFO, FIFO, BaseTransform, RalfApplication, RalfConfig, Record, BaseScheduler
from ralf.v2.operator import OperatorConfig, SimpyOperatorConfig, RayOperatorConfig
import time

from typing import List
from collections import defaultdict
from dataclasses import dataclass
import redis


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

class RedisConnector(BaseTransform):

    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password
    
    def on_event(self, record: Record) -> Record[SumValue]:
        r = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password
        )
        r.set(record.entry.key, record.entry.value)
        print(f"Record {record.entry.key}, value {r.get(record.entry.key)}")
        return Record(entry=SumValue(key=record.entry.key, value=r.get(record.entry.key)))

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
    redis_ff = sum_ff.transform(
        RedisConnector(
            host='127.0.0.1',
            port='6379',
            password=''
        ),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    app.deploy()
    app.wait()