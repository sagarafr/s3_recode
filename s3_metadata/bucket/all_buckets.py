from dataclasses import dataclass
from typing import List


@dataclass
class Bucket:
    name: str
    creation_date: str

    def to_dict(self):
        return {
            "Name": self.name,
            "CreationDate": self.creation_date
        }


@dataclass
class Buckets:
    buckets: List[Bucket]

    def to_dict(self):
        return {
            "Buckets": {
                "Bucket": [bucket.to_dict() for bucket in self.buckets]
            }
        }
