from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

DEFAULT_INPUT_DATA_PATH = Path.home().joinpath("." + Path.home().stem + "\data")

@dataclass
class Ledger:
    _parent: str
    _child: str

@dataclass
class Record:
    _postname: str
    _description: str
    _ledger: Ledger


@dataclass
class Budget:
    _name: str
    description: str
    records: list[dict]


@dataclass
class Transaction:
    datetime: datetime
    description: str
    location: str
    budget: str
    records: list[dict]
