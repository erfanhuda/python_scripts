from pathlib import Path
from algobro import SUCCESS, DB_WRITE_ERROR
import os

DEFAULT_DATA_PATH = Path.home().joinpath(
    "." + Path.home().stem + "_record.json"
)

DATA_DIR = os.path.dirname(__file__)

class RecordHandler:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def _init_database(self, db_path: Path) -> int:
        try:
            db_path.write_text("[]")
            return SUCCESS
        except OSError:
            return DB_WRITE_ERROR
