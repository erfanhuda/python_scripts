from pathlib import Path
from algobro import SUCCESS, DB_WRITE_ERROR
import os
import datetime
from typing import Optional


DEFAULT_DATA_PATH = Path.home().joinpath(
    "." + Path.home().stem
)
DATA_DIR = os.path.dirname(__file__)

DATA_DIR = os.path.dirname(__file__)
def _default_path():
    date = datetime.datetime.now()
    return {"datetime": DEFAULT_DATA_PATH + date.year + date.month + ".record"}


def _locate_mark_file(dir: str = "X:\\dev\\app\\python_scripts", fileToSearch: str = "requirement.txt") -> list[str]:
    result = []

    for path, dirs, files in os.walk(dir):
        if(fileToSearch in files):
            fullPath = os.path.join(dir, path, fileToSearch)
            result.append(fullPath)

    return result

dir_structure = {
    "dirs": ["accounting"],
    "accounting": ["records", "budget", "data", "controller.ini"],
    "files": [f"{datetime.datetime.now().strftime("%Y-%m")}.trx", f"{datetime.datetime.now().strftime("%Y-%m")}.bud", f"{datetime.datetime.now().strftime("%Y-%m")}.alg"],
    "controller.ini": f"""
        {os.path.dirname(__file__)}
    """,
}

class RecordHandler:
    def __init__(self, db_path: Path = DEFAULT_DATA_PATH) -> None:
        self._db_path = db_path

    def _init_database(self, db_path: Optional[Path]) -> int:
        try:
            db_path.write_text("[]")
            return SUCCESS

        except OSError:
            return DB_WRITE_ERROR


