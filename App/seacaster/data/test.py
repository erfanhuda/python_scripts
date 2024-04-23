import csv
from pathlib import Path

with open("test.procedure") as w:
    file = csv.DictReader(w)
    print(file)