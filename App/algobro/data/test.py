import os

CURRENT_DIR = os.path.dirname(__file__)

with open(CURRENT_DIR + "\\2023-12.transaction") as w:
    print(w.read())