import os
import glob
from pathlib import Path

CURRENT_DIR = os.path.dirname(__file__)
DEFAULT_DIR = os.path.dirname("first_id")

filename = input("Filename : ")
ext = input("Extension : ")
f = open(".".join([filename, ext]), "a")
f.write(f"[{DEFAULT_DIR}]")
f.close()
