import os
import glob
from pathlib import Path

CURRENT_DIR = os.path.dirname(__file__)
DEFAULT_DIR = os.path.dirname("first_id")

# filename = input("Filename : ")
# ext = input("Extension : ")
# f = open(".".join([filename, ext]), "a")
# f.write(f"[{DEFAULT_DIR}]")
# f.close()

def _locate_mark_file(dir: str = "X:\\dev\\app\\python_scripts", fileToSearch: str = "requirement.txt") -> list[str]:
    result = []

    for path, dirs, files in os.walk(dir):
        if(fileToSearch in files):
            fullPath = os.path.join(dir, path, fileToSearch)
            result.append(fullPath)

    return result