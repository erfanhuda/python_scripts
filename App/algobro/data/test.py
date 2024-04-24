import os
<<<<<<< HEAD
import zlib, base64
=======
import glob
from pathlib import Path
>>>>>>> 92f913d2c2b087cdb3bdec93a66c45b742ebd38d

CURRENT_DIR = os.path.dirname(__file__)
DEFAULT_DIR = os.path.dirname("first_id")

<<<<<<< HEAD
file = open(CURRENT_DIR + "\\2023-12.record")
text = file.read()
file.close()

outfile = open(CURRENT_DIR + "\\2023-12_encoded.record", "w")
code = base64.b64encode(zlib.compress(text.encode('utf-8'), 9))
outtext = outfile.write(str(code))
outfile.close()

infile = open(CURRENT_DIR + "\\2023-12_encoded.record")
intext = infile.read()
code = base64.b64decode(intext.encode("utf-8"))
print(intext)
infile.close()
=======
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
>>>>>>> 92f913d2c2b087cdb3bdec93a66c45b742ebd38d
