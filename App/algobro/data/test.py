import os
import zlib, base64

CURRENT_DIR = os.path.dirname(__file__)

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