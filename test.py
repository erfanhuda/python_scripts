import clr
from pythonnet import get_runtime_info

clr.AddReference("System.Collections.Generic")
clr.AddReference("System")

print(get_runtime_info())