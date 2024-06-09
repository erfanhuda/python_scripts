import pythonnet

# pythonnet.load("coreclr", runtime_config="runtimeconfig.json")
import clr

clr.AddReference("System.Collections.Generic")
clr.AddReference("System")

print(pythonnet.get_runtime_info())