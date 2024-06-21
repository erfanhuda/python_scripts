import xml.etree.ElementTree as ET

tree = ET.parse('test.xml')
root = tree.iter()

for i in root:
    print(' ', i.tag, i.attrib)