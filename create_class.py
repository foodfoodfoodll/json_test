import json
import csv
from types import SimpleNamespace


"""
# способ 1
with open(json_path, 'r', encoding="utf-8") as file:
    json_string = file.read()
    json_obj = json.loads(json_string, object_hook=lambda d: SimpleNamespace(**d))
"""

"""
with open(json_path, 'r', encoding="utf-8") as file:
    json_dict = json.load(file)

json_obj = ClassFromDict(json_dict)
print(json_obj.id)
"""

class ClassFromDict:
    def __init__(self, dictionary):
        for k, v in dictionary.items():
            setattr(self, k, v)

csv_dict = []
with open('input2.csv', 'r', encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        csv_dict.append(row)

obj_list = []
for item in csv_dict:
    obj_list.append(ClassFromDict(item))

print(obj_list[0].changeid)