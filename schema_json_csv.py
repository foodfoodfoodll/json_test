import csv
import glob
from json_work.open_json import extract
from json_work.handle_json import h_json
from json_schema import get_schema
import pandas as pd

json_file = "SalesPoint.json"
csv_directory_path = 'csv'
tech_fields = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']
start_table = 'batp_SalesPointDirectory'
s2t_path = 'S2T_mapping.xlsx'

"""
# Получение схемы из json
# Некоторые алиасы без _, надо разбираться, почему
inf = extract.handle_json(json_file)
definitions = inf[0]
meta_class = inf[1]
nodes = inf[2]
mapping_dict = h_json.parsing_json(definitions, nodes, 'batp', tech_fields)

json_schema = get_schema(mapping_dict, tech_fields, meta_class, start_table)
"""


# читаем маппинг из s2t и забираем оттуда схему
excel_df = pd.read_excel('S2T_mapping.xlsx', sheet_name='Mapping', skiprows=2, header=None)[[22, 24]] #[['Таблица.1', 'Код атрибута.1']]
grouped = excel_df.groupby([22])[24].apply(list).reset_index()

json_schema = {}

for index, row in grouped.iterrows():
    json_schema[row[22]] = []
    for item in row[24]:
        json_schema[row[22]].append(item.replace(' ', '').replace('\xa0', '').lower())


def get_csv_schema(path):
    with open(path, 'r', encoding="utf-8") as file:
        return csv.DictReader(file).fieldnames

for file in glob.glob(csv_directory_path + '\*.csv'):
    csv_schema = get_csv_schema(file)
    table_name = file.split('\\')[-1].split('.')[0]
    
    equals_flag = True
    if len(json_schema[table_name]) > len(csv_schema):
        equals_flag=False
        print('В csv нет атрибутов: ' + str([item for item in json_schema[table_name] if item not in csv_schema]))
    elif len(json_schema[table_name]) < len(csv_schema):
        equals_flag=False
        print('В json нет атрибутов: ' + str([item for item in csv_schema if item not in json_schema[table_name]]))
    elif len(json_schema[table_name]) == len(csv_schema):
        for i in range(len(csv_schema)):
            if json_schema[table_name][i] != csv_schema[i]:
                equals_flag=False
                print('Атрибут в json не равен атрибуту из csv: ' + json_schema[table_name][i] + ', ' + csv_schema[i])
    if equals_flag:
        print('Структура таблицы ' + table_name + ' совпадает')
