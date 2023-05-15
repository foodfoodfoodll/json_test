import json
import pandas as pd
import glob
import os

def add_attr(source, result, parent_key, table):
    """
    source:         payload из json
    result:         словарь, куда записывается результат
    parent_key:     участвует в формировании названия атрибута, собирается из путя
    table:          название таблицы, к которой относится атрибут

    преобразование json в список словарей с данными.
    Названия атрибутов собираются из пути в json
    """
    if table not in result.keys():
        result[table] = {}
    for key, value in source.items():
        if type(value) not in [dict, list]:
            k = parent_key + '_' + key
            k = k.lower()
            if k[0] == '_':
                k = k[1:]
            if k not in result[table].keys():
                result[table][k] = []
            result[table][k].append(value)
        elif type(value) is dict:
            add_attr(value, result, parent_key + '_' + key, table)
        elif type(value) is list:
            for i in range(len(value)):
                if key == 'Records':
                    add_attr(value[i], result, parent_key, table + '_' + key)
                else: 
                    add_attr(value[i], result, parent_key + '_' + key, table + '_' + key)

def get_schema_from_s2t(s2t_path, exclude_columns):
    """
    s2t_path:       path s2t-файла
    Получение структура таблиц из s2t.
    Из структура исключаются поля, указанные в списке exclude_columns и хэши
    """
    df = pd.read_excel(s2t_path, sheet_name='Mapping', skiprows=2, header=None)[[22, 24]] #[['Таблица.1', 'Код атрибута.1']]
    grouped = df.groupby([22])[24].apply(list).reset_index()
    del df
    schema = {}
    for index, row in grouped.iterrows():
        schema[row[22]] = []
        for item in row[24]:
            item = item.lower().strip()
            if item not in exclude_columns and item[-5:] != '_hash':
                schema[row[22]].append(item)
    return schema

def get_json_dict_list(json_path):
    """
    json_path:  path файла, в котором сохранены сообщения из кафки в формате json.

    Помещает сообщения кафки в список.
    Каждый элемент списка - словарь, полученный из json
    """
    with open(json_path, 'r', encoding="utf-8") as file:
        lines = file.readlines()
        json_list = []
        for item in lines:
            j = json.loads(item)
            json_list.append(j)
    return json_list

def get_csv_df_list(csv_directory_path, json_df_list):
    """
    csv_directory_path:     директория, в которой хранятся csv-файлы
    json_df_list:           список датафреймов, полученный из json

    Получение списка датафреймов из csv. 
    Название таблицы вытаскивается из названия файла. 
    Порядок колонок приводится к порядку из маппинга.
    """

    list_df_csv = {}
    for file in glob.glob(csv_directory_path + '\*.csv'):
        df_csv = pd.read_csv(file)
        #df_csv = df_csv.replace({numpy.nan: None})
        #Добавить проверку на метакласс
        table_name = file.split('\\')[-1].split('.')[0]
        df_csv = df_csv[list(json_df_list[table_name])]
        list_df_csv[table_name] = pd.DataFrame(data=df_csv)
    return list_df_csv

def from_dict_to_df(json_list, json_df_list, json_schema):
    """
    json_list:      список с сообщениями в формате json
    json_df_list:   список датафреймов
    json_schema:    структура таблиц, полученная из s2t

    Создание датафреймов из словарей json.
    Происходит сравнение со схемой. Если атрибута нет в датафрейме, то добавляется колонка с пустыми значениями
    """
    for table_name, table_data in json_list.items():
        tmp_df = pd.DataFrame(data=table_data)
        num = 0
        for col in json_schema[table_name]:
            if col not in list(tmp_df):
                tmp_df.insert(num, col, [None] * len(tmp_df)) #numpy.full(len(list_df[n]),numpy.nan)
            num+=1
        if table_name not in json_df_list.keys():
            json_df_list[table_name] = pd.DataFrame(data=tmp_df)
        else:
            json_df_list[table_name] = pd.concat([json_df_list[table_name], tmp_df]) 

def compare_json_with_csv(json_df_list, csv_df_list):
    """
    json_df_list:       список датафреймов, полученных из json
    csv_df_list:        список датафреймов, полученных из csv

    Датафреймы с данными одной таблицы склеиваются в один, после чего удаляются дубли.
    Если в результате получен пустой датафрейм, то данные совпадают.
    """
    for table_name in json_df_list.keys():
        if table_name in csv_df_list.keys():
            df = pd.concat([json_df_list[table_name], csv_df_list[table_name]]) 
            df = df.drop_duplicates(keep=False)
            if len(df) == 0:
                print('Данные совпадают: ', table_name)
            else:
                print('Данные не совпадают: ', table_name)
                print(df)
        else:
            print('Не найден csv: ', table_name)

database = 'batp'
exclude_columns = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']
root_directory = './test'


list_dir = os.listdir(root_directory)
baseclass_dirs = {}     # словарь метакласс: адрес директории
for item in list_dir:
    if '.' not in item:
        baseclass_dirs[item] = root_directory + '/' + item

for metaclass, path in baseclass_dirs.items():
    filenames = os.listdir(path)
    for name in filenames:
        if '.' not in name:
            csv_path = path + '/' + name
        elif name[-5:] == '.xlsx':
            s2t_path = path + '/' + name
        elif name[-5:] == '.json':
            json_path = path + '/' + name

    json_schema = get_schema_from_s2t(s2t_path, exclude_columns)
    raw_json_list = get_json_dict_list(json_path)

    json_df_list = {}

    for json_item in raw_json_list:
        json_data_dict = {}
        root_name = database + '_' + json_item['meta']['BaseClass']
        payload = json_item['payload']
        records = payload[0]['Records'][0]

        add_attr(payload[0], json_data_dict, '', root_name)
        del json_data_dict[root_name]
        from_dict_to_df(json_data_dict, json_df_list, json_schema)

    del json_data_dict

    csv_df_list = get_csv_df_list(csv_path, json_df_list)

    compare_json_with_csv(json_df_list, csv_df_list)
