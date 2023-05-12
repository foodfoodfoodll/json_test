import json
import pandas as pd
import csv
import glob
from datetime import datetime

def add_attr(source, result, parent_key, table, n):
    if table not in result.keys():
        result[table] = {}
    for key, value in source.items():
        if type(value) not in [dict, list]:
            k = parent_key + '_' + key
            k = k.lower()
            if k[0] == '_':
                k = k[1:]
            if k not in result[table].keys():
                result[table][k] = [None]*n
            result[table][k].append(value)
        elif type(value) is dict:
            add_attr(value, result, parent_key + '_' + key, table, n)
        elif type(value) is list:
            #hash = key + '_hash'
            # result[table][hash] = ''
            for i in range(len(value)):
                if key == 'Records':
                    add_attr(value[i], result, parent_key, table + '_' + key, n)
                else: 
                    add_attr(value[i], result, parent_key + '_' + key, table + '_' + key, n)


json_path = 'very_big_json.json' # json с данными
csv_directory_path = 'csv'
database = 'batp'
exclude_columns = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']
s2t_path = 'S2T_mapping.xlsx'

# получение схемы таблиц
df = pd.read_excel(s2t_path, sheet_name='Mapping', skiprows=2, header=None)[[22, 24]] #[['Таблица.1', 'Код атрибута.1']]
grouped = df.groupby([22])[24].apply(list).reset_index()
del df

json_schema = {}
for index, row in grouped.iterrows():
    json_schema[row[22]] = []
    for item in row[24]:
        item = item.replace(' ', '').replace('\xa0', '').lower()
        if item not in exclude_columns: #выикнуть хеши
            json_schema[row[22]].append(item)


# получаем данные из json
with open(json_path, 'r', encoding="utf-8") as file:
    lines = file.readlines()
    json_list = []
    for item in lines:
        j = json.loads(item)
        json_list.append(j)

list_df = {}

for json_item in json_list:
    res = {}
    root_name = database + '_' + json_item['meta']['BaseClass']
    payload = json_item['payload']
    records = payload[0]['Records'][0]
    tech_values = {'changeId': json_item['changeId'],
            'changeType' : json_item['changeType'],
            'ChangeTimestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Hdp_Processed_Dttm': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    # преобразование json в список словарей с данными
    add_attr(payload[0], res, '', root_name, 0)
    del res[root_name] # batp_SalesPointDirectory

    #создание датафреймов из json, название колонок создаётся из пути. если какого-то атрибута нет, то добавляется колонка с пустыми значениями 
    for table_name, table_data in res.items():
        tmp_df = pd.DataFrame(data=table_data)
        num = 0
        print(tmp_df)
        for col in json_schema[table_name]:
            if col not in list(tmp_df):
                tmp_df.insert(num, col, [None] * len(tmp_df)) #numpy.full(len(list_df[n]),numpy.nan)
            num+=1
        print(tmp_df)
        if table_name not in list_df.keys():
            list_df[table_name] = pd.DataFrame(data=tmp_df)
        else:
            list_df[table_name] = pd.concat([list_df[table_name], tmp_df]) 
        del tmp_df

del res

# получение списка датафреймов из csv. Название таблицы вытаскивается из названия файла. Порядок колонок приводится к порядку из маппинга
list_df_csv = {}
for file in glob.glob(csv_directory_path + '\*.csv'):
    df_csv = pd.read_csv(file)
    #df_csv = df_csv.replace({numpy.nan: None})
    #Добавить проверку на метакласс
    table_name = file.split('\\')[-1].split('.')[0]
    df_csv = df_csv[list(list_df[table_name])]
    list_df_csv[table_name] = pd.DataFrame(data=df_csv)

# for k, v in list_df_csv.items():
#     print(k)
#     print('\n')


# print(list(list_df['batp_SalesPointDirectory_Records']))
# print(list(list_df_csv['batp_SalesPointDirectory_Records']))

# print(list_df['batp_SalesPointDirectory_Records_Statuses'] == list_df_csv['batp_SalesPointDirectory_Records_Statuses'])

for table_name in list_df.keys():
    if table_name in list_df_csv.keys():
        df = pd.concat([list_df[table_name], list_df_csv[table_name]]) 
        df = df.drop_duplicates(keep=False)
        if len(df) == 0:
            print('Данные совпадают: ', table_name)
        else:
            print('Данные не совпадают: ', table_name)
            print(df)
    else:
        print('Не найден csv: ', table_name)
