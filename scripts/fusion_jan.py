import csv
import json 


def read_json(path_json):
    with open(path_json, 'r') as file:
        data_json = json.load(file)
    
    return data_json

def read_csv(path_csv):
    data_csv = []
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            data_csv.append(row)
    
    return data_csv

def read_data(path, file_type):
    data = []

    if file_type == 'json':
        data = read_json(path)
    elif file_type == 'csv':
        data = read_csv(path)
    
    return data

def get_columns(data):
    return list(data[0].keys())

def rename_columns(data_csv, key_mapping):
    new_data_csv = []

    for old_dict in data_csv:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_data_csv.append(dict_temp)
    
    return new_data_csv

def size_data(data):
    return len(data)

def join_data(dataA, dataB):
    combined_list = [] 
    combined_list.extend(dataA)
    combined_list.extend(dataB)

    return combined_list

def transform_data_table(data, columns_name):

    data_combine_table = [columns_name]
    for row in data:
        line = []
        for columns in columns_name:
            line.append(row.get(columns, 'Indisponivel'))
        data_combine_table.append(line)

    return data_combine_table

def saving_data(data, path):

    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(data)

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#Extracting and reading data
data_json = read_data(path_json, 'json')
name_columns_json = get_columns(data=data_json)
size_data_json = size_data(data_json)
print(f"Name columns json:{name_columns_json}, Size of json data: {size_data_json}\n")

data_csv = read_data(path_csv, 'csv')
name_columns_csv = get_columns(data=data_csv)
size_data_csv = size_data(data_csv)
print(f"Name columns csv: {name_columns_csv}, Size of csv data: {size_data_csv}\n")

#Transform data
key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

data_csv = rename_columns(data_csv, key_mapping)
print(f"Rename columns name of csv: {get_columns(data_csv)}")

data_fusion = join_data(data_csv, data_json)
data_fusion_columns = get_columns(data_fusion)
size_data_fusion = size_data(data_fusion)
print(f"Name columns fusion:{data_fusion_columns}, Size of fuzion data:{size_data_fusion}")

#Load data
data_fusion_table = transform_data_table(data_fusion, data_fusion_columns)
path_data_combined = "data_processed/data_combined.csv"

saving_data(data_fusion_table, path=path_data_combined)