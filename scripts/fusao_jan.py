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

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#initializing reding the data
data_json = read_data(path_json, 'json')
name_columns_json = get_columns(data=data_json)
print(data_json[0], name_columns_json)

data_csv = read_data(path_csv, 'csv')
name_columns_csv = get_columns(data=data_csv)
print(data_csv[0], name_columns_csv)

#transform data
key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}
