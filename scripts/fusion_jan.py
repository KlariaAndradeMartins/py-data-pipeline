from process_data import Data

#Extracting and reading data
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

data_company_A = Data.read_data(path_json, 'json')
print(data_company_A.columns_name)
print(data_company_A.size_data)

data_company_B = Data.read_data(path_csv, 'csv')
print(data_company_B.columns_name)
print(data_company_B.size_data)

#Transform data
key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

data_company_B.rename_columns(key_mapping=key_mapping)
print(data_company_B.columns_name)

fusion_data = Data.join(data_company_A, data_company_B)
print(fusion_data.columns_name)
print(fusion_data.size_data)

#Load 
path_data_combined = "data_processed/data_combined.csv"
fusion_data.saving_data(path=path_data_combined)
print(path_data_combined)
