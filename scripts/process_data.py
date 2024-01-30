import csv 
import json 

class Data:
    def __init__(self, data):
        #Atributos
        self.data = data 
        self.columns_name = self.get_columns()
        self.size_data = self.__size_data()

    #Metodos da Classe 
    def read_json(path):
        data_json = []
        with open(path, 'r') as file:
            data_json = json.load(file)
        
        return data_json

    def read_csv(path):
        data_csv = []
        with open(path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                data_csv.append(row)
        
        return data_csv
    @classmethod
    def read_data(cls, path, file_type):
        data = []

        if file_type == 'json':
            data = cls.read_json(path)
        elif file_type == 'csv':
            data = cls.read_csv(path)
        
        return cls(data)
    
    def get_columns(self):
        return list(self.data[-1].keys())
    
    def rename_columns(self, key_mapping):
        new_dados = []

        for old_dict in self.data:
                dict_temp = {}
                for old_key, value in old_dict.items():
                        dict_temp[key_mapping[old_key]] = value
                new_dados.append(dict_temp)

        self.data = new_dados
        self.columns_name = self.get_columns()

    def __size_data(self):
        return len(self.data)
    
    def join(dataA, dataB):
        combined_list = []
        combined_list.extend(dataA.data)
        combined_list.extend(dataB.data)

        return Data(combined_list)
    
    def transform_data_table(self):
        data_combine_table = [self.columns_name]

        for row in self.data:
            line = []
            for column in self.columns_name: 
                line.append(row.get(column, 'Indisponivel'))
        data_combine_table.append(line)

        return data_combine_table
    
    def saving_data(self, path):
        data_combined_table = self.transform_data_table()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(data_combined_table)


