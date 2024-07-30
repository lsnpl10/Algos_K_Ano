import pandas as pd
import sys
import os
import numpy as np
import time
# Définir le chemin exact de votre répertoire 'ola'
def ola_Anonymization(data,columns_names,QI_INDEX,QI_NAMES, GEN_PATH,data_name,K_VALUE,MAX_SUPPRESSION,INFO_LOSS_CHOICE):
    data = pd.DataFrame(data, columns=columns_names)
    # target_dir = r'C:\Users\lison\Source\Repos\MA2\TFE\5algos\k-anonymity-main\algorithms\ola'
    # os.chdir(target_dir)
    # sys.path.append(target_dir)
    try:
        from algorithms.ola.crowds.kanonymity.ola import anonymize
        #print("Module imported successfully")
    except ModuleNotFoundError as e:
        print("Error importing module:", e)
    from algorithms.ola.crowds.kanonymity.ola import anonymize
    from algorithms.ola.crowds.kanonymity.information_loss import dm_star_loss,entropy_loss,prec_loss
    from algorithms.ola.crowds.kanonymity.generalizations import GenRule
    
    INFO_LOSS_DICT = {"dm_star_loss":dm_star_loss, "entropy_loss":entropy_loss,"prec_loss":prec_loss}
    
    # Globally pandas printing options: Show all columns and rows if displaying tables.
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # Prevent line breaks of rows
    pd.set_option('display.expand_frame_repr', False)
    
    
    # # Lire la première ligne pour obtenir les noms des colonnes
    # with open(dataset_path, 'r') as file:
    #     column_names = file.readline().strip().split(';')
    
    # # Lire le reste du fichier pour obtenir les données
    # data = pd.read_csv(dataset_path, sep=';', na_values='?', engine='python', skiprows=1, header=None, names=column_names)
    
    # # column_names = [
    # # 'user_id','rating','timestamp','age','gender','occupation','zip_code','movie_id','movie_title','release_date','IMDb_URL','movie_type'
    # # ]

    
    dict_of_dicts = {}
    
    for i, qi_name in enumerate(QI_NAMES):
        file_path = os.path.join(GEN_PATH, f'{data_name}_hierarchy_{qi_name}.csv')
        with open(file_path,"r", encoding='utf-8-sig') as fin:
            lines = fin.readlines()
        dict_of_dicts[f"Dict_{qi_name}"] = {}
        numberOfLevel = len(lines[0].split(";")) -1
        dict_temp={}
        for line in lines :
            line = line.replace("\n", "")
            line = line.split(";")
            dict_temp[line[0]] = line[1:]
        dict_of_dicts[f"Dict_{qi_name}"]=[dict_temp,numberOfLevel]
    
    def f1(level, value, Dict):
        try:
            return Dict[value][level]
        except KeyError as e:
            print(f"Error: Key {e} not found in the dictionary.")
            return None
        except IndexError:
            print(f"Error: Level {level} is out of range for value {value}.")
            return None
        except TypeError:
            print("Error: Provided dictionary structure is invalid.")
            return None
    
    generalization_rules = {}
    
    for name in QI_NAMES:
        Dict=dict_of_dicts[f"Dict_{name}"][0]
        #print(Dict)
        numberOfLevel=dict_of_dicts[f"Dict_{name}"][1]
        #print(numberOfLevel)
        generalization_rules[name] = GenRule(f1, numberOfLevel, Dict)  
    
    
    data = data.applymap(lambda x: str(x) if isinstance(x, int) else x)
    t0 = time.time()
    data_anonymized, transformation = anonymize(data, generalization_rules=generalization_rules, k=K_VALUE, max_sup=MAX_SUPPRESSION, info_loss=INFO_LOSS_DICT[INFO_LOSS_CHOICE])
    t1 = time.time()-t0
    remaining_columns = [col for col in data_anonymized.columns if col not in QI_NAMES]
    new_column_order = QI_NAMES + remaining_columns
    data_anonymized = data_anonymized[new_column_order]
    data_anonymized_final = data_anonymized.values.tolist()
    
    return data_anonymized_final, t1
    
