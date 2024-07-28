import pandas as pd
import sys
import os
import numpy as np
# Définir le chemin exact de votre répertoire 'ola'
target_dir = r'C:\Users\lison\Source\Repos\MA2\TFE\5algos\k-anonymity-main\algorithms\ola'
os.chdir(target_dir)
sys.path.append(target_dir)
try:
    from crowds.kanonymity.ola import anonymize
    #print("Module imported successfully")
except ModuleNotFoundError as e:
    print("Error importing module:", e)
from crowds.kanonymity.ola import anonymize
from crowds.kanonymity.information_loss import dm_star_loss,entropy_loss,prec_loss
from crowds.kanonymity.generalizations import GenRule

DATASET='analysis'


# Globally pandas printing options: Show all columns and rows if displaying tables.
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
# Prevent line breaks of rows
pd.set_option('display.expand_frame_repr', False)

dataset_path = "C:\\Users\\lison\\Source\\Repos\\MA2\\TFE\\5algos\\k-anonymity-main\\data\\" + DATASET + "\\" + DATASET + ".csv"

# Lire la première ligne pour obtenir les noms des colonnes
with open(dataset_path, 'r') as file:
    column_names = file.readline().strip().split(';')

# Lire le reste du fichier pour obtenir les données
data = pd.read_csv(dataset_path, sep=';', na_values='?', engine='python', skiprows=1, header=None, names=column_names)

# column_names = [
# 'user_id','rating','timestamp','age','gender','occupation','zip_code','movie_id','movie_title','release_date','IMDb_URL','movie_type'
# ]

if DATASET=='movie':
    QI_INDEX=[3,4,5,6]
elif DATASET=='analysis': 
    QI_INDEX = [1,2,3,5,6]
elif DATASET=='segmentation':
    QI_INDEX = [1,2,3,4,5,6,18]
    
QI_NAMES = list(np.array(column_names)[QI_INDEX])

dict_of_dicts = {}

for name in QI_NAMES:
    file_path="C://Users//lison//Source//Repos//MA2//TFE//5algos//k-anonymity-main//data//"+DATASET+"//hierarchies//"+DATASET+"_hierarchy_"+name+".csv"
    with open(file_path,"r", encoding='utf-8-sig') as fin:
        lines = fin.readlines()
    dict_of_dicts[f"Dict_{name}"] = {}
    numberOfLevel = len(lines[0].split(";")) -1
    dict_temp={}
    for line in lines :
        line = line.replace("\n", "")
        line = line.split(";")
        dict_temp[line[0]] = line[1:]
    dict_of_dicts[f"Dict_{name}"]=[dict_temp,numberOfLevel]

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
print(generalization_rules)

#data = pd.read_csv("C:\\Users\\lison\\Source\\Repos\\MA2\\TFE\\5algos\\k-anonymity-main\\data\\" + DATASET + "\\" + DATASET + ".csv", names=column_names, sep=';', na_values='?', engine='python', skiprows=1, index_col=False)
data = data.applymap(lambda x: str(x) if isinstance(x, int) else x)
data_anonymized, transformation = anonymize(data, generalization_rules=generalization_rules, k=2, max_sup=0.0, info_loss=entropy_loss)
print(data_anonymized.head())

