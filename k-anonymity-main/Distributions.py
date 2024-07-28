#fichier pour créer les csv avec des distributions particulières dans les données QIDs et SA
import pandas as pd
import numpy as np

#DATASETS=['movie','analysis','segmentation']
DATASET='segmentation'
dataset_path = "C:\\Users\\lison\\Source\\Repos\\MA2\\TFE\\5algos\\k-anonymity-main\\data\\" + DATASET + "\\" + DATASET + ".csv"

with open(dataset_path, 'r') as file:
    columns = file.readline().strip().split(';')
data = pd.read_csv(dataset_path, sep=';', na_values='?', engine='python', skiprows=1, header=None, names=columns)

if DATASET=='movie':
    QI_INDEX=[3,4,5,6]
    modifications = {
    'age': {10: 1/7, 20: 1/7, 30:1/7, 40:1/7, 50: 1/7, 60: 1/7, 70:1/7},
    'gender': {'F': 0.5, 'M':0.5},
    'occupation': {'student':0.1,'other':0.1,'educator':0.1,'administrator':0.1,'engineer':0.1,'programmer':0.1,'librarian':0.1,'writer':0.1,'executive':0.1,'sciencist':0.1},
    'zip_code': {'01701':0.05,'01720':0.05,'01754':0.05,'01810':0.05,'01824':0.05,'01915':0.05,
                 '01913':0.05,'01940':0.05,'01945':0.05,'01960':0.05,'01970':0.05,'02110':0.05,
                 '02113':0.05,'02125':0.05,'02138':0.05,'02139':0.05,'02136':0.05,'02146':0.05,
                 '02140':0.05,'02143':0.05},
}
elif DATASET=='analysis': 
    QI_INDEX = [1,2,3,5,6]
    modifications = {
    'Year_Birth': {1940: 1/6, 1950: 1/6, 1960:1/6, 1970: 1/6, 1980: 1/6, 1990: 1/6},
    'Marital_Status': {'Together': 1/5, 'Married':1/5, 'Single': 1/5, 'Divorced':1/5, 'Widow': 1/5},
    'Kidhome': {0: 1/3, 1:1/3, 2: 1/3},
    'Teenhome': {0: 1/3, 1:1/3, 2: 1/3},
    'Education': {'PhD': 1/5, 'Master':1/5, 'Basic': 1/5, 'Graduation':1/5, '2nCycle': 1/5}
}
elif DATASET=='segmentation':
    QI_INDEX = [1,2,3,4,5,6,18]
    modifications = {
    'Age': {10: 1/7, 20: 1/7, 30:1/7, 40:1/7, 50: 1/7, 60: 1/7, 70:1/7},
    'Gender': {'Female': 0.5, 'Male':0.5},
    'Marital_Status': {'Married':1/5, 'Single': 1/5, 'Divorced':1/5, 'Widowed': 1/5, 'Separated':1/5},
    'Preferred_Language': {'English':1/5, 'French': 1/5, 'German':1/5, 'Mandarin': 1/5, 'Spanish':1/5},
    'Occupation': {'Entrepreneur':1/9,'Manager':1/9,'Nurse':1/9,'Artist':1/9,'Salesperson':1/9,'Lawyer':1/9,'Teacher':1/9,'Doctor':1/9,'Engineer':1/9},
    'Education_Level': {'Associate_Degree': 1/5, 'Doctorate':1/5, "Bachelor's_Degree": 1/5, "Master's_Degree":1/5, 'High_School_Diploma': 1/5},
    'Geographic_Information': {'Mizoram':0.05,'Goa':0.05,'Rajasthan':0.05,'Sikkim':0.05,'West_Bengal':0.05,
                 'Manipur':0.05,'Gujarat':0.05,'Andaman_and_Nicobar_Islands':0.05,'Tripura':0.05,'Nagaland':0.05,
                 'Maharashtra':0.05,'Telangana':0.05,'Delhi':0.05,'Chandigarh':0.05,'Jharkhand':0.05,
                 'Haryana':0.05,'Bihar':0.05,'Lakshadweep':0.05,'Arunachal_Pradesh':0.05,'Dadra_and_Nagar_Haveli':0.05,},
}
QI_NAMES = list(np.array(columns)[QI_INDEX])

# Analyser les distributions des quasi-identifiants
for col in QI_NAMES:
    print(f'Distribution pour {col}:')
    print(data[col].value_counts(normalize=True))
    print()
    
def modify_column_values(df, column, value_percentages):
    """
    Modifie les valeurs d'une colonne selon les pourcentages spécifiés.
    
    :param df: Le DataFrame à modifier.
    :param column: Le nom de la colonne à modifier.
    :param value_percentages: Un dictionnaire où les clés sont les valeurs à insérer et les valeurs sont les pourcentages correspondants.
    """
    total_rows = len(df)
    indices = np.random.permutation(df.index)
    current_index = 0

    for value, percentage in value_percentages.items():
        num_rows = int(total_rows * percentage)
        selected_indices = indices[current_index:current_index + num_rows]
        df.loc[selected_indices, column] = value
        current_index += num_rows

    # Si le total des pourcentages n'est pas 100%, assigner la valeur du dernier groupe aux lignes restantes
    if current_index < total_rows:
        df.loc[indices[current_index:], column] = list(value_percentages.keys())[-1]

data_new_distribution=data.copy()
# Appliquer les modifications
for column, value_percentages in modifications.items():
    modify_column_values(data_new_distribution, column, value_percentages)

data_new_distribution.to_csv("C:\\Users\\lison\\Source\\Repos\\MA2\\TFE\\5algos\\k-anonymity-main\\data\\" + DATASET + "\\distribution" + DATASET + ".csv", sep=';', index=False)

for col in QI_NAMES:
    print(f'New distribution pour {col}:')
    print(data_new_distribution[col].value_counts(normalize=True))
    print()

k_test=[2,3,4,5,6,7,8,9,10,11]
#unique_combinations = data.groupby(QI_NAMES).size()
unique_combinations = data_new_distribution.groupby(QI_NAMES).size()

for k in k_test:
    anonymity_level = (unique_combinations >= k).sum()
    size=data.shape[0]
    x=k*anonymity_level
    pourcentage=x/size
    print(f'Nombre de combinaisons uniques avec au moins {k} occurrences: {anonymity_level}')
    print(f'{x} données anonymes sur {size} ({pourcentage}%)')