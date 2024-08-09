# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 17:55:51 2024

@author: lison
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Charger les données
file_path = 'Results_oka_knn.txt'
df = pd.read_csv(file_path, sep=';')

# Filtrer les données en fonction de K et Nbre_QIDs
filters = {
    'movie': 4,
    'littlemovie': 4,
    'analysis': 5,
    'littleanalysis': 5,
    'littlesegmentation': 5,
    'segmentation': 5,
    'distributionsegmentation': 5
}

filtered_df = pd.DataFrame()
K=10

for dataset, qids in filters.items():
    temp_df = df[(df['dataset'] == dataset) & (df['K'] == K) & (df['Nbre_QIDs'] == qids)]
    # Sélectionner une seule ligne par test (par exemple, iteration 0)
    temp_df = temp_df[temp_df['iteration'] == 0]
    filtered_df = pd.concat([filtered_df, temp_df])

# Tracer les graphes
sns.set(style="whitegrid")

x="taille_ds"
y="NCP"
# Créer un graphe pour chaque dataset
for dataset in filters.keys():
    dataset_df = filtered_df[(filtered_df['dataset'] == dataset) & 
                             (filtered_df['parameters_algo'].isin(['oka', 'knn']))]
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=dataset_df, x=x, y=y,hue='parameters_algo', marker='o')
    
    plt.title(f'Dataset : {dataset}, K={K}')
    plt.xlabel(f'{x}')
    plt.ylabel(f'{y}')
    
    plt.show()

