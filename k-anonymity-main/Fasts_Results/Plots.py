# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 17:55:51 2024

@author: lison
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Charger les données
file_path = 'Results_fasts.txt'
df = pd.read_csv(file_path, sep=';')

# Filtrer les données en fonction de K et Nbre_QIDs
filters = {
    'movie': 4,
    'littlemovie': 4,
    'analysis': 4,
    'littleanalysis': 4,
    'littlesegmentation': 4,
    'segmentation': 4,
    'distributionsegmentation': 4
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
y="time"
# Créer un graphe pour chaque dataset
for dataset in filters.keys():
    dataset_df = filtered_df[(filtered_df['dataset'] == dataset)]
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=dataset_df, x=x, y=y, hue='algo', marker='o')
    
    plt.title(f'Dataset : {dataset}, K={K}')
    plt.xlabel(f'{x}')
    plt.ylabel(f'{y}')
    
    plt.show()

