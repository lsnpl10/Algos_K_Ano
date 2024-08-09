# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 17:55:51 2024

@author: lison
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Charger les données
file_path = 'Datafly_tout.csv'
df = pd.read_csv(file_path, sep=';')

# Filtrer les données en fonction de K et Nbre_QIDs
filters = {
    'movie': 4,
    'littlemovie': 4,
    'analysis': 5,
    'littleanalysis': 5,
    'littlesegmentation': 7,
    'segmentation': 7
}

filtered_df = pd.DataFrame()

for dataset, qids in filters.items():
    temp_df = df[(df['dataset'] == dataset) & (df['K'] == 10) & (df['Nbre_QIDs'] == qids)]
    # Sélectionner une seule ligne par test (par exemple, iteration 0)
    temp_df = temp_df[temp_df['iteration'] == 0]
    filtered_df = pd.concat([filtered_df, temp_df])

# Tracer les graphes
sns.set(style="whitegrid")


# Créer un graphe pour chaque dataset
for dataset in filters.keys():
    dataset_df = filtered_df[filtered_df['dataset'] == dataset]
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=dataset_df, x='taille_ds', y='NCP', marker='o')
    
    plt.title(f'Running Time vs. Dataset Size for {dataset}')
    plt.xlabel('Dataset Size')
    plt.ylabel('NCP')
    
    plt.show()

