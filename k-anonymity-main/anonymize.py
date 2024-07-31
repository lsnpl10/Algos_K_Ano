from utils.types import AnonMethod
import os
import argparse
import numpy as np
import pandas as pd
from metrics import NCP, DM, CAVG, metric_sa
import itertools
from algorithms import (
        k_anonymize,
        read_tree)
from datasets import get_dataset_params
from utils.data import read_raw, write_anon, numberize_categories
import csv

def get_combinations(lst):
    all_combinations = []
    for r in range(1, len(lst) + 1):
        combinations_r = [list(comb) for comb in itertools.combinations(lst, r)]
        all_combinations.extend(combinations_r)
    return all_combinations


def write_results(results, first_write=False):
    fieldnames = [
        "Num_test", "iteration", "dataset", "taille_ds", "algo", "parameters_algo",
        "K", "Nbre_QIDs", "QIDs", "SA", "time", "NCP", "CAVG_before", "CAVG_after",
        "DM_before", "DM_after", "L_div_score"
    ]
    
    mode = 'w' if first_write else 'a'
    
    with open('results_glob.csv', mode, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        
        if first_write:
            writer.writeheader()
        
        writer.writerow(results)


class Anonymizer:
    def __init__(self, args):
        self.method = args.method
        #assert self.method in ["mondrian", "topdown", "cluster", "mondrian_ldiv", "classic_mondrian", "datafly", "ola", "incognito"]
        #cluster déjà fait et incognito très long donc à voir plus tard
        assert self.method in ["mondrian", "topdown", "mondrian_ldiv", "classic_mondrian", "datafly", "ola", "incognito"]
        self.k = args.k
        self.info_loss_choice = args.info_loss_choice
        self.use_is_cat2 = args.use_is_cat2
        self.data_name = args.dataset
        self.csv_path = args.dataset+'.csv'
        self.taille_ds=args.taille_ds
        # Data path
        self.path = os.path.join('data', args.dataset)  # trailing /

        # Dataset path
        self.data_path = os.path.join(self.path, self.csv_path)
        # Generalization hierarchies path
        self.gen_path = os.path.join(
            self.path,
            'hierarchies')  # trailing /

        # folder for all results
        res_folder = os.path.join(
            'results', 
            args.dataset, 
            self.method)
        #print(res_folder) #path results\dataset\algo
        # path for anonymized datasets
        self.anon_folder = res_folder  # trailing /
        
        os.makedirs(self.anon_folder, exist_ok=True)

    def anonymize(self):
        data = pd.read_csv(self.data_path, delimiter=';')
        ATT_NAMES = list(data.columns)
        print(ATT_NAMES)
        dico=data_params['dico']
        sa_name=data_params['target_var']
        sa_index=ATT_NAMES.index(sa_name)
        print(sa_index)
        
        IS_CAT2 = [dico[qi] for qi in QI_INDEX]
        #IS_CAT2 = data_params['is_category'] #pour les algos qui implémentent les nombres dynamiquement
        
        QI_NAMES = list(np.array(ATT_NAMES)[QI_INDEX])
        print('qi_names:',QI_NAMES)
        nbre_qids=len(QI_INDEX)
        IS_CAT = [True] * len(QI_INDEX) # is all cat because all hierarchies are provided
        SA_INDEX = [index for index in range(len(ATT_NAMES)) if index not in QI_INDEX]
        SA_var = [ATT_NAMES[i] for i in SA_INDEX]

        ATT_TREES = read_tree(
            self.gen_path, 
            self.data_name, 
            ATT_NAMES, 
            QI_INDEX, IS_CAT)

        raw_data, header = read_raw(
            self.path, 
            self.data_name, 
            self.taille_ds, QI_INDEX, IS_CAT)

        anon_params = {
            "name" :self.method,
            "att_trees" :ATT_TREES,
            "value" :self.k,
            "qi_index" :QI_INDEX, 
            "sa_index" :SA_INDEX
        }

        if self.method == AnonMethod.CLASSIC_MONDRIAN:
            chosen_is_cat = IS_CAT2 if self.use_is_cat2 else IS_CAT  # Choose between IS_CAT and IS_CAT2
            mapping_dict, raw_data = numberize_categories(raw_data, QI_INDEX, SA_INDEX, chosen_is_cat)
            anon_params.update({'mapping_dict': mapping_dict})
            anon_params.update({'is_cat': chosen_is_cat})

        if self.method == AnonMethod.DATAFLY:
            anon_params.update({
                'K_VALUE': self.k,
                'dataset': data,
                "columns_names":ATT_NAMES,
                'qi_names': QI_NAMES,
                #'csv_path': self.data_path,
                'data_name': self.data_name,
                'dgh_folder': self.gen_path,
                'res_folder': self.anon_folder,
                'csv_path': self.data_path,
                'taille_ds':self.taille_ds})
        
        if self.method == AnonMethod.OLA:
            anon_params.update({
                'data': data,
                "columns_names":ATT_NAMES,
                'QI_INDEX': QI_INDEX,
                'QI_NAMES': QI_NAMES,
                'GEN_PATH': self.gen_path,
                'data_name': self.data_name,
                'K_VALUE': self.k,
                'MAX_SUPPRESSION': 0.0,
                'INFO_LOSS_CHOICE': self.info_loss_choice #"dm_star_loss", "entropy_loss","prec_loss"
                })
            
        if self.method == AnonMethod.INCOGNITO:
            anon_params.update({
                'data': data,
                'csv_path': self.data_path,
                "columns_names":ATT_NAMES,
                'QI_INDEX': QI_INDEX,
                'QI_NAMES': QI_NAMES,
                'GEN_PATH': self.gen_path,
                'data_name': self.data_name,
                'K_VALUE': self.k
                })
            
        anon_params.update({'data': raw_data})

        print(f"Anonymize with {self.method}")
        anon_data, runtime = k_anonymize(anon_params)

        # Write anonymized table
        if anon_data is not None:
            nodes_count = write_anon(
                self.anon_folder, 
                anon_data, 
                header, 
                self.k, 
                self.data_name,
                self.taille_ds, 
                QI_NAMES, 
                nbre_qids, 
                self.method)


        if self.method == AnonMethod.CLASSIC_MONDRIAN:
            ncp_score, runtime = runtime
        else:
            # Normalized Certainty Penalty
            ncp = NCP(anon_data, QI_INDEX, ATT_TREES)
            ncp_score = ncp.compute_score()

        # Discernibility Metric

        raw_dm = DM(raw_data, QI_INDEX, self.k)
        raw_dm_score = raw_dm.compute_score()

        anon_dm = DM(anon_data, QI_INDEX, self.k)
        anon_dm_score = anon_dm.compute_score()

        # raw_sa = metric_sa(raw_data, QI_INDEX, sa_index)
        # raw_sa_score = raw_sa.compute_score()
        
        #sa metric
        
        thresholds=[10, 20, 30, 40, 50,60,70,80,90,100]
        anon_sa_scores = []
        for threshold in thresholds:
            anon_sa = metric_sa(anon_data, QI_INDEX, sa_index, threshold)
            anon_sa_score = anon_sa.compute_score()
            anon_sa_scores.append(anon_sa_score)
            
        # Average Equivalence Class

        raw_cavg = CAVG(raw_data, QI_INDEX, self.k)
        raw_cavg_score = raw_cavg.compute_score()

        anon_cavg = CAVG(anon_data, QI_INDEX, self.k)
        anon_cavg_score = anon_cavg.compute_score()

        print(f"NCP score (lower is better): {ncp_score:.3f}")
        print(f"CAVG score (near 1 is better): BEFORE: {raw_cavg_score:.3f} || AFTER: {anon_cavg_score:.3f}")
        print(f"DM score (lower is better): BEFORE: {raw_dm_score} || AFTER: {anon_dm_score}")
        print(f"Metric L-diversity : {anon_sa_scores}")
        print(f"Time execution: {runtime:.3f}s")
        print(f"QID:{QI_NAMES}")
        return ncp_score, raw_cavg_score, anon_cavg_score, raw_dm_score, anon_dm_score, runtime, anon_sa_scores,QI_NAMES,nbre_qids,sa_name


def main(args):
    anonymizer = Anonymizer(args)
    return anonymizer.anonymize()
    #ncp_score, raw_cavg_score, anon_cavg_score, raw_dm_score, anon_dm_score, runtime, anon_sa_scores = anonymizer.anonymize()

if __name__ == '__main__':
    
    DATASETS=['movie','analysis','segmentation','distributionmovie','distributionanalysis','distributionsegmentation', 'littlemovie','littleanalysis','littlesegmentation']
    #algos=["datafly"]
    algos=["mondrian", "topdown", "classic_mondrian", "ola"]
    #k_list=[2]
    k_list=[2,5,10,15,20,30,50,75,100,150,200,300,500]
    #algos=["mondrian", "topdown", "mondrian_ldiv", "classic_mondrian", "datafly", "ola","cluster"]
    num_test = 0
    first_write = True
    for dataset in DATASETS:
        if dataset == 'analysis' or dataset == 'littleanalysis' or dataset == 'distributionanalysis':
            TAILLE_DATA_TEST=[50,100,150,250,500,750,1000,1250,1500,1750,2000,2216]
        elif dataset == 'movie' or dataset == 'littlemovie' or dataset == 'distributionmovie':
            #TAILLE_DATA_TEST=[50]
            TAILLE_DATA_TEST=[50,100,150,250,350,500,700,943]
        elif dataset=='segmentation' or dataset=='littlesegmentation' or dataset=='distributionsegmentation':
            TAILLE_DATA_TEST=[500,2000,5000,10000,15000,25000,35000,45000,53503]
        
        data_params = get_dataset_params(dataset)
        QI_INDEXs = data_params['qi_index']
        QI_INDEX_list = get_combinations(QI_INDEXs)

        for QI_INDEX in QI_INDEX_list :
            for taille_ds in TAILLE_DATA_TEST: 
                for algo in algos:
                    for k in k_list:
                        if k < taille_ds:
                            print("k=",k)
                            if algo == 'ola':
                                info_loss_choices = ["dm_star_loss", "entropy_loss", "prec_loss"]
                            else:
                                info_loss_choices = [None]
                            for info_loss_choice in info_loss_choices:
                                print("info_loss_choice for OLA :",info_loss_choice)
                                for i in range (3):
                                    print("taille DS :", taille_ds)
                                    parser = argparse.ArgumentParser('K-Anonymize')
                                    parser.add_argument('--method', type=str, default=algo,
                                                        help="K-Anonymity Method")
                                    parser.add_argument('--k', type=int, default=k,
                                                        help="K-Anonymity or L-Diversity")
                                    parser.add_argument('--dataset', type=str, default=dataset,
                                                        help="Dataset to anonymize")
                                    parser.add_argument('--taille_ds', type=int, default=taille_ds,
                                                        help="Taille dataset")
                                    parser.add_argument('--info_loss_choice', type=str, default=info_loss_choice, 
                                                        help="Info loss choice for OLA")
                                    parser.add_argument('--use_is_cat2', type=bool, default=False, help="Choice dyn/stat for numerical QIDs for classic_mondrian")
                                    args = parser.parse_args()
                                    if algo == 'classic_mondrian':
                                        for use_is_cat2 in [False, True]:
                                            if use_is_cat2 == True :
                                                print("DYNAMIC")
                                                type_hierarchy="dynamic"
                                            else : 
                                                print("STATIC")
                                                type_hierarchy="static"
                                            args.use_is_cat2 = use_is_cat2
                                            results=main(args)
                                            num_test += 1
                                            
                                            parameters_algo = type_hierarchy if algo == 'classic_mondrian' else (info_loss_choice if algo == 'ola' else "0")
                                            
                                            write_results({
                                                "Num_test": num_test,
                                                "iteration": i,
                                                "dataset": dataset,
                                                "taille_ds": taille_ds,
                                                "algo": algo,
                                                "parameters_algo": parameters_algo,
                                                "K": k,
                                                "Nbre_QIDs": results[8],
                                                "QIDs": ','.join(results[7]),
                                                "SA": results[9],
                                                "time": results[5],
                                                "NCP": results[0],
                                                "CAVG_before": results[1],
                                                "CAVG_after": results[2],
                                                "DM_before": results[3],
                                                "DM_after": results[4],
                                                "L_div_score": ','.join(map(str, results[6]))
                                            }, first_write)
                                            
                                            first_write = False
                                    else:
                                        try :
                                            main(args)
                                            results = main(args)
                                            num_test += 1
                                            
                                            parameters_algo = info_loss_choice if algo == 'ola' else "0"
                                            
                                            write_results({
                                                "Num_test": num_test,
                                                "iteration": i,
                                                "dataset": dataset,
                                                "taille_ds": taille_ds,
                                                "algo": algo,
                                                "parameters_algo": parameters_algo,
                                                "K": k,
                                                "Nbre_QIDs": results[8],
                                                "QIDs": ','.join(results[7]),
                                                "SA":results[9],
                                                "time": results[5],
                                                "NCP": results[0],
                                                "CAVG_before": results[1],
                                                "CAVG_after": results[2],
                                                "DM_before": results[3],
                                                "DM_after": results[4],
                                                "L_div_score": ','.join(map(str, results[6]))
                                            }, first_write)
                                            
                                            first_write = False
                                            
                                        except : 
                                            print("!!!!!!!!!datafly avec k>> par rapport à DS!!!!!!!")