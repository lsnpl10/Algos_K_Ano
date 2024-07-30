# -*- coding: utf-8 -*-

import csv
import os
import pickle


def read_raw(path, dataset, taille_ds, qi_index, is_cat, delimiter=';', sort_count=False):
    """Reads dataset from a csv file. Split in header and data

        :param file_path: Path to the csv file
        :param delimiter: Character that is used as delimiter in the csv file
        :return: 2d list: data[col][row], list: header[col]
    """

    numeric_dict = [{} for elem in qi_index]
    data = []
    with open(os.path.join(path, f'{dataset}.csv')) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter)
        header = next(csv_reader)
        for row in csv_reader:
            if len(data)>=taille_ds:
                break;
            data.append(row)  

        # save the count of each value
        for i, qii, cat in zip(range(len(qi_index)), qi_index, is_cat):
            if not cat:
                try:
                    numeric_dict[i][row[qii]] += 1
                except KeyError:
                    numeric_dict[i][row[qii]] = 1
    print(len(data))
    return (data, header)


def reorder_columns(data, qi_index):
    res = []
    for row in data:
        qi = [elem for i, elem in enumerate(row) if i in qi_index]
        non_qi = [elem for i, elem in enumerate(row) if i not in qi_index]
        res.append([*qi, *non_qi])
    return res


def restore_column_order(data, qi_index):
    res = []
    for row in data:
        new_row = row[len(qi_index):]
        for i, elem in zip(qi_index, row[:len(qi_index)]):
            new_row.insert(i, elem)
        res.append(new_row)
    return res


def transform_columns(data):
    res = [[] for _ in range(len(data[0]))]
    for row in data:
        print(row)
        for i, column in enumerate(row):
            print(column)
            res[i].append(column)
    return res


def write_anon(path, anon_data, header, k, dataset, taille_ds, qids_names, nbre_qids, algo_name, delimiter=';'):
    if isinstance(anon_data, dict):
        #print(anon_data)
        anon_data = anon_data.values()
    else:
        # Sort by ID (first column)
        anon_data = sorted(anon_data, key=lambda x: int(x[0]))
        anon_data = [anon_data]
        
    qids_names_str = "_".join(qids_names)
    
    for i, data in enumerate(anon_data):
        filename = f"anonymized_{dataset}_k_{k}_taille_{taille_ds}_nbre_qi_{nbre_qids}_qids_names_{qids_names_str}_algo_{algo_name}.csv"
        filepath = os.path.join(path, filename)
        with open(filepath, mode='w', newline='') as anon_file:
            anon_writer = csv.writer(anon_file, delimiter=delimiter)
            anon_writer.writerow(header)
            anon_writer.writerows(data)
    return len(anon_data)

def numberize_categories(data, qi_index, sa_index, is_cat):
    num_qis = len(qi_index)
    mapping_dict=[{} for i in range(num_qis)]
    
    iterative_id = [0 for i in range(num_qis)]
    # print("is_cat",is_cat) #is_cat [False, True, True, True]
    # print("qi_index",qi_index) #qi_index [4] donc index dans la bdd du QID
    for record in data:
        #print(record) chaque ligne de la bdd
        for idx, i in enumerate(qi_index):
            # print(qi_index,[idx],i) #qi_index=[4] ok [0] 4
            # print("is_cat[idx]0",is_cat[idx])
            if is_cat[idx]: #si c'est catégoriel
                value = record[i]
                #print("is_cat[idx]1",is_cat[idx],value)
                if value not in mapping_dict[idx].keys():
                    mapping_dict[idx][value] = iterative_id[idx]
                    iterative_id[idx] += 1
                    #print("mapping_dict",mapping_dict)


    new_data = []
    for record in data:
        new_record = []
        for idx, i in enumerate(qi_index):
            value = record[i]
            if is_cat[idx]: #on entre jamais, c'est jamais caatégoriel
                new_record.append(mapping_dict[idx][value])
                #print("is_cat[idx]2",is_cat[idx],value)
            else:
                    new_record.append(float(value))

        for idx, i in enumerate(sa_index):
            value = record[i]
            new_record.append(value)

        new_data.append(new_record)

    restore_dict = [{v:k for k,v in dict_.items()} for dict_ in mapping_dict]

    return restore_dict, new_data