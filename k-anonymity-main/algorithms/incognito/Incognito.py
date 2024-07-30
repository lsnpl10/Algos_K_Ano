import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import time
from dgh import CsvTable
from vertex import Vertex
from edge import Edge
from graph import Graph
from copy import deepcopy
from mergesort import MergeSort
import pandas as pd
import numpy as n

class Incognito:
    def __init__(self,data,csv_path,columns_names,QI_INDEX,QI_NAMES,GEN_PATH,data_name):
        self.columns_names=columns_names
        self.QI_INDEX=QI_INDEX
        #self.GEN_PATH=GEN_PATH
        self.dataname=data_name
        self.combinations_list = []
        self.main_graph = None
        self.k_anon_combinations = []
        self.checker = []
        self.quasi_iden = QI_NAMES
        #print(list(n.array(column_names)[QI_INDEX]))
        self.checker_quasi = []
        self.r_attribute_gen = {}
        self.dgh_paths = {}
        header=GEN_PATH
        #header = "C://Users//lison//Source//Repos//MA2//TFE//5algos//k-anonymity-main//data//"+DATASET+"//hierarchies//"
        for item in self.quasi_iden:
            self.dgh_paths[item] = f'{header}\\{data_name}_hierarchy_{item}.csv'  
        self.csv_dgh = CsvTable(
            csv_path, dgh_paths=self.dgh_paths, dgh_objs=None)
        self.dgh_trees = self.csv_dgh.dghs
        self.table=data
        print(self.table.head(3))
        print(type(self.table))
        # self.table = pd.read_csv(csv_path, header=0, delimiter=';')
        # print(self.table.head(3))
        # print(type(self.table))
        #print(self.table)
        # # self.table['date'] = pd.to_datetime(
        # #     self.table['date'], format='%m/%d/%Y')
        # # self.table['date_month'] = self.table['date'].dt.month
        # # self.table['date_day'] = self.table['date'].dt.day
        # # self.table['date_year'] = self.table['date'].dt.year
        # # self.table.drop('date', axis=1, inplace=True)
        # # self.table = self.csv_dgh.table.re
        self.path = csv_path
        #csv_path=nom du fichier à anonymiser
    def main_algorithm(self, kAnon):
        self.generate_quasi_combinations(1)
        self.create_graphs_for_r_attributes()
        queue = []
        total_comb = 0

        for i in range(1, len(self.quasi_iden) + 1):
            for x in range(len(self.combinations_list)):
                total_comb += len(self.combinations_list[x])
                temp_graph = self.r_attribute_gen[self.combinations_list[x]]
                queue = self.sort_by_height(temp_graph.get_roots())
                #print(f'quasi iden: {self.quasi_iden[i-1]}')
                #print(f'quasi comb: {self.combinations_list[x]}')
                while queue:
                    node = queue.pop(0)
                    issaKnon = False

                    if node.is_marked():
                        print("marked node:", node.get_data())

                    else:
                        issaKnon = self.generalize_with_level(
                            s=node.get_data(), k_anon=kAnon)
                        #print("work node:", node.get_data())
                        #print("Am I k-Anon?", issaKnon)
                        # print()
                        if issaKnon:
                            self.mark_all_gens(node)
                        else:
                            queue = node.get_direct_generalizations(
                                deepcopy(queue))
                            # queue.extend(further_gens)
                            queue = self.sort_by_height(queue)

            self.generate_quasi_combinations(i + 1)
            self.create_graphs_for_r_attributes()
        # print(f'len org: {total_comb}')
        # print(f'len kanon: {len(self.k_anon_combinations)}')

    def view_gen_combinations(self):

        #print("Combinations that are k-Anon:")
        for m in range(len(self.k_anon_combinations)):
            print(self.k_anon_combinations[m])

    def print_k_anon_tables(self):

        #print("Generalized Tables that are k-Anon:")
        for i in range(len(self.k_anon_combinations)):
            self.generalize_with_level(s=self.k_anon_combinations[i])

    def generate_quasi_combinations(self, r=None):
        self.combinations_list = []
        quasiIden = self.quasi_iden

        if r is None:
            r = len(quasiIden)

        self.combination(
            quasiIden, [None] * len(quasiIden), 0, len(quasiIden) - 1, 0, r)

    def combination(self, input, temp, start, end, index, r):
        if index == r:
            combine = temp[0] + " " + str(self.get_DG_height(temp[0]))
            for j in range(1, r):
                combine = combine + ":" + \
                    temp[j] + " " + str(self.get_DG_height(temp[j]))
            self.combinations_list.append(combine)

        for i in range(start, end+1):
            if end-i+1 >= r-index:
                temp[index] = input[i]
                self.combination(input, temp, i+1, end, index+1, r)

    def get_DG_height(self, quasi):
        for k, v in self.dgh_trees.items():
            if k.lower() == quasi.lower():
                return str(v.hierarchies['*'].get_height()-1)
        return ""

    def descend_from_top_vertex(self, v):
        arr = v.get_data().split(":")
        quasi_id_top_heights = {}

        for i in range(len(arr)):
            quasi_id = arr[i].split(" ")[0]
            top_height = int(arr[i].split(" ")[1])
            quasi_id_top_heights[quasi_id] = top_height

        quasi_id = list(quasi_id_top_heights.keys())

        for i in range(len(quasi_id)):
            old_value = quasi_id_top_heights[quasi_id[i]]
            new_value = quasi_id_top_heights[quasi_id[i]] - 1

            if new_value >= 0:
                quasi_id_top_heights[quasi_id[i]] = new_value

                possible_vertex = ""

                for j in range(len(quasi_id)):
                    possible_vertex += quasi_id[j].strip() + " " + \
                        str(quasi_id_top_heights[quasi_id[j]]) + ":"

                quasi_id_top_heights[quasi_id[i]] = old_value

                vertex2 = Vertex(possible_vertex[:-1])
                vertex2 = self.main_graph.has_vertex(vertex2)
                edge = Edge(vertex2, v)
                vertex2.add_incident_edge(edge)
                self.main_graph.add_edge_obj(edge)
                self.main_graph.add_vertex(vertex2)

        # main_graph.print_out()

    def check_if_at_bottom(self, v):
        arr = v.get_data().split(":")
        for i in range(len(arr)):
            index = int(arr[i].split(" ")[1])
            if index != 0:
                return False
        return True

    def create_graphs_for_r_attributes(self):

        for j in range(len(self.combinations_list)):
            top_root = Vertex(self.combinations_list[j])
            self.main_graph = Graph()
            self.main_graph.add_vertex(top_root)
            orig_length = len(self.main_graph.get_vertices())
            self.descend_from_top_vertex(self.main_graph.get_vertex(0))

            while not self.check_if_at_bottom(self.main_graph.get_vertex(orig_length-1)):
                new_length = len(self.main_graph.get_vertices())

                if new_length > orig_length:
                    for i in range(orig_length, new_length):
                        self.descend_from_top_vertex(
                            self.main_graph.get_vertex(i))

                orig_length = new_length

            self.r_attribute_gen[self.combinations_list[j]
                                 ] = self.main_graph.copy()

    def sort_by_height(self, queue):
        ms = MergeSort()
        return ms.sort(queue)

    def add_new_elem(self, item, dgh):
        if item == n.nan:
            #print("item==nan")
            return item
        if isinstance(item, float):
            try:
                item = str(int(item))
            except:
                #print("except")
                return n.nan
        else:
            item = str(item)
        new_element = dgh.generalize(item)
        return new_element

    def generate_table_with_dgh_table(self, old_table, dgh_tree, column_to_generalize):
        #print(f"avant2 = {old_table.iloc[2189]}")
        new_table = deepcopy(old_table)
        #print(f"apre2 = {new_table.iloc[2189]}")
        new_table[column_to_generalize] = new_table[column_to_generalize].apply(self.add_new_elem,
                                                                                dgh=dgh_tree)
    
        return new_table

    def get_table_comb(self, combs):
        print("combs",combs)
        for i in reversed(range(1, len(self.quasi_iden)+1)):
            for comb in combs:
                count = 0
                arr = comb.split(':')
                if len(arr) < i:
                    continue
                for item in arr:
                    if int(item.split(' ')[1]) != 0:
                        count += 1
                if count == len(arr):
                    print(f'Combination selected: {comb}')
                    return comb
        print("ici")
        return None

    def return_kanon_table(self):

        #print("Generalized Table that is k-Anon:")
        kanon_comb = self.get_table_comb(self.k_anon_combinations)
        new_table=self.generalize_with_level(s=kanon_comb)
        #print(new_table)
        return new_table

    def generalize_with_level(self, s, k_anon=None):
        #print(f"avant = {self.table.iloc[2189]}")
        new_table = deepcopy(self.table)
        #print(f"apres = {new_table.iloc[2189]}")
        # new_table.to_csv('temp.csv')
        # new_dgh = CsvTable('temp.csv', dgh_paths=None, dgh_objs=self.dgh_trees)
        new_dgh = self.csv_dgh
        new_dgh = new_dgh.dghs
        arr = s.split(":")
        index = -1
        t_tree = None
        self.checker_quasi = []
        self.checker_quasi = [item.split(' ')[0] for item in arr]
        for i in range(len(arr)):
            for j, (k, v) in enumerate(new_dgh.items()):
                if k == arr[i][:arr[i].index(" ")]:
                    index = list(new_table.columns).index(k)
                    t_tree = v
                    col_work = k

            generalization_level = int(arr[i][arr[i].index(" ") + 1:])
            # generalization_level = t_tree.gen_levels['*']-1
            x = 0

            while x < generalization_level:
                new_table = self.generate_table_with_dgh_table(
                    new_table, t_tree, col_work)
                if index not in self.checker:
                    self.checker.append(index)
                x += 1

        if k_anon is not None:
            return self.check_table(k_anon, new_table)
        #new_table.to_csv(DATASET+'_result.csv')
        #self.new_table=new_table
        #print((new_table))
        return new_table

    def mark_all_gens(self, v):
        self.addListOfGeneralizations(v)
        for edge in v.get_incident_edges():
            self.mark_all_gens(edge.get_to())

    def addListOfGeneralizations(self, node):
        node.set_mark(True)
        if node.get_data() not in self.k_anon_combinations:
            self.k_anon_combinations.append(node.get_data())

    def get_quasi_col_num(self, table):

        quasi_col_num = []
        for quasi in self.checker_quasi:
            quasi_col_num.append(list(table.columns).index(quasi))
        return quasi_col_num

    def get_freq_set(self, table):

        quasi_col_num = self.get_quasi_col_num(table)
        freq_set = {}
        i = 0
        while i < len(table):
            quasi_iden = []
            for x in quasi_col_num:
                quasi_iden.append(table.iat[i, x])
            quasi_iden = tuple(quasi_iden)
            if quasi_iden in freq_set:
                freq_set[quasi_iden] += 1
            else:
                freq_set[quasi_iden] = 1
            i += 1
        return freq_set

    def check_table(self, kanon, table):

        freq_set = self.get_freq_set(table)
        for k, v in freq_set.items():
            if n.nan in k:
                continue
            if v < kanon:
                return False
        return True
    
def incognito_Anonymization(data,csv_path,columns_names,QI_INDEX,QI_NAMES,GEN_PATH,data_name,K_VALUE): 
    
    data = pd.DataFrame(data, columns=columns_names)
    start_time = time.time() 
    #incog = Incognito(dataset_path)
    incog = Incognito(data,csv_path,columns_names,QI_INDEX,QI_NAMES,GEN_PATH,data_name)
    incog.main_algorithm(K_VALUE)
    # displaying generalizations
    #incog.view_gen_combinations()
    incog.checker = []
    new_table=incog.return_kanon_table()
    

    remaining_columns = [col for col in new_table.columns if col not in QI_NAMES]
    new_column_order = QI_NAMES + remaining_columns
    data_anonymized = new_table[new_column_order]
    data_anonymized_final = data_anonymized.values.tolist()
    run_time=time.time()-start_time
    # print(incog.checker)
    # print((time.time()-start_time))
    return data_anonymized_final, run_time