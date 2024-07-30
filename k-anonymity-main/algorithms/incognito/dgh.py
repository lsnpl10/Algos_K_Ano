import csv
from io import StringIO
from tree import Node, Tree
import sys



class CsvDGH():

    def __init__(self, dgh_path):

        self.label = None
        self.hierarchies = dict()
        self.gen_levels = dict()

        try:
            with open(dgh_path, 'r') as file:
                for line in file:

                    try:
                        csv_reader = csv.reader(StringIO(line), delimiter=';')
                        #print('csv_reader',csv_reader)
                    except IOError:
                        raise
                    values = next(csv_reader)
                    #print('values',values)
                    if values[-1] not in self.hierarchies:
                        self.hierarchies[values[-1]] = Tree(Node(values[-1]))
                        self.hierarchies[values[-1]].set_height()
                        print("1",self.hierarchies[values[-1]])
                        self.gen_levels[values[-1]] = len(values) - 1
                        print("autres",self.gen_levels[values[-1]])
                    self._insert_hierarchy(values[:-1], self.hierarchies[values[-1]])
                self.hierarchies['*'].set_height()
                print("*",self.hierarchies['*'])
# * a l'air d'être danes les hierarchies
        except FileNotFoundError:
            raise
        except IOError:
            raise
        # print(dgh_path)
        
    @staticmethod
    def _insert_hierarchy(values, tree):
        current_node = tree.root

        for i, value in enumerate(reversed(values)):

            if value in current_node.children:
                current_node = current_node.children[value]
                continue
            else:
                # Insert the hierarchy from this node:
                for v in list(reversed(values))[i:]:
                    current_node.add_child(Node(v))
                    current_node = current_node.children[v]
                return True

        return False
    
    def generalize(self, value, gen_level=None):
        # if value == None :
        #    print("Value = None")
        for hierarchy in self.hierarchies:
            if gen_level is None:
                node = self.hierarchies[hierarchy].bfs_search(value)
                print("node2",node)
            else:
                node = self.hierarchies[hierarchy].bfs_search(
                    value,
                    self.gen_levels[hierarchy] - gen_level)   
                print("node2",node)
            if node is None:
                #print("node is None")
                continue
            elif node.parent is None:
                #print("node.parent is None")
                print('value',value)
                return value
            else:
                print('parent',node.parent.data)
                return node.parent.data
      #  print('value',value)
        raise KeyError(value)
        #return "*"
    
class CsvTable():
    def __init__(self, pt_path: str, dgh_paths: dict, dgh_objs: dict):
        self.table = None
        self.attributes = dict()
        try:
            self.table = open(pt_path, 'r')
        except FileNotFoundError:
            raise
        self.dghs = dict()
        if dgh_paths == None:
            self.dghs = dgh_objs
        else:
            for attribute in dgh_paths:
                self._add_dgh(dgh_paths[attribute], attribute)
        
    def _init_table(self):

        try:
            csv_reader = csv.reader(StringIO(next(self.table)), delimiter=';')
        except IOError:
            raise

        for i, attribute in enumerate(next(csv_reader)):
            self.attributes[attribute] = i

    def _add_dgh(self, dgh_path, attribute):
        try:
            self.dghs[attribute] = CsvDGH(dgh_path)
        except FileNotFoundError:
            raise
        except IOError:
            raise
