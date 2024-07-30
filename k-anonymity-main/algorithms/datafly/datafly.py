import argparse
import csv
import sys
from datetime import datetime
from io import StringIO
import os
from .dgh import CsvDGH

_DEBUG = False


class _Table:

    def __init__(self, pt_path: str, dgh_paths: dict):

        """
        Instantiates a table and the specified Domain Generalization Hierarchies from the
        corresponding files.

        :param pt_path:             Path to the table to anonymize.
        :param dgh_paths:           Dictionary whose values are paths to DGH files and whose keys
                                    are the corresponding attribute names.
        :raises IOError:            If a file cannot be read.
        :raises FileNotFoundError:  If a file cannot be found.
        """

        self.table = None
        """
        Reference to the table file.
        """
        self.attributes = dict()
        """
        Dictionary whose keys are the table attributes names and whose values are the corresponding
        column indices.
        """
        self._init_table(pt_path)
        """
        Reference to the table file.
        """
        self.dghs = dict()
        """
        Dictionary whose values are DGH instances and whose keys are the corresponding attribute 
        names.
        """
        
        #self.taille_ds=taille_ds
        
        for attribute in dgh_paths:
            self._add_dgh(dgh_paths[attribute], attribute)

    def __del__(self):

        """
        Closes the table file.
        """

        self.table.close()

    def anonymize(self, taille_ds:int, qi_names: list, k: int, output_path: str, v=True):

        """
        Writes a k-anonymous representation of this table on a new file. The maximum number of
        suppressed rows is k.

        :param qi_names:    List of names of the Quasi Identifiers attributes to consider during
                            k-anonymization.
        :param k:           Level of anonymity.
        :param output_path: Path to the output file.
        :param v:           If True prints some logging.
        :raises KeyError:   If a QI attribute name is not valid.
        :raises IOError:    If the output file cannot be written.
        """

        global _DEBUG

        if v:
            _DEBUG = False

        self._debug("[DEBUG] Creating the output file...", _DEBUG)
        # try:
        #     output = open(output_path, 'w+')
        # except IOError:
        #     raise
        # self._log("[LOG] Created output file.", endl=True, enabled=v)

        # Start reading the table file from the top:
        self.table.seek(0)

        self._debug("[DEBUG] Instantiating the QI frequency dictionary...", _DEBUG)
        # Dictionary whose keys are sequences of values for the Quasi Identifiers and whose values
        # are couples (n, s) where n is the number of occurrences of a sequence and s is a set
        # containing the indices of the rows in the original table file with those QI values:
        qi_frequency = dict()

        self._debug("[DEBUG] Instantiating the attributes domains dictionary...", _DEBUG)
        # Dictionary whose keys are the indices in the QI attribute names list, and whose values are
        # sets containing the corresponding domain elements:
        domains = dict()
        for i, attribute in enumerate(qi_names):
            domains[i] = set()

        # Dictionary whose keys are the indices in the QI attribute names list, and whose values are
        # the current levels of generalization, from 0 (not generalized):
        gen_levels = dict()
        for i, attribute in enumerate(qi_names):
            gen_levels[i] = 0
        for i, row in enumerate(self.table):
            if i >= (taille_ds+1):  # Limit the number of rows processed
                break
        for i, row in enumerate(self.table):

            qi_sequence = self._get_values(row, qi_names, i)
            #print("qi_sequence",qi_sequence)
            # Skip if this row must be ignored:
            if qi_sequence is None:
                self._debug("[DEBUG] Ignoring row %d with values '%s'..." % (i, row.strip()),
                            _DEBUG)
                continue
            else:
                qi_sequence = tuple(qi_sequence)

            if qi_sequence in qi_frequency:
                occurrences = qi_frequency[qi_sequence][0] + 1
                rows_set = qi_frequency[qi_sequence][1].union([i])
                qi_frequency[qi_sequence] = (occurrences, rows_set)
            else:
                # Initialize number of occurrences and set of row indices:
                qi_frequency[qi_sequence] = (1, set())
                qi_frequency[qi_sequence][1].add(i)

                # Update domain set for each attribute in this sequence:
                for j, value in enumerate(qi_sequence):
                    domains[j].add(value)

            self._log("[LOG] Read line %d from the input file." % i, endl=False, enabled=v)

        self._log('', endl=True, enabled=v)

        lines = []
        while True:

            # Number of tuples which are not k-anonymous.
            count = 0

            for qi_sequence in qi_frequency:

                # Check number of occurrences of this sequence:
                if qi_frequency[qi_sequence][0] < k:
                    # Update the number of tuples which are not k-anonymous:
                    count += qi_frequency[qi_sequence][0]
            self._debug("[DEBUG] %d tuples are not yet k-anonymous..." % count, _DEBUG)
            self._log("[LOG] %d tuples are not yet k-anonymous..." % count, endl=True, enabled=v)

            # Limit the number of tuples to suppress to k:
            if count > k:

                # Get the attribute whose domain has the max cardinality:
                max_cardinality, max_attribute_idx = 0, None
                for attribute_idx in domains:
                    if len(domains[attribute_idx]) > max_cardinality:
                        max_cardinality = len(domains[attribute_idx])
                        max_attribute_idx = attribute_idx

                # Index of the attribute to generalize:
                attribute_idx = max_attribute_idx
                self._debug("[DEBUG] Attribute with most distinct values is '%s'..." %
                            qi_names[attribute_idx], _DEBUG)
                self._log("[LOG] Current attribute with most distinct values is '%s'." %
                          qi_names[attribute_idx], endl=True, enabled=v)

                # Generalize each value for that attribute and update the attribute set in the
                # domains dictionary:
                domains[attribute_idx] = set()
                # Look up table for the generalized values, to avoid searching in hierarchies:
                generalizations = dict()

                # Note: using the list of keys since the dictionary is changed in size at runtime
                # and it can't be used an iterator:
                for j, qi_sequence in enumerate(list(qi_frequency)):

                    self._log("[LOG] Generalizing attribute '%s' for sequence %d..." %
                              (qi_names[attribute_idx], j), endl=False, enabled=v)

                    # Get the generalized value:
                    if qi_sequence[attribute_idx] in generalizations:
                        # Find directly the generalized value in the look up table:
                        generalized_value = generalizations[attribute_idx]
                    else:
                        self._debug(
                            "[DEBUG] Generalizing value '%s'..." % qi_sequence[attribute_idx],
                            _DEBUG)
                        # Get the corresponding generalized value from the attribute DGH:
                       
                        # print(self.dghs[qi_names[attribute_idx]])
                        try:
                            generalized_value = self.dghs[qi_names[attribute_idx]].generalize(
                                qi_sequence[attribute_idx],
                                gen_levels[attribute_idx])
                        except KeyError as error:
                            self._log('', endl=True, enabled=True)
                            self._log("[ERROR] Value '%s' is not in hierarchy for attribute '%s'."
                                      % (error.args[0], qi_names[attribute_idx]),
                                      endl=True, enabled=True)
                            # output.close()
                            return

                        if generalized_value is None:
                            # Skip if it's a hierarchy root:
                            continue

                        # Add to the look up table:
                        generalizations[attribute_idx] = generalized_value

                    # Tuple with generalized value:
                    new_qi_sequence = list(qi_sequence)
                    new_qi_sequence[attribute_idx] = generalized_value
                    new_qi_sequence = tuple(new_qi_sequence)

                    # Check if there is already a tuple like this one:
                    if new_qi_sequence in qi_frequency:
                        # Update the already existing one:
                        # Update the number of occurrences:
                        occurrences = qi_frequency[new_qi_sequence][0] \
                                      + qi_frequency[qi_sequence][0]
                        # Unite the row indices sets:
                        rows_set = qi_frequency[new_qi_sequence][1]\
                            .union(qi_frequency[qi_sequence][1])
                        qi_frequency[new_qi_sequence] = (occurrences, rows_set)
                        # Remove the old sequence:
                        qi_frequency.pop(qi_sequence)
                    else:
                        # Add new tuple and remove the old one:
                        qi_frequency[new_qi_sequence] = qi_frequency.pop(qi_sequence)

                    # Update domain set with this attribute value:
                    domains[attribute_idx].add(qi_sequence[attribute_idx])

                self._log('', endl=True, enabled=v)

                # Update current level of generalization:
                gen_levels[attribute_idx] += 1

                self._log("[LOG] Generalized attribute '%s'. Current generalization level is %d." %
                          (qi_names[attribute_idx], gen_levels[attribute_idx]), endl=True,
                          enabled=v)

            else:

                self._debug("[DEBUG] Suppressing max k non k-anonymous tuples...")
                # Drop tuples which occur less than k times:
                qi_sequences = []
                for qi_sequence, data in qi_frequency.items():
                    if data[0] < k:
                        qi_sequences.append(qi_sequence)
                for qi_sequence in qi_sequences:
                    qi_frequency.pop(qi_sequence)

                self._log("[LOG] Suppressed %d tuples." % count, endl=True, enabled=v)

                # Start to read the table file from the start:
                self.table.seek(0)

                self._debug("[DEBUG] Writing the anonymized table...", _DEBUG)
                self._log("[LOG] Writing anonymized table...", endl=True, enabled=v)
                for i, row in enumerate(self.table):
                    if i>=(taille_ds+1):
                        break
                    self._debug("[DEBUG] Reading row %d from original table..." % i, _DEBUG)
                    table_row = self._get_values(row, list(self.attributes), i)
                    # print("i",i)
                    # print("table_row",table_row)
                    # Skip this row if it must be ignored:
                    if table_row is None:
                        self._debug("[DEBUG] Skipped reading row %d from original table..." % i,
                                    _DEBUG)
                        continue

                    # Find sequence corresponding to this row index:
                    for qi_sequence in qi_frequency:
                        if i in qi_frequency[qi_sequence][1]:
                            line = self._set_values(table_row, qi_sequence, qi_names)
                            lines.append(line.strip().split(','))
                            self._debug("[DEBUG] Writing line %d from original table to anonymized "
                                        "table..." % i, _DEBUG)
                            # print(line, file=output, end="")
                            break

                break

        # output.close()

        self._log("[LOG] All done.", endl=True, enabled=v)
        return lines

    @staticmethod
    def _log(content, enabled=True, endl=True):

        """
        Prints a log message.

        :param content: Content of the message.
        :param enabled: If False the message is not printed.
        """

        if enabled:
            if endl:
                print(content)
            else:
                sys.stdout.write('\r' + content)

    @staticmethod
    def _debug(content, enabled=False):

        """
        Prints a debug message.

        :param content: Content of the message.
        :param enabled: If False the message is not printed.
        """

        if enabled:
            print(content)

    def _init_table(self, pt_path: str):

        """
        Gets a reference to the table file and instantiates the attribute dictionary.

        :param pt_path:             Path to the table file.
        :raises IOError:            If the file cannot be read.
        :raises FileNotFoundError:  If the file cannot be found.
        """

        try:
            self.table = open(pt_path, 'r')
        except FileNotFoundError:
            raise

    def _get_values(self, row: str, attributes: list, row_index=None):

        """
        Gets the values corresponding to the given attributes from a row.

        :param row:         Line of the table file.
        :param attributes:  Names of the attributes to get the data of.
        :param row_index:   Index of the row in the table file.
        :return:            List of corresponding values if valid, None if this row must be ignored.
        :raises KeyError:   If an attribute name is not valid.
        :raises IOError:    If the row cannot be read.
        """

        # Ignore empty lines:
        if row.strip() == '':
            return None

    def _set_values(self, row, values, attributes: list) -> str:

        """
        Sets the values of a row for the given attributes and returns the row as a formatted string.

        :param row:         List of values of the row.
        :param values:      Values to set.
        :param attributes:  Names of the attributes to set.
        :return:            The new row as a formatted string.
        """

        pass

    def _add_dgh(self, dgh_path: str, attribute: str):

        """
        Adds a Domain Generalization Hierarchy to this table DGH collection, from its file.

        :param dgh_path:            Path to the DGH file.
        :param attribute:           Name of the attribute with this DGH.
        :raises IOError:            If the file cannot be read.
        :raises FileNotFoundError:  If the file cannot be found.
        """

        pass


class CsvTable(_Table):

    def __init__(self, taille_ds:int, pt_path: str, dgh_paths: dict):

        super().__init__(pt_path, dgh_paths)

    def __del__(self):

        super().__del__()

    def anonymize(self,taille_ds, qi_names, k, output_path, v=False):

        return super().anonymize(taille_ds,qi_names, k, output_path, v)

    def _init_table(self, pt_path):
        #ici on lit les colonnes
        super()._init_table(pt_path)

        try:
            # Try to read the first line (which contains the attribute names):
            csv_reader = csv.reader(StringIO(next(self.table)), delimiter=';')
        except IOError:
            raise

        # Initialize the dictionary of table attributes:
        for i, attribute in enumerate(next(csv_reader)):
            #print(attribute)
            self.attributes[attribute] = i
        #print(self.attributes)

    def _get_values(self, row: str, attributes: list, row_index=None):
        #ici on lit les valeurs de la table après les colonnes
        super()._get_values(row, attributes, row_index)
        num_lines_to_read = 50
        #print("row",row)
        #print("row_index",row_index)
        # Ignore the first line (which contains the attribute names):
        
        if row_index is not None and row_index == 0:
            return None
        
        # if row_index is not None and row_index >= num_lines_to_read:
        #     return None
        # Try to parse the row:
        try:
            csv_reader = csv.reader(StringIO(row), delimiter=';')
            #print(csv_reader)
        except IOError:
            raise
        parsed_row = next(csv_reader)
        #print('parsed',parsed_row)
        values = list()
        for attribute in attributes:
            if attribute in self.attributes:
                values.append(parsed_row[self.attributes[attribute]])
                
                # print("la",parsed_row[self.attributes[attribute]])
                # print('values',values)
            else:
                raise KeyError(attribute)
        #print(values)
        #values correspond à une ligne du jeu de données
        return values

    def _set_values(self, row: list, values, attributes: list):

        for i, attribute in enumerate(attributes):
            row[self.attributes[attribute]] = values[i]

        values = StringIO()
        csv_writer = csv.writer(values)
        csv_writer.writerow(row)
        
        return values.getvalue()

    def _add_dgh(self, dgh_path, attribute):

        try:
            self.dghs[attribute] = CsvDGH(dgh_path)
        except FileNotFoundError:
            raise
        except IOError:
            raise


def datafly(k, dataset, columns_names, qi_names, data_name, dgh_folder, res_folder, csv_path, taille_ds):
 
    start = datetime.now()

    dgh_paths = dict()
    for i, qi_name in enumerate(qi_names):
        dgh_paths[qi_name] = os.path.join(dgh_folder, f'{data_name}_hierarchy_{qi_name}.csv')
    
    output = f"{res_folder}/{data_name}_anonymized_{k}.csv"
    table = CsvTable(taille_ds, csv_path, dgh_paths)
    data = table.anonymize(taille_ds,qi_names, k, output, v=False)

    end = (datetime.now() - start).total_seconds()

    return data, end
#%%
#import argparse
# import csv
# import sys
# from datetime import datetime
# from io import StringIO
# import os
# import pandas as pd
# from .dgh import CsvDGH

# _DEBUG = False

# class _Table:
#     def __init__(self, dataset, columns_names: list, dgh_paths: dict):
#         print("ici",type(dataset))
#         dataset = pd.DataFrame(dataset, columns=columns_names)
#         print("la",type(dataset))
#         #print(dataset)
#         self.table = dataset
#         self.attributes = {col: i for i, col in enumerate(dataset.columns)}
#         self.dghs = dict()
#         for attribute in dgh_paths:
#             self._add_dgh(dgh_paths[attribute], attribute)

#     def anonymize(self, qi_names: list, k: int, output_path: str, v=True):
#         global _DEBUG
#         if v:
#             _DEBUG = False

#         self._debug("[DEBUG] Instantiating the QI frequency dictionary...", _DEBUG)
#         qi_frequency = dict()
#         domains = {i: set() for i, attribute in enumerate(qi_names)}
#         gen_levels = {i: 0 for i, attribute in enumerate(qi_names)}

#         for i, row in self.table.iterrows():
#             qi_sequence = self._get_values(row, qi_names, i)
#             if qi_sequence is None:
#                 self._debug(f"[DEBUG] Ignoring row {i} with values '{row.to_string()}'...", _DEBUG)
#                 continue
#             qi_sequence = tuple(qi_sequence)

#             if qi_sequence in qi_frequency:
#                 occurrences, rows_set = qi_frequency[qi_sequence]
#                 qi_frequency[qi_sequence] = (occurrences + 1, rows_set.union([i]))
#             else:
#                 qi_frequency[qi_sequence] = (1, {i})
#                 for j, value in enumerate(qi_sequence):
#                     domains[j].add(value)

#             self._log(f"[LOG] Read line {i} from the input DataFrame.", endl=False, enabled=v)

#         self._log('', endl=True, enabled=v)

#         lines = []
#         while True:
#             count = sum(occurrences for occurrences, _ in qi_frequency.values() if occurrences < k)
#             self._debug(f"[DEBUG] {count} tuples are not yet k-anonymous...", _DEBUG)
#             self._log(f"[LOG] {count} tuples are not yet k-anonymous...", endl=True, enabled=v)

#             if count > k:
#                 max_attribute_idx = max(domains, key=lambda x: len(domains[x]))
#                 self._debug(f"[DEBUG] Attribute with most distinct values is '{qi_names[max_attribute_idx]}'...", _DEBUG)
#                 self._log(f"[LOG] Current attribute with most distinct values is '{qi_names[max_attribute_idx]}'.", endl=True, enabled=v)

#                 domains[max_attribute_idx] = set()
#                 generalizations = dict()

#                 for j, qi_sequence in enumerate(list(qi_frequency)):
#                     self._log(f"[LOG] Generalizing attribute '{qi_names[max_attribute_idx]}' for sequence {j}...", endl=False, enabled=v)

#                     if qi_sequence[max_attribute_idx] in generalizations:
#                         generalized_value = generalizations[max_attribute_idx]
#                     else:
#                         try:
#                             generalized_value = self.dghs[qi_names[max_attribute_idx]].generalize(
#                                 qi_sequence[max_attribute_idx],
#                                 gen_levels[max_attribute_idx])
#                         except KeyError as error:
#                             self._log('', endl=True, enabled=True)
#                             self._log(f"[ERROR] Value '{error.args[0]}' is not in hierarchy for attribute '{qi_names[max_attribute_idx]}'.",
#                                       endl=True, enabled=True)
#                             return

#                         if generalized_value is None:
#                             continue

#                         generalizations[max_attribute_idx] = generalized_value

#                     new_qi_sequence = list(qi_sequence)
#                     new_qi_sequence[max_attribute_idx] = generalized_value
#                     new_qi_sequence = tuple(new_qi_sequence)

#                     if new_qi_sequence in qi_frequency:
#                         occurrences, rows_set = qi_frequency[new_qi_sequence]
#                         new_occurrences = occurrences + qi_frequency[qi_sequence][0]
#                         new_rows_set = rows_set.union(qi_frequency[qi_sequence][1])
#                         qi_frequency[new_qi_sequence] = (new_occurrences, new_rows_set)
#                         qi_frequency.pop(qi_sequence)
#                     else:
#                         qi_frequency[new_qi_sequence] = qi_frequency.pop(qi_sequence)

#                     domains[max_attribute_idx].add(qi_sequence[max_attribute_idx])

#                 self._log('', endl=True, enabled=v)
#                 gen_levels[max_attribute_idx] += 1
#                 self._log(f"[LOG] Generalized attribute '{qi_names[max_attribute_idx]}'. Current generalization level is {gen_levels[max_attribute_idx]}.",
#                           endl=True, enabled=v)
#             else:
#                 self._debug("[DEBUG] Suppressing max k non k-anonymous tuples...", _DEBUG)
#                 qi_sequences = [qi_sequence for qi_sequence, (occurrences, _) in qi_frequency.items() if occurrences < k]
#                 for qi_sequence in qi_sequences:
#                     qi_frequency.pop(qi_sequence)

#                 self._log(f"[LOG] Suppressed {count} tuples.", endl=True, enabled=v)

#                 self._debug("[DEBUG] Writing the anonymized table...", _DEBUG)
#                 self._log("[LOG] Writing anonymized table...", endl=True, enabled=v)
                
#                 for i, row in self.table.iterrows():
#                     self._debug(f"[DEBUG] Reading row {i} from original DataFrame...", _DEBUG)
#                     table_row = self._get_values(row, list(self.attributes), i)

#                     if table_row is None:
#                         self._debug(f"[DEBUG] Skipped reading row {i} from original DataFrame...", _DEBUG)
#                         continue

#                     for qi_sequence, (_, rows_set) in qi_frequency.items():
#                         if i in rows_set:
#                             line = self._set_values(table_row, qi_sequence, qi_names)
#                             lines.append(line)
#                             self._debug(f"[DEBUG] Writing line {i} from original DataFrame to anonymized DataFrame...", _DEBUG)
#                             break

#                 break

#         self._log("[LOG] All done.", endl=True, enabled=v)
#         return pd.DataFrame(lines, columns=self.table.columns)

#     @staticmethod
#     def _log(content, enabled=True, endl=True):
#         if enabled:
#             if endl:
#                 print(content)
#             else:
#                 sys.stdout.write('\r' + content)

#     @staticmethod
#     def _debug(content, enabled=False):
#         if enabled:
#             print(content)

#     def _get_values(self, row, attributes: list, row_index=None):
#         if row_index == 0:
#             return None
#         return [row[attr] for attr in attributes if attr in self.attributes]

#     def _set_values(self, row, values, attributes: list):
#         for i, attribute in enumerate(attributes):
#             row[self.attributes[attribute]] = values[i]
#         return row

#     def _add_dgh(self, dgh_path, attribute):
#         try:
#             self.dghs[attribute] = CsvDGH(dgh_path)
#         except FileNotFoundError:
#             raise
#         except IOError:
#             raise

# class CsvTable(_Table):
#     def __init__(self, dataset, columns_names: list, dgh_paths: dict):
#         super().__init__(dataset, columns_names, dgh_paths)
#         #print(dataset, dataset(type))
# def datafly(k, dataset, columns_names, qi_names, data_name, dgh_folder, res_folder):
#     start = datetime.now()
#     print(dataset)
#     print(type(dataset))
#     dataset = pd.DataFrame(dataset, columns=columns_names)
#     print(type(dataset))
#     dataset=dataset.values.tolist()
#     print(f"Dataset shape: {len(dataset)}")
#     print(f"Columns: {columns_names}")
#     print(f"QI names: {qi_names}")

#     dgh_paths = {qi_name: os.path.join(dgh_folder, f'{data_name}_hierarchy_{qi_name}.csv') for qi_name in qi_names}
    
#     output = f"{res_folder}/{data_name}_anonymized_{k}.csv"
#     table = CsvTable(dataset, columns_names, dgh_paths)
#     print("Table initialized")
    
#     anonymized_data = table.anonymize(qi_names, k, output, v=True)  # Set v=True for verbose output
#     print(f"Anonymized data shape: {anonymized_data.shape if anonymized_data is not None else 'None'}")

#     end = (datetime.now() - start).total_seconds()

#     return anonymized_data, end
