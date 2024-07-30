# import os
# from .datafly import datafly

# # def datafly_anonymize(k, csv_path, qi_names, data_name, dgh_folder, res_folder):
# #     return datafly(k, qi_names, csv_path, data_name, dgh_folder, res_folder)

# def datafly_anonymize(k, dataset, qi_names, data_name, dgh_folder, res_folder):
#     return datafly(k, qi_names, dataset, data_name, dgh_folder, res_folder)

import os
from .datafly import datafly

def datafly_anonymize(k, dataset, columns_names, qi_names, data_name, dgh_folder, res_folder, csv_path,taille_ds):
    return datafly(k, dataset, columns_names, qi_names, data_name, dgh_folder, res_folder, csv_path,taille_ds)