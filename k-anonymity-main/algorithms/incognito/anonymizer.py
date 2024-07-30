import copy
import sys
import os

from .incognito import \
    incognito_Anonymization
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils.data import reorder_columns, restore_column_order

def incognito_anonymize(data,csv_path,columns_names,QI_INDEX,QI_NAMES,GEN_PATH,data_name,K_VALUE):
    """
    Incognito Anonymization
    """
    result, runtime = incognito_Anonymization(data,csv_path,columns_names,QI_INDEX,QI_NAMES,GEN_PATH,data_name,K_VALUE)
    

    return restore_column_order(result, QI_INDEX), runtime
