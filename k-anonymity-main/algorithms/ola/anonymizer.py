import copy
import sys
import os

from .ola import \
    ola_Anonymization
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils.data import reorder_columns, restore_column_order

def ola_anonymize(data,columns_names,QI_INDEX,QI_NAMES, GEN_PATH,data_name,K_VALUE,MAX_SUPPRESSION,INFO_LOSS_CHOICE):
    """
    Ola Anonymization
    """

    result, runtime = ola_Anonymization(data,columns_names,QI_INDEX,QI_NAMES, GEN_PATH,data_name,K_VALUE,MAX_SUPPRESSION,INFO_LOSS_CHOICE)
    

    return restore_column_order(result, QI_INDEX), runtime
