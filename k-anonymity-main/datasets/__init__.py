
from utils.types import Dataset

def get_dataset_params(name):
    if name == Dataset.ADULT:
        QI_INDEX = [1, 2, 3, 4, 5, 6, 7, 8]
        target_var = 'salary-class'
        IS_CAT = [True, False, True, True, True, True, True, True]
        max_numeric = {"age": 50.5}
    elif name == Dataset.CMC:
        QI_INDEX = [1, 2, 4]
        target_var = 'method'
        IS_CAT = [False, True, False]
        max_numeric = {"age": 32.5, "children": 8}
    elif name == Dataset.MGM:
        QI_INDEX = [1, 2, 3, 4, 5]
        target_var = 'severity'
        IS_CAT = [True, False, True, True, True]
        max_numeric = {"age": 50.5}
    elif name == Dataset.CAHOUSING:
        QI_INDEX = [1, 2, 3, 8, 9]
        target_var = 'ocean_proximity'
        IS_CAT = [False, False, False, False, False]
        max_numeric = {"latitude": 119.33, "longitude": 37.245, "housing_median_age": 32.5,
                    "median_house_value": 257500, "median_income": 5.2035}
    elif name == Dataset.INFORMS:
        QI_INDEX = [3, 4, 6, 13, 16]
        target_var = "poverty"
        IS_CAT = [True, True, True, True, False]
        max_numeric = {"DOBMM": None, "DOBYY": None, "RACEX":None, "EDUCYEAR": None, "income": None}
    elif name == Dataset.ITALIA:
        QI_INDEX = [1, 2, 3]
        target_var = "disease"
        IS_CAT = [False, True, False]
        max_numeric = {"age": 50, "city_birth": None, "zip_code":50000}
        
    elif name == Dataset.MOVIE or name == Dataset.DISTRIBUTIONMOVIE or name == Dataset.LITTLEMOVIE:
        QI_INDEX = [3,4,5,6]
        target_var = "rating"
        IS_CAT = [False, True, True, True]
        
        #max_numeric = {"age": 50}
    
    elif name == Dataset.ANALYSIS or name == Dataset.DISTRIBUTIONANALYSIS or name == Dataset.LITTLEANALYSIS:
        QI_INDEX = [1,2,3,5,6]
        target_var = "Income"
        IS_CAT = [False, True, True, True, True]
        max_numeric = {"Year_Birth": 50}
    
    elif name == Dataset.SEGMENTATION or name == Dataset.DISTRIBUTIONSEGMENTATION or name == Dataset.LITTLESEGMENTATION:
        QI_INDEX = [1,2,3,4,5,6,18]
        target_var = "Segmentation_Group"
        IS_CAT = [False, True, True, True, True, True, True]
        max_numeric = {"Age": 50}
    
    
    else:
        print(f"Not support {name} dataset")
        raise ValueError
    dico = {qi: is_cat for qi, is_cat in zip(QI_INDEX, IS_CAT)}
    return {
        'qi_index': QI_INDEX,
        # 'is_category': IS_CAT,
        'dico': dico,
        'target_var': target_var
        #'max_numeric': max_numeric
    }