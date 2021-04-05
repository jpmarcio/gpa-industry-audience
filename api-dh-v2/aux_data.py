import pandas as pd
import numpy as np
import json
from datetime import datetime

from queries import Queries


red_file = 'files/redrates.csv'
dna_file = 'files/dna-list.json'

#redemption = pd.read_csv(red_file, sep=',', dtype={'lxx_code': object})
#redemption['banner_code'] = np.where(redemption['banner'] == 'PA+', 'PA', 'EX','CLEX')

dna_list = []
with open(dna_file) as j:
    dna_list = json.load(j)





def get_column_from_dna_id(list_ids):
    ret_list = []
    for id in list_ids:
        ret_list.append([d['dh_column'] for d in dna_list if d['codigoDnaCliente'] == id][0])
    return ret_list