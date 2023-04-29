import refinitiv.data as rd
from refinitiv.data.content import search
import pandas as pd
from resources.parameters import *
import csv


rd.open_session()

#TO_CSV
to_csv=dict(sep=',', na_rep='.', float_format=None, columns=None, header=True, index=True, index_label=None, mode='w', encoding="utf-8-sig", compression='infer', quoting=csv.QUOTE_ALL, quotechar='"', chunksize=10000, date_format=None, doublequote=True, escapechar='\\', decimal='.', errors='strict')
#READ_CSV
read_csv=dict(sep=',', delimiter=None, header='infer', names=None, index_col=None, usecols=None, dtype=str, engine=None, converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, skipfooter=0, nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True, keep_date_col=False, dayfirst=False, cache_dates=True, iterator=False, chunksize=None, compression='infer', thousands=None, decimal='.', lineterminator=None, quotechar='"', quoting=csv.QUOTE_ALL, doublequote=True, escapechar='\\', comment=None, encoding=None, dialect=None, delim_whitespace=False, low_memory=True, memory_map=False, float_precision=None)

frames=None
item="search"
#COLUMN -CHANGE
name_to_search=pd.read_csv(resources+item+".csv", **read_csv)[item].tolist()

def get_RIC(query, top=10):
    response = search.Definition(
        # view = search.SearchViews.INSTRUMENTS,
        query = query,
        top = top,
        select = 'RIC,DocumentTitle').get_data()
    return response.data.df

res = []
for name in name_to_search: 
    table = get_RIC(name, 10)
    table['firm_name'] = name
    res.append(table)

res1 = pd.concat(res, axis=0)

res1.to_csv(results+"ric_search.csv", **to_csv)

