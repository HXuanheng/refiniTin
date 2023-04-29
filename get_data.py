import pandas as pd
import eikon as ek
import csv
import json
from resources.parameters import *
import warnings
import re
import os


def clean_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '', filename)


#TO_CSV
to_csv=dict(sep=',', na_rep='.', float_format=None, columns=None, header=True, index=True, index_label=None, mode='w', encoding="utf-8-sig", compression='infer', quoting=csv.QUOTE_ALL, quotechar='"', chunksize=10000, date_format=None, doublequote=True, escapechar='\\', decimal='.', errors='strict')
#READ_CSV
read_csv=dict(sep=',', delimiter=None, header='infer', names=None, index_col=None, usecols=None, dtype=str, engine=None, converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, skipfooter=0, nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True, keep_date_col=False, dayfirst=False, cache_dates=True, iterator=False, chunksize=None, compression='infer', thousands=None, decimal='.', lineterminator=None, quotechar='"', quoting=csv.QUOTE_ALL, doublequote=True, escapechar='\\', comment=None, encoding=None, dialect=None, delim_whitespace=False, low_memory=True, memory_map=False, float_precision=None)

#PRELIMINARY
ek.set_app_key(appkey)

def get_data():
    frames=None
    item="instruments"
    #COLUMN -CHANGE
    instruments=pd.read_csv(resources+item+".csv", **read_csv)[item].tolist()
    #FIELDS
    item="fields"
    fields=pd.read_csv(resources+item+".csv", **read_csv)[item].tolist()
    #PARAMETERS
    item="parameters"
    parameters=pd.read_csv(resources+item+".csv", **read_csv)[item].tolist()
    frames=[None]*len(instruments)*len(parameters)
    i=0
    for instrument in instruments:
        for parameter in parameters:
            try:
                df,err=ek.get_data(instrument, fields, parameters=parameter, field_name=False, raw_output=False, debug=False)
            except:
                # Raise a warning instead of an exception
                warnings.warn('Firm ' + instrument + ' Parameter ' + parameter + ' raise warning!!!', RuntimeWarning)
            filename = parameter[-8:-2]
            clean_ins = clean_filename(instrument)
            df.to_csv(results+'/to_append/'+clean_ins+'_'+filename+'.csv', **to_csv)
            print('Firm ' + instrument + ' Parameter ' + parameter + ' completed...')

def append_data():
    all_files = os.listdir(results+'/to_append/')
    all_csv_files = [file for file in all_files if file.endswith('.csv')]

    df_list = []
    for file in all_csv_files:
        file_path = os.path.join(results+'/to_append/', file)
        df = pd.read_csv(file_path)
        df_list.append(df)
        
    concatenated_df = pd.concat(df_list, axis=0)
    concatenated_df.to_csv(results+'data_appended.csv', index=False)
    # data=pd.concat(frames).reset_index(drop=True)
    # data.to_csv(results+"data_output.csv", **to_csv)

if __name__ == "__main__":
    get_data()
    append_data()
    print("Process Completed...")