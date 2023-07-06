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
to_csv=dict(sep=',', na_rep='.', float_format=None, columns=None, header=True, index=False, index_label=None, mode='w', encoding="utf-8-sig", compression='infer', quoting=csv.QUOTE_ALL, quotechar='"', chunksize=10000, date_format=None, doublequote=True, escapechar='\\', decimal='.', errors='strict')
#READ_CSV
read_csv=dict(sep=',', delimiter=None, header='infer', names=None, index_col=None, usecols=None, dtype=str, engine=None, converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, skipfooter=0, nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True, keep_date_col=False, dayfirst=False, cache_dates=True, iterator=False, chunksize=None, compression='infer', thousands=None, decimal='.', lineterminator=None, quotechar='"', quoting=csv.QUOTE_ALL, doublequote=True, escapechar='\\', comment=None, encoding=None, dialect=None, delim_whitespace=False, low_memory=True, memory_map=False, float_precision=None)

#PRELIMINARY
ek.set_app_key(appkey)


def read_data(item):
    return pd.read_csv(resources+item+".csv", **read_csv)[item].tolist()

def get_data(instruments,chunkname):
    fields = read_data('fields')
    parameters = read_data('parameters')
    for parameter in parameters:
            filename = re.findall(r'"([^"]*)"',parameter)
            filename = filename[-1]
            try:
                df,err=ek.get_data(instruments, 
                                   fields, 
                                   parameters=parameter, 
                                   field_name=False, 
                                   raw_output=False, 
                                   debug=False)
                df['parameter'] = filename
                df.to_csv(results+'/to_append/'+filename+'_'+chunkname+'.csv', **to_csv)
                print('Parameter '+parameter+' completed...')
            except:
                # Raise a warning instead of an exception
                warnings.warn('Parameter '+filename+'_'+chunkname+' raise warning!!!', RuntimeWarning)

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

def main():
    instruments = read_data('instruments')
    # Determine the size of each chunk
    chunk_size = 2000

    # Calculate the number of chunks
    total_instruments = len(instruments)
    num_chunks = (total_instruments + chunk_size - 1) // chunk_size

    # Iterate over the range of chunk numbers
    for chunk_number in range(num_chunks):
        # Determine the start and end indices for the current chunk
        start_index = chunk_number * chunk_size
        end_index = min(start_index + chunk_size, total_instruments)
        
        # Extract the chunk from the instruments vector
        chunk = instruments[start_index:end_index]

        # Call the get_data function with the current chunk and chunk number
        get_data(chunk, str(chunk_number))

    # Append data    
    append_data()
    print("Process Completed...")

if __name__ == "__main__":
    main()
