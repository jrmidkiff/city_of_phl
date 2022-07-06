# Author: James Midkiff
# 3 July 2022

import requests
import json
import pandas as pd
import time
import passyunk
import numpy as np

###### Function
def get_data(url, params, data_designation):
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise Exception(f'Status Code: {r.status_code} - Reason: {r.reason}')
    try:
        text = json.loads(r.text)
        rv_text = text[data_designation]
        rv = pd.json_normalize(rv_text)
    except json.JSONDecodeError:
        raise Exception('json module unable to decode text')
    except KeyError: # Checks for no data
        raise KeyError(text)
    return rv

##### OPA
def get_opa():
    OPA_URL = 'https://phl.carto.com/api/v2/sql'
    opa_params = {'q':
    '''
    SELECT pin, location, unit, house_number, street_code, street_designation, street_direction,
        street_name, suffix, zip_code, building_code, building_code_description
    FROM opa_properties_public
    '''}

    start = time.time()
    opa = get_data(OPA_URL, opa_params, 'rows')
    end = time.time()
    print(f'OPA: {opa.shape[0]} rows, {opa.shape[1]} columns')
    print(f'Full OPA API pull required {round(end - start, 0)} seconds')

    opa['pin'] = opa['pin'].astype(str)
    opa['ADDR_JRM'] = opa['location'] + ' UNIT ' + opa['unit'].fillna('')
    opa['ADDR_JRM'] = opa['ADDR_JRM'].str.removesuffix('UNIT ').str.strip()
    return opa

##### DOR
# It looks like DOR API is limited to 2000 records per pull
def get_dor(): 
    DOR_URL = 'https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/DOR_Parcel/FeatureServer/0/query'

    x = -1
    dor = pd.DataFrame()
    start = time.time()
    while True:
        DOR_PARAMS = {
            'where': f'OBJECTID > {x} AND STATUS IN (1,3)',
            'outFields': 'OBJECTID, ADDR_SOURCE, ADDR_STD, PIN, HOUSE, STEX, FRAC, STDIR, STNAM, STDESSUF, STDES, STEX_FRAC, STEX_SUF, UNIT, CONDOFLAG',
            'returnGeometry': False,
            'f': 'pjson'}
        dor_append = get_data(DOR_URL, DOR_PARAMS, 'features')
        if dor_append.empty:
            break
        dor = pd.concat([dor, dor_append], ignore_index=True)
        x = dor['attributes.OBJECTID'].max()
        print(f'objectid: {x}')
    end = time.time()

    dor.columns = dor.columns.str.removeprefix('attributes.')
    dor['HOUSE'] = dor['HOUSE'].astype('str').str.removesuffix('.0')
    dor['STEX'] = dor['STEX'].astype('str').str.removesuffix('.0')
    dor['PIN'] = dor['PIN'].astype('str').str.removesuffix('.0')
    dor['STDIR'] = dor['STDIR'].replace('<Null>', '')
    dor['HOUSE'] = dor['HOUSE'].replace('nan', '')
    dor = dor.replace('^\s*$', '', regex=True)
    dor = dor.fillna('')
    
    dor['ADDR_JRM'] = dor[['HOUSE']] + '-' 
    dor['ADDR_JRM'] = dor['ADDR_JRM'] + dor['STEX']
    dor['ADDR_JRM'] = dor['ADDR_JRM'].str.removesuffix('-') + ' ' 
    dor['ADDR_JRM'] = dor[['ADDR_JRM', 'FRAC', 'STDIR', 'STNAM', 'STDES', 'STDESSUF']].agg(' '.join, axis=1) + ' UNIT '
    dor['ADDR_JRM'] = dor['ADDR_JRM'] + dor['UNIT']
    dor['ADDR_JRM'] = dor['ADDR_JRM'].str.removesuffix('UNIT ').str.strip()
    dor['ADDR_JRM'] = dor['ADDR_JRM'].replace('\s{1,}', ' ', regex=True)
    
    print(f'DOR: {dor.shape[0]} rows, {dor.shape[1]} columns')
    print(f'Serial DOR API pull required {round(end - start, 0)} seconds')
    
    return dor 

def q1(opa, dor): 
    joined = opa.loc[:,['pin']].merge(
        dor.loc[:,['PIN']], left_on='pin', right_on='PIN', how='left')
    initial = opa.shape[0]
    matched = joined['PIN'].count()
    return matched / initial

def q2a(opa, dor): 
    pass

def q2b(opa, dor): 
    pass

def q2c(opa, dor): 
    pass    

def q3(opa, dor): 
    joined = opa.loc[:,['pin']].merge(
        dor.loc[:,['PIN']], left_on='pin', right_on='PIN', how='left')

if __name__ == '__main__': 
    opa = get_opa()
    dor = get_dor()
    with open('answers.csv', 'w') as f: 
        f.write(f'1,Percent of OPA Parcels Aligned with DOR by PIN,{str(q1(opa, dor))}')
        f.write(f'2a,,{str(q2a(opa, dor))}')
        f.write(f'2b,,{str(q2b(opa, dor))}')
        f.write(f'2c,,{str(q2c(opa, dor))}')
        f.write(f'3,Percent of OPA Parcels Not Aligned with DOR by Pin (Condos),{str(q3(opa, dor))}')

        

