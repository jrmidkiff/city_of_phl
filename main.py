# Author: James Midkiff
# 8 July 2022

import requests
import json
import pandas as pd # pandas >= 1.4.0, Python >= 3.9
import time
from passyunk.parser import PassyunkParser

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
    SELECT ST_AsText(the_geom) AS the_geom_text, ST_Y(the_geom) AS lat, ST_X(the_geom) AS lng, 
        pin, category_code, location, unit, house_number, street_code, 
        street_designation, street_direction, street_name, suffix, zip_code, 
        building_code, building_code_description
    FROM opa_properties_public
    '''}
    print('\nGathering OPA Records')
    start = time.time()
    opa = get_data(OPA_URL, opa_params, 'rows')
    end = time.time()
    print(f'OPA: {opa.shape[0]} rows, {opa.shape[1]} columns')
    print(f'Full OPA API pull required {round(end - start, 0)} seconds\n')

    opa['pin'] = opa['pin'].astype(str)
    opa['unit'] = opa['unit'].fillna('').str.strip()
    opa['ADDR_JRM'] = opa['location'] + ' UNIT ' + opa['unit']
    opa['ADDR_JRM'] = opa['ADDR_JRM'].str.removesuffix('UNIT ').str.strip()
    opa['category_code'] = opa['category_code'].str.strip()

    opa.index.name = 'opa'

    return opa

##### DOR
# It looks like DOR API is limited to 2000 records per pull
def get_dor(): 
    DOR_URL = 'https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/DOR_Parcel/FeatureServer/0/query'
    print('Gathering DOR Records')
    x = -1
    dor = pd.DataFrame()
    start = time.time()
    while True:
        DOR_PARAMS = {
            'where': f'OBJECTID > {x} AND STATUS IN (1,3)',
            'outFields': 'OBJECTID, ADDR_SOURCE, ADDR_STD, PIN, HOUSE, SUF, STEX, FRAC, STDIR, STNAM, STDESSUF, STDES, STEX_FRAC, STEX_SUF, UNIT, CONDOFLAG',
            'returnGeometry': False,
            'f': 'pjson'}
        dor_append = get_data(DOR_URL, DOR_PARAMS, 'features')
        if dor_append.empty:
            break
        dor = pd.concat([dor, dor_append], ignore_index=True)
        x = dor['attributes.OBJECTID'].max()
        print(f'    Record Number: {x}')
    end = time.time()

    dor.columns = dor.columns.str.removeprefix('attributes.')
    dor['HOUSE'] = dor['HOUSE'].astype('str').str.removesuffix('.0')
    dor['STEX'] = dor['STEX'].astype('str').str.removesuffix('.0')
    dor['PIN'] = dor['PIN'].astype('str').str.removesuffix('.0')
    dor['STDIR'] = dor['STDIR'].replace('<Null>', '')
    dor['HOUSE'] = dor['HOUSE'].replace('nan', '')
    dor['STEX'] = dor['STEX'].replace('nan', '')
    dor = dor.replace('^\s*$', '', regex=True)
    dor = dor.fillna('')
    
    dor['ADDR_JRM'] = dor['HOUSE'] + dor['SUF'] + '-' 
    dor['ADDR_JRM'] = dor['ADDR_JRM'] + dor['STEX']
    dor['ADDR_JRM'] = dor['ADDR_JRM'].str.removesuffix('-') + ' ' 
    dor['ADDR_JRM'] = dor[['ADDR_JRM', 'FRAC', 'STDIR', 'STNAM', 'STDES', 'STDESSUF']].agg(' '.join, axis=1) + ' UNIT '
    dor['ADDR_JRM'] = dor['ADDR_JRM'] + dor['UNIT']
    dor['ADDR_JRM'] = dor['ADDR_JRM'].str.removesuffix('UNIT ').str.strip()
    dor['ADDR_JRM'] = dor['ADDR_JRM'].replace('\s{1,}', ' ', regex=True)
    
    dor.index.name = 'dor'

    print(f'DOR: {dor.shape[0]} rows, {dor.shape[1]} columns')
    print(f'Serial DOR API pull required {round(end - start, 0)} seconds')
    
    return dor 

def merge_percentage(opa_slice, dor_slice, return_joined=False): 
    if dor_slice.name == opa_slice.name: 
        dor_slice.name = dor_slice.name + '_dor'
    joined = pd.merge(
        left=opa_slice, right=dor_slice, how='left', 
        left_on=opa_slice.name, right_on=dor_slice.name)
    joined = joined.drop_duplicates(keep='first') # Only DOR has PIN duplicates
    
    if return_joined: 
        return joined
    
    initial = opa_slice.shape[0]
    matched = joined[dor_slice.name].count()
    return str(matched / initial)

def parse(opa, dor, p): 
    for df in [opa, dor]: 
        if df.index.duplicated().any(): 
            raise IndexError(f'{df.index.name} indices not unique')
        addr_output, addr_base = [], []
        print(f'\nParsing {df.index.name} Addresses')
        for tup in df['ADDR_JRM'].iteritems(): 
            parsed = p.parse(tup[1])
            addr_output.append(parsed['components']['output_address'])
            addr_base.append(parsed['components']['base_address'])
            if tup[0] % 10000 == 0: 
                print(f'    Index: {tup[0]}')
        df['ADDR_OUTPUT'] = addr_output
        df['ADDR_BASE'] = addr_base
    # passyunk has issues with dor['OBJECTID'] in (548176, 564229) etc.                
    return opa, dor

def q1(opa, dor): 
    return merge_percentage(opa['pin'], dor['PIN']) 

def q2a(opa, dor): 
    return merge_percentage(opa['ADDR_JRM'], dor['ADDR_JRM']) 

def q2b(opa, dor): 
    return merge_percentage(opa['ADDR_OUTPUT'], dor['ADDR_OUTPUT'])

def q2c(opa, dor): 
    return merge_percentage(opa['ADDR_BASE'], dor['ADDR_BASE'])   

def q3(opa, dor): 
    resid = opa[opa['category_code'] == '1']
    resid_condos = resid[
        ((resid['the_geom_text'].duplicated(keep=False)) & 
            ~resid['the_geom_text'].isnull()) & 
        (resid['building_code_description'].str.contains('condo', case=False, na=False))].sort_values('the_geom_text')
    
    opa = (opa
        .merge(right=dor['PIN'], how='left', left_on='pin', right_on='PIN')
        .drop_duplicates(keep='first')
        .rename(columns={'PIN': 'MATCH_DOR'})
        .merge(
            right=resid_condos['pin'].rename('CONDO_JRM'), 
            how='left', left_on='pin', right_on='CONDO_JRM'))

    non_matched_condos = opa[opa['MATCH_DOR'].isnull() & ~opa['CONDO_JRM'].isnull()].shape[0]
    return non_matched_condos

if __name__ == '__main__': 
    start = time.time()
    opa = get_opa()
    dor = get_dor()
    p = PassyunkParser()
    opa, dor = parse(opa, dor, p)
    with open('answers.csv', 'w') as f: 
        f.write(f'1,Percent of OPA Parcels Aligned with DOR by PIN,{q1(opa, dor)}\n')
        f.write(f'2a,Percent of OPA Parcels Aligned with DOR by Concatenated Address,{q2a(opa, dor)}\n')
        f.write(f'2b,Percent of OPA Parcels Aligned with DOR by Full Address,{q2b(opa, dor)}\n')
        f.write(f'2c,Percent of OPA Parcels Aligned with DOR by Base Address,{q2c(opa, dor)}\n')
        f.write(f'3,Number of OPA Parcels Not Aligned with DOR by Pin (Condos),{q3(opa, dor)}\n')
    end = time.time()
    print("\nTask Complete - Answers written to 'answers.csv'")
    print(f'Time Required: {round(end-start, 0)} seconds')