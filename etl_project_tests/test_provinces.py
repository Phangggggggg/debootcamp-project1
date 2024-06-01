import pytest
import pandas as pd
from dotenv import load_dotenv
import os
from etl_project import extract_provinces_df,extract_population_df,transformx

@pytest.fixture
def setup():
    load_dotenv()

    
def test_extract_province():
    file_path='etl_project/data/provinces.csv'
    df = extract_provinces_df(file_path=file_path)
    cols = ['province_id','province_name','lat','long']
    assert len(df) == 77
    assert len(df.columns) == len(cols)
    assert df.columns.to_list() in cols
    
def test_extract_poplation_df():
    file_path='etl_project/data/poplation.xlsx'
    province_df = extract_provinces_df(file_path=file_path)

    regions = ['Central','Northern','Southern','Northeastern'] 
    df = extract_population_df(pronvince_df=province_df,file_path=file_path)
    assert len(df) > 0
    df['provinces'].isin(regions).sum() == 0
    

    
    
    


    
