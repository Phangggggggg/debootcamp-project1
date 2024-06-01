import pytest
import pandas as pd
from dotenv import load_dotenv
import os
from etl_project.pipeline import extract_provinces_df,extract_population_df
import sys


@pytest.fixture
def setup():
    load_dotenv()

    
def test_extract_province():
    file_path='etl_project/data/provinces.csv'
    df = extract_provinces_df(file_path=file_path)
    cols = {'province_id','province_name','lat','long'}
    assert len(df) == 77
    assert len(df.columns) == len(cols)
    df_cols = set(df.columns.to_list())
    assert df_cols == cols
    
def test_extract_poplation_df():
    file_path='etl_project/data/provinces.csv'
    province_df = extract_provinces_df(file_path=file_path)
    file_path = 'etl_project/data/population.xlsx'
    df = extract_population_df(pronvince_df=province_df,file_path=file_path)
    cols = {'province_id','num_population','year'}
    assert len(df) > 0
    df_cols = set(df.columns.to_list())
    assert df_cols == cols
    assert (df['num_population'] >= 0).all()
    assert df['province_id'].isin(province_df['province_id']).all()

    

    
    
    


    
