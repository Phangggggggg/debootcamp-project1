from pathlib import Path
from typing import Literal
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import yaml
import re
from shapely import Point,MultiPoint
from connectors.postgres import PostgreSqlClient
from connectors.air_pollution_api import AirPollutionApiClient
from datetime import datetime
import os
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime,select
import json
from pandas import json_normalize


def find_centroid(geom):
    geom = MultiPoint(geom.to_list())
    centroid = geom.centroid
    return centroid

def extract_provinces_df(file_path:str) -> pd.DataFrame:
    
        cols_mapping  = {
            "CHANGWAT_E": "province_name",
            "LAT": "lat",
            "LONG": "long"  
        }
        df = pd.read_csv(file_path,encoding='utf-8')
        cols = ['TAMBON_E','AMPHOE_E','CHANGWAT_E','LAT','LONG']
        df = df[cols]
        df = df.dropna(how='all')  
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LAT, df.LONG))
        gdf.groupby('CHANGWAT_E')['geometry'].apply(lambda group: find_centroid(geom=group.geometry))
        gdf = gdf.drop_duplicates(subset=['CHANGWAT_E']).reset_index(drop=True)
        gdf['LAT'] = gdf.geometry.x
        gdf['LONG'] = gdf.geometry.y
        gdf.rename(columns=cols_mapping,inplace=True)
        df = gdf.drop(columns=['geometry','TAMBON_E','AMPHOE_E'])
        df['province_id'] = [i+1 for i in range(len(df))]
        df['province_name'] = df['province_name'].str.strip()

        return df

def extract_population_df(pronvince_df:pd.DataFrame,file_path:str) -> pd.DataFrame:

    dtype = {
        'year': 'Int64',
         'num_population': 'float64',
         'province_id': 'Int64'
    }

    df = pd.read_excel(file_path,skiprows=3,skipfooter=2)
    df = df.dropna(how='all')  
    df.columns = df.columns.astype(str)

    regions = df['Region'].unique()
    df = df[(df['Age group'] == 'Total') & ~(df['Province'].isin(regions))]
    df['Province'] = df['Province'].str.replace(" Province","").str.strip()
    numeric_columns = [col for col in df.columns if re.search(r'\d', col)]
    df = pd.melt(df, id_vars=['Province'], value_vars=numeric_columns,value_name='num_population')
    df = df.merge(pronvince_df[['province_name','province_id']], left_on=['Province'], right_on=['province_name'], how='left')
    df = df.drop(columns=['Province','province_name'])
    df = df.rename(columns={
        "variable":"year"
    })
    df = df.astype(dtype=dtype)
    
    return df

def generate_lat_long(provinces_id):
    table = postgresql_client.get_table(table_name='province')
    stmt = select([table.c.province_id, table.c.lat,table.c.long]).where(table.c.province_id.in_(provinces_id))
    df = postgresql_client.execute_query(stmt=stmt)
    return df


def extract_air_pollution_api(latlong_df:pd.DataFrame,start_date:str,end_date:str,date_format:str):
    for idx,row in latlong_df.iterrows():
        province_id = row['province_id']
        extract_air_pollution_api_helper(api_key=API_KEY,start_date=start_date,end_date=end_date,date_format=date_format,lat=row['lat'],long=row['long'])

def extract_air_pollution_api_helper(api_key:str,start_date:str,end_date:str,date_format:str,lat:int,long:int) -> pd.DataFrame:
    today_date = datetime.now()
    api_client = AirPollutionApiClient(api_key=api_key)
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    start = int(start_date.timestamp())
    end =   int(end_date.timestamp())
    
    result = []
    if end_date < today_date:
        data = api_client.get_historical_data(
            lat=lat,
            long=long,
            start=start,
            end=end
        )
    elif end_date == today_date:
        end_date = end_date - 1
        end =   int(datetime.strptime(end_date,date_format).timestamp())
    
        data = api_client.get_historical_data(
            lat=lat,
            long=long,
            start=start,
            end=end
        )
        today_data = api_client.get_current_data(
            lat=lat,
            long=long
        )
    else:
        raise ValueError('End date cannot be in the future')
     
                      

    
    
    
def get_pipeline_config(config_path='etl_project/config.yaml'):
    if not Path(config_path).exists():
        raise Exception(f"The given config path: {config_path} does not exist.")
    with open(config_path) as yaml_file:
        pipeline_config = yaml.safe_load(yaml_file)
        return pipeline_config


# Read table structure in json and create metadata
def read_table_structure(table_name, file_location):
    # Read json file
    with open(file_location, "r") as file:
        table_definition = json.load(file)

    # Changing type to sqlalchemy
    type_mapping = {
        'DOUBLE PRECISION': Float,
        'INTEGER': Integer,
        'DATETIME': DateTime,
        'TIMESTAMP': DateTime,
        'VARCHAR': String
    }

    # Define table
    columns = table_definition[table_name]['columns']
    primary_key = table_definition[table_name]['primaryKey']

    # Create metadata
    metadata = MetaData()

    # Create table
    table = Table(
        table_name,
        metadata,
        *(Column(col['name'], type_mapping[col['type']], primary_key=(col['name'] in primary_key)) for col in columns)
    )
    return metadata


# Write table
def create(
        table_name: str,
        metadata: MetaData,
        postgresql_client: PostgreSqlClient
) -> None:
    postgresql_client.create_table(
        table_name=table_name, metadata=metadata
    )



# Write multi tables
def create_multi_table(table_names: list, file_location:str,):
    for table_name in table_names:
        metadata = read_table_structure(table_name, file_location)
        if not postgresql_client.has_table(table_name=table_name):
            create(table_name=table_name, metadata=metadata, postgresql_client=postgresql_client)


def load_df_to_postgres(df:pd.DataFrame,chunk_size:int, table:str,method:Literal['insert','upsert']):
    data = df.to_dict(orient='records')
 
    table = postgresql_client.get_table(table_name=table)
    for chunk_start in range(0, len(data), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(data))
        chunk_data = data[chunk_start:chunk_end]

        if method == 'insert':
            postgresql_client.insert(data=chunk_data, table=table, metadata=table.metadata)
        elif method == 'upsert':
            postgresql_client.upsert(data=chunk_data, table=table, metadata=table.metadata)
        else:
            raise ValueError(f"Unsupported method: {method}. Please use 'insert' or 'upsert'.")


            
        


def run_air_pollution_pipleine():
    
    create_multi_table(table_names, pipeline_config['table_structure']['path'])
    province_df = extract_provinces_df(file_path=pipeline_config['dataset_paths']['provinces'])
    load_df_to_postgres(df=province_df,chunk_size=1000,table='province',method='upsert')
    population_df = extract_population_df(pronvince_df=province_df,file_path=pipeline_config['dataset_paths']['population'])
    load_df_to_postgres(df=population_df,chunk_size=1000,table='population',method='upsert')
    latlong_df = generate_lat_long(provinces_id=pipeline_config['pipeline']['province_ids'])
    extract_air_pollution_api(latlong_df=latlong_df,start_date=pipeline_config['pipeline']['air_pollution_start_date'],end_date=pipeline_config['pipeline']['air_pollution_end_date'],date_format=pipeline_config['pipeline']['datetime_format'])
if __name__ == "__main__":
    load_dotenv()
    pipeline_config = get_pipeline_config()
    
    
    API_KEY = os.environ.get("API_KEY")
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")

    postgresql_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    # Select physical table to create
    table_names = ['air_pollution',
                   'province',
                   'population'
                   ]

    run_air_pollution_pipleine()
