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




def extract_air_pollution_api(latlong_df:pd.DataFrame):
    dfs = []
    for idx,row in latlong_df.iterrows():
        province_id = row['province_id']
        data = extract_air_pollution_api_helper(api_key=API_KEY, start_date=pipeline_config['pipeline']['air_pollution_start_date'], end_date=pipeline_config['pipeline']['air_pollution_end_date'],
                                             date_format=pipeline_config['pipeline']['datetime_format'], lat=row['lat'], long=row['long'])
        df = json_normalize(data=data)
        df['province_id'] = province_id
        dfs.append(df)
    return pd.concat(dfs)
        
def transform_air_pollution_df(air_pollution_df:pd.DataFrame):
    
    dtype = {
        "created_at" :"datetime64[ns]",
        "air_quality_index": "Int64",
                "province_id": "Int64",
   
    }
    df = air_pollution_df.rename(columns={
        'main.aqi': 'air_quality_index',
        'components.co': 'co',
        'components.no': 'no',
        'components.no2': 'no2',
           'components.so2': 'so2',
        'components.o3': 'o3',
         'components.nh3': 'nh3',
            'components.pm10': 'pm10',
                        'components.pm2_5': 'pm2_5',
         'dt': 'date_time'
    })
    df['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['date_time'] =  df['date_time'].apply(lambda tstmp:  datetime.fromtimestamp(tstmp))
    df = df.dropna(subset=['date_time', 'province_id'])
    df = df.astype(dtype=dtype)
    
    return df
    
def extract_air_pollution_api_helper(api_key:str,start_date:str,end_date:str,date_format:str,lat:int,long:int) -> pd.DataFrame:
    today_date = datetime.now()
    api_client = AirPollutionApiClient(api_key=api_key)
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    start = int(start_date.timestamp())
    end =   int(end_date.timestamp())

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
        ) + api_client.get_current_data(
            lat=lat,
            long=long
        )
        
    else:
        raise ValueError('End date cannot be in the future')
    return data
                      

    
    
    
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


def load_df_to_postgres(df:pd.DataFrame, table:str,method:Literal['insert','upsert'],chunk_size:int):
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
    if postgresql_client.count_table(table_name='province') == 0:
        province_df = extract_provinces_df(file_path=pipeline_config['dataset_paths']['provinces'])
        load_df_to_postgres(df=province_df,chunk_size=CHUNK_SIZE,table='province',method='upsert')
    if postgresql_client.count_table(table_name='population') == 0:
        population_df = extract_population_df(pronvince_df=province_df,file_path=pipeline_config['dataset_paths']['population'])
        load_df_to_postgres(df=population_df,chunk_size=CHUNK_SIZE,table='population',method='upsert')
    latlong_df = generate_lat_long(provinces_id=pipeline_config['pipeline']['province_ids'])
    air_pollution_df = extract_air_pollution_api(latlong_df=latlong_df)
    air_pollution_df = transform_air_pollution_df(air_pollution_df=air_pollution_df)
    load_df_to_postgres(df=air_pollution_df,chunk_size=CHUNK_SIZE,table='air_pollution',method='upsert')
    
    
if __name__ == "__main__":
    load_dotenv()
    pipeline_config = get_pipeline_config()
    
    CHUNK_SIZE = pipeline_config['pipeline']['chunk_size']
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
