from pathlib import Path
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import yaml
import re
from shapely import Point,MultiPoint

def find_centroid(geom):
    geom = MultiPoint(geom.to_list())
    centroid = geom.centroid
    return centroid

def create_provinces_df(file_path:str) -> pd.DataFrame:
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
    return df

def create_population_df(pronvince_df:pd.DataFrame,file_path:str) -> pd.DataFrame:

    df = pd.read_excel(file_path,skiprows=3,skipfooter=2)
    df = df.dropna(how='all')  
    df.columns = df.columns.astype(str)
    regions = df['Region'].unique()
    df = df[(df['Age group'] == 'Total') & ~(df['Province'].isin(regions))]
    df['Province'] = df['Province'].str.replace(" Province","")
    numeric_columns = [col for col in df.columns if re.search(r'\d', col)]
    df = pd.melt(df, id_vars=['Province'], value_vars=numeric_columns,value_name='year')
    print(1)

def get_pipeline_config(config_path='etl_project/config.yaml'):
    if not Path(config_path).exists():
        raise Exception(f"The given config path: {config_path} does not exist.")
    with open(config_path) as yaml_file:
        pipeline_config = yaml.safe_load(yaml_file)
        return pipeline_config

if __name__ == "__main__":
    load_dotenv()
    pipeline_config = get_pipeline_config()
    province_df = create_provinces_df(file_path=pipeline_config['dataset_paths']['provinces'])
    create_population_df(pronvince_df=province_df,file_path=pipeline_config['dataset_paths']['population'])

    