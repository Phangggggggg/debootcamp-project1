from pathlib import Path
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import yaml
from shapely import Point,MultiPoint

def find_centroid(geom):
    geom = MultiPoint(geom.to_list())
    centroid = geom.centroid
    return centroid

def create_provinces_df(file_path:str) -> pd.DataFrame:
    cols_mapping  = {
        "CHANGWAT_E": "province_id",
        "LAT": "lat",
        "LONG": "long"
        
    }
    df = pd.read_csv(file_path,encoding='utf-8')
    cols = ['TAMBON_E','AMPHOE_E','CHANGWAT_E','LAT','LONG']
    df = df[cols]
    df = df.dropna(how='all')  
    grouped = df.groupby('CHANGWAT_E')  
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LAT, df.LONG))
    gdf.groupby('CHANGWAT_E')['geometry'].apply(lambda group: find_centroid(geom=group.geometry))
    gdf = gdf.drop_duplicates(subset=['CHANGWAT_E']).reset_index(drop=True)
    gdf['LAT'] = gdf.geometry.x
    gdf['LONG'] = gdf.geometry.y
    gdf.rename(columns=cols_mapping,inplace=True)
    df = gdf.drop(columns=['geometry'])
    df['province_id'] = [i+1 for i in range(len(df))]
    return df

def get_pipeline_config(config_path='etl_project/config.yaml'):
    if not Path(config_path).exists():
        raise Exception(f"The given config path: {config_path} does not exist.")
    with open(config_path) as yaml_file:
        pipeline_config = yaml.safe_load(yaml_file)
        return pipeline_config

if __name__ == "__main__":
    load_dotenv()
    pipeline_config = get_pipeline_config()
    create_provinces_df(file_path=pipeline_config['dataset_paths']['provinces'])

    