from pathlib import Path
from typing import Literal
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import yaml
import re
from shapely import Point, MultiPoint


from etl_project.connectors.postgres import PostgreSqlClient
from etl_project.connectors.air_pollution_api import AirPollutionApiClient
from etl_project.connectors.metadata_logging import MetaDataLogging,MetaDataLoggingStatus
from etl_project.connectors.pipeline_logging import PipelineLogging

from datetime import datetime
import os
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    select,
    inspect,
)
import json
from pandas import json_normalize
import schedule
import time


def find_centroid(geom):
    geom = MultiPoint(geom.to_list())
    centroid = geom.centroid
    return centroid


def extract_provinces_df(file_path: str) -> pd.DataFrame:
    """Read provinces.csv, exclude null values and rename columns"""
    cols_mapping = {"CHANGWAT_E": "province_name", "LAT": "lat", "LONG": "long"}
    df = pd.read_csv(file_path, encoding="utf-8")
    cols = ["TAMBON_E", "AMPHOE_E", "CHANGWAT_E", "LAT", "LONG"]
    df = df[cols]
    df = df.dropna(how="all")
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LAT, df.LONG))
    gdf.groupby("CHANGWAT_E")["geometry"].apply(
        lambda group: find_centroid(geom=group.geometry)
    )
    gdf = gdf.drop_duplicates(subset=["CHANGWAT_E"]).reset_index(drop=True)
    gdf["LAT"] = gdf.geometry.x
    gdf["LONG"] = gdf.geometry.y
    gdf.rename(columns=cols_mapping, inplace=True)
    df = gdf.drop(columns=["geometry", "TAMBON_E", "AMPHOE_E"])
    df["province_id"] = [i + 1 for i in range(len(df))]
    df["province_name"] = df["province_name"].str.strip()

    return df


def extract_population_df(pronvince_df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    """Read population.csv, exclude null values and rename columns"""
    dtype = {"year": "Int64", "num_population": "float64", "province_id": "Int64"}

    df = pd.read_excel(file_path, skiprows=3, skipfooter=2)
    df = df.dropna(how="all")
    df.columns = df.columns.astype(str)

    regions = df["Region"].unique()
    df = df[(df["Age group"] == "Total") & ~(df["Province"].isin(regions))]
    df["Province"] = df["Province"].str.replace(" Province", "").str.strip()
    numeric_columns = [col for col in df.columns if re.search(r"\d", col)]
    df = pd.melt(
        df,
        id_vars=["Province"],
        value_vars=numeric_columns,
        value_name="num_population",
    )
    df = df.merge(
        pronvince_df[["province_name", "province_id"]],
        left_on=["Province"],
        right_on=["province_name"],
        how="left",
    )
    df = df.drop(columns=["Province", "province_name"])
    df = df.rename(columns={"variable": "year"})
    df = df.astype(dtype=dtype)

    return df


def generate_lat_long(provinces_id,postgresql_client):
    """Generate latitudes and longtitudes"""
    table = postgresql_client.get_table(table_name="province")
    stmt = select([table.c.province_id, table.c.lat, table.c.long]).where(
        table.c.province_id.in_(provinces_id)
    )
    df = postgresql_client.execute_query(stmt=stmt)
    return df


def extract_air_pollution_api(latlong_df: pd.DataFrame,API_KEY:str):
    """Using API key to extract data"""
    dfs = []
    for idx, row in latlong_df.iterrows():
        province_id = row["province_id"]
        data = extract_air_pollution_api_helper(
            api_key=API_KEY,
            start_date=pipeline_config["pipeline"]["air_pollution_start_date"],
            end_date=pipeline_config["pipeline"]["air_pollution_end_date"],
            date_format=pipeline_config["pipeline"]["datetime_format"],
            lat=row["lat"],
            long=row["long"],
        )
        df = json_normalize(data=data)
        df["province_id"] = province_id
        dfs.append(df)
    return pd.concat(dfs)


def transform_air_pollution_df(air_pollution_df: pd.DataFrame):
    """Rename columns and add datetime"""
    dtype = {
        "created_at": "datetime64[ns]",
        "air_quality_index": "Int64",
        "province_id": "Int64",
    }
    df = air_pollution_df.rename(
        columns={
            "main.aqi": "air_quality_index",
            "components.co": "co",
            "components.no": "no",
            "components.no2": "no2",
            "components.so2": "so2",
            "components.o3": "o3",
            "components.nh3": "nh3",
            "components.pm10": "pm10",
            "components.pm2_5": "pm2_5",
            "dt": "date_time",
        }
    )
    df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["date_time"] = df["date_time"].apply(lambda tstmp: datetime.fromtimestamp(tstmp))
    df = df.dropna(subset=["date_time", "province_id"])
    df = df.astype(dtype=dtype)

    return df


def extract_air_pollution_api_helper(
    api_key: str, start_date: str, end_date: str, date_format: str, lat: int, long: int
) -> pd.DataFrame:
    """Get historical data and current data combined from Air Pollution API"""
    today_date = datetime.now()
    api_client = AirPollutionApiClient(api_key=api_key)
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    start = int(start_date.timestamp())
    end = int(end_date.timestamp())

    if end_date < today_date:
        data = api_client.get_historical_data(lat=lat, long=long, start=start, end=end)
    elif end_date == today_date:
        end_date = end_date - 1
        end = int(datetime.strptime(end_date, date_format).timestamp())

        data = api_client.get_historical_data(
            lat=lat, long=long, start=start, end=end
        ) + api_client.get_current_data(lat=lat, long=long)

    else:
        raise ValueError("End date cannot be in the future")
    return data


def get_pipeline_config(config_path="etl_project/config.yaml"):
    """Get information from config file"""
    if not Path(config_path).exists():
        raise Exception(f"The given config path: {config_path} does not exist.")
    with open(config_path) as yaml_file:
        pipeline_config = yaml.safe_load(yaml_file)
        return pipeline_config


def read_table_structure(table_name, file_location):
    """Read table structure in json and create metadata"""
    # Read json file
    with open(file_location, "r") as file:
        table_definition = json.load(file)

    # Changing type to sqlalchemy
    type_mapping = {
        "DOUBLE PRECISION": Float,
        "INTEGER": Integer,
        "DATETIME": DateTime,
        "TIMESTAMP": DateTime,
        "VARCHAR": String,
    }

    # Define table
    columns = table_definition[table_name]["columns"]
    primary_key = table_definition.get(table_name, {}).get("primaryKey", None)

    # Create metadata
    metadata = MetaData()

    # Create table
    if primary_key:
        table = Table(
            table_name,
            metadata,
            *(
                Column(
                    col["name"],
                    type_mapping[col["type"]],
                    primary_key=(col["name"] in primary_key),
                )
                for col in columns
            ),
        )
    else:
        table = Table(
            table_name,
            metadata,
            *(Column(col["name"], type_mapping[col["type"]]) for col in columns),
        )
    return metadata



def create(
    table_name: str, metadata: MetaData, postgresql_client: PostgreSqlClient
) -> None:
    """Write single table to progres database"""
    postgresql_client.create_table(table_name=table_name, metadata=metadata)



def create_multi_table(
    table_names: list,
    file_location: str,
    postgresql_client
):
    """Write multiple table to progres database"""
    for table_name in table_names:
        metadata = read_table_structure(table_name, file_location)
        if not postgresql_client.has_table(table_name=table_name):
            create(
                table_name=table_name,
                metadata=metadata,
                postgresql_client=postgresql_client,
            )


def load_df_to_postgres(
    df: pd.DataFrame, table: str, method: Literal["insert", "upsert"], chunk_size: int,postgresql_client
):
    """Load dataframe to progres database"""
    data = df.to_dict(orient="records")

    table = postgresql_client.get_table(table_name=table)
    for chunk_start in range(0, len(data), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(data))
        chunk_data = data[chunk_start:chunk_end]

        if method == "insert":
            postgresql_client.insert(
                data=chunk_data, table=table, metadata=table.metadata
            )
        elif method == "upsert":
            postgresql_client.upsert(
                data=chunk_data, table=table, metadata=table.metadata
            )
        else:
            raise ValueError(
                f"Unsupported method: {method}. Please use 'insert' or 'upsert'."
            )


def run_air_pollution_pipleine(pipeline_config):
    """Pipleline to extract air pollution data and load it into database"""
    
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=pipeline_config.get("log_folder_path"),
    )
    CHUNK_SIZE = pipeline_config["pipeline"]["chunk_size"]
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
    # pipeline logging
    pipeline_logging.logger.info("Connected to postgres.")
    pipeline_logging.logger.info("Starting pipeline run")
    # metadata logging
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_config.get("name"),
        postgresql_client=postgresql_client,
        config=pipeline_config.get("config"),
    )
    # run pipeline
    try:
        metadata_logger.log()  # log start

        # create environment variables (if any)
        pipeline_logging.logger.info("Getting pipeline environment variables")

        # Create target table
        pipeline_logging.logger.info("Creating tables in postgres")
        create_multi_table(table_names, pipeline_config["table_structure"]["path"],postgresql_client)

        # Extract and Load
        pipeline_logging.logger.info("Extracting & Loading data")
        if postgresql_client.count_table(table_name="province") == 0:
            province_df = extract_provinces_df(
                file_path=pipeline_config["dataset_paths"]["provinces"]
            )
            load_df_to_postgres(
                df=province_df, chunk_size=CHUNK_SIZE, table="province", method="upsert"
            )
        if postgresql_client.count_table(table_name="population") == 0:
            population_df = extract_population_df(
                pronvince_df=province_df,
                file_path=pipeline_config["dataset_paths"]["population"],
            )
            load_df_to_postgres(
                df=population_df,
                chunk_size=CHUNK_SIZE,
                table="population",
                method="upsert",
                postgresql_client=postgresql_client
            )
        latlong_df = generate_lat_long(
            provinces_id=pipeline_config["pipeline"]["province_ids"],postgresql_client=postgresql_client
        )
        air_pollution_df = extract_air_pollution_api(latlong_df=latlong_df,API_KEY=API_KEY)
        air_pollution_df = transform_air_pollution_df(air_pollution_df=air_pollution_df)
        load_df_to_postgres(
            df=air_pollution_df,
            chunk_size=CHUNK_SIZE,
            table="air_pollution",
            method="upsert",
            postgresql_client=postgresql_client
        )
        pipeline_logging.logger.info("Pipeline run successful")

    # If error
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()

    # Checking what views exist in database
    pipeline_logging.logger.info("Inspecting database views")
    inspector = inspect(postgresql_client.engine)

    sql_folder_path = pipeline_config.get("sql_folder_path")
    for sql_file in os.listdir(sql_folder_path):
        view = sql_file.split(".")[0]  # name of view to match the name of the sql file
        file_path = os.path.join(sql_folder_path, sql_file)
        if view not in inspector.get_view_names():
            pipeline_logging.logger.info(f"View {view} does not exist - Creating view")
            with open(file_path, "r") as f:
                sql_query = f.read()
                postgresql_client.engine.execute(f"create view {view} as {sql_query};")
                pipeline_logging.logger.info(f"Successfully created view {view}")
        else:
            pipeline_logging.logger.info(f"View {view} already exists in database")



if __name__ == "__main__":
    load_dotenv()
    pipeline_config = get_pipeline_config()

    # Select physical table to create
    table_names = pipeline_config["table_names"]
    sql_folder_path = pipeline_config["sql_folder_path"]

    schedule.every(pipeline_config['schedule']['run_seconds']).seconds.do(
        run_air_pollution_pipleine,
        pipeline_config=pipeline_config
    )
    
    while True:
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))

    # run_air_pollution_pipleine(pipeline_config=pipeline_config)
