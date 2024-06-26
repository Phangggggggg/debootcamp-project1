import requests
from requests.adapters import HTTPAdapter, Retry
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import os

class AirPollutionApiClient:
    def __init__(self,api_key:str) -> None:
        """
        Initialize the AirPollutionApiClient object with the provided API key.

        Args:
        - api_key (str): The API key required for accessing the OpenWeatherMap API.
        
        Raises:
        - Exception: If the API key is None.
        """
        if api_key is None:
            raise Exception("API key is None.")
        self.api_key = api_key
        self.base_url = "http://api.openweathermap.org/data/2.5/air_pollution"
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])

        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
    def get_current_data(self,lat:int,long:int) -> json:
        """
        Retrieve current air pollution data for a specific location.

        Args:
        - lat (int): Latitude of the location.
        - long (int): Longitude of the location.

        Returns:
        - dict: JSON data containing current air pollution information.

        Raises:
        - Exception: If the request to the API fails or returns a non-200 status code.
        """
        url = f"{self.base_url}?lat={lat}&lon={long}&appid={self.api_key}"

        response = self.session.get(url=url)
        if response.status_code != 200:
            raise Exception( f"Failed to extract data from Air Pollution API. Status Code: {response.status_code}. Response: {response.text}")
        return response.json()['list']
    
    
    def get_historical_data(self,lat:int,long:int,start:int,end:int) -> json:
        """
        Retrieve historical air pollution data for a specific location within a specified time range.

        Args:
        - lat (int): Latitude of the location.
        - long (int): Longitude of the location.
        - start (int): Start time (Unix timestamp) of the historical data range.
        - end (int): End time (Unix timestamp) of the historical data range.

        Returns:
        - dict: JSON data containing historical air pollution information.

        Raises:
        - Exception: If the request to the API fails or returns a non-200 status code.
        """
        url = f"{self.base_url}/history?lat={lat}&lon={long}&start={start}&end={end}&appid={self.api_key}"
        response = self.session.get(url=url)
        if response.status_code != 200:
            raise Exception( f"Failed to extract data from Air Pollution API. Status Code: {response.status_code}. Response: {response.text}")
        return response.json()['list']
    
  


