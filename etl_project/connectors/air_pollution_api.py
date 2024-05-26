import requests
import dotenv
class AirPollutionApiClients:
    def __init__(self,api_key:str) -> None:
        if api_key is None:
            raise Exception("API key is None.")
        self.api_key = api_key
        self.base_url = "http://api.openweathermap.org/data/2.5/air_pollution"
        
    def get_current_data(self,lat:int,long:int):
        url = f"{self.base_url}?lat={lat}&lon={long}&appid={self.api_key}"
        response = requests.get(url=url)
        if response.status_code != 200:
            raise Exception( f"Failed to extract data from Air Pollution API. Status Code: {response.status_code}. Response: {response.text}")
        return response.json()
    


AirPollutionApiClients()


        
        
    