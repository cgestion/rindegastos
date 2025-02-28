import requests
from dotenv import load_dotenv

load_dotenv()
token = os.getenv('API_TOKEN')

class APIAvailabilityException(Exception):
    pass

def check_api_availability():
    url = "https://api.rindegastos.com/v1/getExpenses"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            try:
                response_data = response.json()
                if "Error" in response_data:
                    raise APIAvailabilityException(f"API returned an error: {response_data['Error']}")
                return True
            except ValueError:
                raise APIAvailabilityException("API returned malformed JSON")
        else:
            raise APIAvailabilityException(f"API responded with a non-200 status code: {response.status_code}")
    except requests.exceptions.Timeout:
        raise APIAvailabilityException("Request timed out. The server might be unavailable or slow.")
    except requests.exceptions.RequestException as e:
        raise APIAvailabilityException(f"An error occurred: {e}")