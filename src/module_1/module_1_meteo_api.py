from typing import Dict
from urllib.parse import urlencode
import sys
import time
# import pandas as pd
import json
import requests


API_URL = "https://climate-api.open-meteo.com/v1/climate?"

COORDINATES = {
    "Madrid": {"latitude": 40.416775, "longitude": -3.703790},
    "London": {"latitude": 51.507351, "longitude": -0.127758},
    "Rio": {"latitude": -22.906847, "longitude": -43.172896},
}

START_DATE = "1950-01-01"
END_DATE = "2050-01-01"

MODELS = "CMCC_CM2_VHR4,FGOALS_f3_H,HiRAM_SIT_HR,MRI_AGCM3_2_S,EC_Earth3P_HR,MPI_ESM1_2_XR,NICAM16_8S"  # noqa

VARIABLES = "temperature_2m_mean,precipitation_sum,soil_moisture_0_to_10cm_mean"

NUM_ATTEMPTS = 5

TIMEOUT_LIMIT = 10


# TODO Check if it's better to use logger (.info / .Warning)
def _print_2_stderr(*strings: str):
    """
    Private function to avoid the repetition of code
    in order to print debug info in the stderr
    """
    print(*strings, file=sys.stderr)


def get_data_meteo_api(city: Dict[str, float]):
    """"
    Responsible of getting the climate information about the city passed
    as an argument from the API.
    """

    url_params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": START_DATE,
        "end_date": END_DATE,
        "models": MODELS,
        "daily": VARIABLES,
    }

    # safe = "," avoids possible problems due to the comma separated values
    # from the models string
    url_ready = API_URL + urlencode(url_params, safe=",")

    return request_with_backoff(url_ready)


def request_with_backoff(url: str):
    """
    Function in charge of making the api requests implementing a
    backoff system to control the behavior of the program in case of
    calling the api too many times.
    Would be a good idea to implement as parameters things
    like headers of the url, payload... to make it more generic
    Also, it would become more efficient by actually checking
    the rate limit of each api so that no time is wasted
    """

    call_count = 0
    time_2_wait = 1

    for call_count in range(NUM_ATTEMPTS-1):
        try:
            # By setting a timeout we will avoid getting stuck forever
            res = requests.get(url, timeout=TIMEOUT_LIMIT)

            # This function will raise the relevant exceptions by
            # checking the status_code for us
            res.raise_for_status()

        # In case of overloading the api
        except requests.exceptions.ConnectionError as exc_ce:
            _print_2_stderr("Connection error\n", exc_ce)
            call_count += 1
            time.sleep(time_2_wait)
            time_2_wait *= 2
            continue

        # In case we obtain a status code different to 200
        except requests.exceptions.HTTPError as exc_http:
            _print_2_stderr("Status code returned different to 200:\n", exc_http)

            if res.status_code == 404:
                raise  # Doesn't make sense to keep trying if it's a 404

            call_count += 1
            time.sleep(time_2_wait)
            time_2_wait *= 2
            continue

        # In case the api is taking too long to answer
        except requests.exceptions.Timeout as exc_to:
            _print_2_stderr("Timeout error\n", exc_to)
            call_count += 1
            time.sleep(time_2_wait)
            time_2_wait *= 2
            continue

        return json.loads(res.text)  # Alternatively use content.decode("UTF-8")

    # If the execution thread arrives here it means the program ran out of attempts
    _print_2_stderr("Number of attempts limit reached!")
    return -1


def main():
    for city in COORDINATES:
        print(get_data_meteo_api(COORDINATES[city]))


if __name__ == "__main__":
    main()
