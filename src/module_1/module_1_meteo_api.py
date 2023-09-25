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


def get_data_meteo_api(city: dict[str, float]):
    lat = COORDINATES[city]["latitude"]
    long = COORDINATES[city]["longitude"]

    res = requests.get(
        f"{API_URL}latitude={lat}&longitude={long}"
        f"&start_date={START_DATE}&end_date={END_DATE}"
        f"&models={MODELS}&daily={VARIABLES}",
        timeout=10,
    )

    # Ideally printed in stderr
    print("status code:", res.status_code)

    if res.status_code != 200:
        print("error:", res.json()["error"], "\nreason:", res.json()["reason"])

    stuff = res.json()["daily"]


def main():
    for c in COORDINATES:
        print(c)
        get_data_meteo_api(c)


if __name__ == "__main__":
    main()
