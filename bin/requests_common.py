import requests
import time


def get_unix_timestamp(date_str):
    try:
        timestamp = (
            int(time.mktime(time.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ"))) * 1000
        )
        return timestamp
    except ValueError:
        print(f"Invalid date format: {date_str}")
        return None


def get_request(key, url, encoding):
    api_key = key
    base_url = url
    accepted_encoding = encoding

    headers = {
        "Accept-Encoding": accepted_encoding,  # Enables Compression
        "Authorization": f"bearer {api_key}",
    }

    response = requests.get(base_url, headers=headers)
    response_json = response.json()

    if response.status_code == 200:
        return response_json
    else:
        return f"Error: {response_json.get('error', 'Unknown error')}"


def get_request_payload(
    key, url, encoding, incremental_start_date, incremental_end_date, interval
):
    api_key = key
    base_url = url
    accepted_encoding = encoding

    if incremental_start_date == None:
        headers = {
            "Accept-Encoding": accepted_encoding,
            "Authorization": f"bearer {api_key}",
        }
        response = requests.get(base_url, headers=headers)
    else:
        interval = interval
        start_date = get_unix_timestamp(incremental_start_date)
        end_date = get_unix_timestamp(incremental_end_date)

        if start_date is None or end_date is None:
            return "Error: Date Conversion"

        headers = {
            "Accept-Encoding": accepted_encoding,
            "Authorization": f"bearer {api_key}",
        }

        params = {"start": start_date, "end": end_date, "interval": interval}

        response = requests.get(base_url, headers=headers, params=params)

    response_json = response.json()
    if response.status_code == 200:
        return response_json
    elif response_json == None:
        return f"Error: None Object Returned."
    else:
        return f"Error: {response_json.get('error', 'Unknown error')}"
