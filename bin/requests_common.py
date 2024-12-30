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


def get_request_payload(key, url, encoding, start, end, interval):
    api_key = key
    base_url = url
    accepted_encoding = encoding
    interval = interval

    start_timestamp = start
    end_timestamp = end

    start_date = get_unix_timestamp(start_timestamp)
    end_date = get_unix_timestamp(end_timestamp)

    if start_date is None or end_date is None:
        return None

    param = {
        "Accept-Encoding": accepted_encoding,  # Enables Compression
        "start": start_date,
        "end": end_date,
        "interval": interval,
    }

    response = requests.get(base_url, params=param)
    response_json = response.json()

    if response.status_code == 200:
        # dict_to_csv(response_json, file_path + file_name)
        return response_json
    else:
        return f"Error: {response_json.get('error', 'Unknown error')}"
