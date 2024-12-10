import csv
import json
import pandas as pd
from flatten_dict import flatten

def dict_to_csv(my_dict, csv_filename):
    try:
        # Open the CSV file for writing
        with open(csv_filename + ".csv", 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)

            # Write the header (keys of the dictionary)
            writer.writerow(my_dict.keys())

            # Write the values (values of the dictionary)
            writer.writerow(my_dict.values())

        return f'{csv_filename}.csv'
    except Exception as e:
        return f"Error: {e}"


def response_to_parquet(response_data, parquet_filename):
    try:
        # Load response data to DataFrame
        df = pd.DataFrame(response_data['data'])
       
        # Write the DataFrame to Parquet
        df.to_parquet(f"{parquet_filename}.parquet", engine='pyarrow')  # You can choose 'fastparquet' if preferred

        return f'{parquet_filename}.parquet' 
    except Exception as e:
        return f"Error: {e}"