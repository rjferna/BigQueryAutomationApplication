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


def dict_to_json(my_dict, json_filename):
    try:
        # Write the dictionary to a JSON file
        with open(json_filename + ".json", 'w') as json_file:
            json.dump(my_dict, json_file, indent=4)  # Indent for readability (optional)

        return f'{json_filename}.json' 
    except Exception as e:
        return f"Error: {e}"
    
def dict_to_parquet(my_dict, parquet_filename):
    try:
        # Convert dictionary to DataFrame
        df = pd.DataFrame(my_dict.items(), columns=['key', 'value'])
        
        # Write the DataFrame to Parquet
        df.to_parquet(f"{parquet_filename}.parquet", engine='pyarrow')  # You can choose 'fastparquet' if preferred

        return f'{parquet_filename}.parquet' 
    except Exception as e:
        return f"Error: {e}"
    
def flatten_dict_to_parquet(my_dict, parquet_filename):
    try:
        # flatten nested dictionary.
        flat_dict = flatten(my_dict, reducer='dot')

        #Convert to dictionary
        df = pd.DataFrame([flat_dict])
        
        # Write the DataFrame to Parquet
        df.to_parquet(f"{parquet_filename}.parquet", engine='pyarrow')  # You can choose 'fastparquet' if preferred


        return f'{parquet_filename}.parquet' 
    except Exception as e:
        return f"Error: {e}"    