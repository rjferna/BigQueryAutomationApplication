import csv
import json

def dict_to_csv(my_dict, csv_filename):
    """
    Converts a dictionary to a CSV file.
    Args: my_dict (dict): The input dictionary; csv_filename (str): The desired filename for the CSV output.
    Returns: str: A message indicating success or an error.
    """
    try:
        # Open the CSV file for writing
        with open(csv_filename + ".csv", 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)

            # Write the header (keys of the dictionary)
            writer.writerow(my_dict.keys())

            # Write the values (values of the dictionary)
            writer.writerow(my_dict.values())

        return f"CSV file '{csv_filename}.csv' created successfully!"
    except Exception as e:
        return f"Error: {e}"


def dict_to_json(my_dict, json_filename):
    """
    Converts a dictionary to a JSON file.
    Args: my_dict (dict): The input dictionary;  json_filename (str): The desired filename for the JSON output.
    Returns: None
    """
    try:
        # Write the dictionary to a JSON file
        with open(json_filename + ".json", 'w') as json_file:
            json.dump(my_dict, json_file, indent=4)  # Indent for readability (optional)

        return f"JSON file '{json_filename}' created successfully!"
    except Exception as e:
        return f"Error: {e}"