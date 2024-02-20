import requests
import csv
import re

# base URL for the SWAPI API
base_url = "https://swapi.dev/api/"

# number of pages to ingest
num_pages = 5

# Define the folder to save the ingested data
work_folder = "work/"

# pattern to remove special characters(space won't be considered as a special character)
pattern = r'[^a-zA-Z0-9\s]'

# perform data ingestion, processing and save as CSV
def ingest_data(endpoint, folder):
    # initial request to get the first page of data
    response = requests.get(endpoint)
    data = response.json()
    results = data["results"]

    # Iterate over the remaining pages and append the results
    for page in range(2, num_pages + 1):
        url = f'{endpoint}{page}'
        response = requests.get(url)
        data = response.json()
        try:
            new_results = data['results']
        except KeyError:
            new_results = data
            
        results.append(new_results)

    # Transform the data applying the pattern and lowercasing the strings
    transformed_data = []
    for row in results:
        transformed_row = {}
        for item in row:
            if isinstance(row[item], str):
                transformed_row[item] = re.sub(pattern, '', row[item]).lower()
        transformed_data.append(transformed_row)

    # Save the transformed data as CSV
    filename = endpoint.split("/")[-2] + ".csv"
    filepath = folder + filename
    with open(filepath, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=transformed_data[0].keys())
        writer.writeheader()
        writer.writerows(transformed_data)

# Ingest people data
people_endpoint = base_url + "people/"
ingest_data(people_endpoint, work_folder)

# Ingest planets data
planets_endpoint = base_url + "planets/"
ingest_data(planets_endpoint, work_folder)

# Ingest films data
films_endpoint = base_url + "films/"
ingest_data(films_endpoint, work_folder)
    