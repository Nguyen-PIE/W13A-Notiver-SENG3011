import os
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

data_file = os.path.join(BASE_DIR, "LGA_DATA.json")

with open(data_file, 'r') as file:
    lga_data = json.load(file)

# Returns the population of a given LGA as an int. If LGA not found, returns -1
def get_lga_population(lga_name):
    lga_name = lga_name.strip().upper()
    population = lga_data.get(lga_name, {}).get("Population", "-1")

    population = int(population.replace(',', ''))

    return population
