import json

data_file = "SUBURB_TO_LGA_DATA.json"

with open(data_file, 'r') as file:
    suburb_data = json.load(file)

# Given the name of a suburb, returns the name of the LGA it resides in.
# If no matching LGA is found, returns "LGA not found"
# If a suburb resides in more than one LGA, returns the LGA that comes last alphabetically
def suburb_to_lga(suburb_name):
    suburb_name = suburb_name.strip().upper()
    lga = suburb_data.get(suburb_name, {}).get("councilname", "LGA not found")
    return lga.title()
