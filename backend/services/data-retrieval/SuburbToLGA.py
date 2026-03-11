import json

data_file = "SUBURB_TO_LGA_DATA.json"

with open(data_file, 'r') as file:
    suburb_data = json.load(file)

def suburb_to_lga(suburb_name):
    suburb_name = suburb_name.strip().upper()
    lga = suburb_data.get(suburb_name, {}).get("councilname", "LGA not found")
    return lga.title()

print(suburb_to_lga("randwick"))
