import requests

API_KEY = 'DEIN_BUNGIE_API_KEY'
HEADERS = {'X-API-Key': API_KEY}

def get_manifest():
    url = 'https://www.bungie.net/Platform/Destiny2/Manifest/'
    response = requests.get(url, headers=HEADERS)
    return response.json()

def get_inventory_item_definitions_path(manifest):
    return manifest['Response']['jsonWorldComponentContentPaths']['en']['DestinyInventoryItemDefinition']

def find_item_id_by_name(item_name):
    manifest = get_manifest()
    item_def_path = get_inventory_item_definitions_path(manifest)
    full_url = 'https://www.bungie.net' + item_def_path
    response = requests.get(full_url)
    all_items = response.json()

    for item_hash, item_data in all_items.items():
        if item_data.get('displayProperties', {}).get('name') == item_name:
            return item_hash
    return None

# Beispielnutzung
item_name = input("Gib den Item-Namen ein: ")
item_id = find_item_id_by_name(item_name)

if item_id:
    print(f"Die Item ID von '{item_name}' ist: {item_id}")
else:
    print("Item nicht gefunden.")