from pandas import DataFrame

def get_item_definition_manifest():
    from requests import get
    #from json import dump, load
    api_key = ''
    with open('api.key') as f:
        api_key = f.read()
    headers = {'X-API-Key': api_key}
    url = 'https://www.bungie.net/Platform/Destiny2/Manifest/'
    full_items_url = 'https://www.bungie.net' + get(url, headers=headers).json()['Response']['jsonWorldComponentContentPaths']['en']['DestinyInventoryItemDefinition']
    #with open('item_definition_manifest.json', 'w') as f:
    #    dump(get(full_items_url).json(), f)
    #item_definition_manifest = None
    #with open('item_definition_manifest.json', 'r') as f:
    #    item_definition_manifest = load(f)
    #return item_definition_manifest
    return get(full_items_url).json()

def get_plug_set_definition_manifest():
    from requests import get
    #from json import dump, load
    api_key = ''
    with open('api.key') as f:
        api_key = f.read()
    headers = {'X-API-Key': api_key}
    url = 'https://www.bungie.net/Platform/Destiny2/Manifest/'
    full_plug_set_url = 'https://www.bungie.net' + get(url, headers=headers).json()['Response']['jsonWorldComponentContentPaths']['en']['DestinyPlugSetDefinition']
    #with open('plug_set_definition_manifest.json', 'w') as f:
    #    dump(get(full_plug_set_url).json(), f)
    #plug_set_definition_manifest = None
    #with open('plug_set_definition_manifest.json', 'r') as f:
    #    plug_set_definition_manifest = load(f)
    #return plug_set_definition_manifest
    return get(full_plug_set_url).json()

def import_gsheet(sheet_id:str, sheet_page:str)-> DataFrame:
    from pandas import read_csv
    return read_csv(f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&id={sheet_id}&gid={sheet_page}')

def get_item_ids(item_definition_manifest, item_names:list[str], item_category:int)->DataFrame:
    item_definition_names = []
    item_ids = []
    for item_id, item_data in item_definition_manifest.items():
        if item_category in item_data.get('itemCategoryHashes', {}) and not 3109687656 in item_data.get('itemCategoryHashes', {}):
            item_name = item_data.get('displayProperties', {}).get('name').replace('û','u')
            if '(' in item_name:
                seperator = item_name.find(" (")
                item_name = item_name[:seperator]
            if item_name in item_names:
                item_definition_names.append(item_name)
                item_ids.append(item_id)
    return DataFrame({'Name':item_definition_names, 'Id':item_ids}).astype(str)

def get_weapon_perks(item_definition_manifest, plug_set_definition_manifest, weapon_ids:list[str])->dict[str:list[str]]:
    weapon_perks = {}
    for weapon_id in weapon_ids:
        weapon_sockets = item_definition_manifest.get(weapon_id, {}).get('sockets', {}).get('socketEntries', {})
        perks = []
        for socket in weapon_sockets:
            plug_set_id = False
            if 'randomizedPlugSetHash' in socket:
                plug_set_id = socket.get('randomizedPlugSetHash')
            elif 'reusablePlugSetHash' in socket:
                plug_set_id = socket.get('reusablePlugSetHash')
            if plug_set_id:
                plug_items = plug_set_definition_manifest.get(str(plug_set_id), {}).get('reusablePlugItems', {})
                for plug_item in plug_items:
                    perk_id = plug_item.get('plugItemHash')
                    perks.append(perk_id)
        weapon_perks[weapon_id] = perks
    return weapon_perks

def get_perk_names(item_definition_manifest, raw_perk_ids:list[str])->DataFrame:
    perk_names = []
    perk_enhanced = []
    perk_ids = []
    for perk_id in raw_perk_ids:
        perk_infos = item_definition_manifest.get(str(perk_id), {})
        perk_display = perk_infos.get('itemTypeDisplayName')
        if perk_display:
            perk_ids.append(perk_id)
            perk_names.append(perk_infos.get('displayProperties', {}).get('name'))
            perk_enhanced.append('Enhanced' in perk_display)
    return DataFrame({'Id':perk_ids, 'Name':perk_names, 'Enhanced':perk_enhanced})

def get_weapon_rolls(weapons:DataFrame) -> list[DataFrame]:
    weapon_records = weapons.to_records(index=False)
    weapon_rolls = []
    temp_roll = []
    for record in weapon_records:
        if not temp_roll:
            temp_roll.append(record)
        else:
            if record['Name']!='-':
                weapon_rolls.append(DataFrame.from_records(temp_roll, columns=weapons.columns))
                temp_roll.clear()
                temp_roll.append(record)            
            else:
                temp_roll.append(record)
    weapon_rolls.append(DataFrame.from_records(temp_roll, columns=weapons.columns))
    return weapon_rolls

def add_ids_to_weapon_roll(weapon_roll:DataFrame, weapon_ids:DataFrame, weapon_perks:dict[str:list[str]], perk_ids:DataFrame, imporportant_perk_cols:list[str])->dict:
    from pandas import concat
    weapon_ids = weapon_ids[weapon_ids['Name']==weapon_roll['Name'][0]]
    perk_cols = []
    wanted_weapon_perks = []
    other_cols = []
    for col in weapon_roll.columns:
        if 'Perk' in col:
            perk_cols.append(col)
            perk_col = weapon_roll[weapon_roll[col]!='-'][col].to_list()
            wanted_weapon_perks = wanted_weapon_perks+perk_col
        else:
            other_cols.append(col)
    weapon_perk_ids = perk_ids[perk_ids['Name'].isin(wanted_weapon_perks)]
    weapon_ids = weapon_ids[weapon_ids['Name']==weapon_roll['Name'][0]]['Id'].to_list()
    weapon_rolls_with_id = {'Empty':True, 'Ids':[], 'Sub':0}
    for weapon_id in weapon_ids:
        possible_weapon_perks = weapon_perk_ids[weapon_perk_ids['Id'].isin(weapon_perks[weapon_id])]
        enhanced = possible_weapon_perks[possible_weapon_perks['Enhanced']].drop(columns=['Enhanced'])
        unenhanced = possible_weapon_perks[possible_weapon_perks['Enhanced']==False].drop(columns=['Enhanced'])
        weapon_perks_roll = weapon_roll[perk_cols]
        weapon_description = weapon_roll[other_cols]
        for col in perk_cols:
            if weapon_perks_roll[col].isin(enhanced['Name']).any():
                enhanced_weapon_perks_roll = weapon_perks_roll.merge(enhanced.astype(str), how='left', left_on=col, right_on='Name')
                unenhanced_weapon_perks_roll = weapon_perks_roll.merge(unenhanced.astype(str), how='left', left_on=col, right_on='Name')
                weapon_perks_roll = concat([unenhanced_weapon_perks_roll, enhanced_weapon_perks_roll])
            else:
                weapon_perks_roll = weapon_perks_roll.merge(unenhanced.astype(str), how='left', left_on=col, right_on='Name')
            weapon_perks_roll = weapon_perks_roll.drop(columns=[col, 'Name'])
            weapon_perks_roll = weapon_perks_roll.rename(columns={'Id':col}).fillna('-')
            leftover_perks = weapon_perks_roll[weapon_perks_roll[col].isin(possible_weapon_perks)][col]
            for leftover in leftover_perks:
                weapon_perk_ids.replace(leftover, '-')
        is_possible = True
        for col in imporportant_perk_cols:
            is_possible = is_possible and not weapon_perks_roll[weapon_perks_roll[col]!='-'].empty
        if is_possible:
            if weapon_rolls_with_id['Empty']:
                for col in weapon_description.columns:
                    weapon_description_col = str(weapon_description[weapon_description[col]!='-'][col].to_list())
                    weapon_description_col = weapon_description_col.replace('[','').replace(']','').replace("'",'')
                    weapon_rolls_with_id[col] = weapon_description_col
                    weapon_rolls_with_id['Empty'] = False
            weapon_rolls_with_id['Ids'].append(weapon_id)
            weapon_rolls_with_id[weapon_id] = weapon_perks_roll
    return weapon_rolls_with_id

def mulitply_weapon_perks(weapon_roll:DataFrame)->DataFrame:
    from itertools import product
    weapon_perks = []
    perk_cols = weapon_roll.columns.to_list()
    for col in perk_cols:
        perks = weapon_roll[weapon_roll[col]!='-'][col].drop_duplicates()
        if perks.empty:
            perks = ['']
        weapon_perks.append(perks)
    weapon_roll = DataFrame(list(product(*weapon_perks)), columns=perk_cols).drop_duplicates()
    return weapon_roll[perk_cols]

def add_weapon_sub_rolls(weapon_roll:dict, cols_to_drop:list[str])->list[dict]:
    all_sub_rolls = []
    cols_dropped = []
    n = 1
    for col in cols_to_drop:
        sub_roll = weapon_roll.copy()
        sub_roll['Sub'] = n
        n = n+1
        cols_dropped.append(col)
        for weapon_id in weapon_roll['Ids']:
            sub_roll[weapon_id] = weapon_roll[weapon_id].drop(columns=cols_dropped).drop_duplicates()
        all_sub_rolls.append(sub_roll)
    return all_sub_rolls

def weapon_roll_to_dim_str(weapon_id:str, weapon_roll:DataFrame)->str:
    perk_cols = weapon_roll.columns.to_list()
    weapon_roll = weapon_roll.copy()
    weapon_roll['Id'] = 'dimwishlist:item='+weapon_id+'&perks='
    weapon_str = weapon_roll[['Id']+perk_cols].to_csv(index=False, header=False)
    return weapon_str.replace('=,','=').replace('-,','')

def weapon_roll_to_recipes_str(weapon_id:str, weapon_roll:DataFrame, use_cols:list[str], source:str)->str:
    recipes_str = '{"source":"'+source.replace('"','')+'","perks":['
    for col in weapon_roll.columns:
        if col in use_cols and not weapon_roll.empty:
            recipes_str = recipes_str+str(weapon_roll[col].drop_duplicates().tolist()).replace("'",'')+','
        else:
            recipes_str = recipes_str+'[],'
    recipes_str = recipes_str+'[]],"itemHash":'+weapon_id+'},'
    return recipes_str

def add_ids_to_class_items(class_items:DataFrame, class_item_ids:DataFrame, class_item_perk_ids:DataFrame)->DataFrame:
    perk_cols = class_items.columns.to_series()[class_items.columns.to_series().str.find('Perk')>=0].to_list()
    class_items_with_ids = class_items.copy()
    for col in perk_cols:
        possible_ids = class_item_perk_ids[class_item_perk_ids['Name'].isin(class_items_with_ids[col])]
        class_items_with_ids = class_items_with_ids.merge(possible_ids, how='left', left_on=col, right_on='Name')
        class_items_with_ids = class_items_with_ids.drop(columns=[col, 'Name_y'])
        class_items_with_ids = class_items_with_ids.rename(columns={'Id':col,'Name_x':'Name'})
    return class_items_with_ids.merge(class_item_ids, on='Name', how='inner')

def class_items_with_ids_to_dim_str(class_items_with_ids:DataFrame)->str:
    dim_str = '//Exotic Class Items\n'
    for row in class_items_with_ids.to_records(index=False):
        dim_str = dim_str+'dimwishlist:item='+row['Id']+'&perks='+row['Perk 1']+','+row['Perk 2']
        dim_str = dim_str+'#notes:Use-Case:'+row['Use-Case']+', Source: '+row['Source']+' Required: '+row['Required']
        dim_str = dim_str.replace("'",'')+'\n'
    return dim_str

#procedure
sheet_id = '1vJQ7Z-BGwtgbbm_Y7VR2hhlGfDv5FSSwsRMIE8ozBsA'
weapons_page = '140599422'
class_items_page = '64126422'

cope_weapons = import_gsheet(sheet_id, weapons_page).drop(columns='Unnamed: 0')
cope_weapons.fillna('-', inplace=True)
cope_weapons = cope_weapons.astype(str)
for col in cope_weapons.columns:
    cope_weapons[col] = cope_weapons[col].str.replace('\r','').str.replace('\n','')

cope_class_items = import_gsheet(sheet_id, class_items_page).drop(columns='Unnamed: 0')
cope_class_items = cope_class_items.astype(str)
for col in cope_class_items.columns:
    cope_class_items[col] = cope_class_items[col].str.replace('\r','').str.replace('\n','')

item_definition_manifest = get_item_definition_manifest()
plug_set_definition_manifest = get_plug_set_definition_manifest()

weapon_names = cope_weapons[cope_weapons['Name']!='-']['Name'].unique().tolist()
weapon_ids = get_item_ids(item_definition_manifest, weapon_names, item_category=1)
weapon_perks = get_weapon_perks(item_definition_manifest, plug_set_definition_manifest, weapon_ids['Id'].to_list())
raw_perk_ids = list(set(sum(list(weapon_perks.values()), [])))
perk_ids = get_perk_names(item_definition_manifest, raw_perk_ids)

class_item_names = cope_class_items['Name'].unique().tolist()
class_item_ids = get_item_ids(item_definition_manifest, class_item_names, item_category=20)
class_item_perk_names = cope_class_items['Perk 1'].unique().tolist()+cope_class_items['Perk 2'].unique().tolist()
class_item_perk_ids = get_item_ids(item_definition_manifest, class_item_perk_names, item_category=59)
class_items_with_ids = add_ids_to_class_items(cope_class_items, class_item_ids, class_item_perk_ids)
class_items_dim_str = class_items_with_ids_to_dim_str(class_items_with_ids)

weapon_rolls = get_weapon_rolls(cope_weapons)

weapon_rolls_with_ids = []
for weapon_roll in weapon_rolls:
    weapon_rolls_with_id = add_ids_to_weapon_roll(weapon_roll, weapon_ids, weapon_perks, perk_ids, ['Perk 3', 'Perk 4'])
    if not weapon_rolls_with_id['Empty']:
        weapon_rolls_with_ids.append(weapon_rolls_with_id)

for weapon_roll in weapon_rolls_with_ids:
    for weapon_id in weapon_roll['Ids']:
        weapon_roll[weapon_id] = mulitply_weapon_perks(weapon_roll[weapon_id])
    if weapon_roll['Craftable']=='No' and weapon_roll['Name']!='Ergo Sum':
        weapon_rolls_with_ids = weapon_rolls_with_ids+add_weapon_sub_rolls(weapon_roll, ['Perk 1','Perk 2'])

dim_wl = class_items_dim_str+'\n'
for weapon_roll in weapon_rolls_with_ids:
    for weapon_id in weapon_roll['Ids']:
        roll_str = '//'+weapon_roll['Name']+'\n//notes:'
        if weapon_roll['Name']=='Ergo Sum':
            roll_str = roll_str+'Intrinsic: '+weapon_roll['Intrinsic']+'; '
            roll_str = roll_str+'Energy: '+weapon_roll['Energy']+'; '
        roll_str = roll_str+'Masterwork: '+weapon_roll['Masterwork']
        roll_str = roll_str+'; Use-Case: '+weapon_roll['Use-Case']
        roll_str = roll_str+'; Source: '+weapon_roll['Source']
        roll_str = roll_str+'; Required: '+weapon_roll['Required']
        if weapon_roll['Sub']>0:
            roll_str = roll_str+'; Substitute: #'+str(weapon_roll['Sub'])
        roll_str = roll_str+'\n'+weapon_roll_to_dim_str(weapon_id, weapon_roll[weapon_id])+'\n'
        dim_wl = dim_wl+roll_str
with open('cope_list_dim.txt', 'w') as f:
    f.write(dim_wl.replace('\r\n','\n'))

farm_list_str = '['
for weapon_roll in weapon_rolls_with_ids:
    if weapon_roll['Required']=='Yes':
        if weapon_roll['Sub']==0:
            for weapon_id in weapon_roll['Ids']:
                if weapon_roll['Craftable']=='No':
                    farm_list_str = farm_list_str+weapon_roll_to_recipes_str(weapon_id, weapon_roll[weapon_id], ['Perk 3','Perk 4','Perk 5'], weapon_roll['Source'])
                else:
                    farm_list_str = farm_list_str+weapon_roll_to_recipes_str(weapon_id, weapon_roll[weapon_id], weapon_roll[weapon_id].columns, weapon_roll['Source'])
farm_list_str = farm_list_str[:-1]+']'
with open('cope_list_recipes.json', 'w') as f:
    f.write(farm_list_str)