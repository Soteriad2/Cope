from pandas import DataFrame, to_numeric

def get_item_definition_manifest():
    from requests import get
    from json import dump, load
    #api_key = ''
    #with open('api.key') as f:
    #    api_key = f.read()
    #headers = {'X-API-Key': api_key}
    #url = 'https://www.bungie.net/Platform/Destiny2/Manifest/'
    #full_items_url = 'https://www.bungie.net' + get(url, headers=headers).json()['Response']['jsonWorldComponentContentPaths']['en']['DestinyInventoryItemDefinition']
    #with open('item_definition_manifest.json', 'w') as f:
    #    dump(get(full_items_url).json(), f)
    item_definition_manifest = None
    with open('item_definition_manifest.json', 'r') as f:
        item_definition_manifest = load(f)
    return item_definition_manifest
    #return get(full_items_url).json()

def get_plug_set_definition_manifest():
    from requests import get
    from json import dump, load
    #api_key = ''
    #with open('api.key') as f:
    #    api_key = f.read()
    #headers = {'X-API-Key': api_key}
    #url = 'https://www.bungie.net/Platform/Destiny2/Manifest/'
    #full_plug_set_url = 'https://www.bungie.net' + get(url, headers=headers).json()['Response']['jsonWorldComponentContentPaths']['en']['DestinyPlugSetDefinition']
    #with open('plug_set_definition_manifest.json', 'w') as f:
    #    dump(get(full_plug_set_url).json(), f)
    plug_set_definition_manifest = None
    with open('plug_set_definition_manifest.json', 'r') as f:
        plug_set_definition_manifest = load(f)
    return plug_set_definition_manifest
    #return get(full_items_url).json()

def import_gsheet(sheet_id:str, sheet_name:str, sheet_page:str)-> DataFrame:
    from pandas import read_csv
    return read_csv(f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name+sheet_page}')

def get_weapon_ids(item_definition_manifest, weapons:list[str])->DataFrame:
    weapon_names = []
    weapon_ids = []
    for item_id, item_data in item_definition_manifest.items():
        if 1 in item_data.get('itemCategoryHashes', {}) and not 3109687656 in item_data.get('itemCategoryHashes', {}):
            weapon_name = item_data.get('displayProperties', {}).get('name')
            if '(' in weapon_name:
                seperator = weapon_name.find(" (")
                weapon_name = weapon_name[:seperator]
            if weapon_name in weapons:
                weapon_names.append(weapon_name)
                weapon_ids.append(item_id)
    return DataFrame({'Name':weapon_names, 'Id':weapon_ids}).astype(str)

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

def add_ids_to_weapon_roll(weapon_roll:DataFrame, weapon_ids:DataFrame, weapon_perks:dict[str:list[str]], perk_ids:DataFrame)->dict:
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
        if not (weapon_perks_roll['Perk 3'].unique()[0]=='-' or weapon_perks_roll['Perk 4'].unique()[0]=='-'):
            if weapon_rolls_with_id['Empty']:
                for col in weapon_description.columns:
                    weapon_rolls_with_id[col] = weapon_description[weapon_description[col]!='-'][col].to_list()
                    weapon_rolls_with_id['Empty'] = False
            weapon_rolls_with_id['Ids'].append(weapon_id)
            weapon_rolls_with_id[weapon_id] = weapon_perks_roll
    return weapon_rolls_with_id

def mulitply_perks(weapon_roll:DataFrame)->DataFrame:
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

def add_sub_rolls(weapon_roll:dict, cols_to_drop:list[str])->list[dict]:
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

def to_dim_str(weapon_id:str, weapon_roll:DataFrame)->str:
    perk_cols = weapon_roll.columns.to_list()
    weapon_roll = weapon_roll.copy()
    weapon_roll['Id'] = 'dimwishlist:item='+weapon_id+'&perks='
    weapon_str = weapon_roll[['Id']+perk_cols].to_csv(index=False, header=False)
    return weapon_str.replace('=,','=').replace('-,','')

def to_recipes_str(weapon_id:str, weapon_roll:DataFrame, use_cols:list[str], source:list[str])->str:
    recipes_str = '{"source":"'+str(source).replace('[', '').replace(']', '').replace("'",'').replace('"','').replace("'",'')+'","perks":['
    for col in weapon_roll.columns:
        if col in use_cols and not weapon_roll.empty:
            recipes_str = recipes_str+str(weapon_roll[col].drop_duplicates().tolist()).replace("'",'')+','
        else:
            recipes_str = recipes_str+'[],'
    recipes_str = recipes_str+'[]],"itemHash":'+weapon_id+'},'
    return recipes_str.replace(' ','')

#procedure
sheet_id = '1vJQ7Z-BGwtgbbm_Y7VR2hhlGfDv5FSSwsRMIE8ozBsA'
sheet_name = 'Cope_List'
sheet_page = '!Weapons'

cope = import_gsheet(sheet_id, sheet_name, sheet_page).drop(columns='Unnamed: 0')
cope.fillna('-', inplace=True)
cope = cope.astype(str)
for col in cope.columns:
    cope[col] = cope[col].str.replace('\r','').str.replace('\n','')

item_definition_manifest = get_item_definition_manifest()
plug_set_definition_manifest = get_plug_set_definition_manifest()

weapon_names = cope[cope['Name']!='-']['Name'].unique().tolist()
weapon_ids = get_weapon_ids(item_definition_manifest, weapon_names)
weapon_perks = get_weapon_perks(item_definition_manifest, plug_set_definition_manifest, weapon_ids['Id'].to_list())
raw_perk_ids = list(set(sum(list(weapon_perks.values()), [])))
perk_ids = get_perk_names(item_definition_manifest, raw_perk_ids)

weapon_rolls = get_weapon_rolls(cope)

weapon_rolls_with_ids = []
for weapon_roll in weapon_rolls:
    weapon_rolls_with_id = add_ids_to_weapon_roll(weapon_roll, weapon_ids, weapon_perks, perk_ids)
    if not weapon_rolls_with_id['Empty']:
        weapon_rolls_with_ids.append(weapon_rolls_with_id)

for weapon_roll in weapon_rolls_with_ids:
    for weapon_id in weapon_roll['Ids']:
        weapon_roll[weapon_id] = mulitply_perks(weapon_roll[weapon_id])
    if weapon_roll['Craftable?'][0]=='No':
        weapon_rolls_with_ids = weapon_rolls_with_ids+add_sub_rolls(weapon_roll, ['Perk 1','Perk 2'])

dim_wl = ''
for weapon_roll in weapon_rolls_with_ids:
    for weapon_id in weapon_roll['Ids']:
        roll_str = '//'+str(weapon_roll['Name'])+'\n'
        roll_str = roll_str+'//notes:Masterwork: '+str(weapon_roll['Masterwork'])
        roll_str = roll_str+'; Use-Case: '+str(weapon_roll['Use-Case'])
        roll_str = roll_str+'; Source: '+str(weapon_roll['Source'])
        roll_str = roll_str+'; Required: '+str(weapon_roll['Required?'])
        if weapon_roll['Sub']>0:
            roll_str = roll_str+'; Substitute: #'+str(weapon_roll['Sub'])
        roll_str = roll_str.replace('[', '').replace(']', '').replace("'",'')
        roll_str = roll_str+'\n'+to_dim_str(weapon_id, weapon_roll[weapon_id])+'\n'
        dim_wl = dim_wl+roll_str
with open('cope_list_dim.txt', 'w') as f:
    f.write(dim_wl.replace('\r\n','\n'))

farm_list_str = '['
for weapon_roll in weapon_rolls_with_ids:
    if weapon_roll['Required?'][0]=='Yes':
        if weapon_roll['Sub']==0:
            for weapon_id in weapon_roll['Ids']:
                if weapon_roll['Craftable?'][0]=='No':
                    farm_list_str = farm_list_str+to_recipes_str(weapon_id, weapon_roll[weapon_id], ['Perk 3','Perk 4','Perk 5'], weapon_roll['Source'])
                else:
                    farm_list_str = farm_list_str+to_recipes_str(weapon_id, weapon_roll[weapon_id], weapon_roll[weapon_id].columns, weapon_roll['Source'])
farm_list_str = farm_list_str[:-1]+']'
with open('cope_list_recipes.json', 'w') as f:
    f.write(farm_list_str)