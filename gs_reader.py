from pandas import read_csv, DataFrame

sheet_name = 'Cope_List'
sheet_id = '1vJQ7Z-BGwtgbbm_Y7VR2hhlGfDv5FSSwsRMIE8ozBsA'
url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'

cope_df = read_csv(url).drop(columns='Unnamed: 0')
cope_df.fillna(method='ffill', inplace=True)

weapon_names = []
weapon_ids = []
with open('Cope List') as cope_dim_raw:
    weapon_name = ""
    for line in cope_dim_raw:
        if "// " in line:
            weapon_name = line.replace("// ","").replace('\n', '').replace('\r', '').replace('Ã»', 'û')
            seperator = weapon_name.find("(")
            weapon_name = weapon_name[:seperator][:-1]
            print(weapon_name, len(weapon_name))
        if weapon_name!="" and "dimwishlist:item=" in line:
            seperator = line.find("&")
            weapon_id = line[:seperator].replace("dimwishlist:item=","")
            weapon_ids.append(weapon_id)
            weapon_names.append(weapon_name)
            weapon_name = ""
weapons_translation_df = DataFrame({'Name':weapon_names, 'Id':weapon_ids}).drop_duplicates()
cope_df = cope_df.merge(weapons_translation_df, left_on='Name', right_on='Name', how='inner')
cope_df['IN'] = cope_df['Name'].isin(weapons_translation_df['Name'])
print(cope_df[cope_df['IN']==False])