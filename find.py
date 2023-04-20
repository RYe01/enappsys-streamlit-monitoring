import json

countries = ['eu', 'al', 'at', 'ba', 'be', 'bg', 'ch', 'cz', 'de', 'dk', 'ee', 'es', 'fi', 'fr', 'gb', 'gr', 'hr', 'hu', 'isem', 'it', 'xk', 'lt', 'lv', 'me', 'mk', 'nl', 'no', 'pl', 'pt', 'ro', 'rs', 'se', 'si', 'sk']

for cc in countries:
    with open(f'./chart_mappings_per_country/{cc}-chart_mapping.json', "r") as json_file:
        data = json.load(json_file)

    for chart_name in data.keys():
        for pair_name, pair_values in data[chart_name].items():
            datatype = pair_values["DATATYPE"]
            entity = pair_values["ENTITY"]
            # if '_fuel_mix_forecast' in datatype.lower():
            with open('find.txt', "a") as find:
                find.write(f"{datatype} - {entity} '\n'")
