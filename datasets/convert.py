import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('file_name', type = str)

args = parser.parse_args()

org_file = args.file_name

trajs = json.load(open(org_file))
new_trajs = []

for key, value in trajs.items():
    new_dict = {}
    new_dict['id'] = key
    new_dict['points'] = value
    new_trajs.append(new_dict)
                    
with open(org_file.split('.')[0] + '_converted.json', 'w') as f:
    json.dump(new_trajs, f)