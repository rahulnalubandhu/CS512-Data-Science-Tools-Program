# Team Name : Group_6
#refrences :
# https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy

import csv
import json
import pandas as pd
from pathlib import Path

#read the dataset
with open("yelp_academic_dataset_business.json", "r", encoding='utf-8') as infile:
    data = infile.readlines() #read all lines
    json_data = []
    for line in data :
        line_data = json.loads(line)
        if any(val == "None" or val == "" for val in line_data.values()): # handles all Null or empty values. It doest read the whole row
            continue
        json_data.append(line_data)

json_dataframe = pd.DataFrame.from_records(json_data[:300])
json_dataframe_new = json_dataframe[['name', 'state', 'city', 'review_count']]


with open("format_json.json", "w") as outfile:
    outfile.write('[')
    for i, row in json_dataframe_new.iterrows():
        json.dump(row.to_dict(), outfile)
        if i < len(json_dataframe_new) - 1:
            outfile.write(',')
            outfile.write('\n')
    outfile.write(']')

#convert from json to csv
json_dataframe_new.to_csv('format.csv',index = False)


#convert from csv to json
with open("format.csv", "r", encoding='utf-8') as csvFile:
    csv_read = csv.DictReader(csvFile)
    conv_json_data = {}
    for line, rows in enumerate(csv_read, start=1):
        conv_json_data.update({"Business {:02}".format(line):rows})
    with open("format.json", "w", encoding='utf-8') as jsonFile:
        json.dump(conv_json_data, jsonFile, indent=4)