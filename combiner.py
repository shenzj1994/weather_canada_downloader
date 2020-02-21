from os import listdir
from os.path import isfile, join

import pandas as pd

csv_dir = './download/2020-02-21-105226/'
files = [f for f in listdir(csv_dir) if isfile(join(csv_dir, f))]
print(files)

# combine all files in the list
combined_csv = pd.concat([pd.read_csv(csv_dir + f) for f in files])
# export to csv
combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8')
