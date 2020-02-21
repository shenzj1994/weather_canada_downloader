from os import listdir
from os.path import isfile, join

import pandas as pd

download_dir = './download/'
files = [f for f in listdir(download_dir) if isfile(join(download_dir, f))]
print(files)

# combine all files in the list
combined_csv = pd.concat([pd.read_csv(download_dir + f) for f in files])
# export to csv
combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8')
