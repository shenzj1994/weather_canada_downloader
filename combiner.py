from os import listdir
from os.path import isfile, join
import pandas as pd


class CSVCombiner:
    csv_dir = None
    output_file = None

    def __init__(self, source_dir, output_file):
        self.csv_dir = source_dir
        self.output_file = output_file

    def run(self):
        files = [f for f in listdir(self.csv_dir) if isfile(join(self.csv_dir, f))]
        print(files)

        # combine all files in the list
        combined_csv = pd.concat([pd.read_csv(self.csv_dir + f) for f in files])
        # export to csv
        combined_csv.to_csv(self.output_file, index=False, encoding='utf-8')
