from os import listdir
from os.path import isfile, join

import pandas as pd


class CSVCombiner:

    def __init__(self, source_dir, output_file):
        self.csv_dir = source_dir
        self.output_file = output_file
        self.input_encoding = 'utf-8'
        self.output_encoding = 'utf-8'

    def set_input_encoding(self, encoding):
        self.input_encoding = encoding

    def set_output_encoding(self, encoding):
        self.output_encoding = encoding

    def run(self):
        files = [f for f in listdir(self.csv_dir) if isfile(join(self.csv_dir, f))]
        print(files)

        # combine all files in the list
        combined_csv = pd.concat([pd.read_csv(self.csv_dir + f, encoding=self.input_encoding) for f in files])
        # export to csv
        combined_csv.to_csv(self.output_file, index=False, encoding=self.output_encoding)
