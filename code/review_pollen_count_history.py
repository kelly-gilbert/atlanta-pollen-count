"""
"""


import os
import pandas as pd


data_dir = 'C:\\Projects\\atlanta-pollen-count\\data\\'

os.chdir(data_dir)


# read all of the files into a data frame
counter = 1
for file in os.listdir():
    if file.find('contributor') == -1:
        temp_df = pd.read_csv(data_dir + file, index_col=False)
        if counter == 1:
            all_df = temp_df
        else:
          all_df = pd.concat([all_df, temp_df])
        counter += 1
        print('all_df record count = ' + str(all_df['date'].count()))


# check severity levels
print(all_df['severity_level'].value_counts())

print(all_df.groupby('severity_level').min())
print(all_df.groupby('severity_level').max())
