import pandas as pd
import os
import glob
from package import *

def join_csvs():
    """
    Opens each annual csv file, converts to dataframe, unions them together and returns a master dataset
    """
    master_df = pd.DataFrame()
    file_path = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/'
    csv_files = glob.glob(os.path.join(file_path, "*.csv"))

    for f in csv_files:
        df = pd.read_csv(f)
        print('Location:', f)
        print('File Name:', f.split("\\")[-1])
        master_df = pd.concat([master_df, df])

    return master_df


if __name__ == '__main__':
    combinedDF = join_csvs()
    save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/Combined'
    save_file = 'Final_Combined_Dataset'
    try:
        datascraper.save_df(combinedDF, save_dir, save_file)
        print('{} saved successfully.'.format(save_file))
        print('File successfully saved at {}.'.format(save_dir))
    except:
        print('I don\'t think the file saved, you should double check.')


