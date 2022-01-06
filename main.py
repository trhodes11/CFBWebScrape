from package import *
import sys
import os
import datetime
import time
import pandas as pd
import numpy as np
import random


def rearrange_columns(df):
    df.insert(0, 'Team_temp', df['Team'])
    df.insert(1, 'Season_temp', df['Season'])
    df.insert(2, 'Week_temp', df['Week'])
    df.drop('Team', axis=1, inplace=True)
    df.drop('Season', axis=1, inplace=True)
    df.drop('Week', axis=1, inplace=True)
    df.rename(columns={'Team_temp': 'Team'}, inplace=True)
    df.rename(columns={'Season_temp': 'Season'}, inplace=True)
    df.rename(columns={'Week_temp': 'Week'}, inplace=True)

    return df


def main_hist(this_url, this_season, this_week, this_date, table_name):
    print('Starting scraper... \n')

    # create a soup table
    data_table = datascraper.get_first_table(this_url)  # this will select the table on the site
    print('URL pull complete. \n')

    # select these names of the columns based on the header table
    column_names = datascraper.list_of_headers(data_table)

    # create data frame from the website table
    final_data_frame = datascraper.create_df(soup_table=data_table, list_of_column_names=column_names)
    final_data_frame['Season'] = this_season
    final_data_frame['Week'] = this_week
    print('Completed Data table for Season ' + this_season + ', Week ' + this_week + ' Table ' + table_name)
    print('Data Table complete. \n')

    # do you want to save the data frame to excel workbook?
    # save_decision = input('Do you want to save table to an excel worksheet? (Y/N)').upper()
    save_decision = 'N'
    if save_decision == 'Y':
        # path_decision = input('Do you want to use your current directory {}? (Y/N) '.format(os.getcwd())).upper()
        path_decision = 'Y'
        if path_decision == 'Y':
            # save_dir = os.getcwd()
            save_dir = 'C:/Users/chris/PycharmProjects/CFBWebScrape/scraped_data' + this_week
        else:
            save_dir = input('Please enter the full path of the save location: ')
        # save_file = input('Please enter a file name (with no extension): ')
        save_file = this_season + '_WK_' + this_week + '_DT_' + this_date + '_' + table_name

        try:
            datascraper.save_df(final_data_frame, save_dir, save_file)
            print('{} saved successfully.'.format(save_file))
            print('File successfully saved at {}.'.format(save_dir))
        except:
            print('I don\'t think the file saved, you should double check.')
    return final_data_frame


"""
# This is the initial entry point into the code
"""
if __name__ == '__main__':

    """
    # Ignore - Configuration settings
    """
    desired_width = 320
    pd.set_option('display.width', desired_width)
    pd.set_option('display.max_columns', 60)

    save_dir = 'C:/Users/chris/PycharmProjects/CFBWebScrape/scraped_data'
    # save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/'

    run_type = 'historic season'  # This is used to tell the below code which mode we want to run in
    # run_type = 'current week'

    """
    # This try statement is a check to see whether the program was started using the command line/terminal or using an 
    # editor (like pycharm). We will always be starting it from pycharm, so the try statement will always fail and 
    # skip down into the except code
    """
    # try:
    # main_hist(sys.argv[1])
    # except IndexError as e:
    """
    # This is where we start; We will have two modes
    # 1. 'historic season' mode will scrape each of 9 tables for each week from week 5-14 (a total of 9 tables x 10
    # weeks/season = 90 tables per season). This will be done for each season from 2010-2019 (90 tables x 10 seasons
    # = 900 tables). All 9 tables/week will be unioned together to consolidate the 900 tables into a single
    # dataframe. This dataframe will be saved to a csv file and used to build a prediction Model.
    # 2. 'current week' mode will scrape each of 9 tables for the most recent Monday in order to build a prediction 
    # request dataset.
    """

    # url = input('Please enter url: ')

    """
    # Depending on which mode run_type is set to, we will go into either the if or the elif statement
    """
    if run_type == 'historic season':
        """
        # Create an empty dataframe that will be populated with scraped data each time a new week's worth of 
        # data is scraped
        """
        master_df = pd.DataFrame()

        """
        # We want to set 'season' to each value in the list 2010-2018; for each value of season we go into the 
        # for loop and scrape each TR table for each week of the season from week 5-14
        """
        # for season in ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']:
        for season in ['2015']:
            season_df = pd.DataFrame()
            """
            # Each season has it's own season start date, so depending on which season we are at in the for loop
            # we will set the season start date to the appropriate date. For example, if the season is 2010 OR 2021,
            # we will set the season_start_date to Sep 27, YYYY, formatted as YYYY-9-27. 
            # NOTE: The date that is being assigned is the last Monday before the start of Week 5 for that season.
            # Example: In 2010, Week 5 games were played 9/30/2010-10/2/2010. Therefore, the data that will be used
            # in the model for games that occurred in Week 5 will be 9/27/2010 which is the Monday after Week 4.
            """
            if (season == '2010') | (season == '2021'):
                season_start_date = datetime.date(int(season), 9, 6)
            elif (season == '2011') | (season == '2016'):
                season_start_date = datetime.date(int(season), 9, 5)
            elif (season == '2012') | (season == '2018'):
                season_start_date = datetime.date(int(season), 9, 3)
            elif (season == '2013') | (season == '2019'):
                season_start_date = datetime.date(int(season), 9, 2)
            elif season == '2014':
                season_start_date = datetime.date(int(season), 9, 1)
            elif season == '2015':
                season_start_date = datetime.date(int(season), 9, 7)
            elif season == '2017':
                season_start_date = datetime.date(int(season), 9, 4)
            elif season == '2018':
                season_start_date = datetime.date(int(season), 9, 3)
            elif season == '2019':
                season_start_date = datetime.date(int(season), 9, 3)
            elif season == '2020':
                season_start_date = datetime.date(int(season), 9, 7)

            """
            # Now that we know the season_start_date for the current season, we can set the start date for the 
            # variable that will keep track of the current week (this_week_date) that we are scraping data for. 
            # Since we will start at week 5, we can set this_week_date to the season_start_date which is the 
            # Monday before week 5 games.
            """
            this_week_date = season_start_date

            # The next line will convert the date variable to a string so that we can use it in filenames
            season_start_date_str = season_start_date.strftime("%Y-%m-%d")

            # Next line just stores the string version of the date into a this_week_date_str variable that we will
            # update each time we go through the for loop that goes through each week
            this_week_date_str = season_start_date_str

            """
            # The variable 'week' will update each time through the loop, starting at 5 and incrementing until
            # it reaches the value of 15 which will cause it to exit the loop (week = 14 will be the last time it
            # enters the loop
            """
            for week in range(2, 16):
                """
                # For each week of the current season, we will build a url string for each of the tables we want to
                # scrape using the current value of 'this_week_date_str'. The 'this_week_date_str' variable will 
                # start at 2010-9-26  and will be updated at the bottom of this loop to add 7 days so that each time
                # we go through the loop it will scrape the next weeks table.
                """
                scoring_offense_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-game' \
                                              + '?date=' \
                                              + this_week_date_str
                so_df = main_hist(scoring_offense_url_current, season, str(week), this_week_date_str, 'scoring_offense')
                so_df.rename(columns={'Rank': 'Rank_Scoring_Offense',
                                      season: 'Current_Season_Scoring_Offense',
                                      str(int(season) - 1): 'Previous_Season_Scoring_Offense',
                                      'Last 3': 'Last_3_Scoring_Offense',
                                      'Last 1': 'Last_1_Scoring_Offense',
                                      'Home': 'At_Home_Scoring_Offense',
                                      'Away': 'Away_Scoring_Offense'
                                      }, inplace=True)
                so_df['Team'] = so_df['Team'].str.strip()
                if season == '2010':
                    so_df['Rank_Scoring_Offense'] = so_df.index + 1
                so_df = so_df.replace('--', np.nan)
                so_df = so_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_average_scoring_margin_url_current = 'https://www.teamrankings.com/college-football/stat/average-scoring-margin' \
                                                          + '?date=' \
                                                          + this_week_date_str
                tasm_df = main_hist(team_average_scoring_margin_url_current, season, str(week), this_week_date_str,
                                    'team_average_scoring_margin')
                tasm_df.rename(columns={'Rank': 'Rank_Team_Average_Scoring_Margin',
                                        season: 'Current_Season_Team_Scoring_Average_Margin',
                                        str(int(season) - 1): 'Previous_Season_Team_Average_Scoring_Margin',
                                        'Last 3': 'Last_3_Team_Average_Scoring_Margin',
                                        'Last 1': 'Last_1_Team_Average_Scoring_Margin',
                                        'Home': 'At_Home_Team_Average_Scoring_Margin',
                                        'Away': 'Away_Team_Average_Scoring_Margin'
                                        }, inplace=True)
                tasm_df['Team'] = tasm_df['Team'].str.strip()
                if season == '2010':
                    tasm_df['Rank_Team_Average_Scoring_Margin'] = tasm_df.index + 1
                tasm_df = tasm_df.replace('--', np.nan)
                tasm_df = tasm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point' \
                                                   + '?date=' \
                                                   + this_week_date_str
                typp_df = main_hist(team_yards_per_point_url_current, season, str(week), this_week_date_str,
                                    'team_yards_per_point')
                typp_df.rename(columns={'Rank': 'Rank_Team_Yards_per_Point',
                                        season: 'Current_Season_Team_Yards_per_Point',
                                        str(int(season) - 1): 'Previous_Season_Team_yards_per_Point',
                                        'Last 3': 'Last_3_Team_Yards_per_Point',
                                        'Last 1': 'Last_1_Team_Yards_per_Point',
                                        'Home': 'At_Home_Team_Yards_per_Point',
                                        'Away': 'Away_Team_Yards_per_Point'
                                        }, inplace=True)
                typp_df['Team'] = typp_df['Team'].str.strip()
                if season == '2010':
                    typp_df['Rank_Team_Yards_per_Point'] = typp_df.index + 1
                typp_df = typp_df.replace('--', np.nan)
                typp_df = typp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_yards_per_point_margin_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point-margin' \
                                                          + '?date=' \
                                                          + this_week_date_str
                typm_df = main_hist(team_yards_per_point_margin_url_current, season, str(week), this_week_date_str,
                                    'team_yards_per_point_margin')
                typm_df.rename(columns={'Rank': 'Rank_Team_Yards_per_Point_Margin',
                                        season: 'Current_Season_Team_Yards_per_Point_Margin',
                                        str(int(season) - 1): 'Previous_Season_Team_yards_per_Point_Margin',
                                        'Last 3': 'Last_3_Team_Yards_per_Point_Margin',
                                        'Last 1': 'Last_1_Team_Yards_per_Point_Margin',
                                        'Home': 'At_Home_Team_Yards_per_Point_Margin',
                                        'Away': 'Away_Team_Yards_per_Point_Margin'
                                        }, inplace=True)
                typm_df['Team'] = typm_df['Team'].str.strip()
                if season == '2010':
                    typm_df['Rank_Team_Yards_per_Point_Margin'] = typm_df.index + 1
                typm_df = typm_df.replace('--', np.nan)
                typm_df = typm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_points_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play' \
                                                   + '?date=' \
                                                   + this_week_date_str
                typ_df = main_hist(team_points_per_play_url_current, season, str(week), this_week_date_str,
                                   'team_points_per_play')
                typ_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play',
                                       season: 'Current_Season_Team_Points_per_Play',
                                       str(int(season) - 1): 'Previous_Season_Team_Points_per_Play',
                                       'Last 3': 'Last_3_Team_Points_per_Play',
                                       'Last 1': 'Last_1_Team_Points_per_Play',
                                       'Home': 'At_Home_Team_Points_per_Play',
                                       'Away': 'Away_Team_Points_per_Play'
                                       }, inplace=True)
                typ_df['Team'] = typ_df['Team'].str.strip()
                if season == '2010':
                    typ_df['Rank_Team_Points_per_Play'] = typ_df.index + 1
                typ_df = typ_df.replace('--', np.nan)
                typ_df = typ_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_points_per_play_margin_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play-margin' \
                                                          + '?date=' \
                                                          + this_week_date_str
                typpm_df = main_hist(team_points_per_play_margin_url_current, season, str(week), this_week_date_str,
                                     'team_points_per_play_margin')
                typpm_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play_Margin',
                                         season: 'Current_Season_Team_Points_per_Play_Margin',
                                         str(int(season) - 1): 'Previous_Season_Team_Points_per_Play_Margin',
                                         'Last 3': 'Last_3_Team_Points_per_Play_Margin',
                                         'Last 1': 'Last_1_Team_Points_per_Play_Margin',
                                         'Home': 'At_Home_Team_Points_per_Play_Margin',
                                         'Away': 'Away_Team_Points_per_Play_Margin'
                                         }, inplace=True)
                typpm_df['Team'] = typpm_df['Team'].str.strip()
                if season == '2010':
                    typpm_df['Rank_Team_Points_per_Play_Margin'] = typpm_df.index + 1
                typpm_df = typpm_df.replace('--', np.nan)
                typpm_df = typpm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_red_zone_scoring_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-attempts-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
                trs_df = main_hist(team_red_zone_scoring_attempts_per_game_url_current, season, str(week),
                                   this_week_date_str,
                                   'team_red_zone_scoring_attempts_per_game')
                trs_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Attempts_per_Game',
                                       season: 'Current_Season_Team_Red_Zone_Scoring_Attempts_per_Game',
                                       str(int(season) - 1): 'Previous_Season_Team_Red_Zone_Scoring_Attempts_per_Game',
                                       'Last 3': 'Last_3_Team_Red_Zone_Scoring_Attempts_per_Game',
                                       'Last 1': 'Last_1_Team_Red_Zone_Scoring_Attempts_per_Game',
                                       'Home': 'At_Home_Team_Red_Zone_Scoring_Attempts_per_Game',
                                       'Away': 'Away_Team_Red_Zone_Scoring_Attempts_per_Game'
                                       }, inplace=True)
                trs_df['Team'] = trs_df['Team'].str.strip()
                if season == '2010':
                    trs_df['Rank_Team_Red_Zone_Scoring_Attempts_per_Game'] = trs_df.index + 1
                trs_df = trs_df.replace('--', np.nan)
                trs_df = trs_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_red_zone_scores_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scores-per-game' \
                                                            + '?date=' \
                                                            + this_week_date_str
                trsp_df = main_hist(team_red_zone_scores_per_game_url_current, season, str(week), this_week_date_str,
                                    'team_red_zone_scores_per_game')
                trsp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scores_per_Game',
                                        season: 'Current_Season_Team_Red_Zone_Scores_per_Game',
                                        str(int(season) - 1): 'Previous_Season_Team_Red_Zone_Scores_per_Game',
                                        'Last 3': 'Last_3_Team_Red_Zone_Scores_per_Game',
                                        'Last 1': 'Last_1_Team_Red_Zone_Scores_per_Game',
                                        'Home': 'At_Home_Team_Red_Zone_Scores_per_Game',
                                        'Away': 'Away_Team_Red_Zone_Scores_per_Game'
                                        }, inplace=True)
                trsp_df['Team'] = trsp_df['Team'].str.strip()
                if season == '2010':
                    trsp_df['Rank_Team_Red_Zone_Scores_per_Game'] = trsp_df.index + 1
                trsp_df = trsp_df.replace('--', np.nan)
                trsp_df = trsp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_red_zone_scoring_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-pct' \
                                                               + '?date=' \
                                                               + this_week_date_str
                trspp_df = main_hist(team_red_zone_scoring_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'team_red_zone_scoring_percentage')
                trspp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Percentage',
                                         season: 'Current_Season_Team_Red_Zone_Scoring_Percentage',
                                         str(int(season) - 1): 'Previous_Season_Team_Red_Zone_Scoring_Percentage',
                                         'Last 3': 'Last_3_Team_Red_Zone_Scoring_Percentage',
                                         'Last 1': 'Last_1_Team_Red_Zone_Scoring_Percentage',
                                         'Home': 'At_Home_Team_Red_Zone_Scoring_Percentage',
                                         'Away': 'Away_Team_Red_Zone_Scoring_Percentage'
                                         }, inplace=True)
                trspp_df['Team'] = trspp_df['Team'].str.strip()
                if season == '2010':
                    trspp_df['Rank_Team_Red_Zone_Scoring_Percentage'] = trspp_df.index + 1
                trspp_df = trspp_df.replace('--', np.nan)
                for c in trspp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        trspp_df[c] = trspp_df[c].str.rstrip('%').astype('float') / 100.0
                trspp_df = trspp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_offensive_touchdowns_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-touchdowns-per-game' \
                                                                 + '?date=' \
                                                                 + this_week_date_str
                tot_df = main_hist(team_offensive_touchdowns_per_game_url_current, season, str(week),
                                   this_week_date_str,
                                   'team_offensive_touchdowns_per_game')
                tot_df.rename(columns={'Rank': 'Rank_Team_Offensive_Touchdowns_per_Game',
                                       season: 'Current_Season_Team_Offensive_Touchdowns_per_Game',
                                       str(int(season) - 1): 'Previous_Season_Team_Offensive_Touchdowns_per_Game',
                                       'Last 3': 'Last_3_Team_Offensive_Touchdowns_per_Game',
                                       'Last 1': 'Last_1_Team_Offensive_Touchdowns_per_Game',
                                       'Home': 'At_Home_Team_Offensive_Touchdowns_per_Game',
                                       'Away': 'Away_Team_Offensive_Touchdowns_per_Game'
                                       }, inplace=True)
                tot_df['Team'] = tot_df['Team'].str.strip()
                if season == '2010':
                    tot_df['Rank_Team_Offensive_Touchdowns_per_Game'] = tot_df.index + 1
                tot_df = tot_df.replace('--', np.nan)
                tot_df = tot_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_offensive_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-points-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
                top_df = main_hist(team_offensive_points_per_game_url_current, season, str(week), this_week_date_str,
                                   'team_offensive_points_per_game')
                top_df.rename(columns={'Rank': 'Rank_Team_Offensive_Points_per_Game',
                                       season: 'Current_Season_Team_Offensive_Points_per_Game',
                                       str(int(season) - 1): 'Previous_Season_Team_Offensive_Points_per_Game',
                                       'Last 3': 'Last_3_Team_Offensive_Points_per_Game',
                                       'Last 1': 'Last_1_Team_Offensive_Points_per_Game',
                                       'Home': 'At_Home_Team_Offensive_Points_per_Game',
                                       'Away': 'Away_Team_Offensive_Points_per_Game'
                                       }, inplace=True)
                top_df['Team'] = top_df['Team'].str.strip()
                if season == '2010':
                    top_df['Rank_Team_Offensive_Points_per_Game'] = top_df.index + 1
                top_df = top_df.replace('--', np.nan)
                top_df = top_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_offensive_point_share_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-point-share-pct' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                tops_df = main_hist(team_offensive_point_share_percentage_url_current, season, str(week),
                                    this_week_date_str,
                                    'team_offensive_point_share_percentage')
                tops_df.rename(columns={'Rank': 'Rank_Team_Offensive_Point_Share_Percentage',
                                        season: 'Current_Season_Team_Offensive_Point_Share_Percentage',
                                        str(int(season) - 1): 'Previous_Season_Team_Offensive_Point_Share_Percentage',
                                        'Last 3': 'Last_3_Team_Offensive_Point_Share_Percentage',
                                        'Last 1': 'Last_1_Team_Offensive_Point_Share_Percentage',
                                        'Home': 'At_Home_Team_Offensive_Point_Share_Percentage',
                                        'Away': 'Away_Team_Offensive_Point_Share_Percentage'
                                        }, inplace=True)
                tops_df['Team'] = tops_df['Team'].str.strip()
                if season == '2010':
                    tops_df['Rank_Team_Offensive_Point_Share_Percentage'] = tops_df.index + 1
                tops_df = tops_df.replace('--', np.nan)
                for c in tops_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        tops_df[c] = tops_df[c].str.rstrip('%').astype('float') / 100.0
                tops_df = tops_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_first_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-points-per-game' \
                                                                 + '?date=' \
                                                                 + this_week_date_str
                fiq_df = main_hist(team_first_quarter_points_per_game_url_current, season, str(week),
                                   this_week_date_str,
                                   'team_first_quarter_points_per_game')
                fiq_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Points_per_Game',
                                       season: 'Current_Season_Team_First_Quarter_Points_per_Game',
                                       str(int(season) - 1): 'Previous_Season_Team_First_Quarter_Points_per_Game',
                                       'Last 3': 'Last_3_Team_First_Quarter_Points_per_Game',
                                       'Last 1': 'Last_1_Team_First_Quarter_Points_per_Game',
                                       'Home': 'At_Home_Team_First_Quarter_Points_per_Game',
                                       'Away': 'Away_Team_First_Quarter_Points_per_Game'
                                       }, inplace=True)
                fiq_df['Team'] = fiq_df['Team'].str.strip()
                if season == '2010':
                    fiq_df['Rank_Team_First_Quarter_Points_per_Game'] = fiq_df.index + 1
                fiq_df = fiq_df.replace('--', np.nan)
                fiq_df = fiq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_second_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-points-per-game' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
                sq_df = main_hist(team_second_quarter_points_per_game_url_current, season, str(week),
                                  this_week_date_str,
                                  'team_second_quarter_points_per_game')
                sq_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Points_per_Game',
                                      season: 'Current_Season_Team_Second_Quarter_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Second_Quarter_Points_per_Game',
                                      'Last 3': 'Last_3_Team_Second_Quarter_Points_per_Game',
                                      'Last 1': 'Last_1_Team_Second_Quarter_Points_per_Game',
                                      'Home': 'At_Home_Team_Second_Quarter_Points_per_Game',
                                      'Away': 'Away_Team_Second_Quarter_Points_per_Game'
                                      }, inplace=True)
                sq_df['Team'] = sq_df['Team'].str.strip()
                if season == '2010':
                    sq_df['Rank_Team_Second_Quarter_Points_per_Game'] = sq_df.index + 1
                sq_df = sq_df.replace('--', np.nan)
                sq_df = sq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_third_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/3rd-quarter-points-per-game' \
                                                                 + '?date=' \
                                                                 + this_week_date_str
                tq_df = main_hist(team_third_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_third_quarter_points_per_game')
                tq_df.rename(columns={'Rank': 'Rank_Team_Third_Quarter_Points_per_Game',
                                      season: 'Current_Season_Team_Third_Quarter_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Third_Quarter_Points_per_Game',
                                      'Last 3': 'Last_3_Team_Third_Quarter_Points_per_Game',
                                      'Last 1': 'Last_1_Team_Third_Quarter_Points_per_Game',
                                      'Home': 'At_Home_Team_Third_Quarter_Points_per_Game',
                                      'Away': 'Away_Team_Third_Quarter_Points_per_Game'
                                      }, inplace=True)
                tq_df['Team'] = tq_df['Team'].str.strip()
                if season == '2010':
                    tq_df['Rank_Team_Third_Quarter_Points_per_Game'] = tq_df.index + 1
                tq_df = tq_df.replace('--', np.nan)
                tq_df = tq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_fourth_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-points-per-game' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
                fq_df = main_hist(team_fourth_quarter_points_per_game_url_current, season, str(week),
                                  this_week_date_str,
                                  'team_fourth_quarter_points_per_game')
                fq_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Points_per_Game',
                                      season: 'Current_Season_Team_Fourth_Quarter_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Fourth_Quarter_Points_per_Game',
                                      'Last 3': 'Last_3_Team_Fourth_Quarter_Points_per_Game',
                                      'Last 1': 'Last_1_Team_Fourth_Quarter_Points_per_Game',
                                      'Home': 'At_Home_Team_Fourth_Quarter_Points_per_Game',
                                      'Away': 'Away_Team_Fourth_Quarter_Points_per_Game'
                                      }, inplace=True)
                fq_df['Team'] = fq_df['Team'].str.strip()
                if season == '2010':
                    fq_df['Rank_Team_Fourth_Quarter_Points_per_Game'] = fq_df.index + 1
                fq_df = fq_df.replace('--', np.nan)
                fq_df = fq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_overtime_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/overtime-points-per-game' \
                                                            + '?date=' \
                                                            + this_week_date_str
                ot_df = main_hist(team_overtime_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_overtime_points_per_game')
                ot_df.rename(columns={'Rank': 'Rank_Team_Overtime_Points_per_Game',
                                      season: 'Current_Season_Team_Overtime_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Overtime_Points_per_Game',
                                      'Last 3': 'Last_3_Team_Overtime_Points_per_Game',
                                      'Last 1': 'Last_1_Team_Overtime_Points_per_Game',
                                      'Home': 'At_Home_Team_Overtime_Points_per_Game',
                                      'Away': 'Away_Team_Overtime_Points_per_Game'
                                      }, inplace=True)
                ot_df['Team'] = ot_df['Team'].str.strip()
                if season == '2010':
                    ot_df['Rank_Team_Overtime_Points_per_Game'] = ot_df.index + 1
                ot_df = ot_df.replace('--', np.nan)
                ot_df = ot_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_first_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-points-per-game' \
                                                              + '?date=' \
                                                              + this_week_date_str
                fh_df = main_hist(team_first_half_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_first_half_points_per_game')
                fh_df.rename(columns={'Rank': 'Rank_Team_First_Half_Points_per_Game',
                                      season: 'Current_Season_Team_First-Half_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_First-Half_Points_per_Game',
                                      'Last 3': 'Last_3_Team_First-Half_Points_per_Game',
                                      'Last 1': 'Last_1_Team_First-Half_Points_per_Game',
                                      'Home': 'At_Home_Team_First_Half_Points_per_Game',
                                      'Away': 'Away_Team_First-Half_Points_per_Game'
                                      }, inplace=True)
                fh_df['Team'] = fh_df['Team'].str.strip()
                if season == '2010':
                    fh_df['Rank_Team_First_Half_Points_per_Game'] = fh_df.index + 1
                fh_df = fh_df.replace('--', np.nan)
                fh_df = fh_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_second_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-points-per-game' \
                                                               + '?date=' \
                                                               + this_week_date_str
                sh_df = main_hist(team_second_half_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_second_half_points_per_game')
                sh_df.rename(columns={'Rank': 'Rank_Team_Second_Half_Points_per_Game',
                                      season: 'Current_Season_Team_Second-Half_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Second-Half_Points_per_Game',
                                      'Last 3': 'Last_3_Team_Second-Half_Points_per_Game',
                                      'Last 1': 'Last_1_Team_Second-Half_Points_per_Game',
                                      'Home': 'At_Home_Team_Second_Half_Points_per_Game',
                                      'Away': 'Away_Team_Second-Half_Points_per_Game'
                                      }, inplace=True)
                sh_df['Team'] = sh_df['Team'].str.strip()
                if season == '2010':
                    sh_df['Rank_Team_Second_Half_Points_per_Game'] = sh_df.index + 1
                sh_df = sh_df.replace('--', np.nan)
                sh_df = sh_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_first_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-time-of-possession-share-pct' \
                                                                                  + '?date=' \
                                                                                  + this_week_date_str
                fiqtp_df = main_hist(team_first_quarter_time_of_possession_share_percent_url_current, season, str(week),
                                     this_week_date_str,
                                     'team_first_quarter_time_of_possession_share_percent')
                fiqtp_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                         season: 'Current_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                         str(int(
                                             season) - 1): 'Previous_Season_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                         'Last 3': 'Last_3_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                         'Last 1': 'Last_1_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                         'Home': 'At_Home_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                         'Away': 'Away_Team_First_Quarter_Time_of_Possession_Share_Percent'
                                         }, inplace=True)
                fiqtp_df['Team'] = fiqtp_df['Team'].str.strip()
                if season == '2010':
                    fiqtp_df['Rank_Team_First_Quarter_Time_of_Possession_Share_Percent'] = fiqtp_df.index + 1
                fiqtp_df = fiqtp_df.replace('--', np.nan)
                fiqtp_df = fiqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_second_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-time-of-possession-share-pct' \
                                                                                   + '?date=' \
                                                                                   + this_week_date_str
                sqtp_df = main_hist(team_second_quarter_time_of_possession_share_percent_url_current, season, str(week),
                                    this_week_date_str,
                                    'team_second_quarter_time_of_possession_share_percent')
                sqtp_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last_3_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last_1_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Second_Quarter_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                sqtp_df['Team'] = sqtp_df['Team'].str.strip()
                if season == '2010':
                    sqtp_df['Rank_Team_Second_Quarter_Time_of_Possession_Share_Percent'] = sqtp_df.index + 1
                sqtp_df = sqtp_df.replace('--', np.nan)
                sqtp_df = sqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_third_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/3rd-quarter-time-of-possession-share-pct' \
                                                                                  + '?date=' \
                                                                                  + this_week_date_str
                tqtp_df = main_hist(team_third_quarter_time_of_possession_share_percent_url_current, season, str(week),
                                    this_week_date_str,
                                    'team_third_quarter_time_of_possession_share_percent')
                tqtp_df.rename(columns={'Rank': 'Rank_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last_3_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last_1_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Third_Quarter_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                tqtp_df['Team'] = tqtp_df['Team'].str.strip()
                if season == '2010':
                    tqtp_df['Rank_Team_Third_Quarter_Time_of_Possession_Share_Percent'] = tqtp_df.index + 1
                tqtp_df = tqtp_df.replace('--', np.nan)
                tqtp_df = tqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_fourth_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-time-of-possession-share-pct' \
                                                                                   + '?date=' \
                                                                                   + this_week_date_str
                fqtp_df = main_hist(team_fourth_quarter_time_of_possession_share_percent_url_current, season, str(week),
                                    this_week_date_str,
                                    'team_fourth_quarter_time_of_possession_share_percent')
                fqtp_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last_3_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last_1_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Fourth_Quarter_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                fqtp_df['Team'] = fqtp_df['Team'].str.strip()
                if season == '2010':
                    fqtp_df['Rank_Team_Fourth_Quarter_Time_of_Possession_Share_Percent'] = fqtp_df.index + 1
                fqtp_df = fqtp_df.replace('--', np.nan)
                fqtp_df = fqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_first_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-time-of-possession-share-pct' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
                fhtp_df = main_hist(team_first_half_time_of_possession_share_percent_url_current, season, str(week),
                                    this_week_date_str,
                                    'team_first_half_time_of_possession_share_percent')
                fhtp_df.rename(columns={'Rank': 'Rank_Team_First_Half_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_First_Half_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last_3_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last_1_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_First_Half_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                fhtp_df['Team'] = fhtp_df['Team'].str.strip()
                if season == '2010':
                    fhtp_df['Rank_Team_First_Half_Time_of_Possession_Share_Percent'] = fhtp_df.index + 1
                fhtp_df = fhtp_df.replace('--', np.nan)
                fhtp_df = fhtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                team_second_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-time-of-possession-share-pct' \
                                                                                + '?date=' \
                                                                                + this_week_date_str
                shtp_df = main_hist(team_second_half_time_of_possession_share_percent_url_current, season, str(week),
                                    this_week_date_str,
                                    'team_second_half_time_of_possession_share_percent')
                shtp_df.rename(columns={'Rank': 'Rank_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last_3_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last_1_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Second_Half_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                shtp_df['Team'] = shtp_df['Team'].str.strip()
                if season == '2010':
                    shtp_df['Rank_Team_Second_Half_Time_of_Possession_Share_Percent'] = shtp_df.index + 1
                shtp_df = shtp_df.replace('--', np.nan)
                shtp_df = shtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-game' \
                                                           + '?date=' \
                                                           + this_week_date_str
                toypg_df = main_hist(total_offense_yards_per_game_url_current, season, str(week), this_week_date_str,
                                     'total_offense_yards_per_game')
                toypg_df.rename(columns={'Rank': 'Rank_Total_Offense_yards_per_game',
                                         season: 'Current_Season_Total_Offense_Yards_per_Game',
                                         str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Game',
                                         'Last 3': 'Last_3_Total_Offense_Yards_per_Game',
                                         'Last 1': 'Last_1_Total_Offense_Yards_per_Game',
                                         'Home': 'At_Home_Total_Offense_Yards_per_Game',
                                         'Away': 'Away_Total_Offense_Yards_per_Game'
                                         }, inplace=True)
                toypg_df['Team'] = toypg_df['Team'].str.strip()
                if season == '2010':
                    toypg_df['Rank_Total_Offense_yards_per_game'] = toypg_df.index + 1
                toypg_df = toypg_df.replace('--', np.nan)
                toypg_df = toypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_plays_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/plays-per-game' \
                                                           + '?date=' \
                                                           + this_week_date_str
                toppg_df = main_hist(total_offense_plays_per_game_url_current, season, str(week), this_week_date_str,
                                     'total_offense_plays_per_game')
                toppg_df.rename(columns={'Rank': 'Rank_Total_Offense_Plays_per_Game',
                                         season: 'Current_Season_Total_Offense_Plays_per_Game',
                                         str(int(season) - 1): 'Previous_Season_Total_Offense_Plays_per_Game',
                                         'Last 3': 'Last_3_Total_Offense_Plays_per_Game',
                                         'Last 1': 'Last_1_Total_Offense_Plays_per_Game',
                                         'Home': 'At_Home_Total_Offense_Plays_per_Game',
                                         'Away': 'Away_Total_Offense_Plays_per_Game'
                                         }, inplace=True)
                toppg_df['Team'] = toppg_df['Team'].str.strip()
                if season == '2010':
                    toppg_df['Rank_Total_Offense_Plays_per_Game'] = toppg_df.index + 1
                toppg_df = toppg_df.replace('--', np.nan)
                toppg_df = toppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_yards_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-play' \
                                                           + '?date=' \
                                                           + this_week_date_str
                toypp_df = main_hist(total_offense_yards_per_play_url_current, season, str(week), this_week_date_str,
                                     'total_offense_yards_per_play')
                toypp_df.rename(columns={'Rank': 'Rank_Total_Offense_Yards_per_Play',
                                         season: 'Current_Season_Total_Offense_Yards_per_Play',
                                         str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Play',
                                         'Last 3': 'Last_3_Total_Offense_Yards_per_Play',
                                         'Last 1': 'Last_1_Total_Offense_Yards_per_Play',
                                         'Home': 'At_Home_Total_Offense_Yards_per_Play',
                                         'Away': 'Away_Total_Offense_Yards_per_Play'
                                         }, inplace=True)
                toypp_df['Team'] = toypp_df['Team'].str.strip()
                if season == '2010':
                    toypp_df['Rank_Total_Offense_Yards_per_Play'] = toypp_df.index + 1
                toypp_df = toypp_df.replace('--', np.nan)
                toypp_df = toypp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_third_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-downs-per-game' \
                                                                + '?date=' \
                                                                + this_week_date_str
                totdpg_df = main_hist(total_offense_third_down_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'total_offense_third_down_per_game')
                totdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_per_Game',
                                          season: 'Current_Season_Total_Offense_Third_Down_per_Game',
                                          str(int(season) - 1): 'Previous_Season_Total_Offense_Third_Down_per_Game',
                                          'Last 3': 'Last_3_Total_Offense_Third_Down_per_Game',
                                          'Last 1': 'Last_1_Total_Offense_Third_Down_per_Game',
                                          'Home': 'At_Home_Total_Offense_Third_Down_per_Game',
                                          'Away': 'Away_Total_Offense_Third_Down_per_Game'
                                          }, inplace=True)
                totdpg_df['Team'] = totdpg_df['Team'].str.strip()
                if season == '2010':
                    totdpg_df['Rank_Total_Offense_Third_Down_per_Game'] = totdpg_df.index + 1
                totdpg_df = totdpg_df.replace('--', np.nan)
                totdpg_df = totdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_third_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversions-per-game' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
                totdcpg_df = main_hist(total_offense_third_down_conversions_per_game_url_current, season, str(week),
                                       this_week_date_str,
                                       'total_offense_third_down_conversions_per_game')
                totdcpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_Conversions_per_Game',
                                           season: 'Current_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                           'Last 3': 'Last_3_Total_Offense_Third_Down_Conversions_per_Game',
                                           'Last 1': 'Last_1_Total_Offense_Third_Down_Conversions_per_Game',
                                           'Home': 'At_Home_Total_Offense_Third_Down_Conversions_per_Game',
                                           'Away': 'Away_Total_Offense_Third-Down-Conversions_per_Game'
                                           }, inplace=True)
                totdcpg_df['Team'] = totdcpg_df['Team'].str.strip()
                if season == '2010':
                    totdcpg_df['Rank_Total_Offense_Third_Down_Conversions_per_Game'] = totdcpg_df.index + 1
                totdcpg_df = totdcpg_df.replace('--', np.nan)
                totdcpg_df = totdcpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_fourth_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-downs-per-game' \
                                                                 + '?date=' \
                                                                 + this_week_date_str
                tofdpg_df = main_hist(total_offense_fourth_down_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'total_offense_fourth_down_per_game')
                tofdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_per_Game',
                                          season: 'Current_Season_Total_Offense_Fourth_Down_per_Game',
                                          str(int(season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_per_Game',
                                          'Last 3': 'Last_3_Total_Offense_Fourth_Down_per_Game',
                                          'Last 1': 'Last_1_Total_Offense_Fourth_Down_per_Game',
                                          'Home': 'At_Home_Total_Offense_Fourth_Down_per_Game',
                                          'Away': 'Away_Total_Offense_Fourth-Down_per_Game'
                                          }, inplace=True)
                tofdpg_df['Team'] = tofdpg_df['Team'].str.strip()
                if season == '2010':
                    tofdpg_df['Rank_Total_Offense_Fourth_Down_per_Game'] = tofdpg_df.index + 1
                tofdpg_df = tofdpg_df.replace('--', np.nan)
                tofdpg_df = tofdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_fourth_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-down-conversions-per-game' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
                tofdcpg_df = main_hist(total_offense_fourth_down_conversions_per_game_url_current, season, str(week),
                                       this_week_date_str,
                                       'total_offense_fourth_down_conversions_per_game')
                tofdcpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           season: 'Current_Season_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Last 3': 'Last_3_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Last 1': 'Last_1_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Home': 'At_Home_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Away': 'Away_Total_Offense_Fourth_Down-Conversions_per_Game'
                                           }, inplace=True)
                tofdcpg_df['Team'] = tofdcpg_df['Team'].str.strip()
                if season == '2010':
                    tofdcpg_df['Rank_Total_Offense_Fourth_Down_Conversions_per_Game'] = tofdcpg_df.index + 1
                tofdcpg_df = tofdcpg_df.replace('--', np.nan)
                tofdcpg_df = tofdcpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_average_time_of_possession_url_current = 'https://www.teamrankings.com/college-football/stat/average-time-of-possession-net-of-ot' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
                toatp_df = main_hist(total_offense_average_time_of_possession_url_current, season, str(week),
                                     this_week_date_str,
                                     'total_offense_average_time_of_possession')
                toatp_df.rename(columns={'Rank': 'Rank_Total_Offense_Average_Time_of_Possession',
                                         season: 'Current_Season_Total_Offense_Average_Time_of_Possession',
                                         str(int(
                                             season) - 1): 'Previous_Season_Total_Offense_Average_Time_of_Possession',
                                         'Last 3': 'Last_3_Total_Offense_Average_Time_of_Possession',
                                         'Last 1': 'Last_1_Total_Offense_Average_Time_of_Possession',
                                         'Home': 'At_Home_Total_Offense_Average_Time_of_Possession',
                                         'Away': 'Away_Total_Offense_Average_Time_of_Possession'
                                         }, inplace=True)
                toatp_df['Team'] = toatp_df['Team'].str.strip()
                if season == '2010':
                    toatp_df['Rank_Total_Offense_Average_Time_of_Possession'] = toatp_df.index + 1
                toatp_df = toatp_df.replace('--', np.nan)
                toatp_df = toatp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_average_time_of_possession_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/time-of-possession-pct-net-of-ot' \
                                                                                  + '?date=' \
                                                                                  + this_week_date_str
                toatpp_df = main_hist(total_offense_average_time_of_possession_percentage_url_current, season,
                                      str(week), this_week_date_str,
                                      'total_offense_average_time_of_possession_percentage')
                toatpp_df.rename(columns={'Rank': 'Rank_Total_Offense_Average_Time_of_Possession_Percentage',
                                          season: 'Current_Season_Total_Offense_Average_Time_of_Possession_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Offense_Average_Time_of_Possession_Percentage',
                                          'Last 3': 'Last_3_Total_Offense_Average_Time_of_Possession_Percentage',
                                          'Last 1': 'Last_1_Total_Offense_Average_Time_of_Possession_Percentage',
                                          'Home': 'At_Home_Total_Offense_Average_Time_of_Possession_Percentage',
                                          'Away': 'Away_Total_Offense_Average_Time_of_Possession_Percentage'
                                          }, inplace=True)
                toatpp_df['Team'] = toatpp_df['Team'].str.strip()
                if season == '2010':
                    toatpp_df['Rank_Total_Offense_Average_Time_of_Possession_Percentage'] = toatpp_df.index + 1
                toatpp_df = toatpp_df.replace('--', np.nan)
                for c in toatpp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        toatpp_df[c] = toatpp_df[c].str.rstrip('%').astype('float') / 100.0
                toatpp_df = toatpp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_third_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversion-pct' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
                totdcp_df = main_hist(total_offense_third_down_conversion_percent_url_current, season,
                                      str(week), this_week_date_str,
                                      'total_offense_third_down_conversion_percent')
                totdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_Conversion_Percent',
                                          season: 'Current_Season_Total_Offense_Third_Down-Conversion_Percent',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversion_Percent',
                                          'Last 3': 'Last_3_Total_Offense_Third_Down_Conversion_Percent',
                                          'Last 1': 'Last_1_Total_Offense_Third_Down_Conversion_Percent',
                                          'Home': 'At_Home_Total_Offense_Third_Down_Conversion_Percent',
                                          'Away': 'Away_Total_Offense_Third_Down_Conversion_Percent'
                                          }, inplace=True)
                totdcp_df['Team'] = totdcp_df['Team'].str.strip()
                if season == '2010':
                    totdcp_df['Rank_Total_Offense_Third_Down_Conversion_Percent'] = totdcp_df.index + 1
                totdcp_df = totdcp_df.replace('--', np.nan)
                totdcp_df = totdcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_fourth_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-down-conversion-pct' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
                tofdcp_df = main_hist(total_offense_fourth_down_conversion_percent_url_current, season, str(week),
                                      this_week_date_str,
                                      'total_offense_fourth_down_conversion_percent')
                tofdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_Conversion_Percent',
                                          season: 'Current_Season_Total_Offense_Fourth_Down-Conversion_Percent',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Last 3': 'Last_3_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Last 1': 'Last_1_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Home': 'At_Home_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Away': 'Away_Total_Offense_Fourth_Down_Conversion_Percent'
                                          }, inplace=True)
                tofdcp_df['Team'] = tofdcp_df['Team'].str.strip()
                if season == '2010':
                    tofdcp_df['Rank_Total_Offense_Fourth_Down_Conversion_Percent'] = tofdcp_df.index + 1
                tofdcp_df = tofdcp_df.replace('--', np.nan)
                tofdcp_df = tofdcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_punts_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-play' \
                                                           + '?date=' \
                                                           + this_week_date_str
                toppp_df = main_hist(total_offense_punts_per_play_url_current, season, str(week), this_week_date_str,
                                     'total_offense_punts_per_play')
                toppp_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Play',
                                         season: 'Current_Season_Total_Offense_Punts_per_Play',
                                         str(int(
                                             season) - 1): 'Previous_Season_Total_Offense_Punts_per_Play',
                                         'Last 3': 'Last_3_Total_Offense_Punts_per_Play',
                                         'Last 1': 'Last_1_Total_Offense_Punts_per_Play',
                                         'Home': 'At_Home_Total_Offense_Punts_per_Play',
                                         'Away': 'Away_Total_Offense_Punts_per_Play'
                                         }, inplace=True)
                toppp_df['Team'] = toppp_df['Team'].str.strip()
                if season == '2010':
                    toppp_df['Rank_Total_Offense_Punts_per_Play'] = toppp_df.index + 1
                toppp_df = toppp_df.replace('--', np.nan)
                toppp_df = toppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_offense_punts_per_offensive_score_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-offensive-score' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
                toppos_df = main_hist(total_offense_punts_per_offensive_score_url_current, season, str(week),
                                      this_week_date_str,
                                      'total_offense_punts_per_offensive score')
                toppos_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Offensive_Score',
                                          season: 'Current_Season_Total_Offense_Punts_per_Offensive_Score',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Offense_Punts_per_Offensive_Score',
                                          'Last 3': 'Last_3_Total_Offense_Punts_per_Offensive_Score',
                                          'Last 1': 'Last_1_Total_Offense_Punts_per_Offensive_Score',
                                          'Home': 'At_Home_Total_Offense_Punts_per_Offensive_Score',
                                          'Away': 'Away_Total_Offense_Punts_per_Offensive_Score'
                                          }, inplace=True)
                toppos_df['Team'] = toppos_df['Team'].str.strip()
                if season == '2010':
                    toppos_df['Rank_Total_Offense_Punts_per_Offensive_Score'] = toppos_df.index + 1
                toppos_df = toppos_df.replace('--', np.nan)
                toppos_df = toppos_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_offense_rushing_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-attempts-per-game' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
                rorapg_df = main_hist(rushing_offense_rushing_attempts_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'rushing_offense_rushing_attempts_per_game')
                rorapg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Attempts_per_Game',
                                          season: 'Current_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Last 3': 'Last_3_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Last 1': 'Last_1_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Home': 'At_Home_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Away': 'Away_Rushing_Offense_Rushing_Attempts_per_Game'
                                          }, inplace=True)
                rorapg_df['Team'] = rorapg_df['Team'].str.strip()
                if season == '2010':
                    rorapg_df['Rank_Rushing_Offense_Rushing_Attempts_per_Game'] = rorapg_df.index + 1
                rorapg_df = rorapg_df.replace('--', np.nan)
                rorapg_df = rorapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_offense_rushing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-per-game' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
                rorypg_df = main_hist(rushing_offense_rushing_yards_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'rushing_offense_rushing_yards_per_game')
                rorypg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Game',
                                          season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Last 3': 'Last_3_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Last 1': 'Last_1_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Game'
                                          }, inplace=True)
                rorypg_df['Team'] = rorypg_df['Team'].str.strip()
                if season == '2010':
                    rorypg_df['Rank_Rushing_Offense_Rushing_Yards_per_Game'] = rorypg_df.index + 1
                rorypg_df = rorypg_df.replace('--', np.nan)
                rorypg_df = rorypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_offense_rushing_yards_per_rush_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-rush-attempt' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
                rorypra_df = main_hist(rushing_offense_rushing_yards_per_rush_attempt_url_current, season, str(week),
                                       this_week_date_str,
                                       'rushing_offense_rushing_yards_per_rush_attempt')
                rorypra_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                           season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                           str(int(
                                               season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                           'Last 3': 'Last_3_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                           'Last 1': 'Last_1_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                           'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                           'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Rush_Attempt'
                                           }, inplace=True)
                rorypra_df['Team'] = rorypra_df['Team'].str.strip()
                if season == '2010':
                    rorypra_df['Rank_Rushing_Offense_Rushing_Yards_per_Rush_Attempt'] = rorypra_df.index + 1
                rorypra_df = rorypra_df.replace('--', np.nan)
                rorypra_df = rorypra_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_offense_rushing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-play-pct' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
                rorpp_df = main_hist(rushing_offense_rushing_play_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'rushing_offense_rushing_play_percentage')
                rorpp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Play_Percentage',
                                         season: 'Current_Season_Rushing_Offense_Rushing_Play_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Play_Percentage',
                                         'Last 3': 'Last_3_Rushing_Offense_Rushing_Play_Percentage',
                                         'Last 1': 'Last_1_Rushing_Offense_Rushing_Play_Percentage',
                                         'Home': 'At_Home_Rushing_Offense_Rushing_Play_Percentage',
                                         'Away': 'Away_Rushing_Offense_Rushing_Play_Percentage'
                                         }, inplace=True)
                rorpp_df['Team'] = rorpp_df['Team'].str.strip()
                if season == '2010':
                    rorpp_df['Rank_Rushing_Offense_Rushing_Play_Percentage'] = rorpp_df.index + 1
                rorpp_df = rorpp_df.replace('--', np.nan)
                for c in rorpp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        rorpp_df[c] = rorpp_df[c].str.rstrip('%').astype('float') / 100.0
                rorpp_df = rorpp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_offense_rushing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-pct' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
                roryp_df = main_hist(rushing_offense_rushing_yards_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'rushing_offense_rushing_yards_percentage')
                roryp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_Percentage',
                                         season: 'Current_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Last 3': 'Last_3_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Last 1': 'Last_1_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Home': 'At_Home_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Away': 'Away_Rushing_Offense_Rushing_Yards_Percentage'
                                         }, inplace=True)
                roryp_df['Team'] = roryp_df['Team'].str.strip()
                if season == '2010':
                    roryp_df['Rank_Rushing_Offense_Rushing_Yards_Percentage'] = roryp_df.index + 1
                roryp_df = roryp_df.replace('--', np.nan)
                for c in roryp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        roryp_df[c] = roryp_df[c].str.rstrip('%').astype('float') / 100.0
                roryp_df = roryp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_pass_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/pass-attempts-per-game' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
                popapg_df = main_hist(passing_offense_pass_attempts_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'passing_offense_pass_attempts_per_game')
                popapg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Pass_Attempts_per_Game',
                                          season: 'Current_Season_Passing_Offense_Pass_Attempts_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_Pass_Attempts_per_Game',
                                          'Last 3': 'Last_3_Passing_Offense_Pass_Attempts_per_Game',
                                          'Last 1': 'Last_1_Passing_Offense_Pass_Attempts_per_Game',
                                          'Home': 'At_Home_Passing_Offense_Pass_Attempts_per_Game',
                                          'Away': 'Away_Passing_Offense_Pass_Attempts_per_Game'
                                          }, inplace=True)
                popapg_df['Team'] = popapg_df['Team'].str.strip()
                if season == '2010':
                    popapg_df['Rank_Passing_Offense_Pass_Attempts_per_Game'] = popapg_df.index + 1
                popapg_df = popapg_df.replace('--', np.nan)
                popapg_df = popapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_completions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/completions-per-game' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
                pocpg_df = main_hist(passing_offense_completions_per_game_url_current, season, str(week),
                                     this_week_date_str,
                                     'passing_offense_completions_per_game')
                pocpg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completions_per_Game',
                                         season: 'Current_Season_Passing_Offense_Completions_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Completions_per_Game',
                                         'Last 3': 'Last_3_Passing_Offense_Completions_per_Game',
                                         'Last 1': 'Last_1_Passing_Offense_Completions_per_Game',
                                         'Home': 'At_Home_Passing_Offense_Completions_per_Game',
                                         'Away': 'Away_Passing_Offense_Completions_per_Game'
                                         }, inplace=True)
                pocpg_df['Team'] = pocpg_df['Team'].str.strip()
                if season == '2010':
                    pocpg_df['Rank_Passing_Offense_Completions_per_Game'] = pocpg_df.index + 1
                pocpg_df = pocpg_df.replace('--', np.nan)
                pocpg_df = pocpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_incompletions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/incompletions-per-game' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
                poipg_df = main_hist(passing_offense_incompletions_per_game_url_current, season, str(week),
                                     this_week_date_str,
                                     'passing_offense_incompletions_per_game')
                poipg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Incompletions_per_Game',
                                         season: 'Current_Season_Passing_Offense_Incompletions_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Incompletions_per_Game',
                                         'Last 3': 'Last_3_Passing_Offense_Incompletions_per_Game',
                                         'Last 1': 'Last_1_Passing_Offense_Incompletions_per_Game',
                                         'Home': 'At_Home_Passing_Offense_Incompletions_per_Game',
                                         'Away': 'Away_Passing_Offense_Incompletions_per_Game'
                                         }, inplace=True)
                poipg_df['Team'] = poipg_df['Team'].str.strip()
                if season == '2010':
                    poipg_df['Rank_Passing_Offense_Incompletions_per_Game'] = poipg_df.index + 1
                poipg_df = poipg_df.replace('--', np.nan)
                poipg_df = poipg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_completion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/completion-pct' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                pocp_df = main_hist(passing_offense_completion_percentage_url_current, season, str(week),
                                    this_week_date_str,
                                    'passing_offense_completion_percentage')
                pocp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completion_Percentage',
                                        season: 'Current_Season_Passing_Offense_Completion_Percentage',
                                        str(int(
                                            season) - 1): 'Previous_Season_Passing_Offense_Completion_Percentage',
                                        'Last 3': 'Last_3_Passing_Offense_Completion_Percentage',
                                        'Last 1': 'Last_1_Passing_Offense_Completion_Percentage',
                                        'Home': 'At_Home_Passing_Offense_Completion_Percentage',
                                        'Away': 'Away_Passing_Offense_Completion_Percentage'
                                        }, inplace=True)
                pocp_df['Team'] = pocp_df['Team'].str.strip()
                if season == '2010':
                    pocp_df['Rank_Passing_Offense_Completion_Percentage'] = pocp_df.index + 1
                pocp_df = pocp_df.replace('--', np.nan)
                for c in pocp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        pocp_df[c] = pocp_df[c].str.rstrip('%').astype('float') / 100.0
                pocp_df = pocp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_passing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-per-game' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
                popypg_df = main_hist(passing_offense_passing_yards_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'passing_offense_passing_yards_per_game')
                popypg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_per_Game',
                                          season: 'Current_Season_Passing_Offense_Passing_Yards_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_per_Game',
                                          'Last 3': 'Last_3_Passing_Offense_Passing_Yards_per_Game',
                                          'Last 1': 'Last_1_Passing_Offense_Passing_Yards_per_Game',
                                          'Home': 'At_Home_Passing_Offense_Passing_Yards_per_Game',
                                          'Away': 'Away_Passing_Offense_Passing_Yards_per_Game'
                                          }, inplace=True)
                popypg_df['Team'] = popypg_df['Team'].str.strip()
                if season == '2010':
                    popypg_df['Rank_Passing_Offense_Passing_Yards_per_Game'] = popypg_df.index + 1
                popypg_df = popypg_df.replace('--', np.nan)
                popypg_df = popypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_qb_sacked_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-per-game' \
                                                                 + '?date=' \
                                                                 + this_week_date_str
                poqspg_df = main_hist(passing_offense_qb_sacked_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'passing_offense_qb_sacked_per_game')
                poqspg_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_per_Game',
                                          season: 'Current_Season_Passing_Offense_QB_Sacked_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_per_Game',
                                          'Last 3': 'Last_3_Passing_Offense_QB_Sacked_per_Game',
                                          'Last 1': 'Last_1_Passing_Offense_QB_Sacked_per_Game',
                                          'Home': 'At_Home_Passing_Offense_QB_Sacked_per_Game',
                                          'Away': 'Away_Passing_Offense_QB_Sacked_per_Game'
                                          }, inplace=True)
                poqspg_df['Team'] = poqspg_df['Team'].str.strip()
                if season == '2010':
                    poqspg_df['Rank_Passing_Offense_QB_Sacked_per_Game'] = poqspg_df.index + 1
                poqspg_df = poqspg_df.replace('--', np.nan)
                poqspg_df = poqspg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_qb_sacked_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-pct' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
                poqsp_df = main_hist(passing_offense_qb_sacked_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'passing_offense_qb_sacked_percentage')
                poqsp_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_Percentage',
                                         season: 'Current_Season_Passing_Offense_QB_Sacked_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_Percentage',
                                         'Last 3': 'Last_3_Passing_Offense_QB_Sacked_Percentage',
                                         'Last 1': 'Last_1_Passing_Offense_QB_Sacked_Percentage',
                                         'Home': 'At_Home_Passing_Offense_QB_Sacked_Percentage',
                                         'Away': 'Away_Passing_Offense_QB_Sacked_Percentage'
                                         }, inplace=True)
                poqsp_df['Team'] = poqsp_df['Team'].str.strip()
                if season == '2010':
                    poqsp_df['Rank_Passing_Offense_QB_Sacked_Percentage'] = poqsp_df.index + 1
                poqsp_df = poqsp_df.replace('--', np.nan)
                for c in poqsp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        poqsp_df[c] = poqsp_df[c].str.rstrip('%').astype('float') / 100.0
                poqsp_df = poqsp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_average_passer_rating_url_current = 'https://www.teamrankings.com/college-football/stat/average-team-passer-rating' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                poapr_df = main_hist(passing_offense_average_passer_rating_url_current, season, str(week),
                                     this_week_date_str,
                                     'passing_offense_average_passer_rating')
                poapr_df.rename(columns={'Rank': 'Rank_Passing_Offense_Average_Passer_Rating',
                                         season: 'Current_Season_Passing_Offense_Average_Passer_Rating',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Average_Passer_Rating',
                                         'Last 3': 'Last_3_Passing_Offense_Average_Passer_Rating',
                                         'Last 1': 'Last_1_Passing_Offense_Average_Passer_Rating',
                                         'Home': 'At_Home_Passing_Offense_Average_Passer_Rating',
                                         'Away': 'Away_Passing_Offense_Average_Passer_Rating'
                                         }, inplace=True)
                poapr_df['Team'] = poapr_df['Team'].str.strip()
                if season == '2010':
                    poapr_df['Rank_Passing_Offense_Average_Passer_Rating'] = poapr_df.index + 1
                poapr_df = poapr_df.replace('--', np.nan)
                poapr_df = poapr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_passing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-play-pct' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
                ppoppp_df = main_hist(passing_offense_passing_play_percentage_url_current, season, str(week),
                                      this_week_date_str,
                                      'passing_offense_passing_play_percentage')
                ppoppp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Play_Percentage',
                                          season: 'Current_Season_Passing_Offense_Passing_Play_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_Passing_Play_Percentage',
                                          'Last 3': 'Last_3_Passing_Offense_Passing_Play_Percentage',
                                          'Last 1': 'Last_1_Passing_Offense_Passing_Play_Percentage',
                                          'Home': 'At_Home_Passing_Offense_Passing_Play_Percentage',
                                          'Away': 'Away_Passing_Offense_Passing_Play_Percentage'
                                          }, inplace=True)
                ppoppp_df['Team'] = ppoppp_df['Team'].str.strip()
                if season == '2010':
                    ppoppp_df['Rank_Passing_Offense_Passing_Play_Percentage'] = ppoppp_df.index + 1
                ppoppp_df = ppoppp_df.replace('--', np.nan)
                for c in ppoppp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        ppoppp_df[c] = ppoppp_df[c].str.rstrip('%').astype('float') / 100.0
                ppoppp_df = ppoppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_passing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-pct' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
                popyp_df = main_hist(passing_offense_passing_yards_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'passing_offense_passing_yards_percentage')
                popyp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_Percentage',
                                         season: 'Current_Season_Passing_Offense_Passing_Yards_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_Percentage',
                                         'Last 3': 'Last_3_Passing_Offense_Passing_Yards_Percentage',
                                         'Last 1': 'Last_1_Passing_Offense_Passing_Yards_Percentage',
                                         'Home': 'At_Home_Passing_Offense_Passing_Yards_Percentage',
                                         'Away': 'Away_Passing_Offense_Passing_Yards_Percentage'
                                         }, inplace=True)
                popyp_df['Team'] = popyp_df['Team'].str.strip()
                if season == '2010':
                    popyp_df['Rank_Passing_Offense_Passing_Yards_Percentage'] = popyp_df.index + 1
                popyp_df = popyp_df.replace('--', np.nan)
                for c in popyp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        popyp_df[c] = popyp_df[c].str.rstrip('%').astype('float') / 100.0
                popyp_df = popyp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_yards_per_pass_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-pass-attempt' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
                poyppa_df = main_hist(passing_offense_yards_per_pass_attempt_url_current, season, str(week),
                                      this_week_date_str,
                                      'passing_offense_yards_per_pass_attempt')
                poyppa_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Pass_Attempt',
                                          season: 'Current_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                          'Last 3': 'Last_3_Passing_Offense_Yards_per_Pass_Attempt',
                                          'Last 1': 'Last_1_Passing_Offense_Yards_per_Pass_Attempt',
                                          'Home': 'At_Home_Passing_Offense_Yards_per_Pass_Attempt',
                                          'Away': 'Away_Passing_Offense_Yards_per_Pass_Attempt'
                                          }, inplace=True)
                poyppa_df['Team'] = poyppa_df['Team'].str.strip()
                if season == '2010':
                    poyppa_df['Rank_Passing_Offense_Yards_per_Pass_Attempt'] = poyppa_df.index + 1
                poyppa_df = poyppa_df.replace('--', np.nan)
                poyppa_df = poyppa_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_offense_yards_per_completion_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-completion' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
                poypc_df = main_hist(passing_offense_yards_per_completion_url_current, season, str(week),
                                     this_week_date_str,
                                     'passing_offense_yards_per_completion')
                poypc_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Completion',
                                         season: 'Current_Season_Passing_Offense_Yards_per_Completion',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Completion',
                                         'Last 3': 'Last_3_Passing_Offense_Yards_per_Completion',
                                         'Last 1': 'Last_1_Passing_Offense_Yards_per_Completion',
                                         'Home': 'At_Home_Passing_Offense_Yards_per_Completion',
                                         'Away': 'Away_Passing_Offense_Yards_per_Completion'
                                         }, inplace=True)
                poypc_df['Team'] = poypc_df['Team'].str.strip()
                if season == '2010':
                    poypc_df['Rank_Passing_Offense_Yards_per_Completion'] = poypc_df.index + 1
                poypc_df = poypc_df.replace('--', np.nan)
                poypc_df = poypc_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_offense_field_goal_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/field-goal-attempts-per-game' \
                                                                                 + '?date=' \
                                                                                 + this_week_date_str
                stofgapg_df = main_hist(special_teams_offense_field_goal_attempts_per_game_url_current, season,
                                        str(week), this_week_date_str,
                                        'special_teams_offense_field_goal_attempts_per_game')
                stofgapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                            season: 'Current_Season_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                            'Last 3': 'Last_3_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                            'Last 1': 'Last_1_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                            'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                            'Away': 'Away_Special_Teams_Offense_Field_Goal_Attempts_per_Game'
                                            }, inplace=True)
                stofgapg_df['Team'] = stofgapg_df['Team'].str.strip()
                if season == '2010':
                    stofgapg_df['Rank_Special_Teams_Offense_Field_Goal_Attempts_per_Game'] = stofgapg_df.index + 1
                stofgapg_df = stofgapg_df.replace('--', np.nan)
                stofgapg_df = stofgapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_offense_field_goals_made_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/field-goals-made-per-game' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                stofgmpg_df = main_hist(special_teams_offense_field_goals_made_per_game_url_current, season, str(week),
                                        this_week_date_str,
                                        'special_teams_offense_field_goals_made_per_game')
                stofgmpg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            season: 'Current_Season_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Last 3': 'Last_3_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Last 1': 'Last_1_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Home': 'At_Home_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Away': 'Away_Special_Teams_Offense_Field_Goals_Made_per_Game'
                                            }, inplace=True)
                stofgmpg_df['Team'] = stofgmpg_df['Team'].str.strip()
                if season == '2010':
                    stofgmpg_df['Rank_Special_Teams_Offense_Field_Goals_Made_per_Game'] = stofgmpg_df.index + 1
                stofgmpg_df = stofgmpg_df.replace('--', np.nan)
                stofgmpg_df = stofgmpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_offense_field_goal_conversion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/field-goal-conversion-pct' \
                                                                                     + '?date=' \
                                                                                     + this_week_date_str
                stofgcp_df = main_hist(special_teams_offense_field_goal_conversion_percentage_url_current, season,
                                       str(week), this_week_date_str,
                                       'special_teams_offense_field_goal_conversion_percentage')
                stofgcp_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                           season: 'Current_Season_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                           str(int(
                                               season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                           'Last 3': 'Last_3_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                           'Last 1': 'Last_1_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                           'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                           'Away': 'Away_Special_Teams_Offense_Field_Goal_Conversion_Percentage'
                                           }, inplace=True)
                stofgcp_df['Team'] = stofgcp_df['Team'].str.strip()
                if season == '2010':
                    stofgcp_df['Rank_Special_Teams_Offense_Field_Goal_Conversion_Percentage'] = stofgcp_df.index + 1
                stofgcp_df = stofgcp_df.replace('--', np.nan)
                for c in stofgcp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        stofgcp_df[c] = stofgcp_df[c].str.rstrip('%').astype('float') / 100.0
                stofgcp_df = stofgcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_offense_punt_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/punt-attempts-per-game' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
                stopapg_df = main_hist(special_teams_offense_punt_attempts_per_game_url_current, season, str(week),
                                       this_week_date_str,
                                       'special_teams_offense_punt_attempts_per_game')
                stopapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           season: 'Current_Season_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Last 3': 'Last_3_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Last 1': 'Last_1_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Home': 'At_Home_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Away': 'Away_Special_Teams_Offense_Punt_Attempts_per_Game'
                                           }, inplace=True)
                stopapg_df['Team'] = stopapg_df['Team'].str.strip()
                if season == '2010':
                    stopapg_df['Rank_Special_Teams_Offense_Punt_Attempts_per_Game'] = stopapg_df.index + 1
                stopapg_df = stopapg_df.replace('--', np.nan)
                stopapg_df = stopapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_offense_gross_punt_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/gross-punt-yards-per-game' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                stogpypg_df = main_hist(special_teams_offense_gross_punt_yards_per_game_url_current, season, str(week),
                                        this_week_date_str,
                                        'special_teams_offense_gross_punt_yards_per_game')
                stogpypg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                            season: 'Current_Season_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                            'Last 3': 'Last_3_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                            'Last 1': 'Last_1_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                            'Home': 'At_Home_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                            'Away': 'Away_Special_Teams_Offense_Gross_Punt_Yards_per_Game'
                                            }, inplace=True)
                stogpypg_df['Team'] = stogpypg_df['Team'].str.strip()
                if season == '2010':
                    stogpypg_df['Rank_Special_Teams_Offense_Gross_Punt_Yards_per_Game'] = stogpypg_df.index + 1
                stogpypg_df = stogpypg_df.replace('--', np.nan)
                stogpypg_df = stogpypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-game' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
                sdoppg_df = main_hist(scoring_defense_opponent_points_per_game_url_current, season, str(week),
                                      this_week_date_str, 'scoring_defense_opponent_points_per_game')
                sdoppg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Game',
                                          season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Game',
                                          'Last 3': 'Last_3_Scoring_Defense_Opponent_Points_per_Game',
                                          'Last 1': 'Last_1_Scoring_Defense_Opponent_Points_per_Game',
                                          'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Game',
                                          'Away': 'Away_Scoring_Defense_Opponent_Points_per_Game'
                                          }, inplace=True)
                sdoppg_df['Team'] = sdoppg_df['Team'].str.strip()
                if season == '2010':
                    sdoppg_df['Rank_Scoring_Defense_Opponent_Points_per_Game'] = sdoppg_df.index + 1
                sdoppg_df = sdoppg_df.replace('--', np.nan)
                sdoppg_df = sdoppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opp_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/opp-yards-per-point' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
                sdoypp_df = main_hist(scoring_defense_opp_yards_per_point_url_current, season, str(week),
                                      this_week_date_str, 'scoring_defense_opp_yards_per_point')
                sdoypp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opp_Yards_per_Point',
                                          season: 'Current_Season_Scoring_Defense_Opp_Yards_per_Point',
                                          str(int(
                                              season) - 1): 'Previous_Season_Scoring_Defense_Opp_Yards_per_Point',
                                          'Last 3': 'Last_3_Scoring_Defense_Opp_Yards_per_Point',
                                          'Last 1': 'Last_1_Scoring_Defense_Opp_Yards_per_Point',
                                          'Home': 'At_Home_Scoring_Defense_Opp_Yards_per_Point',
                                          'Away': 'Away_Scoring_Defense_Opp_Yards_per_Point'
                                          }, inplace=True)
                sdoypp_df['Team'] = sdoypp_df['Team'].str.strip()
                if season == '2010':
                    sdoypp_df['Rank_Scoring_Defense_Opp_Yards_per_Point'] = sdoypp_df.index + 1
                sdoypp_df = sdoypp_df.replace('--', np.nan)
                sdoypp_df = sdoypp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_points_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-play' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
                sdoppp_df = main_hist(scoring_defense_opponent_points_per_play_url_current, season, str(week),
                                      this_week_date_str, 'scoring_defense_opponent_points_per_play')
                sdoppp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Play',
                                          season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Play',
                                          str(int(
                                              season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Play',
                                          'Last 3': 'Last_3_Scoring_Defense_Opponent_Points_per_Play',
                                          'Last 1': 'Last_1_Scoring_Defense_Opponent_Points_per_Play',
                                          'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Play',
                                          'Away': 'Away_Scoring_Defense_Opponent_Points_per_Play'
                                          }, inplace=True)
                sdoppp_df['Team'] = sdoppp_df['Team'].str.strip()
                if season == '2010':
                    sdoppp_df['Rank_Scoring_Defense_Opponent_Points_per_Play'] = sdoppp_df.index + 1
                sdoppp_df = sdoppp_df.replace('--', np.nan)
                sdoppp_df = sdoppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_average_scoring_margin_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-average-scoring-margin' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                sdoasm_df = main_hist(scoring_defense_opponent_average_scoring_margin_url_current, season, str(week),
                                      this_week_date_str, 'scoring_defense_opponent_average_scoring_margin')
                sdoasm_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                          season: 'Current_Season_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                          str(int(
                                              season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                          'Last 3': 'Last_3_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                          'Last 1': 'Last_1_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                          'Home': 'At_Home_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                          'Away': 'Away_Scoring_Defense_Opponent_Average_Scoring_Margin'
                                          }, inplace=True)
                sdoasm_df['Team'] = sdoasm_df['Team'].str.strip()
                if season == '2010':
                    sdoasm_df['Rank_Scoring_Defense_Opponent_Average_Scoring_Margin'] = sdoasm_df.index + 1
                sdoasm_df = sdoasm_df.replace('--', np.nan)
                sdoasm_df = sdoasm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_red_zone_scoring_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-red-zone-scoring-attempts-per-game' \
                                                                                          + '?date=' \
                                                                                          + this_week_date_str
                sdorzsapg_df = main_hist(scoring_defense_opponent_red_zone_scoring_attempts_per_game_url_current,
                                         season, str(week),
                                         this_week_date_str,
                                         'scoring_defense_opponent_red_zone_scoring_attempts_per_game')
                sdorzsapg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                             season: 'Current_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                             str(int(
                                                 season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                             'Last 3': 'Last_3_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                             'Last 1': 'Last_1_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                             'Home': 'At_Home_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                             'Away': 'Away_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game'
                                             }, inplace=True)
                sdorzsapg_df['Team'] = sdorzsapg_df['Team'].str.strip()
                if season == '2010':
                    sdorzsapg_df[
                        'Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game'] = sdorzsapg_df.index + 1
                sdorzsapg_df = sdorzsapg_df.replace('--', np.nan)
                sdorzsapg_df = sdorzsapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_red_zone_scores_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-red-zone-scores-per-game' \
                                                                                + '?date=' \
                                                                                + this_week_date_str
                sdorzspg_df = main_hist(scoring_defense_opponent_red_zone_scores_per_game_url_current,
                                        season, str(week),
                                        this_week_date_str,
                                        'scoring_defense_opponent_red_zone_scores_per_game')
                sdorzspg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                            season: 'Current_Season_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                            'Last 3': 'Last_3_Scoring_Defense_Opponent_Red_Zone_Scoring_Scores_per_Game',
                                            'Last 1': 'Last_1_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                            'Home': 'At_Home_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                            'Away': 'Away_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game'
                                            }, inplace=True)
                sdorzspg_df['Team'] = sdorzspg_df['Team'].str.strip()
                if season == '2010':
                    sdorzspg_df[
                        'Rank_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game'] = sdorzspg_df.index + 1
                sdorzspg_df = sdorzspg_df.replace('--', np.nan)
                sdorzspg_df = sdorzspg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_red_zone_scoring_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-red-zone-scoring-pct' \
                                                                                   + '?date=' \
                                                                                   + this_week_date_str
                sdorzsp_df = main_hist(scoring_defense_opponent_red_zone_scoring_percentage_url_current,
                                       season, str(week),
                                       this_week_date_str,
                                       'scoring_defense_opponent_red_zone_scoring_percentage')
                sdorzsp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                           season: 'Current_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                           str(int(
                                               season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                           'Last 3': 'Last_3_Scoring_Defense_Opponent_Red_Zone_Scoring_Scoring_Percentage',
                                           'Last 1': 'Last_1_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                           'Home': 'At_Home_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                           'Away': 'Away_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage'
                                           }, inplace=True)
                sdorzsp_df['Team'] = sdorzsp_df['Team'].str.strip()
                if season == '2010':
                    sdorzsp_df[
                        'Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage'] = sdorzsp_df.index + 1
                sdorzsp_df = sdorzsp_df.replace('--', np.nan)
                for c in sdorzsp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        sdorzsp_df[c] = sdorzsp_df[c].str.rstrip('%').astype('float') / 100.0
                sdorzsp_df = sdorzsp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_points_per_field_goal_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-field-goal-attempt' \
                                                                                     + '?date=' \
                                                                                     + this_week_date_str
                sdoppfga_df = main_hist(scoring_defense_opponent_points_per_field_goal_attempt_url_current,
                                        season, str(week),
                                        this_week_date_str,
                                        'scoring_defense_opponent_points_per_field_goal_attempt')
                sdoppfga_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                            season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                            str(int(
                                                season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                            'Last 3': 'Last_3_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                            'Last 1': 'Last_1_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                            'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                            'Away': 'Away_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt'
                                            }, inplace=True)
                sdoppfga_df['Team'] = sdoppfga_df['Team'].str.strip()
                if season == '2010':
                    sdoppfga_df[
                        'Rank_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt'] = sdoppfga_df.index + 1
                sdoppfga_df = sdoppfga_df.replace('--', np.nan)
                sdoppfga_df = sdoppfga_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_offensive_touchdowns_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-offensive-touchdowns-per-game' \
                                                                                     + '?date=' \
                                                                                     + this_week_date_str
                sdootpg_df = main_hist(scoring_defense_opponent_offensive_touchdowns_per_game_url_current,
                                       season, str(week),
                                       this_week_date_str,
                                       'scoring_defense_opponent_offensive_touchdowns_per_game')
                sdootpg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                           season: 'Current_Season_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                           'Last 3': 'Last_3_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                           'Last 1': 'Last_1_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                           'Home': 'At_Home_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                           'Away': 'Away_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game'
                                           }, inplace=True)
                sdootpg_df['Team'] = sdootpg_df['Team'].str.strip()
                if season == '2010':
                    sdootpg_df[
                        'Rank_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game'] = sdootpg_df.index + 1
                sdootpg_df = sdootpg_df.replace('--', np.nan)
                sdootpg_df = sdootpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                scoring_defense_opponent_offensive_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-offensive-points-per-game' \
                                                                                 + '?date=' \
                                                                                 + this_week_date_str
                sdooppg_df = main_hist(scoring_defense_opponent_offensive_points_per_game_url_current,
                                       season, str(week),
                                       this_week_date_str,
                                       'scoring_defense_opponent_offensive_points_per_game')
                sdooppg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                           season: 'Current_Season_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                           'Last 3': 'Last_3_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                           'Last 1': 'Last_1_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                           'Home': 'At_Home_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                           'Away': 'Away_Scoring_Defense_Opponent_Offensive_Points_per_Game'
                                           }, inplace=True)
                sdooppg_df['Team'] = sdooppg_df['Team'].str.strip()
                if season == '2010':
                    sdooppg_df[
                        'Rank_Scoring_Defense_Opponent_Offensive_Points_per_Game'] = sdooppg_df.index + 1
                sdooppg_df = sdooppg_df.replace('--', np.nan)
                sdooppg_df = sdooppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-game' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                tdoypg_df = main_hist(total_defense_opponent_yards_per_game_url_current, season, str(week),
                                      this_week_date_str, 'total_defense_opponent_yards_per_game')
                tdoypg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Yards_per_Game',
                                          season: 'Current_Season_Total_Defense_Opponent_Yards_per_Game',
                                          str(int(season) - 1): 'Previous_Season_Total_Defense_Opponent_Yards_per_Game',
                                          'Last 3': 'Last_3_Total_Defense_Opponent_Yards_per_Game',
                                          'Last 1': 'Last_1_Total_Defense_Opponent_Yards_per_Game',
                                          'Home': 'At_Home_Total_Defense_Opponent_Yards_per_Game',
                                          'Away': 'Away_Total_Defense_Opponent_Yards_per_Game'
                                          }, inplace=True)
                tdoypg_df['Team'] = tdoypg_df['Team'].str.strip()
                if season == '2010':
                    tdoypg_df['Rank_Total_Defense_Opponent_Yards_per_Game'] = tdoypg_df.index + 1
                tdoypg_df = tdoypg_df.replace('--', np.nan)
                tdoypg_df = tdoypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_plays_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-plays-per-game' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                tdoppg_df = main_hist(total_defense_opponent_plays_per_game_url_current, season, str(week),
                                      this_week_date_str, 'total_defense_opponent_plays_per_game')
                tdoppg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Plays_per_Game',
                                          season: 'Current_Season_Total_Defense_Opponent_Plays_per_Game',
                                          str(int(season) - 1): 'Previous_Season_Total_Defense_Opponent_Plays_per_Game',
                                          'Last 3': 'Last_3_Total_Defense_Opponent_Plays_per_Game',
                                          'Last 1': 'Last_1_Total_Defense_Opponent_Plays_per_Game',
                                          'Home': 'At_Home_Total_Defense_Opponent_Plays_per_Game',
                                          'Away': 'Away_Total_Defense_Opponent_Plays_per_Game'
                                          }, inplace=True)
                tdoppg_df['Team'] = tdoppg_df['Team'].str.strip()
                if season == '2010':
                    tdoppg_df['Rank_Total_Defense_Opponent_Plays_per_Game'] = tdoppg_df.index + 1
                tdoppg_df = tdoppg_df.replace('--', np.nan)
                tdoppg_df = tdoppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-first-downs-per-game' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
                tdofdpg_df = main_hist(total_defense_opponent_first_downs_per_game_url_current, season, str(week),
                                       this_week_date_str, 'total_defense_opponent_first_downs_per_game')
                tdofdpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_First_Downs_per_Game',
                                           season: 'Current_Season_Total_Defense_Opponent_First_Downs_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Defense_Opponent_First_Downs_per_Game',
                                           'Last 3': 'Last_3_Total_Defense_Opponent_First_Downs_per_Game',
                                           'Last 1': 'Last_1_Total_Defense_Opponent_First_Downs_per_Game',
                                           'Home': 'At_Home_Total_Defense_Opponent_First_Downs_per_Game',
                                           'Away': 'Away_Total_Defense_Opponent_First_Downs_per_Game'
                                           }, inplace=True)
                tdofdpg_df['Team'] = tdofdpg_df['Team'].str.strip()
                if season == '2010':
                    tdofdpg_df['Rank_Total_Defense_Opponent_First_Downs_per_Game'] = tdofdpg_df.index + 1
                tdofdpg_df = tdofdpg_df.replace('--', np.nan)
                tdofdpg_df = tdofdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_third_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-third-downs-per-game' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
                tdotdpg_df = main_hist(total_defense_opponent_third_downs_per_game_url_current, season, str(week),
                                       this_week_date_str, 'total_defense_opponent_third_downs_per_game')
                tdotdpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Third_Downs_per_Game',
                                           season: 'Current_Season_Total_Defense_Opponent_Third_Downs_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Defense_Opponent_Third_Downs_per_Game',
                                           'Last 3': 'Last_3_Total_Defense_Opponent_Third_Downs_per_Game',
                                           'Last 1': 'Last_1_Total_Defense_Opponent_Third_Downs_per_Game',
                                           'Home': 'At_Home_Total_Defense_Opponent_Third_Downs_per_Game',
                                           'Away': 'Away_Total_Defense_Opponent_Third_Downs_per_Game'
                                           }, inplace=True)
                tdotdpg_df['Team'] = tdotdpg_df['Team'].str.strip()
                if season == '2010':
                    tdotdpg_df['Rank_Total_Defense_Opponent_Third_Downs_per_Game'] = tdotdpg_df.index + 1
                tdotdpg_df = tdotdpg_df.replace('--', np.nan)
                tdotdpg_df = tdotdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_third_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-third-down-conversions-per-game' \
                                                                                     + '?date=' \
                                                                                     + this_week_date_str
                tdotdcpg_df = main_hist(total_defense_opponent_third_down_conversions_per_game_url_current, season,
                                        str(week),
                                        this_week_date_str, 'total_defense_opponent_third_down_conversions_per_game')
                tdotdcpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                            season: 'Current_Season_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                            'Last 3': 'Last_3_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                            'Last 1': 'Last_1_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                            'Home': 'At_Home_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                            'Away': 'Away_Total_Defense_Opponent_Third_Down_Conversions_per_Game'
                                            }, inplace=True)
                tdotdcpg_df['Team'] = tdotdcpg_df['Team'].str.strip()
                if season == '2010':
                    tdotdcpg_df['Rank_Total_Defense_Opponent_Third_Down_Conversions_per_Game'] = tdotdcpg_df.index + 1
                tdotdcpg_df = tdotdcpg_df.replace('--', np.nan)
                tdotdcpg_df = tdotdcpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_fourth_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fourth-downs-per-game' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
                tdofodpg_df = main_hist(total_defense_opponent_fourth_downs_per_game_url_current, season,
                                        str(week),
                                        this_week_date_str, 'total_defense_opponent_fourth_downs_per_game')
                tdofodpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                            season: 'Current_Season_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                            'Last 3': 'Last_3_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                            'Last 1': 'Last_1_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                            'Home': 'At_Home_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                            'Away': 'Away_Total_Defense_Opponent_Fourth_Downs_per_Game'
                                            }, inplace=True)
                tdofodpg_df['Team'] = tdofodpg_df['Team'].str.strip()
                if season == '2010':
                    tdofodpg_df['Rank_Total_Defense_Opponent_Fourth_Downs_per_Game'] = tdofodpg_df.index + 1
                tdofodpg_df = tdofodpg_df.replace('--', np.nan)
                tdofodpg_df = tdofodpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_fourth_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fourth-down-conversions-per-game' \
                                                                                      + '?date=' \
                                                                                      + this_week_date_str
                tdofdcpg_df = main_hist(total_defense_opponent_fourth_down_conversions_per_game_url_current, season,
                                        str(week),
                                        this_week_date_str, 'total_defense_opponent_fourth_down_conversions_per_game')
                tdofdcpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game',
                                            season: 'Current_Season_Total_Defense_Opponent_Fourth_Down_Conversions_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Total_Defense_Opponent_Fourth_Down_Conversions_Game',
                                            'Last 3': 'Last_3_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game',
                                            'Last 1': 'Last_1_Total_Defense_Opponent_Fourth_Downs_Conversions_per_Game',
                                            'Home': 'At_Home_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game',
                                            'Away': 'Away_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game'
                                            }, inplace=True)
                tdofdcpg_df['Team'] = tdofdcpg_df['Team'].str.strip()
                if season == '2010':
                    tdofdcpg_df['Rank_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game'] = tdofdcpg_df.index + 1
                tdofdcpg_df = tdofdcpg_df.replace('--', np.nan)
                tdofdcpg_df = tdofdcpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_average_time_of_possession_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-average-time-of-possession-net-of-ot' \
                                                                                + '?date=' \
                                                                                + this_week_date_str
                tdoatop_df = main_hist(total_defense_opponent_average_time_of_possession_url_current, season,
                                       str(week),
                                       this_week_date_str, 'total_defense_opponent_average_time_of_possession')
                tdoatop_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Average_Time_of_Possession',
                                           season: 'Current_Season_Total_Defense_Opponent_Average_Time_of_Possession',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Defense_Opponent_Average_Time_of_Possession',
                                           'Last 3': 'Last_3_Total_Defense_Opponent_Average_Time_of_Possession',
                                           'Last 1': 'Last_1_Total_Defense_Opponent_Average_Time_of_Possession',
                                           'Home': 'At_Home_Total_Defense_Opponent_Average_Time_of_Possession',
                                           'Away': 'Away_Total_Defense_Opponent_Average_Time_of_Possession'
                                           }, inplace=True)
                tdoatop_df['Team'] = tdoatop_df['Team'].str.strip()
                if season == '2010':
                    tdoatop_df['Rank_Total_Defense_Opponent_Average_Time_of_Possession'] = tdoatop_df.index + 1
                tdoatop_df = tdoatop_df.replace('--', np.nan)
                tdoatop_df = tdoatop_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_time_of_possession_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-time-of-possession-pct-net-of-ot' \
                                                                                   + '?date=' \
                                                                                   + this_week_date_str
                tdptopp_df = main_hist(total_defense_opponent_time_of_possession_percentage_url_current, season,
                                       str(week),
                                       this_week_date_str, 'total_defense_opponent_time_of_possession_percentage')
                tdptopp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                           season: 'Current_Season_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                           'Last 3': 'Last_3_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                           'Last 1': 'Last_1_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                           'Home': 'At_Home_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                           'Away': 'Away_Total_Defense_Opponent_Time_of_Possession_Percentage'
                                           }, inplace=True)
                tdptopp_df['Team'] = tdptopp_df['Team'].str.strip()
                if season == '2010':
                    tdptopp_df['Rank_Total_Defense_Opponent_Time_of_Possession_Percentage'] = tdptopp_df.index + 1
                tdptopp_df = tdptopp_df.replace('--', np.nan)
                for c in tdptopp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        tdptopp_df[c] = tdptopp_df[c].str.rstrip('%').astype('float') / 100.0
                tdptopp_df = tdptopp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_third_down_conversion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-third-down-conversion-pct' \
                                                                                      + '?date=' \
                                                                                      + this_week_date_str
                tdotdcp_df = main_hist(total_defense_opponent_third_down_conversion_percentage_url_current, season,
                                       str(week),
                                       this_week_date_str, 'total_defense_opponent_third_down_conversion_percentage')
                tdotdcp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                           season: 'Current_Season_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                           'Last 3': 'Last_3_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                           'Last 1': 'Last_1_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                           'Home': 'At_Home_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                           'Away': 'Away_Total_Defense_Opponent_Third_Down_Conversion_Percentage'
                                           }, inplace=True)
                tdotdcp_df['Team'] = tdotdcp_df['Team'].str.strip()
                if season == '2010':
                    tdotdcp_df['Rank_Total_Defense_Opponent_Third_Down_Conversion_Percentage'] = tdotdcp_df.index + 1
                tdotdcp_df = tdotdcp_df.replace('--', np.nan)
                for c in tdotdcp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        tdotdcp_df[c] = tdotdcp_df[c].str.rstrip('%').astype('float') / 100.0
                tdotdcp_df = tdotdcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_fourth_down_conversion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fourth-down-conversion-pct' \
                                                                                       + '?date=' \
                                                                                       + this_week_date_str
                tdofdcp_df = main_hist(total_defense_opponent_fourth_down_conversion_percentage_url_current, season,
                                       str(week),
                                       this_week_date_str, 'total_defense_opponent_fourth_down_conversion_percentage')
                tdofdcp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                           season: 'Current_Season_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                           'Last 3': 'Last_3_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                           'Last 1': 'Last_1_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                           'Home': 'At_Home_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                           'Away': 'Away_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage'
                                           }, inplace=True)
                tdofdcp_df['Team'] = tdofdcp_df['Team'].str.strip()
                if season == '2010':
                    tdofdcp_df['Rank_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage'] = tdofdcp_df.index + 1
                tdofdcp_df = tdofdcp_df.replace('--', np.nan)
                for c in tdofdcp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        tdofdcp_df[c] = tdofdcp_df[c].str.rstrip('%').astype('float') / 100.0
                tdofdcp_df = tdofdcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_punts_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-punts-per-play' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                tdoppp_df = main_hist(total_defense_opponent_punts_per_play_url_current, season,
                                      str(week),
                                      this_week_date_str, 'total_defense_opponent_punts_per_play')
                tdoppp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Punts_per_Play',
                                          season: 'Current_Season_Total_Defense_Opponent_Punts_per_Play',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Defense_Opponent_Punts_per_Play',
                                          'Last 3': 'Last_3_Total_Defense_Opponent_Punts_per_Play',
                                          'Last 1': 'Last_1_Total_Defense_Opponent_Punts_per_Play',
                                          'Home': 'At_Home_Total_Defense_Opponent_Punts_per_Play',
                                          'Away': 'Away_Total_Defense_Opponent_Punts_per_Play'
                                          }, inplace=True)
                tdoppp_df['Team'] = tdoppp_df['Team'].str.strip()
                if season == '2010':
                    tdoppp_df['Rank_Total_Defense_Opponent_Punts_per_Play'] = tdoppp_df.index + 1
                tdoppp_df = tdoppp_df.replace('--', np.nan)
                tdoppp_df = tdoppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                total_defense_opponent_punts_per_offensive_score_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-punts-per-offensive-score' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
                tdoppos_df = main_hist(total_defense_opponent_punts_per_offensive_score_url_current, season,
                                       str(week),
                                       this_week_date_str, 'total_defense_opponent_punts_per_offensive_score')
                tdoppos_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                           season: 'Current_Season_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                           'Last 3': 'Last_3_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                           'Last 1': 'Last_1_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                           'Home': 'At_Home_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                           'Away': 'Away_Total_Defense_Opponent_Punts_per_Offensive_Score'
                                           }, inplace=True)
                tdoppos_df['Team'] = tdoppos_df['Team'].str.strip()
                if season == '2010':
                    tdoppos_df['Rank_Total_Defense_Opponent_Punts_per_Offensive_Score'] = tdoppos_df.index + 1
                tdoppos_df = tdoppos_df.replace('--', np.nan)
                tdoppos_df = tdoppos_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_defense_opponent_rushing_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-attempts-per-game' \
                                                                                 + '?date=' \
                                                                                 + this_week_date_str
                rdorapg_df = main_hist(rushing_defense_opponent_rushing_attempts_per_game_url_current, season,
                                       str(week),
                                       this_week_date_str, 'rushing_defense_opponent_rushing_attempts_per_game')
                rdorapg_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                           season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Attempts_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                           'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                           'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                           'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                           'Away': 'Away_Rushing_Defense_Opponent_Rushing_Attempts_per_Game'
                                           }, inplace=True)
                rdorapg_df['Team'] = rdorapg_df['Team'].str.strip()
                if season == '2010':
                    rdorapg_df['Rank_Rushing_Defense_Opponent_Rushing_Attempts_per_Game'] = rdorapg_df.index + 1
                rdorapg_df = rdorapg_df.replace('--', np.nan)
                rdorapg_df = rdorapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_defense_opponent_rushing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-yards-per-game' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                rdorypg_df = main_hist(rushing_defense_opponent_rushing_yards_per_game_url_current, season,
                                       str(week),
                                       this_week_date_str, 'rushing_defense_opponent_rushing_yards_per_game')
                rdorypg_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                           season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                           'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                           'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                           'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                           'Away': 'Away_Rushing_Defense_Opponent_Rushing_Yards_per_Game'
                                           }, inplace=True)
                rdorypg_df['Team'] = rdorypg_df['Team'].str.strip()
                if season == '2010':
                    rdorypg_df['Rank_Rushing_Defense_Opponent_Rushing_Yards_per_Game'] = rdorypg_df.index + 1
                rdorypg_df = rdorypg_df.replace('--', np.nan)
                rdorypg_df = rdorypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_defense_opponent_rushing_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-first-downs-per-game' \
                                                                                    + '?date=' \
                                                                                    + this_week_date_str
                rdorfdpg_df = main_hist(rushing_defense_opponent_rushing_first_downs_per_game_url_current, season,
                                        str(week),
                                        this_week_date_str, 'rushing_defense_opponent_rushing_first_downs_per_game')
                rdorfdpg_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                            season: 'Current_Season_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                            'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                            'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                            'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                            'Away': 'Away_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game'
                                            }, inplace=True)
                rdorfdpg_df['Team'] = rdorfdpg_df['Team'].str.strip()
                if season == '2010':
                    rdorfdpg_df['Rank_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game'] = rdorfdpg_df.index + 1
                rdorfdpg_df = rdorfdpg_df.replace('--', np.nan)
                rdorfdpg_df = rdorfdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_defense_opponent_yards_per_rush_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-rush-attempt' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                rdoypra_df = main_hist(rushing_defense_opponent_yards_per_rush_attempt_url_current, season,
                                       str(week),
                                       this_week_date_str, 'rushing_defense_opponent_yards_per_rush_attempt')
                rdoypra_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                           season: 'Current_Season_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                           str(int(
                                               season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                           'Last 3': 'Last_3_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                           'Last 1': 'Last_1_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                           'Home': 'At_Home_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                           'Away': 'Away_Rushing_Defense_Opponent_Yards_per_Rush_Attempt'
                                           }, inplace=True)
                rdoypra_df['Team'] = rdoypra_df['Team'].str.strip()
                if season == '2010':
                    rdoypra_df['Rank_Rushing_Defense_Opponent_Yards_per_Rush_Attempt'] = rdoypra_df.index + 1
                rdoypra_df = rdoypra_df.replace('--', np.nan)
                rdoypra_df = rdoypra_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_defense_opponent_rushing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-play-pct' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
                rdorpp_df = main_hist(rushing_defense_opponent_rushing_play_percentage_url_current, season,
                                      str(week),
                                      this_week_date_str, 'rushing_defense_opponent_rushing_play_percentage')
                rdorpp_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                          season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                          'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                          'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                          'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                          'Away': 'Away_Rushing_Defense_Opponent_Rushing_Play_Percentage'
                                          }, inplace=True)
                rdorpp_df['Team'] = rdorpp_df['Team'].str.strip()
                if season == '2010':
                    rdorpp_df['Rank_Rushing_Defense_Opponent_Rushing_Play_Percentage'] = rdorpp_df.index + 1
                rdorpp_df = rdorpp_df.replace('--', np.nan)
                for c in rdorpp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        rdorpp_df[c] = rdorpp_df[c].str.rstrip('%').astype('float') / 100.0
                rdorpp_df = rdorpp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                rushing_defense_opponent_rushing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-yards-pct' \
                                                                                + '?date=' \
                                                                                + this_week_date_str
                rdoryp_df = main_hist(rushing_defense_opponent_rushing_yards_percentage_url_current, season,
                                      str(week),
                                      this_week_date_str, 'rushing_defense_opponent_rushing_yards_percentage')
                rdoryp_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                          season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                          'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                          'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                          'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                          'Away': 'Away_Rushing_Defense_Opponent_Rushing_Yards_Percentage'
                                          }, inplace=True)
                rdoryp_df['Team'] = rdoryp_df['Team'].str.strip()
                if season == '2010':
                    rdoryp_df['Rank_Rushing_Defense_Opponent_Rushing_Yards_Percentage'] = rdoryp_df.index + 1
                rdoryp_df = rdoryp_df.replace('--', np.nan)
                for c in rdoryp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        rdoryp_df[c] = rdoryp_df[c].str.rstrip('%').astype('float') / 100.0
                rdoryp_df = rdoryp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_pass_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-pass-attempts-per-game' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                pdopapg_df = main_hist(passing_defense_opponent_pass_attempts_per_game_url_current, season,
                                       str(week),
                                       this_week_date_str, 'passing_defense_opponent_pass_attempts_per_game')
                pdopapg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                           season: 'Current_Season_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                           'Last 3': 'Last_3_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                           'Last 1': 'Last_1_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                           'Home': 'At_Home_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                           'Away': 'Away_Passing_Defense_Opponent_Pass_Attempts_per_Game'
                                           }, inplace=True)
                pdopapg_df['Team'] = pdopapg_df['Team'].str.strip()
                if season == '2010':
                    pdopapg_df['Rank_Passing_Defense_Opponent_Pass_Attempts_per_Game'] = pdopapg_df.index + 1
                pdopapg_df = pdopapg_df.replace('--', np.nan)
                pdopapg_df = pdopapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_completions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-completions-per-game' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
                pdocpg_df = main_hist(passing_defense_opponent_completions_per_game_url_current, season,
                                      str(week),
                                      this_week_date_str, 'passing_defense_opponent_completions_per_game')
                pdocpg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Completions_per_Game',
                                          season: 'Current_Season_Passing_Defense_Opponent_Completions_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Defense_Opponent_Completions_per_Game',
                                          'Last 3': 'Last_3_Passing_Defense_Opponent_Completions_per_Game',
                                          'Last 1': 'Last_1_Passing_Defense_Opponent_Completions_per_Game',
                                          'Home': 'At_Home_Passing_Defense_Opponent_Completions_per_Game',
                                          'Away': 'Away_Passing_Defense_Opponent_Completions_per_Game'
                                          }, inplace=True)
                pdocpg_df['Team'] = pdocpg_df['Team'].str.strip()
                if season == '2010':
                    pdocpg_df['Rank_Passing_Defense_Opponent_Completions_per_Game'] = pdocpg_df.index + 1
                pdocpg_df = pdocpg_df.replace('--', np.nan)
                pdocpg_df = pdocpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_incompletions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-incompletions-per-game' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                pdoipg_df = main_hist(passing_defense_opponent_incompletions_per_game_url_current, season,
                                      str(week),
                                      this_week_date_str, 'passing_defense_opponent_incompletions_per_game')
                pdoipg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Incompletions_per_Game',
                                          season: 'Current_Season_Passing_Defense_Opponent_Incompletions_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Defense_Opponent_Incompletions_per_Game',
                                          'Last 3': 'Last_3_Passing_Defense_Opponent_Incompletions_per_Game',
                                          'Last 1': 'Last_1_Passing_Defense_Opponent_Incompletions_per_Game',
                                          'Home': 'At_Home_Passing_Defense_Opponent_Incompletions_per_Game',
                                          'Away': 'Away_Passing_Defense_Opponent_Incompletions_per_Game'
                                          }, inplace=True)
                pdoipg_df['Team'] = pdoipg_df['Team'].str.strip()
                if season == '2010':
                    pdoipg_df['Rank_Passing_Defense_Opponent_Incompletions_per_Game'] = pdoipg_df.index + 1
                pdoipg_df = pdoipg_df.replace('--', np.nan)
                pdoipg_df = pdoipg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_completion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-completion-pct' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
                pdocp_df = main_hist(passing_defense_opponent_completion_percentage_url_current, season,
                                     str(week),
                                     this_week_date_str, 'passing_defense_opponent_completion_percentage')
                pdocp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Completion_Percentage',
                                         season: 'Current_Season_Passing_Defense_Opponent_Completion_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Defense_Opponent_Completion_Percentage',
                                         'Last 3': 'Last_3_Passing_Defense_Opponent_Completion_Percentage',
                                         'Last 1': 'Last_1_Passing_Defense_Opponent_Completion_Percentage',
                                         'Home': 'At_Home_Passing_Defense_Opponent_Completion_Percentage',
                                         'Away': 'Away_Passing_Defense_Opponent_Completion_Percentage'
                                         }, inplace=True)
                pdocp_df['Team'] = pdocp_df['Team'].str.strip()
                if season == '2010':
                    pdocp_df['Rank_Passing_Defense_Opponent_Completion_Percentage'] = pdocp_df.index + 1
                pdocp_df = pdocp_df.replace('--', np.nan)
                for c in pdocp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        pdocp_df[c] = pdocp_df[c].str.rstrip('%').astype('float') / 100.0
                pdocp_df = pdocp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_passing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-yards-per-game' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                pdopypg_df = main_hist(passing_defense_opponent_passing_yards_per_game_url_current, season,
                                       str(week),
                                       this_week_date_str, 'passing_defense_opponent_passing_yards_per_game')
                pdopypg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                           season: 'Current_Season_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                           'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                           'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                           'Home': 'At_Home_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                           'Away': 'Away_Passing_Defense_Opponent_Passing_Yards_per_Game'
                                           }, inplace=True)
                pdopypg_df['Team'] = pdopypg_df['Team'].str.strip()
                if season == '2010':
                    pdopypg_df['Rank_Passing_Defense_Opponent_Passing_Yards_per_Game'] = pdopypg_df.index + 1
                pdopypg_df = pdopypg_df.replace('--', np.nan)
                pdopypg_df = pdopypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_passing_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-first-downs-per-game' \
                                                                                    + '?date=' \
                                                                                    + this_week_date_str
                pdofdpg_df = main_hist(passing_defense_opponent_passing_first_downs_per_game_url_current, season,
                                       str(week),
                                       this_week_date_str, 'passing_defense_opponent_passing_first_downs_per_game')
                pdofdpg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                           season: 'Current_Season_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                           'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                           'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                           'Home': 'At_Home_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                           'Away': 'Away_Passing_Defense_Opponent_Passing_First_Downs_per_Game'
                                           }, inplace=True)
                pdofdpg_df['Team'] = pdofdpg_df['Team'].str.strip()
                if season == '2010':
                    pdofdpg_df['Rank_Passing_Defense_Opponent_Passing_First_Downs_per_Game'] = pdofdpg_df.index + 1
                pdofdpg_df = pdofdpg_df.replace('--', np.nan)
                pdofdpg_df = pdofdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_average_team_passer_rating_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-average-team-passer-rating' \
                                                                                  + '?date=' \
                                                                                  + this_week_date_str
                pdoatpr_df = main_hist(passing_defense_opponent_average_team_passer_rating_url_current, season,
                                       str(week),
                                       this_week_date_str, 'passing_defense_opponent_average_team_passer_rating')
                pdoatpr_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                           season: 'Current_Season_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                           str(int(
                                               season) - 1): 'Previous_Season_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                           'Last 3': 'Last_3_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                           'Last 1': 'Last_1_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                           'Home': 'At_Home_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                           'Away': 'Away_Passing_Defense_Opponent_Average_Team_Passer_Rating'
                                           }, inplace=True)
                pdoatpr_df['Team'] = pdoatpr_df['Team'].str.strip()
                if season == '2010':
                    pdoatpr_df['Rank_Passing_Defense_Opponent_Average_Team_Passer_Rating'] = pdoatpr_df.index + 1
                pdoatpr_df = pdoatpr_df.replace('--', np.nan)
                pdoatpr_df = pdoatpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_team_sack_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/sack-pct' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
                pdtsp_df = main_hist(passing_defense_team_sack_percentage_url_current, season,
                                     str(week),
                                     this_week_date_str, 'passing_defense_team_sack_percentage')
                pdtsp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Team_Sack_Percentage',
                                         season: 'Current_Season_Passing_Defense_Team_Sack_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Defense_Team_Sack_Percentage',
                                         'Last 3': 'Last_3_Passing_Defense_Team_Sack_Percentage',
                                         'Last 1': 'Last_1_Passing_Defense_Team_Sack_Percentage',
                                         'Home': 'At_Home_Passing_Defense_Team_Sack_Percentage',
                                         'Away': 'Away_Passing_Defense_Team_Sack_Percentage'
                                         }, inplace=True)
                pdtsp_df['Team'] = pdtsp_df['Team'].str.strip()
                if season == '2010':
                    pdtsp_df['Rank_Passing_Defense_Team_Sack_Percentage'] = pdtsp_df.index + 1
                pdtsp_df = pdtsp_df.replace('--', np.nan)
                for c in pdtsp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        pdtsp_df[c] = pdtsp_df[c].str.rstrip('%').astype('float') / 100.0
                pdtsp_df = pdtsp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_passing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-play-pct' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
                pdoppp_df = main_hist(passing_defense_opponent_passing_play_percentage_url_current, season,
                                      str(week),
                                      this_week_date_str, 'passing_defense_opponent_passing_play_percentage')
                pdoppp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_Play_Percentage',
                                          season: 'Current_Season_Passing_Defense_Opponent_Passing_Play_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_Play_Percentage',
                                          'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_Play_Percentage',
                                          'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_Play_Percentage',
                                          'Home': 'At_Home_Passing_Defense_Opponent_Passing_Play_Percentage',
                                          'Away': 'Away_Passing_Defense_Opponent_Passing_Play_Percentage'
                                          }, inplace=True)
                pdoppp_df['Team'] = pdoppp_df['Team'].str.strip()
                if season == '2010':
                    pdoppp_df['Rank_Passing_Defense_Opponent_Passing_Play_Percentage'] = pdoppp_df.index + 1
                pdoppp_df = pdoppp_df.replace('--', np.nan)
                for c in pdoppp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        pdoppp_df[c] = pdoppp_df[c].str.rstrip('%').astype('float') / 100.0
                pdoppp_df = pdoppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_passing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-yards-pct' \
                                                                                + '?date=' \
                                                                                + this_week_date_str
                pdopyp_df = main_hist(passing_defense_opponent_passing_yards_percentage_url_current, season,
                                      str(week),
                                      this_week_date_str, 'passing_defense_opponent_passing_yards_percentage')
                pdopyp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                          season: 'Current_Season_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                          'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                          'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                          'Home': 'At_Home_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                          'Away': 'Away_Passing_Defense_Opponent_Passing_Yards_Percentage'
                                          }, inplace=True)
                pdopyp_df['Team'] = pdopyp_df['Team'].str.strip()
                if season == '2010':
                    pdopyp_df['Rank_Passing_Defense_Opponent_Passing_Yards_Percentage'] = pdopyp_df.index + 1
                pdopyp_df = pdopyp_df.replace('--', np.nan)
                for c in pdopyp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        pdopyp_df[c] = pdopyp_df[c].str.rstrip('%').astype('float') / 100.0
                pdopyp_df = pdopyp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_sacks_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/sacks-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
                pdspg_df = main_hist(passing_defense_sacks_per_game_url_current, season,
                                     str(week),
                                     this_week_date_str, 'passing_defense_sacks_per_game')
                pdspg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Sacks_per_Game',
                                         season: 'Current_Season_Passing_Defense_Sacks_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Defense_Sacks_per_Game',
                                         'Last 3': 'Last_3_Passing_Defense_Sacks_per_Game',
                                         'Last 1': 'Last_1_Passing_Defense_Sacks_per_Game',
                                         'Home': 'At_Home_Passing_Defense_Sacks_per_Game',
                                         'Away': 'Away_Passing_Defense_Sacks_per_Game'
                                         }, inplace=True)
                pdspg_df['Team'] = pdspg_df['Team'].str.strip()
                if season == '2010':
                    pdspg_df[
                        'Rank_Passing_Defense_Sacks_per_Game'] = pdspg_df.index + 1
                pdspg_df = pdspg_df.replace('--', np.nan)
                pdspg_df = pdspg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_yards_per_pass_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-pass-attempt' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                pdoyppa_df = main_hist(passing_defense_opponent_yards_per_pass_attempt_url_current, season,
                                       str(week),
                                       this_week_date_str, 'passing_defense_opponent_yards_per_pass_attempt')
                pdoyppa_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                           season: 'Current_Season_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                           str(int(
                                               season) - 1): 'Previous_Season_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                           'Last 3': 'Last_3_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                           'Last 1': 'Last_1_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                           'Home': 'At_Home_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                           'Away': 'Away_Passing_Defense_Opponent_Yards_per_Pass_Attempt'
                                           }, inplace=True)
                pdoyppa_df['Team'] = pdoyppa_df['Team'].str.strip()
                if season == '2010':
                    pdoyppa_df[
                        'Rank_Passing_Defense_Opponent_Yards_per_Pass_Attempt'] = pdoyppa_df.index + 1
                pdoyppa_df = pdoyppa_df.replace('--', np.nan)
                pdoyppa_df = pdoyppa_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                passing_defense_opponent_yards_per_completion_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-completion' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
                pdoypc_df = main_hist(passing_defense_opponent_yards_per_completion_url_current, season,
                                      str(week),
                                      this_week_date_str, 'passing_defense_opponent_yards_per_completion')
                pdoypc_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Yards_per_Completion',
                                          season: 'Current_Season_Passing_Defense_Opponent_Yards_per_Completion',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Defense_Opponent_Yards_per_Completion',
                                          'Last 3': 'Last_3_Passing_Defense_Opponent_Yards_per_Completion',
                                          'Last 1': 'Last_1_Passing_Defense_Opponent_Yards_per_Completion',
                                          'Home': 'At_Home_Passing_Defense_Opponent_Yards_per_Completion',
                                          'Away': 'Away_Passing_Defense_Opponent_Yards_per_Completion'
                                          }, inplace=True)
                pdoypc_df['Team'] = pdoypc_df['Team'].str.strip()
                if season == '2010':
                    pdoypc_df[
                        'Rank_Passing_Defense_Opponent_Yards_per_Completion'] = pdoypc_df.index + 1
                pdoypc_df = pdoypc_df.replace('--', np.nan)
                pdoypc_df = pdoypc_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_defense_opponent_field_goal_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-field-goal-attempts-per-game' \
                                                                                          + '?date=' \
                                                                                          + this_week_date_str
                stdofgapg_df = main_hist(special_teams_defense_opponent_field_goal_attempts_per_game_url_current,
                                         season,
                                         str(week),
                                         this_week_date_str,
                                         'special_teams_defense_opponent_field_goal_attempts_per_game')
                stdofgapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                             season: 'Current_Season_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                             str(int(
                                                 season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                             'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                             'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                             'Home': 'At_Home_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                             'Away': 'Away_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game'
                                             }, inplace=True)
                stdofgapg_df['Team'] = stdofgapg_df['Team'].str.strip()
                if season == '2010':
                    stdofgapg_df[
                        'Rank_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game'] = stdofgapg_df.index + 1
                stdofgapg_df = stdofgapg_df.replace('--', np.nan)
                stdofgapg_df = stdofgapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_defense_opponent_field_goals_made_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-field-goals-made-per-game' \
                                                                                       + '?date=' \
                                                                                       + this_week_date_str
                stdofgmpg_df = main_hist(special_teams_defense_opponent_field_goals_made_per_game_url_current,
                                         season,
                                         str(week),
                                         this_week_date_str,
                                         'special_teams_defense_opponent_field_goals_made_per_game')
                stdofgmpg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                             season: 'Current_Season_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                             str(int(
                                                 season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                             'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                             'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                             'Home': 'At_Home_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                             'Away': 'Away_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game'
                                             }, inplace=True)
                stdofgmpg_df['Team'] = stdofgmpg_df['Team'].str.strip()
                if season == '2010':
                    stdofgmpg_df[
                        'Rank_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game'] = stdofgmpg_df.index + 1
                stdofgmpg_df = stdofgmpg_df.replace('--', np.nan)
                stdofgmpg_df = stdofgmpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_defense_opponent_punt_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-punt-attempts-per-game' \
                                                                                    + '?date=' \
                                                                                    + this_week_date_str
                stdopapg_df = main_hist(special_teams_defense_opponent_punt_attempts_per_game_url_current, season,
                                        str(week),
                                        this_week_date_str,
                                        'special_teams_defense_opponent_punt_attempts_per_game')
                stdopapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                            season: 'Current_Season_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                            'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                            'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                            'Home': 'At_Home_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                            'Away': 'Away_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game'
                                            }, inplace=True)
                stdopapg_df['Team'] = stdopapg_df['Team'].str.strip()
                if season == '2010':
                    stdopapg_df[
                        'Rank_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game'] = stdopapg_df.index + 1
                stdopapg_df = stdopapg_df.replace('--', np.nan)
                stdopapg_df = stdopapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                special_teams_defense_opponent_gross_punt_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-gross-punt-yards-per-game' \
                                                                                       + '?date=' \
                                                                                       + this_week_date_str
                stdogpypg_df = main_hist(special_teams_defense_opponent_gross_punt_yards_per_game_url_current,
                                         season,
                                         str(week),
                                         this_week_date_str,
                                         'special_teams_defense_opponent_gross_punt_yards_per_game')
                stdogpypg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                             season: 'Current_Season_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                             str(int(
                                                 season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                             'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                             'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                             'Home': 'At_Home_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                             'Away': 'Away_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game'
                                             }, inplace=True)
                stdogpypg_df['Team'] = stdogpypg_df['Team'].str.strip()
                if season == '2010':
                    stdogpypg_df[
                        'Rank_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game'] = stdogpypg_df.index + 1
                stdogpypg_df = stdogpypg_df.replace('--', np.nan)
                stdogpypg_df = stdogpypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_interceptions_thrown_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/interceptions-thrown-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
                titpg_df = main_hist(turnovers_interceptions_thrown_per_game_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_interceptions_thrown_per_game')
                titpg_df.rename(columns={'Rank': 'Rank_Turnovers_Interceptions_Thrown_per_Game',
                                         season: 'Current_Season_Turnovers_Interceptions_Thrown_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Turnovers_Interceptions_Thrown_per_Game',
                                         'Last 3': 'Last_3_Turnovers_Interceptions_Thrown_per_Game',
                                         'Last 1': 'Last_1_Turnovers_Interceptions_Thrown_per_Game',
                                         'Home': 'At_Home_Turnovers_Interceptions_Thrown_per_Game',
                                         'Away': 'Away_Turnovers_Interceptions_Thrown_per_Game'
                                         }, inplace=True)
                titpg_df['Team'] = titpg_df['Team'].str.strip()
                if season == '2010':
                    titpg_df[
                        'Rank_Turnovers_Interceptions_Thrown_per_Game'] = titpg_df.index + 1
                titpg_df = titpg_df.replace('--', np.nan)
                titpg_df = titpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_fumbles_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fumbles-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
                tfpg_df = main_hist(turnovers_fumbles_per_game_url_current, season, str(week),
                                    this_week_date_str,
                                    'turnovers_fumbles_per_game')
                tfpg_df.rename(columns={'Rank': 'Rank_Turnovers_Fumbles_per_Game',
                                        season: 'Current_Season_Turnovers_Fumbles_per_Game',
                                        str(int(
                                            season) - 1): 'Previous_Season_Turnovers_Fumbles_per_Game',
                                        'Last 3': 'Last_3_Turnovers_Fumbles_per_Game',
                                        'Last 1': 'Last_1_Turnovers_Fumbles_per_Game',
                                        'Home': 'At_Home_Turnovers_Fumbles_per_Game',
                                        'Away': 'Away_Turnovers_Fumbles_per_Game'
                                        }, inplace=True)
                tfpg_df['Team'] = tfpg_df['Team'].str.strip()
                if season == '2010':
                    tfpg_df[
                        'Rank_Turnovers_Fumbles_per_Game'] = tfpg_df.index + 1
                tfpg_df = tfpg_df.replace('--', np.nan)
                tfpg_df = tfpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_fumbles_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fumbles-lost-per-game' \
                                                              + '?date=' \
                                                              + this_week_date_str
                tflpg_df = main_hist(turnovers_fumbles_lost_per_game_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_fumbles_lost_per_game')
                tflpg_df.rename(columns={'Rank': 'Rank_Turnovers_Fumbles_Lost_per_Game',
                                         season: 'Current_Season_Turnovers_Fumbles_Lost_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Turnovers_Fumbles_Lost_per_Game',
                                         'Last 3': 'Last_3_Turnovers_Fumbles_Lost_per_Game',
                                         'Last 1': 'Last_1_Turnovers_Fumbles_Lost_per_Game',
                                         'Home': 'At_Home_Turnovers_Fumbles_Lost_per_Game',
                                         'Away': 'Away_Turnovers_Fumbles_Lost_per_Game'
                                         }, inplace=True)
                tflpg_df['Team'] = tflpg_df['Team'].str.strip()
                if season == '2010':
                    tflpg_df[
                        'Rank_Turnovers_Fumbles_Lost_per_Game'] = tflpg_df.index + 1
                tflpg_df = tflpg_df.replace('--', np.nan)
                tflpg_df = tflpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_fumbles_not_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fumbles-not-lost-per-game' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
                tfnlpg_df = main_hist(turnovers_fumbles_not_lost_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'turnovers_fumbles_not_lost_per_game')
                tfnlpg_df.rename(columns={'Rank': 'Rank_Turnovers_Fumbles_Not_Lost_per_Game',
                                          season: 'Current_Season_Turnovers_Fumbles_Not_Lost_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Turnovers_Fumbles_Not_Lost_per_Game',
                                          'Last 3': 'Last_3_Turnovers_Fumbles_Not_Lost_per_Game',
                                          'Last 1': 'Last_1_Turnovers_Fumbles_Not_Lost_per_Game',
                                          'Home': 'At_Home_Turnovers_Fumbles_Not_Lost_per_Game',
                                          'Away': 'Away_Turnovers_Fumbles_Not_Lost_per_Game'
                                          }, inplace=True)
                tfnlpg_df['Team'] = tfnlpg_df['Team'].str.strip()
                if season == '2010':
                    tfnlpg_df[
                        'Rank_Turnovers_Fumbles_Not_Lost_per_Game'] = tfnlpg_df.index + 1
                tfnlpg_df = tfnlpg_df.replace('--', np.nan)
                tfnlpg_df = tfnlpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_giveaways_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/giveaways-per-game' \
                                                           + '?date=' \
                                                           + this_week_date_str
                tgpg_df = main_hist(turnovers_giveaways_per_game_url_current, season, str(week), this_week_date_str,
                                    'turnovers_giveaways_per_game')
                tgpg_df.rename(columns={'Rank': 'Rank_Turnovers_Giveaways_per_Game',
                                        season: 'Current_Season_Turnovers_Giveaways_per_Game',
                                        str(int(season) - 1): 'Previous_Season_Turnovers_Giveaways_per_Game',
                                        'Last 3': 'Last_3_Turnovers_Giveaways_per_Game',
                                        'Last 1': 'Last_1_Turnovers_Giveaways_per_Game',
                                        'Home': 'At_Home_Turnovers_Giveaways_per_Game',
                                        'Away': 'Away_Turnovers_Giveaways_per_Game'
                                        }, inplace=True)
                tgpg_df['Team'] = tgpg_df['Team'].str.strip()
                if season == '2010':
                    tgpg_df['Rank_Turnovers_Giveaways_per_Game'] = tgpg_df.index + 1
                tgpg_df = tgpg_df.replace('--', np.nan)
                tgpg_df = tgpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_turnover_margin_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/turnover-margin-per-game' \
                                                                 + '?date=' \
                                                                 + this_week_date_str
                ttmpg_df = main_hist(turnovers_turnover_margin_per_game_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_turnover_margin_per_game')
                ttmpg_df.rename(columns={'Rank': 'Rank_Turnovers_Turnover_Margin_per_Game',
                                         season: 'Current_Season_Turnovers_Turnover_Margin_per_Game',
                                         str(int(season) - 1): 'Previous_Season_Turnovers_Turnover_Margin_per_Game',
                                         'Last 3': 'Last_3_Turnovers_Turnover_Margin_per_Game',
                                         'Last 1': 'Last_1_Turnovers_Turnover_Margin_per_Game',
                                         'Home': 'At_Home_Turnovers_Turnover_Margin_per_Game',
                                         'Away': 'Away_Turnovers_Turnover_Margin_per_Game'
                                         }, inplace=True)
                ttmpg_df['Team'] = ttmpg_df['Team'].str.strip()
                if season == '2010':
                    ttmpg_df['Rank_Turnovers_Turnover_Margin_per_Game'] = ttmpg_df.index + 1
                ttmpg_df = ttmpg_df.replace('--', np.nan)
                ttmpg_df = ttmpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_interceptions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/interceptions-per-game' \
                                                               + '?date=' \
                                                               + this_week_date_str
                tipg_df = main_hist(turnovers_interceptions_per_game_url_current, season, str(week),
                                    this_week_date_str,
                                    'turnovers_interceptions_per_game')
                tipg_df.rename(columns={'Rank': 'Rank_Turnovers_Interceptions_per_Game',
                                        season: 'Current_Season_Turnovers_Interceptions_per_Game',
                                        str(int(season) - 1): 'Previous_Season_Turnovers_Interceptions_per_Game',
                                        'Last 3': 'Last_3_Turnovers_Interceptions_per_Game',
                                        'Last 1': 'Last_1_Turnovers_Interceptions_per_Game',
                                        'Home': 'At_Home_Turnovers_Interceptions_per_Game',
                                        'Away': 'Away_Turnovers_Interceptions_per_Game'
                                        }, inplace=True)
                tipg_df['Team'] = tipg_df['Team'].str.strip()
                if season == '2010':
                    tipg_df['Rank_Turnovers_Interceptions_per_Game'] = tipg_df.index + 1
                tipg_df = tipg_df.replace('--', np.nan)
                tipg_df = tipg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_fumbles_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumbles-per-game' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
                tofpg_df = main_hist(turnovers_opponent_fumbles_per_game_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_opponent_fumbles_per_game')
                tofpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumbles_per_Game',
                                         season: 'Current_Season_Turnovers_Opponent_Fumbles_per_Game',
                                         str(int(season) - 1): 'Previous_Season_Turnovers_Opponent_Fumbles_per_Game',
                                         'Last 3': 'Last_3_Turnovers_Opponent_Fumbles_per_Game',
                                         'Last 1': 'Last_1_Turnovers_Opponent_Fumbles_per_Game',
                                         'Home': 'At_Home_Turnovers_Opponent_Fumbles_per_Game',
                                         'Away': 'Away_Turnovers_Opponent_Fumbles_per_Game'
                                         }, inplace=True)
                tofpg_df['Team'] = tofpg_df['Team'].str.strip()
                if season == '2010':
                    tofpg_df['Rank_Turnovers_Opponent_Fumbles_per_Game'] = tofpg_df.index + 1
                tofpg_df = tofpg_df.replace('--', np.nan)
                tofpg_df = tofpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_fumbles_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumbles-lost-per-game' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
                toflpg_df = main_hist(turnovers_opponent_fumbles_lost_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'turnovers_opponent_fumbles_lost_per_game')
                toflpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                          season: 'Current_Season_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                          'Last 3': 'Last_3_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                          'Last 1': 'Last_1_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                          'Home': 'At_Home_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                          'Away': 'Away_Turnovers_Opponent_Fumbles_Lost_per_Game'
                                          }, inplace=True)
                toflpg_df['Team'] = toflpg_df['Team'].str.strip()
                if season == '2010':
                    toflpg_df['Rank_Turnovers_Opponent_Fumbles_Lost_per_Game'] = toflpg_df.index + 1
                toflpg_df = toflpg_df.replace('--', np.nan)
                toflpg_df = toflpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_fumbles_not_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumbles-not-lost-per-game' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
                tofnlpg_df = main_hist(turnovers_opponent_fumbles_not_lost_per_game_url_current, season, str(week),
                                       this_week_date_str,
                                       'turnovers_opponent_fumbles_not_lost_per_game')
                tofnlpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                           season: 'Current_Season_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                           'Last 3': 'Last_3_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                           'Last 1': 'Last_1_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                           'Home': 'At_Home_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                           'Away': 'Away_Turnovers_Opponent_Fumbles_Not_Lost_per_Game'
                                           }, inplace=True)
                tofnlpg_df['Team'] = tofnlpg_df['Team'].str.strip()
                if season == '2010':
                    tofnlpg_df['Rank_Turnovers_Opponent_Fumbles_Not_Lost_per_Game'] = tofnlpg_df.index + 1
                tofnlpg_df = tofnlpg_df.replace('--', np.nan)
                tofnlpg_df = tofnlpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_takeaways_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/takeaways-per-game' \
                                                           + '?date=' \
                                                           + this_week_date_str
                tttapg_df = main_hist(turnovers_takeaways_per_game_url_current, season, str(week), this_week_date_str,
                                      'turnovers_takeaways_per_game')
                tttapg_df.rename(columns={'Rank': 'Rank_Turnovers_Takeaways_per_Game',
                                          season: 'Current_Season_Turnovers_Takeaways_per_Game',
                                          str(int(season) - 1): 'Previous_Season_Turnovers_Takeaways_per_Game',
                                          'Last 3': 'Last_3_Turnovers_Takeaways_per_Game',
                                          'Last 1': 'Last_1_Turnovers_Takeaways_per_Game',
                                          'Home': 'At_Home_Turnovers_Takeaways_per_Game',
                                          'Away': 'Away_Turnovers_Takeaways_per_Game'
                                          }, inplace=True)
                tttapg_df['Team'] = tttapg_df['Team'].str.strip()
                if season == '2010':
                    tttapg_df['Rank_Turnovers_Takeaways_per_Game'] = tttapg_df.index + 1
                tttapg_df = tttapg_df.replace('--', np.nan)
                tttapg_df = tttapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_turnover_margin_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-turnover-margin-per-game' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
                totmpg_df = main_hist(turnovers_opponent_turnover_margin_per_game_url_current, season, str(week),
                                      this_week_date_str,
                                      'turnovers_opponent_turnover_margin_per_game')
                totmpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Turnover_Margin_per_Game',
                                          season: 'Current_Season_Turnovers_Opponent_Turnover_Margin_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Turnovers_Opponent_Turnover_Margin_per_Game',
                                          'Last 3': 'Last_3_Turnovers_Opponent_Turnover_Margin_per_Game',
                                          'Last 1': 'Last_1_Turnovers_Opponent_Turnover_Margin_per_Game',
                                          'Home': 'At_Home_Turnovers_Opponent_Turnover_Margin_per_Game',
                                          'Away': 'Away_Turnovers_Turnover_Margin_per_Game'
                                          }, inplace=True)
                totmpg_df['Team'] = totmpg_df['Team'].str.strip()
                if season == '2010':
                    totmpg_df['Rank_Turnovers_Opponent_Turnover_Margin_per_Game'] = totmpg_df.index + 1
                totmpg_df = totmpg_df.replace('--', np.nan)
                totmpg_df = totmpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_interceptions_thrown_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/pass-intercepted-pct' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
                titp_df = main_hist(turnovers_interceptions_thrown_percentage_url_current, season, str(week),
                                    this_week_date_str,
                                    'turnovers_interceptions_thrown_percentage')
                titp_df.rename(columns={'Rank': 'Rank_Turnovers_Interceptions_Thrown_Percentage',
                                        season: 'Current_Season_Turnovers_Interceptions_Thrown_Percentage',
                                        str(int(
                                            season) - 1): 'Previous_Season_Turnovers_Interceptions_Thrown_Percentage',
                                        'Last 3': 'Last_3_Turnovers_Interceptions_Thrown_Percentage',
                                        'Last 1': 'Last_1_Turnovers_Interceptions_Thrown_Percentage',
                                        'Home': 'At_Home_Turnovers_Interceptions_Thrown_Percentage',
                                        'Away': 'Away_Turnovers_Interceptions_Thrown_Percentage'
                                        }, inplace=True)
                titp_df['Team'] = titp_df['Team'].str.strip()
                if season == '2010':
                    titp_df['Rank_Turnovers_Interceptions_Thrown_Percentage'] = titp_df.index + 1
                titp_df = titp_df.replace('--', np.nan)
                for c in titp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        titp_df[c] = titp_df[c].str.rstrip('%').astype('float') / 100.0
                titp_df = titp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/fumble-recovery-pct' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
                tfrp_df = main_hist(turnovers_fumble_recovery_percentage_url_current, season, str(week),
                                    this_week_date_str,
                                    'turnovers_fumble_recovery_percentage')
                tfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Fumble_Recovery_Percentage',
                                        season: 'Current_Season_Turnovers_Fumble_Recovery_Percentage',
                                        str(int(
                                            season) - 1): 'Previous_Season_Turnovers_Fumble_Recovery_Percentage',
                                        'Last 3': 'Last_3_Turnovers_Fumble_Recovery_Percentage',
                                        'Last 1': 'Last_1_Turnovers_Fumble_Recovery_Percentage',
                                        'Home': 'At_Home_Turnovers_Fumble_Recovery_Percentage',
                                        'Away': 'Away_Turnovers_Fumble_Recovery_Percentage'
                                        }, inplace=True)
                tfrp_df['Team'] = tfrp_df['Team'].str.strip()
                if season == '2010':
                    tfrp_df['Rank_Turnovers_Fumble_Recovery_Percentage'] = tfrp_df.index + 1
                tfrp_df = tfrp_df.replace('--', np.nan)
                for c in tfrp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        tfrp_df[c] = tfrp_df[c].str.rstrip('%').astype('float') / 100.0
                tfrp_df = tfrp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_giveaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/giveaway-fumble-recovery-pct' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
                tgfrp_df = main_hist(turnovers_giveaway_fumble_recovery_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_giveaway_fumble_recovery_percentage')
                tgfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                         season: 'Current_Season_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                         'Last 3': 'Last_3_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                         'Last 1': 'Last_1_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                         'Home': 'At_Home_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                         'Away': 'Away_Turnovers_Giveaway_Fumble_Recovery_Percentage'
                                         }, inplace=True)
                tgfrp_df['Team'] = tgfrp_df['Team'].str.strip()
                if season == '2010':
                    tgfrp_df['Rank_Turnovers_Giveaway_Fumble_Recovery_Percentage'] = tgfrp_df.index + 1
                tgfrp_df = tgfrp_df.replace('--', np.nan)
                for c in tgfrp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        tgfrp_df[c] = tgfrp_df[c].str.rstrip('%').astype('float') / 100.0
                tgfrp_df = tgfrp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_takeaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/takeaway-fumble-recovery-pct' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
                ttfrp_df = main_hist(turnovers_takeaway_fumble_recovery_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_takeaway_fumble_recovery_percentage')
                ttfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                         season: 'Current_Season_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                         'Last 3': 'Last_3_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                         'Last 1': 'Last_1_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                         'Home': 'At_Home_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                         'Away': 'Away_Turnovers_Takeaway_Fumble_Recovery_Percentage'
                                         }, inplace=True)
                ttfrp_df['Team'] = ttfrp_df['Team'].str.strip()
                if season == '2010':
                    ttfrp_df['Rank_Turnovers_Takeaway_Fumble_Recovery_Percentage'] = ttfrp_df.index + 1
                ttfrp_df = ttfrp_df.replace('--', np.nan)
                for c in ttfrp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        ttfrp_df[c] = ttfrp_df[c].str.rstrip('%').astype('float') / 100.0
                ttfrp_df = ttfrp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_interceptions_thrown_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/interception-pct' \
                                                                                 + '?date=' \
                                                                                 + this_week_date_str
                toitp_df = main_hist(turnovers_opponent_interceptions_thrown_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_opponent_interceptions_thrown_percentage')
                toitp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Interceptions_Thrown_Percentage',
                                         season: 'Current_Season_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                         'Last 3': 'Last_3_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                         'Last 1': 'Last_1_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                         'Home': 'At_Home_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                         'Away': 'Away_Turnovers_Opponents_Interceptions_Thrown_Percentage'
                                         }, inplace=True)
                toitp_df['Team'] = toitp_df['Team'].str.strip()
                if season == '2010':
                    toitp_df['Rank_Turnovers_Opponent_Interceptions_Thrown_Percentage'] = toitp_df.index + 1
                toitp_df = toitp_df.replace('--', np.nan)
                for c in toitp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        toitp_df[c] = toitp_df[c].str.rstrip('%').astype('float') / 100.0
                toitp_df = toitp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumble-recovery-pct' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
                tofrp_df = main_hist(turnovers_opponent_fumble_recovery_percentage_url_current, season, str(week),
                                     this_week_date_str,
                                     'turnovers_opponent_fumble_recovery_percentage')
                tofrp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                         season: 'Current_Season_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                         'Last 3': 'Last_3_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                         'Last 1': 'Last_1_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                         'Home': 'At_Home_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                         'Away': 'Away_Turnovers_Opponent_Fumble_Recovery_Percentage'
                                         }, inplace=True)
                tofrp_df['Team'] = tofrp_df['Team'].str.strip()
                if season == '2010':
                    tofrp_df['Rank_Turnovers_Opponent_Fumble_Recovery_Percentage'] = tofrp_df.index + 1
                tofrp_df = tofrp_df.replace('--', np.nan)
                for c in tofrp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        tofrp_df[c] = tofrp_df[c].str.rstrip('%').astype('float') / 100.0
                tofrp_df = tofrp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_giveaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-giveaway-fumble-recovery-pct' \
                                                                                     + '?date=' \
                                                                                     + this_week_date_str
                togfrp_df = main_hist(turnovers_opponent_giveaway_fumble_recovery_percentage_url_current, season,
                                      str(week),
                                      this_week_date_str,
                                      'turnovers_opponent_giveaway_fumble_recovery_percentage')
                togfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                          season: 'Current_Season_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                          'Last 3': 'Last_3_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                          'Last 1': 'Last_1_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                          'Home': 'At_Home_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                          'Away': 'Away_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage'
                                          }, inplace=True)
                togfrp_df['Team'] = togfrp_df['Team'].str.strip()
                if season == '2010':
                    togfrp_df['Rank_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage'] = togfrp_df.index + 1
                togfrp_df = togfrp_df.replace('--', np.nan)
                for c in togfrp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        togfrp_df[c] = togfrp_df[c].str.rstrip('%').astype('float') / 100.0
                togfrp_df = togfrp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                turnovers_opponent_takeaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-takeaway-fumble-recovery-pct' \
                                                                                     + '?date=' \
                                                                                     + this_week_date_str
                totfrp_df = main_hist(turnovers_opponent_takeaway_fumble_recovery_percentage_url_current, season,
                                      str(week),
                                      this_week_date_str,
                                      'turnovers_opponent_takeaway_fumble_recovery_percentage')
                totfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                          season: 'Current_Season_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                          'Last 3': 'Last_3_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                          'Last 1': 'Last_1_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                          'Home': 'At_Home_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                          'Away': 'Away_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage'
                                          }, inplace=True)
                totfrp_df['Team'] = totfrp_df['Team'].str.strip()
                if season == '2010':
                    totfrp_df['Rank_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage'] = totfrp_df.index + 1
                totfrp_df = totfrp_df.replace('--', np.nan)
                for c in totfrp_df:
                    if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                        totfrp_df[c] = totfrp_df[c].str.rstrip('%').astype('float') / 100.0
                totfrp_df = totfrp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_penalties_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/penalties-per-game' \
                                                           + '?date=' \
                                                           + this_week_date_str
                pppg_df = main_hist(penalties_penalties_per_game_url_current, season,
                                    str(week),
                                    this_week_date_str,
                                    'penalties_penalties_per_game')
                pppg_df.rename(columns={'Rank': 'Rank_Penalties_Penalties_per_Game',
                                        season: 'Current_Season_Penalties_Penalties_per_Game',
                                        str(int(
                                            season) - 1): 'Previous_Season_Penalties_per_Game',
                                        'Last 3': 'Last_3_Penalties_Penalties_per_Game',
                                        'Last 1': 'Last_1_Penalties_Penalties_per_Game',
                                        'Home': 'At_Home_Penalties_Penalties_per_Game',
                                        'Away': 'Away_Turnovers_Penalties_Penalties_per_Game'
                                        }, inplace=True)
                pppg_df['Team'] = pppg_df['Team'].str.strip()
                if season == '2010':
                    pppg_df['Rank_Penalties_Penalties_per_Game'] = pppg_df.index + 1
                pppg_df = pppg_df.replace('--', np.nan)
                pppg_df = pppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_penalty_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/penalty-yards-per-game' \
                                                               + '?date=' \
                                                               + this_week_date_str
                ppypg_df = main_hist(penalties_penalty_yards_per_game_url_current, season,
                                     str(week),
                                     this_week_date_str,
                                     'penalties_penalty_yards_per_game')
                ppypg_df.rename(columns={'Rank': 'Rank_Penalties_Penalty_Yards_per_Game',
                                         season: 'Current_Season_Penalties_Penalty_Yards_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Penalty_Yards_per_Game',
                                         'Last 3': 'Last_3_Penalties_Penalty_Yards_per_Game',
                                         'Last 1': 'Last_1_Penalties_Penalty_Yards_per_Game',
                                         'Home': 'At_Home_Penalties_Penalty_Yards_per_Game',
                                         'Away': 'Away_Turnovers_Penalties_Penalty_Yards_per_Game'
                                         }, inplace=True)
                ppypg_df['Team'] = ppypg_df['Team'].str.strip()
                if season == '2010':
                    ppypg_df['Rank_Penalties_Penalty_Yards_per_Game'] = ppypg_df.index + 1
                ppypg_df = ppypg_df.replace('--', np.nan)
                ppypg_df = ppypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_penalty_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/penalty-first-downs-per-game' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
                ppfdpg_df = main_hist(penalties_penalty_first_downs_per_game_url_current, season,
                                      str(week),
                                      this_week_date_str,
                                      'penalties_penalty_first_downs_per_game')
                ppfdpg_df.rename(columns={'Rank': 'Rank_Penalties_Penalty_First_Downs_per_Game',
                                          season: 'Current_Season_Penalties_Penalty_First_Downs_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Penalty_First_Downs_per_Game',
                                          'Last 3': 'Last_3_Penalties_Penalty_First_Downs_per_Game',
                                          'Last 1': 'Last_1_Penalties_Penalty_First_Downs_per_Game',
                                          'Home': 'At_Home_Penalties_Penalty_First_Downs_per_Game',
                                          'Away': 'Away_Turnovers_Penalties_Penalty_First_Downs_per_Game'
                                          }, inplace=True)
                ppfdpg_df['Team'] = ppfdpg_df['Team'].str.strip()
                if season == '2010':
                    ppfdpg_df['Rank_Penalties_Penalty_First_Downs_per_Game'] = ppfdpg_df.index + 1
                ppfdpg_df = ppfdpg_df.replace('--', np.nan)
                ppfdpg_df = ppfdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_opponent_penalties_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalties-per-game' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                poppg_df = main_hist(penalties_opponent_penalties_per_game_url_current, season,
                                     str(week),
                                     this_week_date_str,
                                     'penalties_opponent_penalties_per_game')
                poppg_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalties_per_Game',
                                         season: 'Current_Season_Penalties_Opponent_Penalties_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Penalties_Opponent_Penalties_per_Game',
                                         'Last 3': 'Last_3_Penalties_Opponent_Penalties_per_Game',
                                         'Last 1': 'Last_1_Penalties_Opponent_Penalties_per_Game',
                                         'Home': 'At_Home_Penalties_Opponent_Penalties_per_Game',
                                         'Away': 'Away_Turnovers_Penalties_Opponent_Penalties_per_Game'
                                         }, inplace=True)
                poppg_df['Team'] = poppg_df['Team'].str.strip()
                if season == '2010':
                    poppg_df['Rank_Penalties_Opponent_Penalties_per_Game'] = poppg_df.index + 1
                poppg_df = poppg_df.replace('--', np.nan)
                poppg_df = poppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_opponent_penalty_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalty-yards-per-game' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
                popypg_df = main_hist(penalties_opponent_penalty_yards_per_game_url_current, season,
                                      str(week),
                                      this_week_date_str,
                                      'penalties_opponent_penalty_yards_per_game')
                popypg_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalty_Yards_per_Game',
                                          season: 'Current_Season_Penalties_Opponent_Penalty_Yards_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Penalties_Opponent_Penalty_Yards_per_Game',
                                          'Last 3': 'Last_3_Penalties_Opponent_Penalty_Yards_per_Game',
                                          'Last 1': 'Last_1_Penalties_Opponent_Penalty_Yards_per_Game',
                                          'Home': 'At_Home_Penalties_Opponent_Penalty_Yards_per_Game',
                                          'Away': 'Away_Turnovers_Penalties_Opponent_Penalty_Yards_per_Game'
                                          }, inplace=True)
                popypg_df['Team'] = popypg_df['Team'].str.strip()
                if season == '2010':
                    popypg_df['Rank_Penalties_Opponent_Penalty_Yards_per_Game'] = popypg_df.index + 1
                popypg_df = popypg_df.replace('--', np.nan)
                popypg_df = popypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_opponent_penalty_first_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalty-first-downs-per-game' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
                popfdpg_df = main_hist(penalties_opponent_penalty_first_down_per_game_url_current, season,
                                       str(week),
                                       this_week_date_str,
                                       'penalties_opponent_penalty_first_downs_per_game')
                popfdpg_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                           season: 'Current_Season_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                           'Last 3': 'Last_3_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                           'Last 1': 'Last_1_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                           'Home': 'At_Home_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                           'Away': 'Away_Turnovers_Penalties_Opponent_Penalty_First_Downs_per_Game'
                                           }, inplace=True)
                popfdpg_df['Team'] = popfdpg_df['Team'].str.strip()
                if season == '2010':
                    popfdpg_df['Rank_Penalties_Opponent_Penalty_First_Downs_per_Game'] = popfdpg_df.index + 1
                popfdpg_df = popfdpg_df.replace('--', np.nan)
                popfdpg_df = popfdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_penalty_yards_per_penalty_url_current = 'https://www.teamrankings.com/college-football/stat/penalty-yards-per-penalty' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
                poptpp_df = main_hist(penalties_penalty_yards_per_penalty_url_current, season,
                                      str(week),
                                      this_week_date_str,
                                      'penalties_penalty_yards_per_penalty')
                poptpp_df.rename(columns={'Rank': 'Rank_Penalties_Penalty_Yards_per_Penalty',
                                          season: 'Current_Season_Penalties_Penalty_Yards_per_Penalty',
                                          str(int(
                                              season) - 1): 'Previous_Season_Penalties_Penalty_Yards_per_Penalty',
                                          'Last 3': 'Last_3_Penalties_Penalty_Yards_per_Penalty',
                                          'Last 1': 'Last_1_Penalties_Penalty_Yards_per_Penalty',
                                          'Home': 'At_Home_Penalties_Penalty_Yards_per_Penalty',
                                          'Away': 'Away_Turnovers_Penalties_Penalty_Yards_per_Penalty'
                                          }, inplace=True)
                poptpp_df['Team'] = poptpp_df['Team'].str.strip()
                if season == '2010':
                    poptpp_df['Rank_Penalties_Penalty_Yards_per_Penalty'] = poptpp_df.index + 1
                poptpp_df = poptpp_df.replace('--', np.nan)
                poptpp_df = poptpp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_penalties_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/penalties-per-play' \
                                                           + '?date=' \
                                                           + this_week_date_str
                pppp_df = main_hist(penalties_penalties_per_play_url_current, season,
                                    str(week),
                                    this_week_date_str,
                                    'penalties_penalties_per_play')
                pppp_df.rename(columns={'Rank': 'Rank_Penalties_Penalties_per_Play',
                                        season: 'Current_Season_Penalties_Penalties_per_Play',
                                        str(int(
                                            season) - 1): 'Previous_Season_Penalties_Penalties_per_Play',
                                        'Last 3': 'Last_3_Penalties_Penalties_per_Play',
                                        'Last 1': 'Last_1_Penalties_Penalties_per_Play',
                                        'Home': 'At_Home_Penalties_Penalties_per_Play',
                                        'Away': 'Away_Turnovers_Penalties_Penalties_per_Play'
                                        }, inplace=True)
                pppp_df['Team'] = pppp_df['Team'].str.strip()
                if season == '2010':
                    pppp_df['Rank_Penalties_Penalties_per_Play'] = pppp_df.index + 1
                pppp_df = pppp_df.replace('--', np.nan)
                pppp_df = pppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_opponent_penalty_yards_per_penalty_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalty-yards-per-penalty' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
                poppypp_df = main_hist(penalties_opponent_penalty_yards_per_penalty_url_current, season,
                                       str(week),
                                       this_week_date_str,
                                       'penalties_opponent_penalty_yards_per_penalty')
                poppypp_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                           season: 'Current_Season_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                           str(int(
                                               season) - 1): 'Previous_Season_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                           'Last 3': 'Last_3_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                           'Last 1': 'Last_1_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                           'Home': 'At_Home_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                           'Away': 'Away_Turnovers_Penalties_Opponent_Penalty_Yards_per_Penalty'
                                           }, inplace=True)
                poppypp_df['Team'] = poppypp_df['Team'].str.strip()
                if season == '2010':
                    poppypp_df['Rank_Penalties_Opponent_Penalty_Yards_per_Penalty'] = poppypp_df.index + 1
                poppypp_df = poppypp_df.replace('--', np.nan)
                poppypp_df = poppypp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                penalties_opponent_penalties_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalties-per-play' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                popppp_df = main_hist(penalties_opponent_penalty_yards_per_penalty_url_current, season,
                                      str(week),
                                      this_week_date_str,
                                      'penalties_opponent_penalties_per_play')
                popppp_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalties_per_Play',
                                          season: 'Current_Season_Penalties_Opponent_Penalties_per_Play',
                                          str(int(
                                              season) - 1): 'Previous_Season_Penalties_Opponent_Penalties_per_Play',
                                          'Last 3': 'Last_3_Penalties_Opponent_Penalties_per_Play',
                                          'Last 1': 'Last_1_Penalties_Opponent_Penalties_per_Play',
                                          'Home': 'At_Home_Penalties_Opponent_Penalties_per_Play',
                                          'Away': 'Away_Turnovers_Penalties_Opponent_Penalties_per_Play'
                                          }, inplace=True)
                popppp_df['Team'] = popppp_df['Team'].str.strip()
                if season == '2010':
                    popppp_df['Rank_Penalties_Opponent_Penalties_per_Play'] = popppp_df.index + 1
                popppp_df = popppp_df.replace('--', np.nan)
                popppp_df = popppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                predictive_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/predictive-by-other' \
                                                       + '?date=' \
                                                       + this_week_date_str
                ppr_df = main_hist(predictive_power_ranking_url_current, season, str(week), this_week_date_str,
                                   'predictive_power_ranking')
                ppr_df.rename(columns={'Rank': 'Rank_Predictive_Power_Ranking',
                                       'Rating': 'Rating_Predictive_Power_Ranking',
                                       'Hi': 'Hi_Predictive_Power_Ranking',
                                       'Low': 'Low_Predictive_Power_Ranking',
                                       'Last': 'Last_Predictive_Power_Ranking'
                                       }, inplace=True)
                ppr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                ppr_df['Team'] = ppr_df['Team'].str.strip()
                if season == '2010':
                    ppr_df['Rank_Predictive_Power_Ranking'] = ppr_df.index + 1
                ppr_df = ppr_df.replace('--', np.nan)
                ppr_df = ppr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                home_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/home-by-other' \
                                                 + '?date=' \
                                                 + this_week_date_str
                hpr_df = main_hist(home_power_ranking_url_current, season, str(week), this_week_date_str,
                                   'home_power_ranking')
                hpr_df.rename(columns={'Rank': 'Rank_Home_Power_Ranking',
                                       'Rating': 'Rating_Home_Power_Ranking',
                                       'Hi': 'Hi_Home_Power_Ranking',
                                       'Low': 'Low_Home_Power_Ranking',
                                       'Last': 'Last_Home_Power_Ranking'
                                       }, inplace=True)
                hpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                hpr_df['Team'] = hpr_df['Team'].str.strip()
                if season == '2010':
                    hpr_df['Rank_Home_Power_Ranking'] = hpr_df.index + 1
                hpr_df = hpr_df.replace('--', np.nan)
                hpr_df = hpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                away_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/away-by-other' \
                                                 + '?date=' \
                                                 + this_week_date_str
                apr_df = main_hist(away_power_ranking_url_current, season, str(week), this_week_date_str,
                                   'away_power_ranking')
                apr_df.rename(columns={'Rank': 'Rank_Away_Power_Ranking',
                                       'Rating': 'Rating_Away_Power_Ranking',
                                       'Hi': 'Hi_Away_Power_Ranking',
                                       'Low': 'Low_Away_Power_Ranking',
                                       'Last': 'Last_Away_Power_Ranking'
                                       }, inplace=True)
                apr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                apr_df['Team'] = apr_df['Team'].str.strip()
                if season == '2010':
                    apr_df['Rank_Away_Power_Ranking'] = apr_df.index + 1
                apr_df = apr_df.replace('--', np.nan)
                apr_df = apr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                neutral_site_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/neutral-by-other' \
                                                         + '?date=' \
                                                         + this_week_date_str
                nspr_df = main_hist(neutral_site_power_ranking_url_current, season, str(week), this_week_date_str,
                                    'neutral_site_power_ranking')
                nspr_df.rename(columns={'Rank': 'Rank_Neutral_Site_Power_Ranking',
                                        'Rating': 'Rating_Neutral_Site_Power_Ranking',
                                        'Hi': 'Hi_Neutral_Site_Power_Ranking',
                                        'Low': 'Low_Neutral_Site_Power_Ranking',
                                        'Last': 'Last_Neutral_Site_Power_Ranking'
                                        }, inplace=True)
                nspr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                nspr_df['Team'] = nspr_df['Team'].str.strip()
                if season == '2010':
                    nspr_df['Rank_Neutral_Site_Power_Ranking'] = nspr_df.index + 1
                nspr_df = nspr_df.replace('--', np.nan)
                nspr_df = nspr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                home_advantage_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/home-adv-by-other' \
                                                           + '?date=' \
                                                           + this_week_date_str
                hapr_df = main_hist(home_advantage_power_ranking_url_current, season, str(week), this_week_date_str,
                                    'home_advantage_power_ranking')
                hapr_df.rename(columns={'Rank': 'Rank_Home_Advantage_Power_Ranking',
                                        'Rating': 'Rating_Home_Advantage_Power_Ranking',
                                        'Hi': 'Hi_Home_Advantage_Power_Ranking',
                                        'Low': 'Low_Home_Advantage_Power_Ranking',
                                        'Last': 'Last_Home_Advantage_Power_Ranking'
                                        }, inplace=True)
                hapr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                hapr_df['Team'] = hapr_df['Team'].str.strip()
                if season == '2010':
                    hapr_df['Rank_Home_Advantage_Power_Ranking'] = hapr_df.index + 1
                hapr_df = hapr_df.replace('--', np.nan)
                hapr_df = hapr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/schedule-strength-by-other' \
                                                                 + '?date=' \
                                                                 + this_week_date_str
                sospr_df = main_hist(strength_of_schedule_power_ranking_url_current, season, str(week),
                                     this_week_date_str, 'strength_of_schedule_power_ranking')
                sospr_df.rename(columns={'Rank': 'Rank_Strength_of_Schedule_Power_Ranking',
                                         'Rating': 'Rating_Strength_of_Schedule_Power_Ranking',
                                         'Hi': 'Hi_Strength_of_Schedule_Power_Ranking',
                                         'Low': 'Low_Strength_of_Schedule_Power_Ranking',
                                         'Last': 'Last_Strength_of_Schedule_Power_Ranking'
                                         }, inplace=True)
                # sospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                sospr_df['Team'] = sospr_df['Team'].str.strip()
                if season == '2010':
                    sospr_df['Rank_Strength_of_Schedule_Power_Ranking'] = sospr_df.index + 1
                sospr_df = sospr_df.replace('--', np.nan)
                sospr_df = sospr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                future_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/future-sos-by-other' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
                fsospr_df = main_hist(future_strength_of_schedule_power_ranking_url_current, season, str(week),
                                      this_week_date_str, 'future_strength_of_schedule_power_ranking')
                fsospr_df.rename(columns={'Rank': 'Rank_Future_Strength_of_Schedule_Power_Ranking',
                                          'Rating': 'Rating_Future_Strength_of_Schedule_Power_Ranking',
                                          'Hi': 'Hi_Future_Strength_of_Schedule_Power_Ranking',
                                          'Low': 'Low_Future_Strength_of_Schedule_Power_Ranking',
                                          'Last': 'Last_Future_Strength_of_Schedule_Power_Ranking'
                                          }, inplace=True)
                # fsospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                fsospr_df['Team'] = fsospr_df['Team'].str.strip()
                if season == '2010':
                    fsospr_df['Rank_Future_Strength_of_Schedule_Power_Ranking'] = fsospr_df.index + 1
                fsospr_df = fsospr_df.replace('--', np.nan)
                fsospr_df = fsospr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                season_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/season-sos-by-other' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
                ssospr_df = main_hist(season_strength_of_schedule_power_ranking_url_current, season, str(week),
                                      this_week_date_str, 'season_strength_of_schedule_power_ranking')
                ssospr_df.rename(columns={'Rank': 'Rank_Season_Strength_of_Schedule_Power_Ranking',
                                          'Rating': 'Rating_Season_Strength_of_Schedule_Power_Ranking',
                                          'Hi': 'Hi_Season_Strength_of_Schedule_Power_Ranking',
                                          'Low': 'Low_Season_Strength_of_Schedule_Power_Ranking',
                                          'Last': 'Last_Season_Strength_of_Schedule_Power_Ranking'
                                          }, inplace=True)
                # ssospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                ssospr_df['Team'] = ssospr_df['Team'].str.strip()
                if season == '2010':
                    ssospr_df['Rank_Season_Strength_of_Schedule_Power_Ranking'] = ssospr_df.index + 1
                ssospr_df = ssospr_df.replace('--', np.nan)
                ssospr_df = ssospr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                strength_of_schedule_power_ranking_basic_method_url_current = 'https://www.teamrankings.com/college-football/ranking/sos-basic-by-other' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
                sosprbm_df = main_hist(strength_of_schedule_power_ranking_basic_method_url_current, season, str(week),
                                       this_week_date_str, 'strength_of_schedule_power_ranking_basic_method')
                sosprbm_df.rename(columns={'Rank': 'Rank_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                           'Rating': 'Rating_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                           'Hi': 'Hi_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                           'Low': 'Low_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                           'Last': 'Last_Strength_of_Schedule_Power_Ranking_Basic_Method'
                                           }, inplace=True)
                # sosprbm_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                sosprbm_df['Team'] = sosprbm_df['Team'].str.strip()
                if season == '2010':
                    sosprbm_df['Rank_Strength_of_Schedule_Power_Ranking_Basic_Method'] = sosprbm_df.index + 1
                sosprbm_df = sosprbm_df.replace('--', np.nan)
                sosprbm_df = sosprbm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                in_conference_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/in-conference-sos-by-other' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
                icsospr_df = main_hist(in_conference_strength_of_schedule_power_ranking_url_current, season, str(week),
                                       this_week_date_str, 'in_conference_strength_of_schedule_power_ranking')
                icsospr_df.rename(columns={'Rank': 'Rank_In_Conference_Strength_of_Schedule_Power_Ranking',
                                           'Rating': 'Rating_In_Conference_Strength_of_Schedule_Power_Ranking',
                                           'Hi': 'Hi_In_Conference_Strength_of_Schedule_Power_Ranking',
                                           'Low': 'Low_In_Conference_Strength_of_Schedule_Power_Ranking',
                                           'Last': 'Last_In-Conference_Strength_of_Schedule_Power_Ranking'
                                           }, inplace=True)
                # icsospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                icsospr_df['Team'] = icsospr_df['Team'].str.strip()
                if season == '2010':
                    icsospr_df['Rank_In_Conference_Strength_of_Schedule_Power_Ranking'] = icsospr_df.index + 1
                icsospr_df = icsospr_df.replace('--', np.nan)
                icsospr_df = icsospr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                nonconference_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/non-conference-sos-by-other' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
                ncsospr_df = main_hist(nonconference_strength_of_schedule_power_ranking_url_current, season, str(week),
                                       this_week_date_str, 'nonconference_strength_of_schedule_power_ranking')
                ncsospr_df.rename(columns={'Rank': 'Rank_Nonconference_Strength_of_Schedule_Power_Ranking',
                                           'Rating': 'Rating_Nonconference_Strength_of_Schedule_Power_Ranking',
                                           'Hi': 'Hi_Nonconference_Strength_of_Schedule_Power_Ranking',
                                           'Low': 'Low_Nonconference_Strength_of_Schedule_Power_Ranking',
                                           'Last': 'Last_Nonconference_Strength_of_Schedule_Power_Ranking'
                                           }, inplace=True)
                # ncsospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                ncsospr_df['Team'] = ncsospr_df['Team'].str.strip()
                if season == '2010':
                    ncsospr_df['Rank_Nonconference_Strength_of_Schedule_Power_Ranking'] = ncsospr_df.index + 1
                ncsospr_df = ncsospr_df.replace('--', np.nan)
                ncsospr_df = ncsospr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                last_10_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/last-10-games-by-other' \
                                                    + '?date=' \
                                                    + this_week_date_str
                l10_df = main_hist(last_10_power_ranking_url_current, season, str(week),
                                   this_week_date_str, 'last_10_power_ranking')
                l10_df.rename(columns={'Rank': 'Rank_Last_10_Power_Ranking',
                                       'Rating': 'Rating_Last_10_Power_Ranking',
                                       'Hi': 'Hi_Last_10_Power_Ranking',
                                       'Low': 'Low_Last_10_Power_Ranking',
                                       'Last': 'Last_Last_10_Power_Ranking'
                                       }, inplace=True)
                # l10_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                l10_df['Team'] = l10_df['Team'].str.strip()
                if season == '2010':
                    l10_df['Rank_Last_10_Power_Ranking'] = l10_df.index + 1
                l10_df = l10_df.replace('--', np.nan)
                l10_df = l10_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                last_5_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/last-5-games-by-other' \
                                                   + '?date=' \
                                                   + this_week_date_str
                l5_df = main_hist(last_5_power_ranking_url_current, season, str(week),
                                  this_week_date_str, 'last_5_power_ranking')
                l5_df.rename(columns={'Rank': 'Rank_Last_5_Power_Ranking',
                                      'Rating': 'Rating_Last_5_Power_Ranking',
                                      'Hi': 'Hi_Last_5_Power_Ranking',
                                      'Low': 'Low_Last_5_Power_Ranking',
                                      'Last': 'Last_Last_5_Power_Ranking'
                                      }, inplace=True)
                # l5_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                l5_df['Team'] = l5_df['Team'].str.strip()
                if season == '2010':
                    l5_df['Rank_Last_5_Power_Ranking'] = l5_df.index + 1
                l5_df = l5_df.replace('--', np.nan)
                l5_df = l5_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                in_conference_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/in-conference-by-other' \
                                                          + '?date=' \
                                                          + this_week_date_str
                icpr_df = main_hist(in_conference_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'in_conference_power_ranking')
                icpr_df.rename(columns={'Rank': 'In_Conference_Power_Ranking',
                                        'Rating': 'Rating_In-Conference_Power_Ranking',
                                        'Hi': 'Hi_In-Conference_Power_Ranking',
                                        'Low': 'Low_In_Conference_Power_Ranking',
                                        'Last': 'Last_In-Conference_Power_Ranking'
                                        }, inplace=True)
                # icpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                icpr_df['Team'] = icpr_df['Team'].str.strip()
                if season == '2010':
                    icpr_df['In_Conference_Power_Ranking'] = icpr_df.index + 1
                icpr_df = icpr_df.replace('--', np.nan)
                icpr_df = icpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                nonconference_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/non-conference-by-other' \
                                                          + '?date=' \
                                                          + this_week_date_str
                ncpr_df = main_hist(nonconference_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'nononference_power_ranking')
                ncpr_df.rename(columns={'Rank': 'Nonconference_Power_Ranking',
                                        'Rating': 'Rating_Nonconference_Power_Ranking',
                                        'Hi': 'Hi_Nonconference_Power_Ranking',
                                        'Low': 'Low_Nonconference_Power_Ranking',
                                        'Last': 'Last_Nonconference_Power_Ranking'
                                        }, inplace=True)
                # ncpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                ncpr_df['Team'] = ncpr_df['Team'].str.strip()
                if season == '2010':
                    ncpr_df['Nonconference_Power_Ranking'] = ncpr_df.index + 1
                ncpr_df = ncpr_df.replace('--', np.nan)
                ncpr_df = ncpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                luck_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/luck-by-other' \
                                                 + '?date=' \
                                                 + this_week_date_str
                lpr_df = main_hist(luck_power_ranking_url_current, season, str(week),
                                   this_week_date_str, 'luck_power_ranking')
                lpr_df.rename(columns={'Rank': 'Luck_Power_Ranking',
                                       'Rating': 'Rating_Luck_Power_Ranking',
                                       'Hi': 'Hi_Luck_Power_Ranking',
                                       'Low': 'Low_Luck_Power_Ranking',
                                       'Last': 'Last_Luck_Power_Ranking'
                                       }, inplace=True)
                lpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                lpr_df['Team'] = lpr_df['Team'].str.strip()
                if season == '2010':
                    lpr_df['Luck_Power_Ranking'] = lpr_df.index + 1
                lpr_df = lpr_df.replace('--', np.nan)
                lpr_df = lpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                consistency_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/consistency-by-other' \
                                                        + '?date=' \
                                                        + this_week_date_str
                cpr_df = main_hist(consistency_power_ranking_url_current, season, str(week),
                                   this_week_date_str, 'consistency_power_ranking')
                cpr_df.rename(columns={'Rank': 'Consistency_Power_Ranking',
                                       'Rating': 'Rating_Consistency_Power_Ranking',
                                       'Hi': 'Hi_Consistency_Power_Ranking',
                                       'Low': 'Low_Consistency_Power_Ranking',
                                       'Last': 'Last_Consistency_Power_Ranking'
                                       }, inplace=True)
                cpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                cpr_df['Team'] = cpr_df['Team'].str.strip()
                if season == '2010':
                    cpr_df['Consistency_Power_Ranking'] = cpr_df.index + 1
                cpr_df = cpr_df.replace('--', np.nan)
                cpr_df = cpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                versus_teams_1_thru_10_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-1-10-by-other' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
                vt1_df = main_hist(versus_teams_1_thru_10_power_ranking_url_current, season, str(week),
                                   this_week_date_str, 'versus_teams_1_thru_10_power_ranking')
                vt1_df.rename(columns={'Rank': 'Versus_Teams_1_Thru_10_Power_Ranking',
                                       'Rating': 'Rating_Versus_Teams_1_Thru_10_Power_Ranking',
                                       'Hi': 'Hi_Versus_Teams_1_Thru_10_Power_Ranking',
                                       'Low': 'Low_Versus_Teams_1_Thru_10_Power_Ranking',
                                       'Last': 'Last_Versus_Teams_1_Thru_10_Power_Ranking'
                                       }, inplace=True)
                # vt1_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                vt1_df['Team'] = vt1_df['Team'].str.strip()
                if season == '2010':
                    vt1_df['Versus_Teams_1_Thru_10_Power_Ranking'] = vt1_df.index + 1
                vt1_df = vt1_df.replace('--', np.nan)
                vt1_df = vt1_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                versus_teams_11_thru_25_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-11-25-by-other' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                vt11_df = main_hist(versus_teams_11_thru_25_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'versus_teams_11_thru_25_power_ranking')
                vt11_df.rename(columns={'Rank': 'Versus_Teams_11_Thru_25_Power_Ranking',
                                        'Rating': 'Rating_Versus_Teams_11_Thru_25_Power_Ranking',
                                        'Hi': 'Hi_Versus_Teams_11_Thru_25_Power_Ranking',
                                        'Low': 'Low_Versus_Teams_11_Thru_25_Power_Ranking',
                                        'Last': 'Last_Versus_Teams_11_Thru_25_Power_Ranking'
                                        }, inplace=True)
                # vt11_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                vt11_df['Team'] = vt11_df['Team'].str.strip()
                if season == '2010':
                    vt11_df['Versus_Teams_11_Thru_25_Power_Ranking'] = vt11_df.index + 1
                vt11_df = vt11_df.replace('--', np.nan)
                vt11_df = vt11_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                versus_teams_26_thru_40_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-26-40-by-other' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                vt26_df = main_hist(versus_teams_26_thru_40_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'versus_teams_26_thru_40_power_ranking')
                vt26_df.rename(columns={'Rank': 'Versus_Teams_26_Thru_40_Power_Ranking',
                                        'Rating': 'Rating_Versus_Teams_26_Thru_40_Power_Ranking',
                                        'Hi': 'Hi_Versus_Teams_26_Thru_40_Power_Ranking',
                                        'Low': 'Low_Versus_Teams_26_Thru_40_Power_Ranking',
                                        'Last': 'Last_Versus_Teams_26_Thru_40_Power_Ranking'
                                        }, inplace=True)
                # vt26_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                vt26_df['Team'] = vt26_df['Team'].str.strip()
                if season == '2010':
                    vt26_df['Versus_Teams_26_Thru_40_Power_Ranking'] = vt26_df.index + 1
                vt26_df = vt26_df.replace('--', np.nan)
                vt26_df = vt26_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                versus_teams_41_thru_75_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-41-75-by-other' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
                vt41_df = main_hist(versus_teams_41_thru_75_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'versus_teams_41_thru_75_power_ranking')
                vt41_df.rename(columns={'Rank': 'Versus_Teams_41_Thru_75_Power_Ranking',
                                        'Rating': 'Rating_Versus_Teams_41_Thru_75_Power_Ranking',
                                        'Hi': 'Hi_Versus_Teams_41_Thru_75_Power_Ranking',
                                        'Low': 'Low_Versus_Teams_41_Thru_75_Power_Ranking',
                                        'Last': 'Last_Versus_Teams_41_Thru_75_Power_Ranking'
                                        }, inplace=True)
                # vt41_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                vt41_df['Team'] = vt41_df['Team'].str.strip()
                if season == '2010':
                    vt41_df['Versus_Teams_41_Thru_75_Power_Ranking'] = vt41_df.index + 1
                vt41_df = vt41_df.replace('--', np.nan)
                vt41_df = vt41_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                versus_teams_76_thru_120_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-76-120-by-other' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
                vt76_df = main_hist(versus_teams_76_thru_120_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'versus_teams_76_thru_120_power_ranking')
                vt76_df.rename(columns={'Rank': 'Versus_Teams_76_Thru_120_Power_Ranking',
                                        'Rating': 'Rating_Versus_Teams_76_Thru_120_Power_Ranking',
                                        'Hi': 'Hi_Versus_Teams_76_Thru_120_Power_Ranking',
                                        'Low': 'Low_Versus_Teams_76_Thru_120_Power_Ranking',
                                        'Last': 'Last_Versus_Teams_76_Thru_120_Power_Ranking'
                                        }, inplace=True)
                # vt76_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                vt76_df['Team'] = vt76_df['Team'].str.strip()
                if season == '2010':
                    vt76_df['Versus_Teams_76_Thru_120_Power_Ranking'] = vt76_df.index + 1
                vt76_df = vt76_df.replace('--', np.nan)
                vt76_df = vt76_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                first_half_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/first-half-by-other' \
                                                       + '?date=' \
                                                       + this_week_date_str
                fhpr_df = main_hist(first_half_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'first_half_power_ranking')
                fhpr_df.rename(columns={'Rank': 'First_Half_Power_Ranking',
                                        'Rating': 'Rating_First_Half_Power_Ranking',
                                        'Hi': 'Hi_First_Half_Power_Ranking',
                                        'Low': 'Low_First_Half_Power_Ranking',
                                        'Last': 'Last_First_Half_Power_Ranking'
                                        }, inplace=True)
                # fhpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                fhpr_df['Team'] = fhpr_df['Team'].str.strip()
                if season == '2010':
                    fhpr_df['First_Half_Power_Ranking'] = fhpr_df.index + 1
                fhpr_df = fhpr_df.replace('--', np.nan)
                fhpr_df = fhpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                second_half_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/second-half-by-other' \
                                                        + '?date=' \
                                                        + this_week_date_str
                shpr_df = main_hist(second_half_power_ranking_url_current, season, str(week),
                                    this_week_date_str, 'second_half_power_ranking')
                shpr_df.rename(columns={'Rank': 'Second_Half_Power_Ranking',
                                        'Rating': 'Rating_Second_Half_Power_Ranking',
                                        'Hi': 'Hi_Second_Half_Power_Ranking',
                                        'Low': 'Low_Second_Half_Power_Ranking',
                                        'Last': 'Last_Second_Half_Power_Ranking'
                                        }, inplace=True)
                # shpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                shpr_df['Team'] = shpr_df['Team'].str.strip()
                if season == '2010':
                    shpr_df['Second_Half_Power_Ranking'] = shpr_df.index + 1
                shpr_df = shpr_df.replace('--', np.nan)
                shpr_df = shpr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(random.uniform(0.2, 2))

                this_week_date = this_week_date + datetime.timedelta(days=7)
                this_week_date_str = this_week_date.strftime("%Y-%m-%d")

                this_week_df = pd.merge(so_df, tasm_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, typp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, typm_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, typ_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, typpm_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, trs_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, trsp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, trspp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tot_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, top_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tops_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fiq_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sq_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tq_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fq_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ot_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fh_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sh_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fiqtp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sqtp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tqtp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fqtp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fhtp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, shtp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toppg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toypp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, totdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, totdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tofdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tofdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toatp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toatpp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, totdcp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tofdcp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toppp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toppos_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rorapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rorypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rorypra_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rorpp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, roryp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, popapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pocpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poipg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pocp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, popypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poqspg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poqsp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poapr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ppoppp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, popyp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poyppa_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poypc_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stofgapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stofgmpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stofgcp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stopapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stogpypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdoppg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdoypp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdoppp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdoasm_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdorzsapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdorzspg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdorzsp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdoppfga_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdootpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sdooppg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdoypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdoppg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdofdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdotdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdotdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdofodpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdofdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdoatop_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdptopp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdotdcp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdofdcp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdoppp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tdoppos_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rdorapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rdorypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rdorfdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rdoypra_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rdorpp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, rdoryp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdopapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdocpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdoipg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdocp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdopypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdofdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdoatpr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdtsp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdoppp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdopyp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdspg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdoyppa_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pdoypc_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stdofgapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stdofgmpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stdopapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, stdogpypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, titpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tfpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tflpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tfnlpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tgpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ttmpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tipg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tofpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toflpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tofnlpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tttapg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, totmpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, titp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tfrp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tgfrp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ttfrp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, toitp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tofrp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tofrp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, totfrp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pppg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ppypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ppfdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poppg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, popypg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, popfdpg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poptpp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, pppp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, poppypp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, popppp_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ppr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, hpr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, apr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, nspr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, hapr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sospr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fsospr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ssospr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sosprbm_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, icsospr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ncsospr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, l10_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, l5_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, icpr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ncpr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, lpr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, cpr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, vt1_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, vt11_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, vt26_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, vt41_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, vt76_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, fhpr_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, shpr_df, on=['Team', 'Season', 'Week'], how='outer')

                this_week_df = rearrange_columns(this_week_df)
                season_df = pd.concat([season_df, this_week_df])
                master_df = pd.concat([master_df, this_week_df])

                time.sleep(3)

            # save_dir = 'c:\Users\chris\PycharmProjects\CFBWebScrape\scraped_data'
            save_file = 'Scraped_TR_Data_Combined_' + season
            try:
                datascraper.save_df(season_df, save_dir, save_file)
                print('{} saved successfully.'.format(save_file))
                print('File successfully saved at {}.'.format(save_dir))
            except:
                print('I don\'t think the file saved, you should double check.')

        # save_dir = 'C:\users\chris\PycharmProjects\CFBWebScrape\scraped_data'
        save_file = 'Scraped_TR_Data_Combined_ALL'
        try:
            datascraper.save_df(master_df, save_dir, save_file)
            print('{} saved successfully.'.format(save_file))
            print('File successfully saved at {}.'.format(save_dir))
        except:
            print('I don\'t think the file saved, you should double check.')

    else:

        master_df = pd.DataFrame()
        season_df = pd.DataFrame()

        season = '2021'
        week = '2'
        week_num = int(week) - 5

        if (season == '2021'):
            season_start_date = datetime.date(int(season), 9, 6)
        elif (season == '2022'):
            season_start_date = datetime.date(int(season), 9, 27)

        """
        # Now that we know the season_start_date for the current season, we can set the start date for the 
        # variable that will keep track of the current week (this_week_date) that we are scraping data for. 
        # Since we will start at week 5, we can set this_week_date to the season_start_date which is the 
        # Monday before week 5 games.
        """
        this_week_date = season_start_date + datetime.timedelta(days=7 * week_num)
        # this_week_date = season_start_date

        # The next line will convert the date variable to a string so that we can use it in filenames
        season_start_date_str = season_start_date.strftime("%Y-%m-%d")
        this_week_date_str = this_week_date.strftime("%Y-%m-%d")

        scoring_offense_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-game' \
                                      + '?date=' \
                                      + this_week_date_str
        so_df = main_hist(scoring_offense_url_current, season, str(week), this_week_date_str, 'scoring_offense')
        so_df.rename(columns={'Rank': 'Rank_Scoring_Offense',
                              season: 'Current_Season_Scoring_Offense',
                              str(int(season) - 1): 'Previous_Season_Scoring_Offense',
                              'Last 3': 'Last_3_Scoring_Offense',
                              'Last 1': 'Last_1_Scoring_Offense',
                              'Home': 'At_Home_Scoring_Offense',
                              'Away': 'Away_Scoring_Offense'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        if season == '2010':
            so_df['Rank_Scoring_Offense'] = so_df.index + 1
        so_df = so_df.replace('--', np.nan)
        so_df = so_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_average_scoring_margin_url_current = 'https://www.teamrankings.com/college-football/stat/average-scoring-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        tasm_df = main_hist(team_average_scoring_margin_url_current, season, str(week), this_week_date_str,
                            'team_average_scoring_margin')
        tasm_df.rename(columns={'Rank': 'Rank_Team_Average_Scoring_Margin',
                                season: 'Current_Season_Team_Scoring_Average_Margin',
                                str(int(season) - 1): 'Previous_Season_Team_Average_Scoring_Margin',
                                'Last 3': 'Last_3_Team_Average_Scoring_Margin',
                                'Last 1': 'Last_1_Team_Average_Scoring_Margin',
                                'Home': 'At_Home_Team_Average_Scoring_Margin',
                                'Away': 'Away_Team_Average_Scoring_Margin'
                                }, inplace=True)
        tasm_df['Team'] = tasm_df['Team'].str.strip()
        if season == '2010':
            tasm_df['Rank_Team_Average_Scoring_Margin'] = tasm_df.index + 1
        tasm_df = tasm_df.replace('--', np.nan)
        tasm_df = tasm_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point' \
                                           + '?date=' \
                                           + this_week_date_str
        typp_df = main_hist(team_yards_per_point_url_current, season, str(week), this_week_date_str,
                            'team_yards_per_point')
        typp_df.rename(columns={'Rank': 'Rank_Team_Yards_per_Point',
                                season: 'Current_Season_Team_Yards_per_Point',
                                str(int(season) - 1): 'Previous_Season_Team_yards_per_Point',
                                'Last 3': 'Last_3_Team_Yards_per_Point',
                                'Last 1': 'Last_1_Team_Yards_per_Point',
                                'Home': 'At_Home_Team_Yards_per_Point',
                                'Away': 'Away_Team_Yards_per_Point'
                                }, inplace=True)
        typp_df['Team'] = typp_df['Team'].str.strip()
        if season == '2010':
            typp_df['Rank_Team_Yards_per_Point'] = typp_df.index + 1
        typp_df = typp_df.replace('--', np.nan)
        typp_df = typp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_yards_per_point_margin_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        typm_df = main_hist(team_yards_per_point_margin_url_current, season, str(week), this_week_date_str,
                            'team_yards_per_point_margin')
        typm_df.rename(columns={'Rank': 'Rank_Team_Yards_per_Point_Margin',
                                season: 'Current_Season_Team_Yards_per_Point_Margin',
                                str(int(season) - 1): 'Previous_Season_Team_yards_per_Point_Margin',
                                'Last 3': 'Last_3_Team_Yards_per_Point_Margin',
                                'Last 1': 'Last_1_Team_Yards_per_Point_Margin',
                                'Home': 'At_Home_Team_Yards_per_Point_Margin',
                                'Away': 'Away_Team_Yards_per_Point_Margin'
                                }, inplace=True)
        typm_df['Team'] = typm_df['Team'].str.strip()
        if season == '2010':
            typm_df['Rank_Team_Yards_per_Point_Margin'] = typm_df.index + 1
        typm_df = typm_df.replace('--', np.nan)
        typm_df = typm_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_points_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play' \
                                           + '?date=' \
                                           + this_week_date_str
        typ_df = main_hist(team_points_per_play_url_current, season, str(week), this_week_date_str,
                           'team_points_per_play')
        typ_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play',
                               season: 'Current_Season_Team_Points_per_Play',
                               str(int(season) - 1): 'Previous_Season_Team_Points_per_Play',
                               'Last 3': 'Last_3_Team_Points_per_Play',
                               'Last 1': 'Last_1_Team_Points_per_Play',
                               'Home': 'At_Home_Team_Points_per_Play',
                               'Away': 'Away_Team_Points_per_Play'
                               }, inplace=True)
        typ_df['Team'] = typ_df['Team'].str.strip()
        if season == '2010':
            typ_df['Rank_Team_Points_per_Play'] = typ_df.index + 1
        typ_df = typ_df.replace('--', np.nan)
        typ_df = typ_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_points_per_play_margin_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        typpm_df = main_hist(team_points_per_play_margin_url_current, season, str(week), this_week_date_str,
                             'team_points_per_play_margin')
        typpm_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play_Margin',
                                 season: 'Current_Season_Team_Points_per_Play_Margin',
                                 str(int(season) - 1): 'Previous_Season_Team_Points_per_Play_Margin',
                                 'Last 3': 'Last_3_Team_Points_per_Play_Margin',
                                 'Last 1': 'Last_1_Team_Points_per_Play_Margin',
                                 'Home': 'At_Home_Team_Points_per_Play_Margin',
                                 'Away': 'Away_Team_Points_per_Play_Margin'
                                 }, inplace=True)
        typpm_df['Team'] = typpm_df['Team'].str.strip()
        if season == '2010':
            typpm_df['Rank_Team_Points_per_Play_Margin'] = typpm_df.index + 1
        typpm_df = typpm_df.replace('--', np.nan)
        typpm_df = typpm_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_red_zone_scoring_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-attempts-per-game' \
                                                              + '?date=' \
                                                              + this_week_date_str
        trs_df = main_hist(team_red_zone_scoring_attempts_per_game_url_current, season, str(week), this_week_date_str,
                           'team_red_zone_scoring_attempts_per_game')
        trs_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Attempts_per_Game',
                               season: 'Current_Season_Team_Red_Zone_Scoring_Attempts_per_Game',
                               str(int(season) - 1): 'Previous_Season_Team_Red_Zone_Scoring_Attempts_per_Game',
                               'Last 3': 'Last_3_Team_Red_Zone_Scoring_Attempts_per_Game',
                               'Last 1': 'Last_1_Team_Red_Zone_Scoring_Attempts_per_Game',
                               'Home': 'At_Home_Team_Red_Zone_Scoring_Attempts_per_Game',
                               'Away': 'Away_Team_Red_Zone_Scoring_Attempts_per_Game'
                               }, inplace=True)
        trs_df['Team'] = trs_df['Team'].str.strip()
        if season == '2010':
            trs_df['Rank_Team_Red_Zone_Scoring_Attempts_per_Game'] = trs_df.index + 1
        trs_df = trs_df.replace('--', np.nan)
        trs_df = trs_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_red_zone_scores_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scores-per-game' \
                                                    + '?date=' \
                                                    + this_week_date_str
        trsp_df = main_hist(team_red_zone_scores_per_game_url_current, season, str(week), this_week_date_str,
                            'team_red_zone_scores_per_game')
        trsp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scores_per_Game',
                                season: 'Current_Season_Team_Red_Zone_Scores_per_Game',
                                str(int(season) - 1): 'Previous_Season_Team_Red_Zone_Scores_per_Game',
                                'Last 3': 'Last_3_Team_Red_Zone_Scores_per_Game',
                                'Last 1': 'Last_1_Team_Red_Zone_Scores_per_Game',
                                'Home': 'At_Home_Team_Red_Zone_Scores_per_Game',
                                'Away': 'Away_Team_Red_Zone_Scores_per_Game'
                                }, inplace=True)
        trsp_df['Team'] = trsp_df['Team'].str.strip()
        if season == '2010':
            trsp_df['Rank_Team_Red_Zone_Scores_per_Game'] = trsp_df.index + 1
        trsp_df = trsp_df.replace('--', np.nan)
        trsp_df = trsp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_red_zone_scoring_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-pct' \
                                                       + '?date=' \
                                                       + this_week_date_str
        trspp_df = main_hist(team_red_zone_scoring_percentage_url_current, season, str(week), this_week_date_str,
                             'team_red_zone_scoring_percentage')
        trspp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Percentage',
                                 season: 'Current_Season_Team_Red_Zone_Scoring_Percentage',
                                 str(int(season) - 1): 'Previous_Season_Team_Red_Zone_Scoring_Percentage',
                                 'Last 3': 'Last_3_Team_Red_Zone_Scoring_Percentage',
                                 'Last 1': 'Last_1_Team_Red_Zone_Scoring_Percentage',
                                 'Home': 'At_Home_Team_Red_Zone_Scoring_Percentage',
                                 'Away': 'Away_Team_Red_Zone_Scoring_Percentage'
                                 }, inplace=True)
        trspp_df['Team'] = trspp_df['Team'].str.strip()
        if season == '2010':
            trspp_df['Rank_Team_Red_Zone_Scoring_Percentage'] = trspp_df.index + 1
        trspp_df = trspp_df.replace('--', np.nan)
        for c in trspp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                trspp_df[c] = trspp_df[c].str.rstrip('%').astype('float') / 100.0
        trspp_df = trspp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_offensive_touchdowns_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-touchdowns-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        tot_df = main_hist(team_offensive_touchdowns_per_game_url_current, season, str(week), this_week_date_str,
                           'team_offensive_touchdowns_per_game')
        tot_df.rename(columns={'Rank': 'Rank_Team_Offensive_Touchdowns_per_Game',
                               season: 'Current_Season_Team_Offensive_Touchdowns_per_Game',
                               str(int(season) - 1): 'Previous_Season_Team_Offensive_Touchdowns_per_Game',
                               'Last 3': 'Last_3_Team_Offensive_Touchdowns_per_Game',
                               'Last 1': 'Last_1_Team_Offensive_Touchdowns_per_Game',
                               'Home': 'At_Home_Team_Offensive_Touchdowns_per_Game',
                               'Away': 'Away_Team_Offensive_Touchdowns_per_Game'
                               }, inplace=True)
        tot_df['Team'] = tot_df['Team'].str.strip()
        if season == '2010':
            tot_df['Rank_Team_Offensive_Touchdowns_per_Game'] = tot_df.index + 1
        tot_df = tot_df.replace('--', np.nan)
        tot_df = tot_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_offensive_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-points-per-game' \
                                                     + '?date=' \
                                                     + this_week_date_str
        top_df = main_hist(team_offensive_points_per_game_url_current, season, str(week), this_week_date_str,
                           'team_offensive_points_per_game')
        top_df.rename(columns={'Rank': 'Rank_Team_Offensive_Points_per_Game',
                               season: 'Current_Season_Team_Offensive_Points_per_Game',
                               str(int(season) - 1): 'Previous_Season_Team_Offensive_Points_per_Game',
                               'Last 3': 'Last_3_Team_Offensive_Points_per_Game',
                               'Last 1': 'Last_1_Team_Offensive_Points_per_Game',
                               'Home': 'At_Home_Team_Offensive_Points_per_Game',
                               'Away': 'Away_Team_Offensive_Points_per_Game'
                               }, inplace=True)
        top_df['Team'] = top_df['Team'].str.strip()
        if season == '2010':
            top_df['Rank_Team_Offensive_Points_per_Game'] = top_df.index + 1
        top_df = top_df.replace('--', np.nan)
        top_df = top_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_offensive_point_share_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-point-share-pct' \
                                                            + '?date=' \
                                                            + this_week_date_str
        tops_df = main_hist(team_offensive_point_share_percentage_url_current, season, str(week), this_week_date_str,
                            'team_offensive_point_share_percentage')
        tops_df.rename(columns={'Rank': 'Rank_Team_Offensive_Point_Share_Percentage',
                                season: 'Current_Season_Team_Offensive_Point_Share_Percentage',
                                str(int(season) - 1): 'Previous_Season_Team_Offensive_Point_Share_Percentage',
                                'Last 3': 'Last_3_Team_Offensive_Point_Share_Percentage',
                                'Last 1': 'Last_1_Team_Offensive_Point_Share_Percentage',
                                'Home': 'At_Home_Team_Offensive_Point_Share_Percentage',
                                'Away': 'Away_Team_Offensive_Point_Share_Percentage'
                                }, inplace=True)
        tops_df['Team'] = tops_df['Team'].str.strip()
        if season == '2010':
            tops_df['Rank_Team_Offensive_Point_Share_Percentage'] = tops_df.index + 1
        tops_df = tops_df.replace('--', np.nan)
        for c in tops_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                tops_df[c] = tops_df[c].str.rstrip('%').astype('float') / 100.0
        tops_df = tops_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_first_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-points-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        fiq_df = main_hist(team_first_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                           'team_first_quarter_points_per_game')
        fiq_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Points_per_Game',
                               season: 'Current_Season_Team_First_Quarter_Points_per_Game',
                               str(int(season) - 1): 'Previous_Season_Team_First_Quarter_Points_per_Game',
                               'Last 3': 'Last_3_Team_First_Quarter_Points_per_Game',
                               'Last 1': 'Last_1_Team_First_Quarter_Points_per_Game',
                               'Home': 'At_Home_Team_First_Quarter_Points_per_Game',
                               'Away': 'Away_Team_First_Quarter_Points_per_Game'
                               }, inplace=True)
        fiq_df['Team'] = fiq_df['Team'].str.strip()
        if season == '2010':
            fiq_df['Rank_Team_First_Quarter_Points_per_Game'] = fiq_df.index + 1
        fiq_df = fiq_df.replace('--', np.nan)
        fiq_df = fiq_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_second_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-points-per-game' \
                                                          + '?date=' \
                                                          + this_week_date_str
        sq_df = main_hist(team_second_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_second_quarter_points_per_game')
        sq_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Points_per_Game',
                              season: 'Current_Season_Team_Second_Quarter_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Second_Quarter_Points_per_Game',
                              'Last 3': 'Last_3_Team_Second_Quarter_Points_per_Game',
                              'Last 1': 'Last_1_Team_Second_Quarter_Points_per_Game',
                              'Home': 'At_Home_Team_Second_Quarter_Points_per_Game',
                              'Away': 'Away_Team_Second_Quarter_Points_per_Game'
                              }, inplace=True)
        sq_df['Team'] = sq_df['Team'].str.strip()
        if season == '2010':
            sq_df['Rank_Team_Second_Quarter_Points_per_Game'] = sq_df.index + 1
        sq_df = sq_df.replace('--', np.nan)
        sq_df = sq_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_third_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/3rd-quarter-points-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        tq_df = main_hist(team_third_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_third_quarter_points_per_game')
        tq_df.rename(columns={'Rank': 'Rank_Team_Third_Quarter_Points_per_Game',
                              season: 'Current_Season_Team_Third_Quarter_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Third_Quarter_Points_per_Game',
                              'Last 3': 'Last_3_Team_Third_Quarter_Points_per_Game',
                              'Last 1': 'Last_1_Team_Third_Quarter_Points_per_Game',
                              'Home': 'At_Home_Team_Third_Quarter_Points_per_Game',
                              'Away': 'Away_Team_Third_Quarter_Points_per_Game'
                              }, inplace=True)
        tq_df['Team'] = tq_df['Team'].str.strip()
        if season == '2010':
            tq_df['Rank_Team_Third_Quarter_Points_per_Game'] = tq_df.index + 1
        tq_df = tq_df.replace('--', np.nan)
        tq_df = tq_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_fourth_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-points-per-game' \
                                                          + '?date=' \
                                                          + this_week_date_str
        fq_df = main_hist(team_fourth_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_fourth_quarter_points_per_game')
        fq_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Points_per_Game',
                              season: 'Current_Season_Team_Fourth_Quarter_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Fourth_Quarter_Points_per_Game',
                              'Last 3': 'Last_3_Team_Fourth_Quarter_Points_per_Game',
                              'Last 1': 'Last_1_Team_Fourth_Quarter_Points_per_Game',
                              'Home': 'At_Home_Team_Fourth_Quarter_Points_per_Game',
                              'Away': 'Away_Team_Fourth_Quarter_Points_per_Game'
                              }, inplace=True)
        fq_df['Team'] = fq_df['Team'].str.strip()
        if season == '2010':
            fq_df['Rank_Team_Fourth_Quarter_Points_per_Game'] = fq_df.index + 1
        fq_df = fq_df.replace('--', np.nan)
        fq_df = fq_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_overtime_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/overtime-points-per-game' \
                                                    + '?date=' \
                                                    + this_week_date_str
        ot_df = main_hist(team_overtime_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_overtime_points_per_game')
        ot_df.rename(columns={'Rank': 'Rank_Team_Overtime_Points_per_Game',
                              season: 'Current_Season_Team_Overtime_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Overtime_Points_per_Game',
                              'Last 3': 'Last_3_Team_Overtime_Points_per_Game',
                              'Last 1': 'Last_1_Team_Overtime_Points_per_Game',
                              'Home': 'At_Home_Team_Overtime_Points_per_Game',
                              'Away': 'Away_Team_Overtime_Points_per_Game'
                              }, inplace=True)
        ot_df['Team'] = ot_df['Team'].str.strip()
        if season == '2010':
            ot_df['Rank_Team_Overtime_Points_per_Game'] = ot_df.index + 1
        ot_df = ot_df.replace('--', np.nan)
        ot_df = ot_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_first_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-points-per-game' \
                                                      + '?date=' \
                                                      + this_week_date_str
        fh_df = main_hist(team_first_half_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_first_half_points_per_game')
        fh_df.rename(columns={'Rank': 'Rank_Team_First_Half_Points_per_Game',
                              season: 'Current_Season_Team_First-Half_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_First-Half_Points_per_Game',
                              'Last 3': 'Last_3_Team_First-Half_Points_per_Game',
                              'Last 1': 'Last_1_Team_First-Half_Points_per_Game',
                              'Home': 'At_Home_Team_First_Half_Points_per_Game',
                              'Away': 'Away_Team_First-Half_Points_per_Game'
                              }, inplace=True)
        fh_df['Team'] = fh_df['Team'].str.strip()
        if season == '2010':
            fh_df['Rank_Team_First_Half_Points_per_Game'] = fh_df.index + 1
        fh_df = fh_df.replace('--', np.nan)
        fh_df = fh_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_second_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-points-per-game' \
                                                       + '?date=' \
                                                       + this_week_date_str
        sh_df = main_hist(team_second_half_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_second_half_points_per_game')
        sh_df.rename(columns={'Rank': 'Rank_Team_Second_Half_Points_per_Game',
                              season: 'Current_Season_Team_Second-Half_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Second-Half_Points_per_Game',
                              'Last 3': 'Last_3_Team_Second-Half_Points_per_Game',
                              'Last 1': 'Last_1_Team_Second-Half_Points_per_Game',
                              'Home': 'At_Home_Team_Second_Half_Points_per_Game',
                              'Away': 'Away_Team_Second-Half_Points_per_Game'
                              }, inplace=True)
        sh_df['Team'] = sh_df['Team'].str.strip()
        if season == '2010':
            sh_df['Rank_Team_Second_Half_Points_per_Game'] = sh_df.index + 1
        sh_df = sh_df.replace('--', np.nan)
        sh_df = sh_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_first_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-time-of-possession-share-pct' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
        fiqtp_df = main_hist(team_first_quarter_time_of_possession_share_percent_url_current, season, str(week),
                             this_week_date_str,
                             'team_first_quarter_time_of_possession_share_percent')
        fiqtp_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                 season: 'Current_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                 str(int(
                                     season) - 1): 'Previous_Season_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                 'Last 3': 'Last_3_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                 'Last 1': 'Last_1_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                 'Home': 'At_Home_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                 'Away': 'Away_Team_First_Quarter_Time_of_Possession_Share_Percent'
                                 }, inplace=True)
        fiqtp_df['Team'] = fiqtp_df['Team'].str.strip()
        if season == '2010':
            fiqtp_df['Rank_Team_First_Quarter_Time_of_Possession_Share_Percent'] = fiqtp_df.index + 1
        fiqtp_df = fiqtp_df.replace('--', np.nan)
        fiqtp_df = fiqtp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_second_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-time-of-possession-share-pct' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
        sqtp_df = main_hist(team_second_quarter_time_of_possession_share_percent_url_current, season, str(week),
                            this_week_date_str,
                            'team_second_quarter_time_of_possession_share_percent')
        sqtp_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last_3_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last_1_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Second_Quarter_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        sqtp_df['Team'] = sqtp_df['Team'].str.strip()
        if season == '2010':
            sqtp_df['Rank_Team_Second_Quarter_Time_of_Possession_Share_Percent'] = sqtp_df.index + 1
        sqtp_df = sqtp_df.replace('--', np.nan)
        sqtp_df = sqtp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_third_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/3rd-quarter-time-of-possession-share-pct' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
        tqtp_df = main_hist(team_third_quarter_time_of_possession_share_percent_url_current, season, str(week),
                            this_week_date_str,
                            'team_third_quarter_time_of_possession_share_percent')
        tqtp_df.rename(columns={'Rank': 'Rank_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last_3_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last_1_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Third_Quarter_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        tqtp_df['Team'] = tqtp_df['Team'].str.strip()
        if season == '2010':
            tqtp_df['Rank_Team_Third_Quarter_Time_of_Possession_Share_Percent'] = tqtp_df.index + 1
        tqtp_df = tqtp_df.replace('--', np.nan)
        tqtp_df = tqtp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_fourth_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-time-of-possession-share-pct' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
        fqtp_df = main_hist(team_fourth_quarter_time_of_possession_share_percent_url_current, season, str(week),
                            this_week_date_str,
                            'team_fourth_quarter_time_of_possession_share_percent')
        fqtp_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last_3_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last_1_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Fourth_Quarter_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        fqtp_df['Team'] = fqtp_df['Team'].str.strip()
        if season == '2010':
            fqtp_df['Rank_Team_Fourth_Quarter_Time_of_Possession_Share_Percent'] = fqtp_df.index + 1
        fqtp_df = fqtp_df.replace('--', np.nan)
        fqtp_df = fqtp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_first_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-time-of-possession-share-pct' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
        fhtp_df = main_hist(team_first_half_time_of_possession_share_percent_url_current, season, str(week),
                            this_week_date_str,
                            'team_first_half_time_of_possession_share_percent')
        fhtp_df.rename(columns={'Rank': 'Rank_Team_First_Half_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_First_Half_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last_3_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last_1_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_First_Half_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        fhtp_df['Team'] = fhtp_df['Team'].str.strip()
        if season == '2010':
            fhtp_df['Rank_Team_First_Half_Time_of_Possession_Share_Percent'] = fhtp_df.index + 1
        fhtp_df = fhtp_df.replace('--', np.nan)
        fhtp_df = fhtp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        team_second_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-time-of-possession-share-pct' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
        shtp_df = main_hist(team_second_half_time_of_possession_share_percent_url_current, season, str(week),
                            this_week_date_str,
                            'team_second_half_time_of_possession_share_percent')
        shtp_df.rename(columns={'Rank': 'Rank_Team_Second_Half_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Second_Half_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last_3_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last_1_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Second_Half_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        shtp_df['Team'] = shtp_df['Team'].str.strip()
        if season == '2010':
            shtp_df['Rank_Team_Second_Half_Time_of_Possession_Share_Percent'] = shtp_df.index + 1
        shtp_df = shtp_df.replace('--', np.nan)
        shtp_df = shtp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-game' \
                                                   + '?date=' \
                                                   + this_week_date_str
        toypg_df = main_hist(total_offense_yards_per_game_url_current, season, str(week), this_week_date_str,
                             'total_offense_yards_per_game')
        toypg_df.rename(columns={'Rank': 'Rank_Total_Offense_yards_per_game',
                                 season: 'Current_Season_Total_Offense_Yards_per_Game',
                                 str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Game',
                                 'Last 3': 'Last_3_Total_Offense_Yards_per_Game',
                                 'Last 1': 'Last_1_Total_Offense_Yards_per_Game',
                                 'Home': 'At_Home_Total_Offense_Yards_per_Game',
                                 'Away': 'Away_Total_Offense_Yards_per_Game'
                                 }, inplace=True)
        toypg_df['Team'] = toypg_df['Team'].str.strip()
        if season == '2010':
            toypg_df['Rank_Total_Offense_yards_per_game'] = toypg_df.index + 1
        toypg_df = toypg_df.replace('--', np.nan)
        toypg_df = toypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_plays_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/plays-per-game' \
                                                   + '?date=' \
                                                   + this_week_date_str
        toppg_df = main_hist(total_offense_plays_per_game_url_current, season, str(week), this_week_date_str,
                             'total_offense_plays_per_game')
        toppg_df.rename(columns={'Rank': 'Rank_Total_Offense_Plays_per_Game',
                                 season: 'Current_Season_Total_Offense_Plays_per_Game',
                                 str(int(season) - 1): 'Previous_Season_Total_Offense_Plays_per_Game',
                                 'Last 3': 'Last_3_Total_Offense_Plays_per_Game',
                                 'Last 1': 'Last_1_Total_Offense_Plays_per_Game',
                                 'Home': 'At_Home_Total_Offense_Plays_per_Game',
                                 'Away': 'Away_Total_Offense_Plays_per_Game'
                                 }, inplace=True)
        toppg_df['Team'] = toppg_df['Team'].str.strip()
        if season == '2010':
            toppg_df['Rank_Total_Offense_Plays_per_Game'] = toppg_df.index + 1
        toppg_df = toppg_df.replace('--', np.nan)
        toppg_df = toppg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_yards_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-play' \
                                                   + '?date=' \
                                                   + this_week_date_str
        toypp_df = main_hist(total_offense_yards_per_play_url_current, season, str(week), this_week_date_str,
                             'total_offense_yards_per_play')
        toypp_df.rename(columns={'Rank': 'Rank_Total_Offense_Yards_per_Play',
                                 season: 'Current_Season_Total_Offense_Yards_per_Play',
                                 str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Play',
                                 'Last 3': 'Last_3_Total_Offense_Yards_per_Play',
                                 'Last 1': 'Last_1_Total_Offense_Yards_per_Play',
                                 'Home': 'At_Home_Total_Offense_Yards_per_Play',
                                 'Away': 'Away_Total_Offense_Yards_per_Play'
                                 }, inplace=True)
        toypp_df['Team'] = toypp_df['Team'].str.strip()
        if season == '2010':
            toypp_df['Rank_Total_Offense_Yards_per_Play'] = toypp_df.index + 1
        toypp_df = toypp_df.replace('--', np.nan)
        toypp_df = toypp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_third_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-downs-per-game' \
                                                        + '?date=' \
                                                        + this_week_date_str
        totdpg_df = main_hist(total_offense_third_down_per_game_url_current, season, str(week), this_week_date_str,
                              'total_offense_third_down_per_game')
        totdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_per_Game',
                                  season: 'Current_Season_Total_Offense_Third_Down_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Total_Offense_Third_Down_per_Game',
                                  'Last 3': 'Last_3_Total_Offense_Third_Down_per_Game',
                                  'Last 1': 'Last_1_Total_Offense_Third_Down_per_Game',
                                  'Home': 'At_Home_Total_Offense_Third_Down_per_Game',
                                  'Away': 'Away_Total_Offense_Third_Down_per_Game'
                                  }, inplace=True)
        totdpg_df['Team'] = totdpg_df['Team'].str.strip()
        if season == '2010':
            totdpg_df['Rank_Total_Offense_Third_Down_per_Game'] = totdpg_df.index + 1
        totdpg_df = totdpg_df.replace('--', np.nan)
        totdpg_df = totdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_third_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversions-per-game' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
        totdcpg_df = main_hist(total_offense_third_down_conversions_per_game_url_current, season, str(week),
                               this_week_date_str,
                               'total_offense_third_down_conversions_per_game')
        totdcpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_Conversions_per_Game',
                                   season: 'Current_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Last 3': 'Last_3_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Last 1': 'Last_1_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Home': 'At_Home_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Away': 'Away_Total_Offense_Third-Down-Conversions_per_Game'
                                   }, inplace=True)
        totdcpg_df['Team'] = totdcpg_df['Team'].str.strip()
        if season == '2010':
            totdcpg_df['Rank_Total_Offense_Third_Down_Conversions_per_Game'] = totdcpg_df.index + 1
        totdcpg_df = totdcpg_df.replace('--', np.nan)
        totdcpg_df = totdcpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_fourth_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-downs-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        tofdpg_df = main_hist(total_offense_fourth_down_per_game_url_current, season, str(week), this_week_date_str,
                              'total_offense_fourth_down_per_game')
        tofdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_per_Game',
                                  season: 'Current_Season_Total_Offense_Fourth_Down_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_per_Game',
                                  'Last 3': 'Last_3_Total_Offense_Fourth_Down_per_Game',
                                  'Last 1': 'Last_1_Total_Offense_Fourth_Down_per_Game',
                                  'Home': 'At_Home_Total_Offense_Fourth_Down_per_Game',
                                  'Away': 'Away_Total_Offense_Fourth-Down_per_Game'
                                  }, inplace=True)
        tofdpg_df['Team'] = tofdpg_df['Team'].str.strip()
        if season == '2010':
            tofdpg_df['Rank_Total_Offense_Fourth_Down_per_Game'] = tofdpg_df.index + 1
        tofdpg_df = tofdpg_df.replace('--', np.nan)
        tofdpg_df = tofdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_fourth_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-down-conversions-per-game' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
        tofdcpg_df = main_hist(total_offense_fourth_down_conversions_per_game_url_current, season, str(week),
                               this_week_date_str,
                               'total_offense_fourth_down_conversions_per_game')
        tofdcpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   season: 'Current_Season_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   'Last 3': 'Last_3_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   'Last 1': 'Last_1_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   'Home': 'At_Home_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   'Away': 'Away_Total_Offense_Fourth_Down-Conversions_per_Game'
                                   }, inplace=True)
        tofdcpg_df['Team'] = tofdcpg_df['Team'].str.strip()
        if season == '2010':
            tofdcpg_df['Rank_Total_Offense_Fourth_Down_Conversions_per_Game'] = tofdcpg_df.index + 1
        tofdcpg_df = tofdcpg_df.replace('--', np.nan)
        tofdcpg_df = tofdcpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_average_time_of_possession_url_current = 'https://www.teamrankings.com/college-football/stat/average-time-of-possession-net-of-ot' \
                                                               + '?date=' \
                                                               + this_week_date_str
        toatp_df = main_hist(total_offense_average_time_of_possession_url_current, season, str(week),
                             this_week_date_str,
                             'total_offense_average_time_of_possession')
        toatp_df.rename(columns={'Rank': 'Rank_Total_Offense_Average_Time_of_Possession',
                                 season: 'Current_Season_Total_Offense_Average_Time_of_Possession',
                                 str(int(
                                     season) - 1): 'Previous_Season_Total_Offense_Average_Time_of_Possession',
                                 'Last 3': 'Last_3_Total_Offense_Average_Time_of_Possession',
                                 'Last 1': 'Last_1_Total_Offense_Average_Time_of_Possession',
                                 'Home': 'At_Home_Total_Offense_Average_Time_of_Possession',
                                 'Away': 'Away_Total_Offense_Average_Time_of_Possession'
                                 }, inplace=True)
        toatp_df['Team'] = toatp_df['Team'].str.strip()
        if season == '2010':
            toatp_df['Rank_Total_Offense_Average_Time_of_Possession'] = toatp_df.index + 1
        toatp_df = toatp_df.replace('--', np.nan)
        toatp_df = toatp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_average_time_of_possession_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/time-of-possession-pct-net-of-ot' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
        toatpp_df = main_hist(total_offense_average_time_of_possession_percentage_url_current, season, str(week),
                              this_week_date_str,
                              'total_offense_average_time_of_possession_percentage')
        toatpp_df.rename(columns={'Rank': 'Rank_Total_Offense_Average_Time_of_Possession_Percentage',
                                  season: 'Current_Season_Total_Offense_Average_Time_of_Possession_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Offense_Average_Time_of_Possession_Percentage',
                                  'Last 3': 'Last_3_Total_Offense_Average_Time_of_Possession_Percentage',
                                  'Last 1': 'Last_1_Total_Offense_Average_Time_of_Possession_Percentage',
                                  'Home': 'At_Home_Total_Offense_Average_Time_of_Possession_Percentage',
                                  'Away': 'Away_Total_Offense_Average_Time_of_Possession_Percentage'
                                  }, inplace=True)
        toatpp_df['Team'] = toatpp_df['Team'].str.strip()
        if season == '2010':
            toatpp_df['Rank_Total_Offense_Average_Time_of_Possession_Percentage'] = toatpp_df.index + 1
        toatpp_df = toatpp_df.replace('--', np.nan)
        for c in toatpp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                toatpp_df[c] = toatpp_df[c].str.rstrip('%').astype('float') / 100.0
        toatpp_df = toatpp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_third_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversion-pct' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
        totdcp_df = main_hist(total_offense_third_down_conversion_percent_url_current, season,
                              str(week), this_week_date_str,
                              'total_offense_third_down_conversion_percent')
        totdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_Conversion_Percent',
                                  season: 'Current_Season_Total_Offense_Third_Down-Conversion_Percent',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversion_Percent',
                                  'Last 3': 'Last_3_Total_Offense_Third_Down_Conversion_Percent',
                                  'Last 1': 'Last_1_Total_Offense_Third_Down_Conversion_Percent',
                                  'Home': 'At_Home_Total_Offense_Third_Down_Conversion_Percent',
                                  'Away': 'Away_Total_Offense_Third_Down_Conversion_Percent'
                                  }, inplace=True)
        totdcp_df['Team'] = totdcp_df['Team'].str.strip()
        if season == '2010':
            totdcp_df['Rank_Total_Offense_Third_Down_Conversion_Percent'] = totdcp_df.index + 1
        totdcp_df = totdcp_df.replace('--', np.nan)
        totdcp_df = totdcp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_fourth_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-down-conversion-pct' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
        tofdcp_df = main_hist(total_offense_fourth_down_conversion_percent_url_current, season, str(week),
                              this_week_date_str,
                              'total_offense_fourth_down_conversion_percent')
        tofdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_Conversion_Percent',
                                  season: 'Current_Season_Total_Offense_Fourth_Down-Conversion_Percent',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Last 3': 'Last_3_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Last 1': 'Last_1_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Home': 'At_Home_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Away': 'Away_Total_Offense_Fourth_Down_Conversion_Percent'
                                  }, inplace=True)
        tofdcp_df['Team'] = tofdcp_df['Team'].str.strip()
        if season == '2010':
            tofdcp_df['Rank_Total_Offense_Fourth_Down_Conversion_Percent'] = tofdcp_df.index + 1
        tofdcp_df = tofdcp_df.replace('--', np.nan)
        tofdcp_df = tofdcp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_punts_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-play' \
                                                   + '?date=' \
                                                   + this_week_date_str
        toppp_df = main_hist(total_offense_punts_per_play_url_current, season, str(week), this_week_date_str,
                             'total_offense_punts_per_play')
        toppp_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Play',
                                 season: 'Current_Season_Total_Offense_Punts_per_Play',
                                 str(int(
                                     season) - 1): 'Previous_Season_Total_Offense_Punts_per_Play',
                                 'Last 3': 'Last_3_Total_Offense_Punts_per_Play',
                                 'Last 1': 'Last_1_Total_Offense_Punts_per_Play',
                                 'Home': 'At_Home_Total_Offense_Punts_per_Play',
                                 'Away': 'Away_Total_Offense_Punts_per_Play'
                                 }, inplace=True)
        toppp_df['Team'] = toppp_df['Team'].str.strip()
        if season == '2010':
            toppp_df['Rank_Total_Offense_Punts_per_Play'] = toppp_df.index + 1
        toppp_df = toppp_df.replace('--', np.nan)
        toppp_df = toppp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_offense_punts_per_offensive_score_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-offensive-score' \
                                                              + '?date=' \
                                                              + this_week_date_str
        toppos_df = main_hist(total_offense_punts_per_offensive_score_url_current, season, str(week),
                              this_week_date_str,
                              'total_offense_punts_per_offensive score')
        toppos_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Offensive_Score',
                                  season: 'Current_Season_Total_Offense_Punts_per_Offensive_Score',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Offense_Punts_per_Offensive_Score',
                                  'Last 3': 'Last_3_Total_Offense_Punts_per_Offensive_Score',
                                  'Last 1': 'Last_1_Total_Offense_Punts_per_Offensive_Score',
                                  'Home': 'At_Home_Total_Offense_Punts_per_Offensive_Score',
                                  'Away': 'Away_Total_Offense_Punts_per_Offensive_Score'
                                  }, inplace=True)
        toppos_df['Team'] = toppos_df['Team'].str.strip()
        if season == '2010':
            toppos_df['Rank_Total_Offense_Punts_per_Offensive_Score'] = toppos_df.index + 1
        toppos_df = toppos_df.replace('--', np.nan)
        toppos_df = toppos_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_offense_rushing_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-attempts-per-game' \
                                                                + '?date=' \
                                                                + this_week_date_str
        rorapg_df = main_hist(rushing_offense_rushing_attempts_per_game_url_current, season, str(week),
                              this_week_date_str,
                              'rushing_offense_rushing_attempts_per_game')
        rorapg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Attempts_per_Game',
                                  season: 'Current_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Last 3': 'Last_3_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Last 1': 'Last_1_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Home': 'At_Home_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Away': 'Away_Rushing_Offense_Rushing_Attempts_per_Game'
                                  }, inplace=True)
        rorapg_df['Team'] = rorapg_df['Team'].str.strip()
        if season == '2010':
            rorapg_df['Rank_Rushing_Offense_Rushing_Attempts_per_Game'] = rorapg_df.index + 1
        rorapg_df = rorapg_df.replace('--', np.nan)
        rorapg_df = rorapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_offense_rushing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        rorypg_df = main_hist(rushing_offense_rushing_yards_per_game_url_current, season, str(week), this_week_date_str,
                              'rushing_offense_rushing_yards_per_game')
        rorypg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Game',
                                  season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Last 3': 'Last_3_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Last 1': 'Last_1_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Game'
                                  }, inplace=True)
        rorypg_df['Team'] = rorypg_df['Team'].str.strip()
        if season == '2010':
            rorypg_df['Rank_Rushing_Offense_Rushing_Yards_per_Game'] = rorypg_df.index + 1
        rorypg_df = rorypg_df.replace('--', np.nan)
        rorypg_df = rorypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_offense_rushing_yards_per_rush_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-rush-attempt' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
        rorypra_df = main_hist(rushing_offense_rushing_yards_per_rush_attempt_url_current, season, str(week),
                               this_week_date_str,
                               'rushing_offense_rushing_yards_per_rush_attempt')
        rorypra_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   str(int(
                                       season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Last 3': 'Last_3_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Last 1': 'Last_1_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Rush_Attempt'
                                   }, inplace=True)
        rorypra_df['Team'] = rorypra_df['Team'].str.strip()
        if season == '2010':
            rorypra_df['Rank_Rushing_Offense_Rushing_Yards_per_Rush_Attempt'] = rorypra_df.index + 1
        rorypra_df = rorypra_df.replace('--', np.nan)
        rorypra_df = rorypra_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_offense_rushing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-play-pct' \
                                                              + '?date=' \
                                                              + this_week_date_str
        rorpp_df = main_hist(rushing_offense_rushing_play_percentage_url_current, season, str(week), this_week_date_str,
                             'rushing_offense_rushing_play_percentage')
        rorpp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Play_Percentage',
                                 season: 'Current_Season_Rushing_Offense_Rushing_Play_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Play_Percentage',
                                 'Last 3': 'Last_3_Rushing_Offense_Rushing_Play_Percentage',
                                 'Last 1': 'Last_1_Rushing_Offense_Rushing_Play_Percentage',
                                 'Home': 'At_Home_Rushing_Offense_Rushing_Play_Percentage',
                                 'Away': 'Away_Rushing_Offense_Rushing_Play_Percentage'
                                 }, inplace=True)
        rorpp_df['Team'] = rorpp_df['Team'].str.strip()
        if season == '2010':
            rorpp_df['Rank_Rushing_Offense_Rushing_Play_Percentage'] = rorpp_df.index + 1
        rorpp_df = rorpp_df.replace('--', np.nan)
        for c in rorpp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                rorpp_df[c] = rorpp_df[c].str.rstrip('%').astype('float') / 100.0
        rorpp_df = rorpp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_offense_rushing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-pct' \
                                                               + '?date=' \
                                                               + this_week_date_str
        roryp_df = main_hist(rushing_offense_rushing_yards_percentage_url_current, season, str(week),
                             this_week_date_str,
                             'rushing_offense_rushing_yards_percentage')
        roryp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_Percentage',
                                 season: 'Current_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Last 3': 'Last_3_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Last 1': 'Last_1_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Home': 'At_Home_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Away': 'Away_Rushing_Offense_Rushing_Yards_Percentage'
                                 }, inplace=True)
        roryp_df['Team'] = roryp_df['Team'].str.strip()
        if season == '2010':
            roryp_df['Rank_Rushing_Offense_Rushing_Yards_Percentage'] = roryp_df.index + 1
        roryp_df = roryp_df.replace('--', np.nan)
        for c in roryp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                roryp_df[c] = roryp_df[c].str.rstrip('%').astype('float') / 100.0
        roryp_df = roryp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_pass_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/pass-attempts-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        popapg_df = main_hist(passing_offense_pass_attempts_per_game_url_current, season, str(week), this_week_date_str,
                              'passing_offense_pass_attempts_per_game')
        popapg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Pass_Attempts_per_Game',
                                  season: 'Current_Season_Passing_Offense_Pass_Attempts_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_Pass_Attempts_per_Game',
                                  'Last 3': 'Last_3_Passing_Offense_Pass_Attempts_per_Game',
                                  'Last 1': 'Last_1_Passing_Offense_Pass_Attempts_per_Game',
                                  'Home': 'At_Home_Passing_Offense_Pass_Attempts_per_Game',
                                  'Away': 'Away_Passing_Offense_Pass_Attempts_per_Game'
                                  }, inplace=True)
        popapg_df['Team'] = popapg_df['Team'].str.strip()
        if season == '2010':
            popapg_df['Rank_Passing_Offense_Pass_Attempts_per_Game'] = popapg_df.index + 1
        popapg_df = popapg_df.replace('--', np.nan)
        popapg_df = popapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_completions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/completions-per-game' \
                                                           + '?date=' \
                                                           + this_week_date_str
        pocpg_df = main_hist(passing_offense_completions_per_game_url_current, season, str(week), this_week_date_str,
                             'passing_offense_completions_per_game')
        pocpg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completions_per_Game',
                                 season: 'Current_Season_Passing_Offense_Completions_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Completions_per_Game',
                                 'Last 3': 'Last_3_Passing_Offense_Completions_per_Game',
                                 'Last 1': 'Last_1_Passing_Offense_Completions_per_Game',
                                 'Home': 'At_Home_Passing_Offense_Completions_per_Game',
                                 'Away': 'Away_Passing_Offense_Completions_per_Game'
                                 }, inplace=True)
        pocpg_df['Team'] = pocpg_df['Team'].str.strip()
        if season == '2010':
            pocpg_df['Rank_Passing_Offense_Completions_per_Game'] = pocpg_df.index + 1
        pocpg_df = pocpg_df.replace('--', np.nan)
        pocpg_df = pocpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_incompletions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/incompletions-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        poipg_df = main_hist(passing_offense_incompletions_per_game_url_current, season, str(week), this_week_date_str,
                             'passing_offense_incompletions_per_game')
        poipg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Incompletions_per_Game',
                                 season: 'Current_Season_Passing_Offense_Incompletions_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Incompletions_per_Game',
                                 'Last 3': 'Last_3_Passing_Offense_Incompletions_per_Game',
                                 'Last 1': 'Last_1_Passing_Offense_Incompletions_per_Game',
                                 'Home': 'At_Home_Passing_Offense_Incompletions_per_Game',
                                 'Away': 'Away_Passing_Offense_Incompletions_per_Game'
                                 }, inplace=True)
        poipg_df['Team'] = poipg_df['Team'].str.strip()
        if season == '2010':
            poipg_df['Rank_Passing_Offense_Incompletions_per_Game'] = poipg_df.index + 1
        poipg_df = poipg_df.replace('--', np.nan)
        poipg_df = poipg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_completion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/completion-pct' \
                                                            + '?date=' \
                                                            + this_week_date_str
        pocp_df = main_hist(passing_offense_completion_percentage_url_current, season, str(week), this_week_date_str,
                            'passing_offense_completion_percentage')
        pocp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completion_Percentage',
                                season: 'Current_Season_Passing_Offense_Completion_Percentage',
                                str(int(
                                    season) - 1): 'Previous_Season_Passing_Offense_Completion_Percentage',
                                'Last 3': 'Last_3_Passing_Offense_Completion_Percentage',
                                'Last 1': 'Last_1_Passing_Offense_Completion_Percentage',
                                'Home': 'At_Home_Passing_Offense_Completion_Percentage',
                                'Away': 'Away_Passing_Offense_Completion_Percentage'
                                }, inplace=True)
        pocp_df['Team'] = pocp_df['Team'].str.strip()
        if season == '2010':
            pocp_df['Rank_Passing_Offense_Completion_Percentage'] = pocp_df.index + 1
        pocp_df = pocp_df.replace('--', np.nan)
        for c in pocp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                pocp_df[c] = pocp_df[c].str.rstrip('%').astype('float') / 100.0
        pocp_df = pocp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_passing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        popypg_df = main_hist(passing_offense_passing_yards_per_game_url_current, season, str(week), this_week_date_str,
                              'passing_offense_passing_yards_per_game')
        popypg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_per_Game',
                                  season: 'Current_Season_Passing_Offense_Passing_Yards_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_per_Game',
                                  'Last 3': 'Last_3_Passing_Offense_Passing_Yards_per_Game',
                                  'Last 1': 'Last_1_Passing_Offense_Passing_Yards_per_Game',
                                  'Home': 'At_Home_Passing_Offense_Passing_Yards_per_Game',
                                  'Away': 'Away_Passing_Offense_Passing_Yards_per_Game'
                                  }, inplace=True)
        popypg_df['Team'] = popypg_df['Team'].str.strip()
        if season == '2010':
            popypg_df['Rank_Passing_Offense_Passing_Yards_per_Game'] = popypg_df.index + 1
        popypg_df = popypg_df.replace('--', np.nan)
        popypg_df = popypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_qb_sacked_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        poqspg_df = main_hist(passing_offense_qb_sacked_per_game_url_current, season, str(week), this_week_date_str,
                              'passing_offense_qb_sacked_per_game')
        poqspg_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_per_Game',
                                  season: 'Current_Season_Passing_Offense_QB_Sacked_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_per_Game',
                                  'Last 3': 'Last_3_Passing_Offense_QB_Sacked_per_Game',
                                  'Last 1': 'Last_1_Passing_Offense_QB_Sacked_per_Game',
                                  'Home': 'At_Home_Passing_Offense_QB_Sacked_per_Game',
                                  'Away': 'Away_Passing_Offense_QB_Sacked_per_Game'
                                  }, inplace=True)
        poqspg_df['Team'] = poqspg_df['Team'].str.strip()
        if season == '2010':
            poqspg_df['Rank_Passing_Offense_QB_Sacked_per_Game'] = poqspg_df.index + 1
        poqspg_df = poqspg_df.replace('--', np.nan)
        poqspg_df = poqspg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_qb_sacked_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-pct' \
                                                           + '?date=' \
                                                           + this_week_date_str
        poqsp_df = main_hist(passing_offense_qb_sacked_percentage_url_current, season, str(week), this_week_date_str,
                             'passing_offense_qb_sacked_percentage')
        poqsp_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_Percentage',
                                 season: 'Current_Season_Passing_Offense_QB_Sacked_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_Percentage',
                                 'Last 3': 'Last_3_Passing_Offense_QB_Sacked_Percentage',
                                 'Last 1': 'Last_1_Passing_Offense_QB_Sacked_Percentage',
                                 'Home': 'At_Home_Passing_Offense_QB_Sacked_Percentage',
                                 'Away': 'Away_Passing_Offense_QB_Sacked_Percentage'
                                 }, inplace=True)
        poqsp_df['Team'] = poqsp_df['Team'].str.strip()
        if season == '2010':
            poqsp_df['Rank_Passing_Offense_QB_Sacked_Percentage'] = poqsp_df.index + 1
        poqsp_df = poqsp_df.replace('--', np.nan)
        for c in poqsp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                poqsp_df[c] = poqsp_df[c].str.rstrip('%').astype('float') / 100.0
        poqsp_df = poqsp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_average_passer_rating_url_current = 'https://www.teamrankings.com/college-football/stat/average-team-passer-rating' \
                                                            + '?date=' \
                                                            + this_week_date_str
        poapr_df = main_hist(passing_offense_average_passer_rating_url_current, season, str(week), this_week_date_str,
                             'passing_offense_average_passer_rating')
        poapr_df.rename(columns={'Rank': 'Rank_Passing_Offense_Average_Passer_Rating',
                                 season: 'Current_Season_Passing_Offense_Average_Passer_Rating',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Average_Passer_Rating',
                                 'Last 3': 'Last_3_Passing_Offense_Average_Passer_Rating',
                                 'Last 1': 'Last_1_Passing_Offense_Average_Passer_Rating',
                                 'Home': 'At_Home_Passing_Offense_Average_Passer_Rating',
                                 'Away': 'Away_Passing_Offense_Average_Passer_Rating'
                                 }, inplace=True)
        poapr_df['Team'] = poapr_df['Team'].str.strip()
        if season == '2010':
            poapr_df['Rank_Passing_Offense_Average_Passer_Rating'] = poapr_df.index + 1
        poapr_df = poapr_df.replace('--', np.nan)
        poapr_df = poapr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_passing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-play-pct' \
                                                              + '?date=' \
                                                              + this_week_date_str
        ppoppp_df = main_hist(passing_offense_passing_play_percentage_url_current, season, str(week),
                              this_week_date_str,
                              'passing_offense_passing_play_percentage')
        ppoppp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Play_Percentage',
                                  season: 'Current_Season_Passing_Offense_Passing_Play_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_Passing_Play_Percentage',
                                  'Last 3': 'Last_3_Passing_Offense_Passing_Play_Percentage',
                                  'Last 1': 'Last_1_Passing_Offense_Passing_Play_Percentage',
                                  'Home': 'At_Home_Passing_Offense_Passing_Play_Percentage',
                                  'Away': 'Away_Passing_Offense_Passing_Play_Percentage'
                                  }, inplace=True)
        ppoppp_df['Team'] = ppoppp_df['Team'].str.strip()
        if season == '2010':
            ppoppp_df['Rank_Passing_Offense_Passing_Play_Percentage'] = ppoppp_df.index + 1
        ppoppp_df = ppoppp_df.replace('--', np.nan)
        for c in ppoppp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                ppoppp_df[c] = ppoppp_df[c].str.rstrip('%').astype('float') / 100.0
        ppoppp_df = ppoppp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_passing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-pct' \
                                                               + '?date=' \
                                                               + this_week_date_str
        popyp_df = main_hist(passing_offense_passing_yards_percentage_url_current, season, str(week),
                             this_week_date_str,
                             'passing_offense_passing_yards_percentage')
        popyp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_Percentage',
                                 season: 'Current_Season_Passing_Offense_Passing_Yards_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_Percentage',
                                 'Last 3': 'Last_3_Passing_Offense_Passing_Yards_Percentage',
                                 'Last 1': 'Last_1_Passing_Offense_Passing_Yards_Percentage',
                                 'Home': 'At_Home_Passing_Offense_Passing_Yards_Percentage',
                                 'Away': 'Away_Passing_Offense_Passing_Yards_Percentage'
                                 }, inplace=True)
        popyp_df['Team'] = popyp_df['Team'].str.strip()
        if season == '2010':
            popyp_df['Rank_Passing_Offense_Passing_Yards_Percentage'] = popyp_df.index + 1
        popyp_df = popyp_df.replace('--', np.nan)
        for c in popyp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                popyp_df[c] = popyp_df[c].str.rstrip('%').astype('float') / 100.0
        popyp_df = popyp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_yards_per_pass_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-pass-attempt' \
                                                             + '?date=' \
                                                             + this_week_date_str
        poyppa_df = main_hist(passing_offense_yards_per_pass_attempt_url_current, season, str(week), this_week_date_str,
                              'passing_offense_yards_per_pass_attempt')
        poyppa_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Pass_Attempt',
                                  season: 'Current_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Last 3': 'Last_3_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Last 1': 'Last_1_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Home': 'At_Home_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Away': 'Away_Passing_Offense_Yards_per_Pass_Attempt'
                                  }, inplace=True)
        poyppa_df['Team'] = poyppa_df['Team'].str.strip()
        if season == '2010':
            poyppa_df['Rank_Passing_Offense_Yards_per_Pass_Attempt'] = poyppa_df.index + 1
        poyppa_df = poyppa_df.replace('--', np.nan)
        poyppa_df = poyppa_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_offense_yards_per_completion_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-completion' \
                                                           + '?date=' \
                                                           + this_week_date_str
        poypc_df = main_hist(passing_offense_yards_per_completion_url_current, season, str(week), this_week_date_str,
                             'passing_offense_yards_per_completion')
        poypc_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Completion',
                                 season: 'Current_Season_Passing_Offense_Yards_per_Completion',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Completion',
                                 'Last 3': 'Last_3_Passing_Offense_Yards_per_Completion',
                                 'Last 1': 'Last_1_Passing_Offense_Yards_per_Completion',
                                 'Home': 'At_Home_Passing_Offense_Yards_per_Completion',
                                 'Away': 'Away_Passing_Offense_Yards_per_Completion'
                                 }, inplace=True)
        poypc_df['Team'] = poypc_df['Team'].str.strip()
        if season == '2010':
            poypc_df['Rank_Passing_Offense_Yards_per_Completion'] = poypc_df.index + 1
        poypc_df = poypc_df.replace('--', np.nan)
        poypc_df = poypc_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_offense_field_goal_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/field-goal-attempts-per-game' \
                                                                         + '?date=' \
                                                                         + this_week_date_str
        stofgapg_df = main_hist(special_teams_offense_field_goal_attempts_per_game_url_current, season, str(week),
                                this_week_date_str,
                                'special_teams_offense_field_goal_attempts_per_game')
        stofgapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    season: 'Current_Season_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    'Last 3': 'Last_3_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    'Last 1': 'Last_1_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    'Away': 'Away_Special_Teams_Offense_Field_Goal_Attempts_per_Game'
                                    }, inplace=True)
        stofgapg_df['Team'] = stofgapg_df['Team'].str.strip()
        if season == '2010':
            stofgapg_df['Rank_Special_Teams_Offense_Field_Goal_Attempts_per_Game'] = stofgapg_df.index + 1
        stofgapg_df = stofgapg_df.replace('--', np.nan)
        stofgapg_df = stofgapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_offense_field_goals_made_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/field-goals-made-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        stofgmpg_df = main_hist(special_teams_offense_field_goals_made_per_game_url_current, season, str(week),
                                this_week_date_str,
                                'special_teams_offense_field_goals_made_per_game')
        stofgmpg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    season: 'Current_Season_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    'Last 3': 'Last_3_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    'Last 1': 'Last_1_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    'Home': 'At_Home_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    'Away': 'Away_Special_Teams_Offense_Field_Goals_Made_per_Game'
                                    }, inplace=True)
        stofgmpg_df['Team'] = stofgmpg_df['Team'].str.strip()
        if season == '2010':
            stofgmpg_df['Rank_Special_Teams_Offense_Field_Goals_Made_per_Game'] = stofgmpg_df.index + 1
        stofgmpg_df = stofgmpg_df.replace('--', np.nan)
        stofgmpg_df = stofgmpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_offense_field_goal_conversion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/field-goal-conversion-pct' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
        stofgcp_df = main_hist(special_teams_offense_field_goal_conversion_percentage_url_current, season, str(week),
                               this_week_date_str,
                               'special_teams_offense_field_goal_conversion_percentage')
        stofgcp_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                   season: 'Current_Season_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                   str(int(
                                       season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                   'Last 3': 'Last_3_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                   'Last 1': 'Last_1_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                   'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                   'Away': 'Away_Special_Teams_Offense_Field_Goal_Conversion_Percentage'
                                   }, inplace=True)
        stofgcp_df['Team'] = stofgcp_df['Team'].str.strip()
        if season == '2010':
            stofgcp_df['Rank_Special_Teams_Offense_Field_Goal_Conversion_Percentage'] = stofgcp_df.index + 1
        stofgcp_df = stofgcp_df.replace('--', np.nan)
        for c in stofgcp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                stofgcp_df[c] = stofgcp_df[c].str.rstrip('%').astype('float') / 100.0
        stofgcp_df = stofgcp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_offense_punt_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/punt-attempts-per-game' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
        stopapg_df = main_hist(special_teams_offense_punt_attempts_per_game_url_current, season, str(week),
                               this_week_date_str,
                               'special_teams_offense_punt_attempts_per_game')
        stopapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   season: 'Current_Season_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   'Last 3': 'Last_3_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   'Last 1': 'Last_1_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   'Home': 'At_Home_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   'Away': 'Away_Special_Teams_Offense_Punt_Attempts_per_Game'
                                   }, inplace=True)
        stopapg_df['Team'] = stopapg_df['Team'].str.strip()
        if season == '2010':
            stopapg_df['Rank_Special_Teams_Offense_Punt_Attempts_per_Game'] = stopapg_df.index + 1
        stopapg_df = stopapg_df.replace('--', np.nan)
        stopapg_df = stopapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_offense_gross_punt_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/gross-punt-yards-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        stogpypg_df = main_hist(special_teams_offense_gross_punt_yards_per_game_url_current, season, str(week),
                                this_week_date_str,
                                'special_teams_offense_gross_punt_yards_per_game')
        stogpypg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    season: 'Current_Season_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    'Last 3': 'Last_3_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    'Last 1': 'Last_1_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    'Home': 'At_Home_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    'Away': 'Away_Special_Teams_Offense_Gross_Punt_Yards_per_Game'
                                    }, inplace=True)
        stogpypg_df['Team'] = stogpypg_df['Team'].str.strip()
        if season == '2010':
            stogpypg_df['Rank_Special_Teams_Offense_Gross_Punt_Yards_per_Game'] = stogpypg_df.index + 1
        stogpypg_df = stogpypg_df.replace('--', np.nan)
        stogpypg_df = stogpypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-game' \
                                                               + '?date=' \
                                                               + this_week_date_str
        sdoppg_df = main_hist(scoring_defense_opponent_points_per_game_url_current, season, str(week),
                              this_week_date_str, 'scoring_defense_opponent_points_per_game')
        sdoppg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Game',
                                  season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Game',
                                  'Last 3': 'Last_3_Scoring_Defense_Opponent_Points_per_Game',
                                  'Last 1': 'Last_1_Scoring_Defense_Opponent_Points_per_Game',
                                  'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Game',
                                  'Away': 'Away_Scoring_Defense_Opponent_Points_per_Game'
                                  }, inplace=True)
        sdoppg_df['Team'] = sdoppg_df['Team'].str.strip()
        if season == '2010':
            sdoppg_df['Rank_Scoring_Defense_Opponent_Points_per_Game'] = sdoppg_df.index + 1
        sdoppg_df = sdoppg_df.replace('--', np.nan)
        sdoppg_df = sdoppg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opp_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/opp-yards-per-point' \
                                                          + '?date=' \
                                                          + this_week_date_str
        sdoypp_df = main_hist(scoring_defense_opp_yards_per_point_url_current, season, str(week),
                              this_week_date_str, 'scoring_defense_opp_yards_per_point')
        sdoypp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opp_Yards_per_Point',
                                  season: 'Current_Season_Scoring_Defense_Opp_Yards_per_Point',
                                  str(int(
                                      season) - 1): 'Previous_Season_Scoring_Defense_Opp_Yards_per_Point',
                                  'Last 3': 'Last_3_Scoring_Defense_Opp_Yards_per_Point',
                                  'Last 1': 'Last_1_Scoring_Defense_Opp_Yards_per_Point',
                                  'Home': 'At_Home_Scoring_Defense_Opp_Yards_per_Point',
                                  'Away': 'Away_Scoring_Defense_Opp_Yards_per_Point'
                                  }, inplace=True)
        sdoypp_df['Team'] = sdoypp_df['Team'].str.strip()
        if season == '2010':
            sdoypp_df['Rank_Scoring_Defense_Opp_Yards_per_Point'] = sdoypp_df.index + 1
        sdoypp_df = sdoypp_df.replace('--', np.nan)
        sdoypp_df = sdoypp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_points_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-play' \
                                                               + '?date=' \
                                                               + this_week_date_str
        sdoppp_df = main_hist(scoring_defense_opponent_points_per_play_url_current, season, str(week),
                              this_week_date_str, 'scoring_defense_opponent_points_per_play')
        sdoppp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Play',
                                  season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Play',
                                  str(int(
                                      season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Play',
                                  'Last 3': 'Last_3_Scoring_Defense_Opponent_Points_per_Play',
                                  'Last 1': 'Last_1_Scoring_Defense_Opponent_Points_per_Play',
                                  'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Play',
                                  'Away': 'Away_Scoring_Defense_Opponent_Points_per_Play'
                                  }, inplace=True)
        sdoppp_df['Team'] = sdoppp_df['Team'].str.strip()
        if season == '2010':
            sdoppp_df['Rank_Scoring_Defense_Opponent_Points_per_Play'] = sdoppp_df.index + 1
        sdoppp_df = sdoppp_df.replace('--', np.nan)
        sdoppp_df = sdoppp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_average_scoring_margin_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-average-scoring-margin' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        sdoasm_df = main_hist(scoring_defense_opponent_average_scoring_margin_url_current, season, str(week),
                              this_week_date_str, 'scoring_defense_opponent_average_scoring_margin')
        sdoasm_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                  season: 'Current_Season_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                  str(int(
                                      season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                  'Last 3': 'Last_3_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                  'Last 1': 'Last_1_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                  'Home': 'At_Home_Scoring_Defense_Opponent_Average_Scoring_Margin',
                                  'Away': 'Away_Scoring_Defense_Opponent_Average_Scoring_Margin'
                                  }, inplace=True)
        sdoasm_df['Team'] = sdoasm_df['Team'].str.strip()
        if season == '2010':
            sdoasm_df['Rank_Scoring_Defense_Opponent_Average_Scoring_Margin'] = sdoasm_df.index + 1
        sdoasm_df = sdoasm_df.replace('--', np.nan)
        sdoasm_df = sdoasm_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_red_zone_scoring_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-red-zone-scoring-attempts-per-game' \
                                                                                  + '?date=' \
                                                                                  + this_week_date_str
        sdorzsapg_df = main_hist(scoring_defense_opponent_red_zone_scoring_attempts_per_game_url_current, season,
                                 str(week),
                                 this_week_date_str, 'scoring_defense_opponent_red_zone_scoring_attempts_per_game')
        sdorzsapg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                     season: 'Current_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                     str(int(
                                         season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                     'Last 3': 'Last_3_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                     'Last 1': 'Last_1_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                     'Home': 'At_Home_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game',
                                     'Away': 'Away_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game'
                                     }, inplace=True)
        sdorzsapg_df['Team'] = sdorzsapg_df['Team'].str.strip()
        if season == '2010':
            sdorzsapg_df['Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Attempts_per_Game'] = sdorzsapg_df.index + 1
        sdorzsapg_df = sdorzsapg_df.replace('--', np.nan)
        sdorzsapg_df = sdorzsapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_red_zone_scores_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-red-zone-scores-per-game' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
        sdorzspg_df = main_hist(scoring_defense_opponent_red_zone_scores_per_game_url_current,
                                season, str(week),
                                this_week_date_str,
                                'scoring_defense_opponent_red_zone_scores_per_game')
        sdorzspg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                    season: 'Current_Season_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                    'Last 3': 'Last_3_Scoring_Defense_Opponent_Red_Zone_Scoring_Scores_per_Game',
                                    'Last 1': 'Last_1_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                    'Home': 'At_Home_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game',
                                    'Away': 'Away_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game'
                                    }, inplace=True)
        sdorzspg_df['Team'] = sdorzspg_df['Team'].str.strip()
        if season == '2010':
            sdorzspg_df[
                'Rank_Scoring_Defense_Opponent_Red_Zone_Scores_per_Game'] = sdorzspg_df.index + 1
        sdorzspg_df = sdorzspg_df.replace('--', np.nan)
        sdorzspg_df = sdorzspg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_red_zone_scoring_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-red-zone-scoring-pct' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
        sdorzsp_df = main_hist(scoring_defense_opponent_red_zone_scoring_percentage_url_current,
                               season, str(week),
                               this_week_date_str,
                               'scoring_defense_opponent_red_zone_scoring_percentage')
        sdorzsp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                   season: 'Current_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                   str(int(
                                       season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                   'Last 3': 'Last_3_Scoring_Defense_Opponent_Red_Zone_Scoring_Scoring_Percentage',
                                   'Last 1': 'Last_1_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                   'Home': 'At_Home_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage',
                                   'Away': 'Away_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage'
                                   }, inplace=True)
        sdorzsp_df['Team'] = sdorzsp_df['Team'].str.strip()
        if season == '2010':
            sdorzsp_df[
                'Rank_Scoring_Defense_Opponent_Red_Zone_Scoring_Percentage'] = sdorzsp_df.index + 1
        sdorzsp_df = sdorzsp_df.replace('--', np.nan)
        for c in sdorzsp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                sdorzsp_df[c] = sdorzsp_df[c].str.rstrip('%').astype('float') / 100.0
        sdorzsp_df = sdorzsp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_points_per_field_goal_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-field-goal-attempt' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
        sdoppfga_df = main_hist(scoring_defense_opponent_points_per_field_goal_attempt_url_current,
                                season, str(week),
                                this_week_date_str,
                                'scoring_defense_opponent_points_per_field_goal_attempt')
        sdoppfga_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                    season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                    str(int(
                                        season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                    'Last 3': 'Last_3_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                    'Last 1': 'Last_1_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                    'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt',
                                    'Away': 'Away_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt'
                                    }, inplace=True)
        sdoppfga_df['Team'] = sdoppfga_df['Team'].str.strip()
        if season == '2010':
            sdoppfga_df[
                'Rank_Scoring_Defense_Opponent_Points_per_Field_Goal_Attempt'] = sdoppfga_df.index + 1
        sdoppfga_df = sdoppfga_df.replace('--', np.nan)
        sdoppfga_df = sdoppfga_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_offensive_touchdowns_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-offensive-touchdowns-per-game' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
        sdootpg_df = main_hist(scoring_defense_opponent_offensive_touchdowns_per_game_url_current,
                               season, str(week),
                               this_week_date_str,
                               'scoring_defense_opponent_offensive_touchdowns_per_game')
        sdootpg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                   season: 'Current_Season_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                   'Last 3': 'Last_3_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                   'Last 1': 'Last_1_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                   'Home': 'At_Home_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game',
                                   'Away': 'Away_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game'
                                   }, inplace=True)
        sdootpg_df['Team'] = sdootpg_df['Team'].str.strip()
        if season == '2010':
            sdootpg_df[
                'Rank_Scoring_Defense_Opponent_Offensive_Touchdowns_per_Game'] = sdootpg_df.index + 1
        sdootpg_df = sdootpg_df.replace('--', np.nan)
        sdootpg_df = sdootpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        scoring_defense_opponent_offensive_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-offensive-points-per-game' \
                                                                         + '?date=' \
                                                                         + this_week_date_str
        sdooppg_df = main_hist(scoring_defense_opponent_offensive_points_per_game_url_current,
                               season, str(week),
                               this_week_date_str,
                               'scoring_defense_opponent_offensive_points_per_game')
        sdooppg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                   season: 'Current_Season_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                   'Last 3': 'Last_3_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                   'Last 1': 'Last_1_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                   'Home': 'At_Home_Scoring_Defense_Opponent_Offensive_Points_per_Game',
                                   'Away': 'Away_Scoring_Defense_Opponent_Offensive_Points_per_Game'
                                   }, inplace=True)
        sdooppg_df['Team'] = sdooppg_df['Team'].str.strip()
        if season == '2010':
            sdooppg_df[
                'Rank_Scoring_Defense_Opponent_Offensive_Points_per_Game'] = sdooppg_df.index + 1
        sdooppg_df = sdooppg_df.replace('--', np.nan)
        sdooppg_df = sdooppg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-game' \
                                                            + '?date=' \
                                                            + this_week_date_str
        tdoypg_df = main_hist(total_defense_opponent_yards_per_game_url_current, season, str(week), this_week_date_str,
                              'total_defense_opponent_yards_per_game')
        tdoypg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Yards_per_Game',
                                  season: 'Current_Season_Total_Defense_Opponent_Yards_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Total_Defense_Opponent_Yards_per_Game',
                                  'Last 3': 'Last_3_Total_Defense_Opponent_Yards_per_Game',
                                  'Last 1': 'Last_1_Total_Defense_Opponent_Yards_per_Game',
                                  'Home': 'At_Home_Total_Defense_Opponent_Yards_per_Game',
                                  'Away': 'Away_Total_Defense_Opponent_Yards_per_Game'
                                  }, inplace=True)
        tdoypg_df['Team'] = tdoypg_df['Team'].str.strip()
        if season == '2010':
            tdoypg_df['Rank_Total_Defense_Opponent_Yards_per_Game'] = tdoypg_df.index + 1
        tdoypg_df = tdoypg_df.replace('--', np.nan)
        tdoypg_df = tdoypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_plays_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-plays-per-game' \
                                                            + '?date=' \
                                                            + this_week_date_str
        tdoppg_df = main_hist(total_defense_opponent_plays_per_game_url_current, season, str(week),
                              this_week_date_str, 'total_defense_opponent_plays_per_game')
        tdoppg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Plays_per_Game',
                                  season: 'Current_Season_Total_Defense_Opponent_Plays_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Total_Defense_Opponent_Plays_per_Game',
                                  'Last 3': 'Last_3_Total_Defense_Opponent_Plays_per_Game',
                                  'Last 1': 'Last_1_Total_Defense_Opponent_Plays_per_Game',
                                  'Home': 'At_Home_Total_Defense_Opponent_Plays_per_Game',
                                  'Away': 'Away_Total_Defense_Opponent_Plays_per_Game'
                                  }, inplace=True)
        tdoppg_df['Team'] = tdoppg_df['Team'].str.strip()
        if season == '2010':
            tdoppg_df['Rank_Total_Defense_Opponent_Plays_per_Game'] = tdoppg_df.index + 1
        tdoppg_df = tdoppg_df.replace('--', np.nan)
        tdoppg_df = tdoppg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-first-downs-per-game' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
        tdofdpg_df = main_hist(total_defense_opponent_first_downs_per_game_url_current, season, str(week),
                               this_week_date_str, 'total_defense_opponent_first_downs_per_game')
        tdofdpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_First_Downs_per_Game',
                                   season: 'Current_Season_Total_Defense_Opponent_First_Downs_per_Game',
                                   str(int(season) - 1): 'Previous_Season_Total_Defense_Opponent_First_Downs_per_Game',
                                   'Last 3': 'Last_3_Total_Defense_Opponent_First_Downs_per_Game',
                                   'Last 1': 'Last_1_Total_Defense_Opponent_First_Downs_per_Game',
                                   'Home': 'At_Home_Total_Defense_Opponent_First_Downs_per_Game',
                                   'Away': 'Away_Total_Defense_Opponent_First_Downs_per_Game'
                                   }, inplace=True)
        tdofdpg_df['Team'] = tdofdpg_df['Team'].str.strip()
        if season == '2010':
            tdofdpg_df['Rank_Total_Defense_Opponent_First_Downs_per_Game'] = tdofdpg_df.index + 1
        tdofdpg_df = tdofdpg_df.replace('--', np.nan)
        tdofdpg_df = tdofdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_third_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-third-downs-per-game' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
        tdotdpg_df = main_hist(total_defense_opponent_third_downs_per_game_url_current, season, str(week),
                               this_week_date_str, 'total_defense_opponent_third_downs_per_game')
        tdotdpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Third_Downs_per_Game',
                                   season: 'Current_Season_Total_Defense_Opponent_Third_Downs_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Defense_Opponent_Third_Downs_per_Game',
                                   'Last 3': 'Last_3_Total_Defense_Opponent_Third_Downs_per_Game',
                                   'Last 1': 'Last_1_Total_Defense_Opponent_Third_Downs_per_Game',
                                   'Home': 'At_Home_Total_Defense_Opponent_Third_Downs_per_Game',
                                   'Away': 'Away_Total_Defense_Opponent_Third_Downs_per_Game'
                                   }, inplace=True)
        tdotdpg_df['Team'] = tdotdpg_df['Team'].str.strip()
        if season == '2010':
            tdotdpg_df['Rank_Total_Defense_Opponent_Third_Downs_per_Game'] = tdotdpg_df.index + 1
        tdotdpg_df = tdotdpg_df.replace('--', np.nan)
        tdotdpg_df = tdotdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_third_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-third-down-conversions-per-game' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
        tdotdcpg_df = main_hist(total_defense_opponent_third_down_conversions_per_game_url_current, season, str(week),
                                this_week_date_str, 'total_defense_opponent_third_down_conversions_per_game')
        tdotdcpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                    season: 'Current_Season_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                    'Last 3': 'Last_3_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                    'Last 1': 'Last_1_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                    'Home': 'At_Home_Total_Defense_Opponent_Third_Down_Conversions_per_Game',
                                    'Away': 'Away_Total_Defense_Opponent_Third_Down_Conversions_per_Game'
                                    }, inplace=True)
        tdotdcpg_df['Team'] = tdotdcpg_df['Team'].str.strip()
        if season == '2010':
            tdotdcpg_df['Rank_Total_Defense_Opponent_Third_Down_Conversions_per_Game'] = tdotdcpg_df.index + 1
        tdotdcpg_df = tdotdcpg_df.replace('--', np.nan)
        tdotdcpg_df = tdotdcpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_fourth_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fourth-downs-per-game' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
        tdofodpg_df = main_hist(total_defense_opponent_fourth_downs_per_game_url_current, season,
                                str(week),
                                this_week_date_str, 'total_defense_opponent_fourth_downs_per_game')
        tdofodpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                    season: 'Current_Season_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                    'Last 3': 'Last_3_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                    'Last 1': 'Last_1_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                    'Home': 'At_Home_Total_Defense_Opponent_Fourth_Downs_per_Game',
                                    'Away': 'Away_Total_Defense_Opponent_Fourth_Downs_per_Game'
                                    }, inplace=True)
        tdofodpg_df['Team'] = tdofodpg_df['Team'].str.strip()
        if season == '2010':
            tdofodpg_df['Rank_Total_Defense_Opponent_Fourth_Downs_per_Game'] = tdofodpg_df.index + 1
        tdofodpg_df = tdofodpg_df.replace('--', np.nan)
        tdofodpg_df = tdofodpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_fourth_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fourth-down-conversions-per-game' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
        tdofdcpg_df = main_hist(total_defense_opponent_fourth_down_conversions_per_game_url_current, season, str(week),
                                this_week_date_str, 'total_defense_opponent_fourth_down_conversions_per_game')
        tdofdcpg_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game',
                                    season: 'Current_Season_Total_Defense_Opponent_Fourth_Down_Conversions_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Total_Defense_Opponent_Fourth_Down_Conversions_Game',
                                    'Last 3': 'Last_3_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game',
                                    'Last 1': 'Last_1_Total_Defense_Opponent_Fourth_Downs_Conversions_per_Game',
                                    'Home': 'At_Home_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game',
                                    'Away': 'Away_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game'
                                    }, inplace=True)
        tdofdcpg_df['Team'] = tdofdcpg_df['Team'].str.strip()
        if season == '2010':
            tdofdcpg_df['Rank_Total_Defense_Opponent_Fourth_Down_Conversions_per_Game'] = tdofdcpg_df.index + 1
        tdofdcpg_df = tdofdcpg_df.replace('--', np.nan)
        tdofdcpg_df = tdofdcpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_average_time_of_possession_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-average-time-of-possession-net-of-ot' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
        tdoatop_df = main_hist(total_defense_opponent_average_time_of_possession_url_current, season,
                               str(week),
                               this_week_date_str, 'total_defense_opponent_average_time_of_possession')
        tdoatop_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Average_Time_of_Possession',
                                   season: 'Current_Season_Total_Defense_Opponent_Average_Time_of_Possession',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Defense_Opponent_Average_Time_of_Possession',
                                   'Last 3': 'Last_3_Total_Defense_Opponent_Average_Time_of_Possession',
                                   'Last 1': 'Last_1_Total_Defense_Opponent_Average_Time_of_Possession',
                                   'Home': 'At_Home_Total_Defense_Opponent_Average_Time_of_Possession',
                                   'Away': 'Away_Total_Defense_Opponent_Average_Time_of_Possession'
                                   }, inplace=True)
        tdoatop_df['Team'] = tdoatop_df['Team'].str.strip()
        if season == '2010':
            tdoatop_df['Rank_Total_Defense_Opponent_Average_Time_of_Possession'] = tdoatop_df.index + 1
        tdoatop_df = tdoatop_df.replace('--', np.nan)
        tdoatop_df = tdoatop_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_time_of_possession_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-time-of-possession-pct-net-of-ot' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
        tdptopp_df = main_hist(total_defense_opponent_time_of_possession_percentage_url_current, season,
                               str(week),
                               this_week_date_str, 'total_defense_opponent_time_of_possession_percentage')
        tdptopp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                   season: 'Current_Season_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                   'Last 3': 'Last_3_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                   'Last 1': 'Last_1_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                   'Home': 'At_Home_Total_Defense_Opponent_Time_of_Possession_Percentage',
                                   'Away': 'Away_Total_Defense_Opponent_Time_of_Possession_Percentage'
                                   }, inplace=True)
        tdptopp_df['Team'] = tdptopp_df['Team'].str.strip()
        if season == '2010':
            tdptopp_df['Rank_Total_Defense_Opponent_Time_of_Possession_Percentage'] = tdptopp_df.index + 1
        tdptopp_df = tdptopp_df.replace('--', np.nan)
        for c in tdptopp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                tdptopp_df[c] = tdptopp_df[c].str.rstrip('%').astype('float') / 100.0
        tdptopp_df = tdptopp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_third_down_conversion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-third-down-conversion-pct' \
                                                                              + '?date=' \
                                                                              + this_week_date_str
        tdotdcp_df = main_hist(total_defense_opponent_third_down_conversion_percentage_url_current, season,
                               str(week),
                               this_week_date_str, 'total_defense_opponent_third_down_conversion_percentage')
        tdotdcp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                   season: 'Current_Season_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                   'Last 3': 'Last_3_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                   'Last 1': 'Last_1_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                   'Home': 'At_Home_Total_Defense_Opponent_Third_Down_Conversion_Percentage',
                                   'Away': 'Away_Total_Defense_Opponent_Third_Down_Conversion_Percentage'
                                   }, inplace=True)
        tdotdcp_df['Team'] = tdotdcp_df['Team'].str.strip()
        if season == '2010':
            tdotdcp_df['Rank_Total_Defense_Opponent_Third_Down_Conversion_Percentage'] = tdotdcp_df.index + 1
        tdotdcp_df = tdotdcp_df.replace('--', np.nan)
        for c in tdotdcp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                tdotdcp_df[c] = tdotdcp_df[c].str.rstrip('%').astype('float') / 100.0
        tdotdcp_df = tdotdcp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_fourth_down_conversion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fourth-down-conversion-pct' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
        tdofdcp_df = main_hist(total_defense_opponent_fourth_down_conversion_percentage_url_current, season,
                               str(week),
                               this_week_date_str, 'total_defense_opponent_fourth_down_conversion_percentage')
        tdofdcp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                   season: 'Current_Season_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                   'Last 3': 'Last_3_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                   'Last 1': 'Last_1_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                   'Home': 'At_Home_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage',
                                   'Away': 'Away_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage'
                                   }, inplace=True)
        tdofdcp_df['Team'] = tdofdcp_df['Team'].str.strip()
        if season == '2010':
            tdofdcp_df['Rank_Total_Defense_Opponent_Fourth_Down_Conversion_Percentage'] = tdofdcp_df.index + 1
        tdofdcp_df = tdofdcp_df.replace('--', np.nan)
        for c in tdofdcp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                tdofdcp_df[c] = tdofdcp_df[c].str.rstrip('%').astype('float') / 100.0
        tdofdcp_df = tdofdcp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_punts_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-punts-per-play' \
                                                            + '?date=' \
                                                            + this_week_date_str
        tdoppp_df = main_hist(total_defense_opponent_punts_per_play_url_current, season,
                              str(week),
                              this_week_date_str, 'total_defense_opponent_punts_per_play')
        tdoppp_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Punts_per_Play',
                                  season: 'Current_Season_Total_Defense_Opponent_Punts_per_Play',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Defense_Opponent_Punts_per_Play',
                                  'Last 3': 'Last_3_Total_Defense_Opponent_Punts_per_Play',
                                  'Last 1': 'Last_1_Total_Defense_Opponent_Punts_per_Play',
                                  'Home': 'At_Home_Total_Defense_Opponent_Punts_per_Play',
                                  'Away': 'Away_Total_Defense_Opponent_Punts_per_Play'
                                  }, inplace=True)
        tdoppp_df['Team'] = tdoppp_df['Team'].str.strip()
        if season == '2010':
            tdoppp_df['Rank_Total_Defense_Opponent_Punts_per_Play'] = tdoppp_df.index + 1
        tdoppp_df = tdoppp_df.replace('--', np.nan)
        tdoppp_df = tdoppp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        total_defense_opponent_punts_per_offensive_score_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-punts-per-offensive-score' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
        tdoppos_df = main_hist(total_defense_opponent_punts_per_offensive_score_url_current, season,
                               str(week),
                               this_week_date_str, 'total_defense_opponent_punts_per_offensive_score')
        tdoppos_df.rename(columns={'Rank': 'Rank_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                   season: 'Current_Season_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                   'Last 3': 'Last_3_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                   'Last 1': 'Last_1_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                   'Home': 'At_Home_Total_Defense_Opponent_Punts_per_Offensive_Score',
                                   'Away': 'Away_Total_Defense_Opponent_Punts_per_Offensive_Score'
                                   }, inplace=True)
        tdoppos_df['Team'] = tdoppos_df['Team'].str.strip()
        if season == '2010':
            tdoppos_df['Rank_Total_Defense_Opponent_Punts_per_Offensive_Score'] = tdoppos_df.index + 1
        tdoppos_df = tdoppos_df.replace('--', np.nan)
        tdoppos_df = tdoppos_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_defense_opponent_rushing_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-attempts-per-game' \
                                                                         + '?date=' \
                                                                         + this_week_date_str
        rdorapg_df = main_hist(rushing_defense_opponent_rushing_attempts_per_game_url_current, season,
                               str(week),
                               this_week_date_str, 'rushing_defense_opponent_rushing_attempts_per_game')
        rdorapg_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                   season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Attempts_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                   'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                   'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                   'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Attempts_per_Game',
                                   'Away': 'Away_Rushing_Defense_Opponent_Rushing_Attempts_per_Game'
                                   }, inplace=True)
        rdorapg_df['Team'] = rdorapg_df['Team'].str.strip()
        if season == '2010':
            rdorapg_df['Rank_Rushing_Defense_Opponent_Rushing_Attempts_per_Game'] = rdorapg_df.index + 1
        rdorapg_df = rdorapg_df.replace('--', np.nan)
        rdorapg_df = rdorapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_defense_opponent_rushing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-yards-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        rdorypg_df = main_hist(rushing_defense_opponent_rushing_yards_per_game_url_current, season,
                               str(week),
                               this_week_date_str, 'rushing_defense_opponent_rushing_yards_per_game')
        rdorypg_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                   season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                   'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                   'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                   'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Yards_per_Game',
                                   'Away': 'Away_Rushing_Defense_Opponent_Rushing_Yards_per_Game'
                                   }, inplace=True)
        rdorypg_df['Team'] = rdorypg_df['Team'].str.strip()
        if season == '2010':
            rdorypg_df['Rank_Rushing_Defense_Opponent_Rushing_Yards_per_Game'] = rdorypg_df.index + 1
        rdorypg_df = rdorypg_df.replace('--', np.nan)
        rdorypg_df = rdorypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_defense_opponent_rushing_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-first-downs-per-game' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
        rdorfdpg_df = main_hist(rushing_defense_opponent_rushing_first_downs_per_game_url_current, season,
                                str(week),
                                this_week_date_str, 'rushing_defense_opponent_rushing_first_downs_per_game')
        rdorfdpg_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                    season: 'Current_Season_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                    'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                    'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                    'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game',
                                    'Away': 'Away_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game'
                                    }, inplace=True)
        rdorfdpg_df['Team'] = rdorfdpg_df['Team'].str.strip()
        if season == '2010':
            rdorfdpg_df['Rank_Rushing_Defense_Opponent_Rushing_First_Downs_per_Game'] = rdorfdpg_df.index + 1
        rdorfdpg_df = rdorfdpg_df.replace('--', np.nan)
        rdorfdpg_df = rdorfdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_defense_opponent_yards_per_rush_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-rush-attempt' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        rdoypra_df = main_hist(rushing_defense_opponent_yards_per_rush_attempt_url_current, season,
                               str(week),
                               this_week_date_str, 'rushing_defense_opponent_yards_per_rush_attempt')
        rdoypra_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                   season: 'Current_Season_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                   str(int(
                                       season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                   'Last 3': 'Last_3_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                   'Last 1': 'Last_1_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                   'Home': 'At_Home_Rushing_Defense_Opponent_Yards_per_Rush_Attempt',
                                   'Away': 'Away_Rushing_Defense_Opponent_Yards_per_Rush_Attempt'
                                   }, inplace=True)
        rdoypra_df['Team'] = rdoypra_df['Team'].str.strip()
        if season == '2010':
            rdoypra_df['Rank_Rushing_Defense_Opponent_Yards_per_Rush_Attempt'] = rdoypra_df.index + 1
        rdoypra_df = rdoypra_df.replace('--', np.nan)
        rdoypra_df = rdoypra_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_defense_opponent_rushing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-play-pct' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
        rdorpp_df = main_hist(rushing_defense_opponent_rushing_play_percentage_url_current, season,
                              str(week),
                              this_week_date_str, 'rushing_defense_opponent_rushing_play_percentage')
        rdorpp_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                  season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                  'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                  'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                  'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Play_Percentage',
                                  'Away': 'Away_Rushing_Defense_Opponent_Rushing_Play_Percentage'
                                  }, inplace=True)
        rdorpp_df['Team'] = rdorpp_df['Team'].str.strip()
        if season == '2010':
            rdorpp_df['Rank_Rushing_Defense_Opponent_Rushing_Play_Percentage'] = rdorpp_df.index + 1
        rdorpp_df = rdorpp_df.replace('--', np.nan)
        for c in rdorpp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                rdorpp_df[c] = rdorpp_df[c].str.rstrip('%').astype('float') / 100.0
        rdorpp_df = rdorpp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        rushing_defense_opponent_rushing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-rushing-yards-pct' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
        rdoryp_df = main_hist(rushing_defense_opponent_rushing_yards_percentage_url_current, season,
                              str(week),
                              this_week_date_str, 'rushing_defense_opponent_rushing_yards_percentage')
        rdoryp_df.rename(columns={'Rank': 'Rank_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                  season: 'Current_Season_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                  'Last 3': 'Last_3_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                  'Last 1': 'Last_1_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                  'Home': 'At_Home_Rushing_Defense_Opponent_Rushing_Yards_Percentage',
                                  'Away': 'Away_Rushing_Defense_Opponent_Rushing_Yards_Percentage'
                                  }, inplace=True)
        rdoryp_df['Team'] = rdoryp_df['Team'].str.strip()
        if season == '2010':
            rdoryp_df['Rank_Rushing_Defense_Opponent_Rushing_Yards_Percentage'] = rdoryp_df.index + 1
        rdoryp_df = rdoryp_df.replace('--', np.nan)
        for c in rdoryp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                rdoryp_df[c] = rdoryp_df[c].str.rstrip('%').astype('float') / 100.0
        rdoryp_df = rdoryp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_pass_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-pass-attempts-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        pdopapg_df = main_hist(passing_defense_opponent_pass_attempts_per_game_url_current, season,
                               str(week),
                               this_week_date_str, 'passing_defense_opponent_pass_attempts_per_game')
        pdopapg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                   season: 'Current_Season_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                   'Last 3': 'Last_3_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                   'Last 1': 'Last_1_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                   'Home': 'At_Home_Passing_Defense_Opponent_Pass_Attempts_per_Game',
                                   'Away': 'Away_Passing_Defense_Opponent_Pass_Attempts_per_Game'
                                   }, inplace=True)
        pdopapg_df['Team'] = pdopapg_df['Team'].str.strip()
        if season == '2010':
            pdopapg_df['Rank_Passing_Defense_Opponent_Pass_Attempts_per_Game'] = pdopapg_df.index + 1
        pdopapg_df = pdopapg_df.replace('--', np.nan)
        pdopapg_df = pdopapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_completions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-completions-per-game' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
        pdocpg_df = main_hist(passing_defense_opponent_completions_per_game_url_current, season,
                              str(week),
                              this_week_date_str, 'passing_defense_opponent_completions_per_game')
        pdocpg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Completions_per_Game',
                                  season: 'Current_Season_Passing_Defense_Opponent_Completions_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Defense_Opponent_Completions_per_Game',
                                  'Last 3': 'Last_3_Passing_Defense_Opponent_Completions_per_Game',
                                  'Last 1': 'Last_1_Passing_Defense_Opponent_Completions_per_Game',
                                  'Home': 'At_Home_Passing_Defense_Opponent_Completions_per_Game',
                                  'Away': 'Away_Passing_Defense_Opponent_Completions_per_Game'
                                  }, inplace=True)
        pdocpg_df['Team'] = pdocpg_df['Team'].str.strip()
        if season == '2010':
            pdocpg_df['Rank_Passing_Defense_Opponent_Completions_per_Game'] = pdocpg_df.index + 1
        pdocpg_df = pdocpg_df.replace('--', np.nan)
        pdocpg_df = pdocpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_incompletions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-incompletions-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        pdoipg_df = main_hist(passing_defense_opponent_incompletions_per_game_url_current, season,
                              str(week),
                              this_week_date_str, 'passing_defense_opponent_incompletions_per_game')
        pdoipg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Incompletions_per_Game',
                                  season: 'Current_Season_Passing_Defense_Opponent_Incompletions_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Defense_Opponent_Incompletions_per_Game',
                                  'Last 3': 'Last_3_Passing_Defense_Opponent_Incompletions_per_Game',
                                  'Last 1': 'Last_1_Passing_Defense_Opponent_Incompletions_per_Game',
                                  'Home': 'At_Home_Passing_Defense_Opponent_Incompletions_per_Game',
                                  'Away': 'Away_Passing_Defense_Opponent_Incompletions_per_Game'
                                  }, inplace=True)
        pdoipg_df['Team'] = pdoipg_df['Team'].str.strip()
        if season == '2010':
            pdoipg_df['Rank_Passing_Defense_Opponent_Incompletions_per_Game'] = pdoipg_df.index + 1
        pdoipg_df = pdoipg_df.replace('--', np.nan)
        pdoipg_df = pdoipg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_completion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-completion-pct' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
        pdocp_df = main_hist(passing_defense_opponent_completion_percentage_url_current, season,
                             str(week),
                             this_week_date_str, 'passing_defense_opponent_completion_percentage')
        pdocp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Completion_Percentage',
                                 season: 'Current_Season_Passing_Defense_Opponent_Completion_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Defense_Opponent_Completion_Percentage',
                                 'Last 3': 'Last_3_Passing_Defense_Opponent_Completion_Percentage',
                                 'Last 1': 'Last_1_Passing_Defense_Opponent_Completion_Percentage',
                                 'Home': 'At_Home_Passing_Defense_Opponent_Completion_Percentage',
                                 'Away': 'Away_Passing_Defense_Opponent_Completion_Percentage'
                                 }, inplace=True)
        pdocp_df['Team'] = pdocp_df['Team'].str.strip()
        if season == '2010':
            pdocp_df['Rank_Passing_Defense_Opponent_Completion_Percentage'] = pdocp_df.index + 1
        pdocp_df = pdocp_df.replace('--', np.nan)
        for c in pdocp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                pdocp_df[c] = pdocp_df[c].str.rstrip('%').astype('float') / 100.0
        pdocp_df = pdocp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_passing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-yards-per-game' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        pdopypg_df = main_hist(passing_defense_opponent_passing_yards_per_game_url_current, season,
                               str(week),
                               this_week_date_str, 'passing_defense_opponent_passing_yards_per_game')
        pdopypg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                   season: 'Current_Season_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                   'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                   'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                   'Home': 'At_Home_Passing_Defense_Opponent_Passing_Yards_per_Game',
                                   'Away': 'Away_Passing_Defense_Opponent_Passing_Yards_per_Game'
                                   }, inplace=True)
        pdopypg_df['Team'] = pdopypg_df['Team'].str.strip()
        if season == '2010':
            pdopypg_df['Rank_Passing_Defense_Opponent_Passing_Yards_per_Game'] = pdopypg_df.index + 1
        pdopypg_df = pdopypg_df.replace('--', np.nan)
        pdopypg_df = pdopypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_passing_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-first-downs-per-game' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
        pdofdpg_df = main_hist(passing_defense_opponent_passing_first_downs_per_game_url_current, season,
                               str(week),
                               this_week_date_str, 'passing_defense_opponent_passing_first_downs_per_game')
        pdofdpg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                   season: 'Current_Season_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                   'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                   'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                   'Home': 'At_Home_Passing_Defense_Opponent_Passing_First_Downs_per_Game',
                                   'Away': 'Away_Passing_Defense_Opponent_Passing_First_Downs_per_Game'
                                   }, inplace=True)
        pdofdpg_df['Team'] = pdofdpg_df['Team'].str.strip()
        if season == '2010':
            pdofdpg_df['Rank_Passing_Defense_Opponent_Passing_First_Downs_per_Game'] = pdofdpg_df.index + 1
        pdofdpg_df = pdofdpg_df.replace('--', np.nan)
        pdofdpg_df = pdofdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_average_team_passer_rating_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-average-team-passer-rating' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
        pdoatpr_df = main_hist(passing_defense_opponent_average_team_passer_rating_url_current, season,
                               str(week),
                               this_week_date_str, 'passing_defense_opponent_average_team_passer_rating')
        pdoatpr_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                   season: 'Current_Season_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                   str(int(
                                       season) - 1): 'Previous_Season_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                   'Last 3': 'Last_3_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                   'Last 1': 'Last_1_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                   'Home': 'At_Home_Passing_Defense_Opponent_Average_Team_Passer_Rating',
                                   'Away': 'Away_Passing_Defense_Opponent_Average_Team_Passer_Rating'
                                   }, inplace=True)
        pdoatpr_df['Team'] = pdoatpr_df['Team'].str.strip()
        if season == '2010':
            pdoatpr_df['Rank_Passing_Defense_Opponent_Average_Team_Passer_Rating'] = pdoatpr_df.index + 1
        pdoatpr_df = pdoatpr_df.replace('--', np.nan)
        pdoatpr_df = pdoatpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_team_sack_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/sack-pct' \
                                                           + '?date=' \
                                                           + this_week_date_str
        pdtsp_df = main_hist(passing_defense_team_sack_percentage_url_current, season,
                             str(week),
                             this_week_date_str, 'passing_defense_team_sack_percentage')
        pdtsp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Team_Sack_Percentage',
                                 season: 'Current_Season_Passing_Defense_Team_Sack_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Defense_Team_Sack_Percentage',
                                 'Last 3': 'Last_3_Passing_Defense_Team_Sack_Percentage',
                                 'Last 1': 'Last_1_Passing_Defense_Team_Sack_Percentage',
                                 'Home': 'At_Home_Passing_Defense_Team_Sack_Percentage',
                                 'Away': 'Away_Passing_Defense_Team_Sack_Percentage'
                                 }, inplace=True)
        pdtsp_df['Team'] = pdtsp_df['Team'].str.strip()
        if season == '2010':
            pdtsp_df['Rank_Passing_Defense_Team_Sack_Percentage'] = pdtsp_df.index + 1
        pdtsp_df = pdtsp_df.replace('--', np.nan)
        for c in pdtsp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                pdtsp_df[c] = pdtsp_df[c].str.rstrip('%').astype('float') / 100.0
        pdtsp_df = pdtsp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_passing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-play-pct' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
        pdoppp_df = main_hist(passing_defense_opponent_passing_play_percentage_url_current, season,
                              str(week),
                              this_week_date_str, 'passing_defense_opponent_passing_play_percentage')
        pdoppp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_Play_Percentage',
                                  season: 'Current_Season_Passing_Defense_Opponent_Passing_Play_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_Play_Percentage',
                                  'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_Play_Percentage',
                                  'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_Play_Percentage',
                                  'Home': 'At_Home_Passing_Defense_Opponent_Passing_Play_Percentage',
                                  'Away': 'Away_Passing_Defense_Opponent_Passing_Play_Percentage'
                                  }, inplace=True)
        pdoppp_df['Team'] = pdoppp_df['Team'].str.strip()
        if season == '2010':
            pdoppp_df['Rank_Passing_Defense_Opponent_Passing_Play_Percentage'] = pdoppp_df.index + 1
        pdoppp_df = pdoppp_df.replace('--', np.nan)
        for c in pdoppp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                pdoppp_df[c] = pdoppp_df[c].str.rstrip('%').astype('float') / 100.0
        pdoppp_df = pdoppp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_passing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-passing-yards-pct' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
        pdopyp_df = main_hist(passing_defense_opponent_passing_yards_percentage_url_current, season,
                              str(week),
                              this_week_date_str, 'passing_defense_opponent_passing_yards_percentage')
        pdopyp_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                  season: 'Current_Season_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                  'Last 3': 'Last_3_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                  'Last 1': 'Last_1_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                  'Home': 'At_Home_Passing_Defense_Opponent_Passing_Yards_Percentage',
                                  'Away': 'Away_Passing_Defense_Opponent_Passing_Yards_Percentage'
                                  }, inplace=True)
        pdopyp_df['Team'] = pdopyp_df['Team'].str.strip()
        if season == '2010':
            pdopyp_df['Rank_Passing_Defense_Opponent_Passing_Yards_Percentage'] = pdopyp_df.index + 1
        pdopyp_df = pdopyp_df.replace('--', np.nan)
        for c in pdopyp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                pdopyp_df[c] = pdopyp_df[c].str.rstrip('%').astype('float') / 100.0
        pdopyp_df = pdopyp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_sacks_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/sacks-per-game' \
                                                     + '?date=' \
                                                     + this_week_date_str
        pdspg_df = main_hist(passing_defense_sacks_per_game_url_current, season,
                             str(week),
                             this_week_date_str, 'passing_defense_sacks_per_game')
        pdspg_df.rename(columns={'Rank': 'Rank_Passing_Defense_Sacks_per_Game',
                                 season: 'Current_Season_Passing_Defense_Sacks_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Defense_Sacks_per_Game',
                                 'Last 3': 'Last_3_Passing_Defense_Sacks_per_Game',
                                 'Last 1': 'Last_1_Passing_Defense_Sacks_per_Game',
                                 'Home': 'At_Home_Passing_Defense_Sacks_per_Game',
                                 'Away': 'Away_Passing_Defense_Sacks_per_Game'
                                 }, inplace=True)
        pdspg_df['Team'] = pdspg_df['Team'].str.strip()
        if season == '2010':
            pdspg_df[
                'Rank_Passing_Defense_Sacks_per_Game'] = pdspg_df.index + 1
        pdspg_df = pdspg_df.replace('--', np.nan)
        pdspg_df = pdspg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_yards_per_pass_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-pass-attempt' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        pdoyppa_df = main_hist(passing_defense_opponent_yards_per_pass_attempt_url_current, season,
                               str(week),
                               this_week_date_str, 'passing_defense_opponent_yards_per_pass_attempt')
        pdoyppa_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                   season: 'Current_Season_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                   str(int(
                                       season) - 1): 'Previous_Season_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                   'Last 3': 'Last_3_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                   'Last 1': 'Last_1_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                   'Home': 'At_Home_Passing_Defense_Opponent_Yards_per_Pass_Attempt',
                                   'Away': 'Away_Passing_Defense_Opponent_Yards_per_Pass_Attempt'
                                   }, inplace=True)
        pdoyppa_df['Team'] = pdoyppa_df['Team'].str.strip()
        if season == '2010':
            pdoyppa_df[
                'Rank_Passing_Defense_Opponent_Yards_per_Pass_Attempt'] = pdoyppa_df.index + 1
        pdoyppa_df = pdoyppa_df.replace('--', np.nan)
        pdoyppa_df = pdoyppa_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        passing_defense_opponent_yards_per_completion_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-completion' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
        pdoypc_df = main_hist(passing_defense_opponent_yards_per_completion_url_current, season,
                              str(week),
                              this_week_date_str, 'passing_defense_opponent_yards_per_completion')
        pdoypc_df.rename(columns={'Rank': 'Rank_Passing_Defense_Opponent_Yards_per_Completion',
                                  season: 'Current_Season_Passing_Defense_Opponent_Yards_per_Completion',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Defense_Opponent_Yards_per_Completion',
                                  'Last 3': 'Last_3_Passing_Defense_Opponent_Yards_per_Completion',
                                  'Last 1': 'Last_1_Passing_Defense_Opponent_Yards_per_Completion',
                                  'Home': 'At_Home_Passing_Defense_Opponent_Yards_per_Completion',
                                  'Away': 'Away_Passing_Defense_Opponent_Yards_per_Completion'
                                  }, inplace=True)
        pdoypc_df['Team'] = pdoypc_df['Team'].str.strip()
        if season == '2010':
            pdoypc_df[
                'Rank_Passing_Defense_Opponent_Yards_per_Completion'] = pdoypc_df.index + 1
        pdoypc_df = pdoypc_df.replace('--', np.nan)
        pdoypc_df = pdoypc_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_defense_opponent_field_goal_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-field-goal-attempts-per-game' \
                                                                                  + '?date=' \
                                                                                  + this_week_date_str
        stdofgapg_df = main_hist(special_teams_defense_opponent_field_goal_attempts_per_game_url_current, season,
                                 str(week),
                                 this_week_date_str, 'special_teams_defense_opponent_field_goal_attempts_per_game')
        stdofgapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                     season: 'Current_Season_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                     str(int(
                                         season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                     'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                     'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                     'Home': 'At_Home_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game',
                                     'Away': 'Away_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game'
                                     }, inplace=True)
        stdofgapg_df['Team'] = stdofgapg_df['Team'].str.strip()
        if season == '2010':
            stdofgapg_df[
                'Rank_Special_Teams_Defense_Opponent_Field_Goal_Attempts_per_Game'] = stdofgapg_df.index + 1
        stdofgapg_df = stdofgapg_df.replace('--', np.nan)
        stdofgapg_df = stdofgapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_defense_opponent_field_goals_made_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-field-goals-made-per-game' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
        stdofgmpg_df = main_hist(special_teams_defense_opponent_field_goals_made_per_game_url_current,
                                 season,
                                 str(week),
                                 this_week_date_str,
                                 'special_teams_defense_opponent_field_goals_made_per_game')
        stdofgmpg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                     season: 'Current_Season_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                     str(int(
                                         season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                     'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                     'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                     'Home': 'At_Home_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game',
                                     'Away': 'Away_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game'
                                     }, inplace=True)
        stdofgmpg_df['Team'] = stdofgmpg_df['Team'].str.strip()
        if season == '2010':
            stdofgmpg_df[
                'Rank_Special_Teams_Defense_Opponent_Field_Goals_Made_per_Game'] = stdofgmpg_df.index + 1
        stdofgmpg_df = stdofgmpg_df.replace('--', np.nan)
        stdofgmpg_df = stdofgmpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_defense_opponent_punt_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-punt-attempts-per-game' \
                                                                            + '?date=' \
                                                                            + this_week_date_str
        stdopapg_df = main_hist(special_teams_defense_opponent_punt_attempts_per_game_url_current, season,
                                str(week),
                                this_week_date_str,
                                'special_teams_defense_opponent_punt_attempts_per_game')
        stdopapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                    season: 'Current_Season_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                    str(int(
                                        season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                    'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                    'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                    'Home': 'At_Home_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game',
                                    'Away': 'Away_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game'
                                    }, inplace=True)
        stdopapg_df['Team'] = stdopapg_df['Team'].str.strip()
        if season == '2010':
            stdopapg_df[
                'Rank_Special_Teams_Defense_Opponent_Punt_Attempts_per_Game'] = stdopapg_df.index + 1
        stdopapg_df = stdopapg_df.replace('--', np.nan)
        stdopapg_df = stdopapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        special_teams_defense_opponent_gross_punt_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-gross-punt-yards-per-game' \
                                                                               + '?date=' \
                                                                               + this_week_date_str
        stdogpypg_df = main_hist(special_teams_defense_opponent_gross_punt_yards_per_game_url_current,
                                 season,
                                 str(week),
                                 this_week_date_str,
                                 'special_teams_defense_opponent_gross_punt_yards_per_game')
        stdogpypg_df.rename(columns={'Rank': 'Rank_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                     season: 'Current_Season_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                     str(int(
                                         season) - 1): 'Previous_Season_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                     'Last 3': 'Last_3_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                     'Last 1': 'Last_1_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                     'Home': 'At_Home_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game',
                                     'Away': 'Away_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game'
                                     }, inplace=True)
        stdogpypg_df['Team'] = stdogpypg_df['Team'].str.strip()
        if season == '2010':
            stdogpypg_df[
                'Rank_Special_Teams_Defense_Opponent_Gross_Punt_Yards_per_Game'] = stdogpypg_df.index + 1
        stdogpypg_df = stdogpypg_df.replace('--', np.nan)
        stdogpypg_df = stdogpypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_interceptions_thrown_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/interceptions-thrown-per-game' \
                                                              + '?date=' \
                                                              + this_week_date_str
        titpg_df = main_hist(turnovers_interceptions_thrown_per_game_url_current, season, str(week),
                             this_week_date_str,
                             'turnovers_interceptions_thrown_per_game')
        titpg_df.rename(columns={'Rank': 'Rank_Turnovers_Interceptions_Thrown_per_Game',
                                 season: 'Current_Season_Turnovers_Interceptions_Thrown_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Turnovers_Interceptions_Thrown_per_Game',
                                 'Last 3': 'Last_3_Turnovers_Interceptions_Thrown_per_Game',
                                 'Last 1': 'Last_1_Turnovers_Interceptions_Thrown_per_Game',
                                 'Home': 'At_Home_Turnovers_Interceptions_Thrown_per_Game',
                                 'Away': 'Away_Turnovers_Interceptions_Thrown_per_Game'
                                 }, inplace=True)
        titpg_df['Team'] = titpg_df['Team'].str.strip()
        if season == '2010':
            titpg_df[
                'Rank_Turnovers_Interceptions_Thrown_per_Game'] = titpg_df.index + 1
        titpg_df = titpg_df.replace('--', np.nan)
        titpg_df = titpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_fumbles_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fumbles-per-game' \
                                                 + '?date=' \
                                                 + this_week_date_str
        tfpg_df = main_hist(turnovers_fumbles_per_game_url_current, season, str(week),
                            this_week_date_str,
                            'turnovers_fumbles_per_game')
        tfpg_df.rename(columns={'Rank': 'Rank_Turnovers_Fumbles_per_Game',
                                season: 'Current_Season_Turnovers_Fumbles_per_Game',
                                str(int(
                                    season) - 1): 'Previous_Season_Turnovers_Fumbles_per_Game',
                                'Last 3': 'Last_3_Turnovers_Fumbles_per_Game',
                                'Last 1': 'Last_1_Turnovers_Fumbles_per_Game',
                                'Home': 'At_Home_Turnovers_Fumbles_per_Game',
                                'Away': 'Away_Turnovers_Fumbles_per_Game'
                                }, inplace=True)
        tfpg_df['Team'] = tfpg_df['Team'].str.strip()
        if season == '2010':
            tfpg_df[
                'Rank_Turnovers_Fumbles_per_Game'] = tfpg_df.index + 1
        tfpg_df = tfpg_df.replace('--', np.nan)
        tfpg_df = tfpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_fumbles_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fumbles-lost-per-game' \
                                                      + '?date=' \
                                                      + this_week_date_str
        tflpg_df = main_hist(turnovers_fumbles_lost_per_game_url_current, season, str(week),
                             this_week_date_str,
                             'turnovers_fumbles_lost_per_game')
        tflpg_df.rename(columns={'Rank': 'Rank_Turnovers_Fumbles_Lost_per_Game',
                                 season: 'Current_Season_Turnovers_Fumbles_Lost_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Turnovers_Fumbles_Lost_per_Game',
                                 'Last 3': 'Last_3_Turnovers_Fumbles_Lost_per_Game',
                                 'Last 1': 'Last_1_Turnovers_Fumbles_Lost_per_Game',
                                 'Home': 'At_Home_Turnovers_Fumbles_Lost_per_Game',
                                 'Away': 'Away_Turnovers_Fumbles_Lost_per_Game'
                                 }, inplace=True)
        tflpg_df['Team'] = tflpg_df['Team'].str.strip()
        if season == '2010':
            tflpg_df[
                'Rank_Turnovers_Fumbles_Lost_per_Game'] = tflpg_df.index + 1
        tflpg_df = tflpg_df.replace('--', np.nan)
        tflpg_df = tflpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_fumbles_not_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fumbles-not-lost-per-game' \
                                                          + '?date=' \
                                                          + this_week_date_str
        tfnlpg_df = main_hist(turnovers_fumbles_not_lost_per_game_url_current, season, str(week),
                              this_week_date_str,
                              'turnovers_fumbles_not_lost_per_game')
        tfnlpg_df.rename(columns={'Rank': 'Rank_Turnovers_Fumbles_Not_Lost_per_Game',
                                  season: 'Current_Season_Turnovers_Fumbles_Not_Lost_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Turnovers_Fumbles_Not_Lost_per_Game',
                                  'Last 3': 'Last_3_Turnovers_Fumbles_Not_Lost_per_Game',
                                  'Last 1': 'Last_1_Turnovers_Fumbles_Not_Lost_per_Game',
                                  'Home': 'At_Home_Turnovers_Fumbles_Not_Lost_per_Game',
                                  'Away': 'Away_Turnovers_Fumbles_Not_Lost_per_Game'
                                  }, inplace=True)
        tfnlpg_df['Team'] = tfnlpg_df['Team'].str.strip()
        if season == '2010':
            tfnlpg_df[
                'Rank_Turnovers_Fumbles_Not_Lost_per_Game'] = tfnlpg_df.index + 1
        tfnlpg_df = tfnlpg_df.replace('--', np.nan)
        tfnlpg_df = tfnlpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_giveaways_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/giveaways-per-game' \
                                                   + '?date=' \
                                                   + this_week_date_str
        tgpg_df = main_hist(turnovers_giveaways_per_game_url_current, season, str(week), this_week_date_str,
                            'turnovers_giveaways_per_game')
        tgpg_df.rename(columns={'Rank': 'Rank_Turnovers_Giveaways_per_Game',
                                season: 'Current_Season_Turnovers_Giveaways_per_Game',
                                str(int(season) - 1): 'Previous_Season_Turnovers_Giveaways_per_Game',
                                'Last 3': 'Last_3_Turnovers_Giveaways_per_Game',
                                'Last 1': 'Last_1_Turnovers_Giveaways_per_Game',
                                'Home': 'At_Home_Turnovers_Giveaways_per_Game',
                                'Away': 'Away_Turnovers_Giveaways_per_Game'
                                }, inplace=True)
        tgpg_df['Team'] = tgpg_df['Team'].str.strip()
        if season == '2010':
            tgpg_df['Rank_Turnovers_Giveaways_per_Game'] = tgpg_df.index + 1
        tgpg_df = tgpg_df.replace('--', np.nan)
        tgpg_df = tgpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_turnover_margin_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/turnover-margin-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        ttmpg_df = main_hist(turnovers_turnover_margin_per_game_url_current, season, str(week), this_week_date_str,
                             'turnovers_turnover_margin_per_game')
        ttmpg_df.rename(columns={'Rank': 'Rank_Turnovers_Turnover_Margin_per_Game',
                                 season: 'Current_Season_Turnovers_Turnover_Margin_per_Game',
                                 str(int(season) - 1): 'Previous_Season_Turnovers_Turnover_Margin_per_Game',
                                 'Last 3': 'Last_3_Turnovers_Turnover_Margin_per_Game',
                                 'Last 1': 'Last_1_Turnovers_Turnover_Margin_per_Game',
                                 'Home': 'At_Home_Turnovers_Turnover_Margin_per_Game',
                                 'Away': 'Away_Turnovers_Turnover_Margin_per_Game'
                                 }, inplace=True)
        ttmpg_df['Team'] = ttmpg_df['Team'].str.strip()
        if season == '2010':
            ttmpg_df['Rank_Turnovers_Turnover_Margin_per_Game'] = ttmpg_df.index + 1
        ttmpg_df = ttmpg_df.replace('--', np.nan)
        ttmpg_df = ttmpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_interceptions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/interceptions-per-game' \
                                                       + '?date=' \
                                                       + this_week_date_str
        tipg_df = main_hist(turnovers_interceptions_per_game_url_current, season, str(week),
                            this_week_date_str,
                            'turnovers_interceptions_per_game')
        tipg_df.rename(columns={'Rank': 'Rank_Turnovers_Interceptions_per_Game',
                                season: 'Current_Season_Turnovers_Interceptions_per_Game',
                                str(int(season) - 1): 'Previous_Season_Turnovers_Interceptions_per_Game',
                                'Last 3': 'Last_3_Turnovers_Interceptions_per_Game',
                                'Last 1': 'Last_1_Turnovers_Interceptions_per_Game',
                                'Home': 'At_Home_Turnovers_Interceptions_per_Game',
                                'Away': 'Away_Turnovers_Interceptions_per_Game'
                                }, inplace=True)
        tipg_df['Team'] = tipg_df['Team'].str.strip()
        if season == '2010':
            tipg_df['Rank_Turnovers_Interceptions_per_Game'] = tipg_df.index + 1
        tipg_df = tipg_df.replace('--', np.nan)
        tipg_df = tipg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_fumbles_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumbles-per-game' \
                                                          + '?date=' \
                                                          + this_week_date_str
        tofpg_df = main_hist(turnovers_opponent_fumbles_per_game_url_current, season, str(week),
                             this_week_date_str,
                             'turnovers_opponent_fumbles_per_game')
        tofpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumbles_per_Game',
                                 season: 'Current_Season_Turnovers_Opponent_Fumbles_per_Game',
                                 str(int(season) - 1): 'Previous_Season_Turnovers_Opponent_Fumbles_per_Game',
                                 'Last 3': 'Last_3_Turnovers_Opponent_Fumbles_per_Game',
                                 'Last 1': 'Last_1_Turnovers_Opponent_Fumbles_per_Game',
                                 'Home': 'At_Home_Turnovers_Opponent_Fumbles_per_Game',
                                 'Away': 'Away_Turnovers_Opponent_Fumbles_per_Game'
                                 }, inplace=True)
        tofpg_df['Team'] = tofpg_df['Team'].str.strip()
        if season == '2010':
            tofpg_df['Rank_Turnovers_Opponent_Fumbles_per_Game'] = tofpg_df.index + 1
        tofpg_df = tofpg_df.replace('--', np.nan)
        tofpg_df = tofpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_fumbles_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumbles-lost-per-game' \
                                                               + '?date=' \
                                                               + this_week_date_str
        toflpg_df = main_hist(turnovers_opponent_fumbles_lost_per_game_url_current, season, str(week),
                              this_week_date_str,
                              'turnovers_opponent_fumbles_lost_per_game')
        toflpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                  season: 'Current_Season_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                  'Last 3': 'Last_3_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                  'Last 1': 'Last_1_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                  'Home': 'At_Home_Turnovers_Opponent_Fumbles_Lost_per_Game',
                                  'Away': 'Away_Turnovers_Opponent_Fumbles_Lost_per_Game'
                                  }, inplace=True)
        toflpg_df['Team'] = toflpg_df['Team'].str.strip()
        if season == '2010':
            toflpg_df['Rank_Turnovers_Opponent_Fumbles_Lost_per_Game'] = toflpg_df.index + 1
        toflpg_df = toflpg_df.replace('--', np.nan)
        toflpg_df = toflpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_fumbles_not_lost_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumbles-not-lost-per-game' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
        tofnlpg_df = main_hist(turnovers_opponent_fumbles_not_lost_per_game_url_current, season, str(week),
                               this_week_date_str,
                               'turnovers_opponent_fumbles_not_lost_per_game')
        tofnlpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                   season: 'Current_Season_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                   'Last 3': 'Last_3_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                   'Last 1': 'Last_1_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                   'Home': 'At_Home_Turnovers_Opponent_Fumbles_Not_Lost_per_Game',
                                   'Away': 'Away_Turnovers_Opponent_Fumbles_Not_Lost_per_Game'
                                   }, inplace=True)
        tofnlpg_df['Team'] = tofnlpg_df['Team'].str.strip()
        if season == '2010':
            tofnlpg_df['Rank_Turnovers_Opponent_Fumbles_Not_Lost_per_Game'] = tofnlpg_df.index + 1
        tofnlpg_df = tofnlpg_df.replace('--', np.nan)
        tofnlpg_df = tofnlpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_takeaways_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/takeaways-per-game' \
                                                   + '?date=' \
                                                   + this_week_date_str
        tttapg_df = main_hist(turnovers_takeaways_per_game_url_current, season, str(week), this_week_date_str,
                              'turnovers_takeaways_per_game')
        tttapg_df.rename(columns={'Rank': 'Rank_Turnovers_Takeaways_per_Game',
                                  season: 'Current_Season_Turnovers_Takeaways_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Turnovers_Takeaways_per_Game',
                                  'Last 3': 'Last_3_Turnovers_Takeaways_per_Game',
                                  'Last 1': 'Last_1_Turnovers_Takeaways_per_Game',
                                  'Home': 'At_Home_Turnovers_Takeaways_per_Game',
                                  'Away': 'Away_Turnovers_Takeaways_per_Game'
                                  }, inplace=True)
        tttapg_df['Team'] = tttapg_df['Team'].str.strip()
        if season == '2010':
            tttapg_df['Rank_Turnovers_Takeaways_per_Game'] = tttapg_df.index + 1
        tttapg_df = tttapg_df.replace('--', np.nan)
        tttapg_df = tttapg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_turnover_margin_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-turnover-margin-per-game' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
        totmpg_df = main_hist(turnovers_opponent_turnover_margin_per_game_url_current, season, str(week),
                              this_week_date_str,
                              'turnovers_opponent_turnover_margin_per_game')
        totmpg_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Turnover_Margin_per_Game',
                                  season: 'Current_Season_Turnovers_Opponent_Turnover_Margin_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Turnovers_Opponent_Turnover_Margin_per_Game',
                                  'Last 3': 'Last_3_Turnovers_Opponent_Turnover_Margin_per_Game',
                                  'Last 1': 'Last_1_Turnovers_Opponent_Turnover_Margin_per_Game',
                                  'Home': 'At_Home_Turnovers_Opponent_Turnover_Margin_per_Game',
                                  'Away': 'Away_Turnovers_Turnover_Margin_per_Game'
                                  }, inplace=True)
        totmpg_df['Team'] = totmpg_df['Team'].str.strip()
        if season == '2010':
            totmpg_df['Rank_Turnovers_Opponent_Turnover_Margin_per_Game'] = totmpg_df.index + 1
        totmpg_df = totmpg_df.replace('--', np.nan)
        totmpg_df = totmpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_interceptions_thrown_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/pass-intercepted-pct' \
                                                                + '?date=' \
                                                                + this_week_date_str
        titp_df = main_hist(turnovers_interceptions_thrown_percentage_url_current, season, str(week),
                            this_week_date_str,
                            'turnovers_interceptions_thrown_percentage')
        titp_df.rename(columns={'Rank': 'Rank_Turnovers_Interceptions_Thrown_Percentage',
                                season: 'Current_Season_Turnovers_Interceptions_Thrown_Percentage',
                                str(int(
                                    season) - 1): 'Previous_Season_Turnovers_Interceptions_Thrown_Percentage',
                                'Last 3': 'Last_3_Turnovers_Interceptions_Thrown_Percentage',
                                'Last 1': 'Last_1_Turnovers_Interceptions_Thrown_Percentage',
                                'Home': 'At_Home_Turnovers_Interceptions_Thrown_Percentage',
                                'Away': 'Away_Turnovers_Interceptions_Thrown_Percentage'
                                }, inplace=True)
        titp_df['Team'] = titp_df['Team'].str.strip()
        if season == '2010':
            titp_df['Rank_Turnovers_Interceptions_Thrown_Percentage'] = titp_df.index + 1
        titp_df = titp_df.replace('--', np.nan)
        for c in titp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                titp_df[c] = titp_df[c].str.rstrip('%').astype('float') / 100.0
        titp_df = titp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/fumble-recovery-pct' \
                                                           + '?date=' \
                                                           + this_week_date_str
        tfrp_df = main_hist(turnovers_fumble_recovery_percentage_url_current, season, str(week),
                            this_week_date_str,
                            'turnovers_fumble_recovery_percentage')
        tfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Fumble_Recovery_Percentage',
                                season: 'Current_Season_Turnovers_Fumble_Recovery_Percentage',
                                str(int(
                                    season) - 1): 'Previous_Season_Turnovers_Fumble_Recovery_Percentage',
                                'Last 3': 'Last_3_Turnovers_Fumble_Recovery_Percentage',
                                'Last 1': 'Last_1_Turnovers_Fumble_Recovery_Percentage',
                                'Home': 'At_Home_Turnovers_Fumble_Recovery_Percentage',
                                'Away': 'Away_Turnovers_Fumble_Recovery_Percentage'
                                }, inplace=True)
        tfrp_df['Team'] = tfrp_df['Team'].str.strip()
        if season == '2010':
            tfrp_df['Rank_Turnovers_Fumble_Recovery_Percentage'] = tfrp_df.index + 1
        tfrp_df = tfrp_df.replace('--', np.nan)
        for c in tfrp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                tfrp_df[c] = tfrp_df[c].str.rstrip('%').astype('float') / 100.0
        tfrp_df = tfrp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_giveaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/giveaway-fumble-recovery-pct' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
        tgfrp_df = main_hist(turnovers_giveaway_fumble_recovery_percentage_url_current, season, str(week),
                             this_week_date_str,
                             'turnovers_giveaway_fumble_recovery_percentage')
        tgfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                 season: 'Current_Season_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                 'Last 3': 'Last_3_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                 'Last 1': 'Last_1_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                 'Home': 'At_Home_Turnovers_Giveaway_Fumble_Recovery_Percentage',
                                 'Away': 'Away_Turnovers_Giveaway_Fumble_Recovery_Percentage'
                                 }, inplace=True)
        tgfrp_df['Team'] = tgfrp_df['Team'].str.strip()
        if season == '2010':
            tgfrp_df['Rank_Turnovers_Giveaway_Fumble_Recovery_Percentage'] = tgfrp_df.index + 1
        tgfrp_df = tgfrp_df.replace('--', np.nan)
        for c in tgfrp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                tgfrp_df[c] = tgfrp_df[c].str.rstrip('%').astype('float') / 100.0
        tgfrp_df = tgfrp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_takeaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/takeaway-fumble-recovery-pct' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
        ttfrp_df = main_hist(turnovers_takeaway_fumble_recovery_percentage_url_current, season, str(week),
                             this_week_date_str,
                             'turnovers_takeaway_fumble_recovery_percentage')
        ttfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                 season: 'Current_Season_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                 'Last 3': 'Last_3_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                 'Last 1': 'Last_1_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                 'Home': 'At_Home_Turnovers_Takeaway_Fumble_Recovery_Percentage',
                                 'Away': 'Away_Turnovers_Takeaway_Fumble_Recovery_Percentage'
                                 }, inplace=True)
        ttfrp_df['Team'] = ttfrp_df['Team'].str.strip()
        if season == '2010':
            ttfrp_df['Rank_Turnovers_Takeaway_Fumble_Recovery_Percentage'] = ttfrp_df.index + 1
        ttfrp_df = ttfrp_df.replace('--', np.nan)
        for c in ttfrp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                ttfrp_df[c] = ttfrp_df[c].str.rstrip('%').astype('float') / 100.0
        ttfrp_df = ttfrp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_interceptions_thrown_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/interception-pct' \
                                                                         + '?date=' \
                                                                         + this_week_date_str
        toitp_df = main_hist(turnovers_opponent_interceptions_thrown_percentage_url_current, season, str(week),
                             this_week_date_str,
                             'turnovers_opponent_interceptions_thrown_percentage')
        toitp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Interceptions_Thrown_Percentage',
                                 season: 'Current_Season_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                 'Last 3': 'Last_3_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                 'Last 1': 'Last_1_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                 'Home': 'At_Home_Turnovers_Opponents_Interceptions_Thrown_Percentage',
                                 'Away': 'Away_Turnovers_Opponents_Interceptions_Thrown_Percentage'
                                 }, inplace=True)
        toitp_df['Team'] = toitp_df['Team'].str.strip()
        if season == '2010':
            toitp_df['Rank_Turnovers_Opponent_Interceptions_Thrown_Percentage'] = toitp_df.index + 1
        toitp_df = toitp_df.replace('--', np.nan)
        for c in toitp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                toitp_df[c] = toitp_df[c].str.rstrip('%').astype('float') / 100.0
        toitp_df = toitp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-fumble-recovery-pct' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
        tofrp_df = main_hist(turnovers_opponent_fumble_recovery_percentage_url_current, season, str(week),
                             this_week_date_str,
                             'turnovers_opponent_fumble_recovery_percentage')
        tofrp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                 season: 'Current_Season_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                 'Last 3': 'Last_3_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                 'Last 1': 'Last_1_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                 'Home': 'At_Home_Turnovers_Opponent_Fumble_Recovery_Percentage',
                                 'Away': 'Away_Turnovers_Opponent_Fumble_Recovery_Percentage'
                                 }, inplace=True)
        tofrp_df['Team'] = tofrp_df['Team'].str.strip()
        if season == '2010':
            tofrp_df['Rank_Turnovers_Opponent_Fumble_Recovery_Percentage'] = tofrp_df.index + 1
        tofrp_df = tofrp_df.replace('--', np.nan)
        for c in tofrp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                tofrp_df[c] = tofrp_df[c].str.rstrip('%').astype('float') / 100.0
        tofrp_df = tofrp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_giveaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-giveaway-fumble-recovery-pct' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
        togfrp_df = main_hist(turnovers_opponent_giveaway_fumble_recovery_percentage_url_current, season, str(week),
                              this_week_date_str,
                              'turnovers_opponent_giveaway_fumble_recovery_percentage')
        togfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                  season: 'Current_Season_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                  'Last 3': 'Last_3_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                  'Last 1': 'Last_1_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                  'Home': 'At_Home_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage',
                                  'Away': 'Away_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage'
                                  }, inplace=True)
        togfrp_df['Team'] = togfrp_df['Team'].str.strip()
        if season == '2010':
            togfrp_df['Rank_Turnovers_Opponent_Giveaway_Fumble_Recovery_Percentage'] = togfrp_df.index + 1
        togfrp_df = togfrp_df.replace('--', np.nan)
        for c in togfrp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                togfrp_df[c] = togfrp_df[c].str.rstrip('%').astype('float') / 100.0
        togfrp_df = togfrp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        turnovers_opponent_takeaway_fumble_recovery_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-takeaway-fumble-recovery-pct' \
                                                                             + '?date=' \
                                                                             + this_week_date_str
        totfrp_df = main_hist(turnovers_opponent_takeaway_fumble_recovery_percentage_url_current, season,
                              str(week),
                              this_week_date_str,
                              'turnovers_opponent_takeaway_fumble_recovery_percentage')
        totfrp_df.rename(columns={'Rank': 'Rank_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                  season: 'Current_Season_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                  str(int(
                                      season) - 1): 'Previous_Season_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                  'Last 3': 'Last_3_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                  'Last 1': 'Last_1_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                  'Home': 'At_Home_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage',
                                  'Away': 'Away_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage'
                                  }, inplace=True)
        totfrp_df['Team'] = totfrp_df['Team'].str.strip()
        if season == '2010':
            totfrp_df['Rank_Turnovers_Opponent_Takeaway_Fumble_Recovery_Percentage'] = totfrp_df.index + 1
        totfrp_df = totfrp_df.replace('--', np.nan)
        for c in totfrp_df:
            if (c != 'Team') & (c != 'Season') & (c != 'Week') & ('Rank' not in c):
                totfrp_df[c] = totfrp_df[c].str.rstrip('%').astype('float') / 100.0
        totfrp_df = totfrp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_penalties_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/penalties-per-game' \
                                                   + '?date=' \
                                                   + this_week_date_str
        pppg_df = main_hist(penalties_penalties_per_game_url_current, season,
                            str(week),
                            this_week_date_str,
                            'penalties_penalties_per_game')
        pppg_df.rename(columns={'Rank': 'Rank_Penalties_Penalties_per_Game',
                                season: 'Current_Season_Penalties_Penalties_per_Game',
                                str(int(
                                    season) - 1): 'Previous_Season_Penalties_per_Game',
                                'Last 3': 'Last_3_Penalties_Penalties_per_Game',
                                'Last 1': 'Last_1_Penalties_Penalties_per_Game',
                                'Home': 'At_Home_Penalties_Penalties_per_Game',
                                'Away': 'Away_Turnovers_Penalties_Penalties_per_Game'
                                }, inplace=True)
        pppg_df['Team'] = pppg_df['Team'].str.strip()
        if season == '2010':
            pppg_df['Rank_Penalties_Penalties_per_Game'] = pppg_df.index + 1
        pppg_df = pppg_df.replace('--', np.nan)
        pppg_df = pppg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_penalty_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/penalty-yards-per-game' \
                                                       + '?date=' \
                                                       + this_week_date_str
        ppypg_df = main_hist(penalties_penalty_yards_per_game_url_current, season,
                             str(week),
                             this_week_date_str,
                             'penalties_penalty_yards_per_game')
        ppypg_df.rename(columns={'Rank': 'Rank_Penalties_Penalty_Yards_per_Game',
                                 season: 'Current_Season_Penalties_Penalty_Yards_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Penalty_Yards_per_Game',
                                 'Last 3': 'Last_3_Penalties_Penalty_Yards_per_Game',
                                 'Last 1': 'Last_1_Penalties_Penalty_Yards_per_Game',
                                 'Home': 'At_Home_Penalties_Penalty_Yards_per_Game',
                                 'Away': 'Away_Turnovers_Penalties_Penalty_Yards_per_Game'
                                 }, inplace=True)
        ppypg_df['Team'] = ppypg_df['Team'].str.strip()
        if season == '2010':
            ppypg_df['Rank_Penalties_Penalty_Yards_per_Game'] = ppypg_df.index + 1
        ppypg_df = ppypg_df.replace('--', np.nan)
        ppypg_df = ppypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_penalty_first_downs_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/penalty-first-downs-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        ppfdpg_df = main_hist(penalties_penalty_first_downs_per_game_url_current, season,
                              str(week),
                              this_week_date_str,
                              'penalties_penalty_first_downs_per_game')
        ppfdpg_df.rename(columns={'Rank': 'Rank_Penalties_Penalty_First_Downs_per_Game',
                                  season: 'Current_Season_Penalties_Penalty_First_Downs_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Penalty_First_Downs_per_Game',
                                  'Last 3': 'Last_3_Penalties_Penalty_First_Downs_per_Game',
                                  'Last 1': 'Last_1_Penalties_Penalty_First_Downs_per_Game',
                                  'Home': 'At_Home_Penalties_Penalty_First_Downs_per_Game',
                                  'Away': 'Away_Turnovers_Penalties_Penalty_First_Downs_per_Game'
                                  }, inplace=True)
        ppfdpg_df['Team'] = ppfdpg_df['Team'].str.strip()
        if season == '2010':
            ppfdpg_df['Rank_Penalties_Penalty_First_Downs_per_Game'] = ppfdpg_df.index + 1
        ppfdpg_df = ppfdpg_df.replace('--', np.nan)
        ppfdpg_df = ppfdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_opponent_penalties_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalties-per-game' \
                                                            + '?date=' \
                                                            + this_week_date_str
        poppg_df = main_hist(penalties_opponent_penalties_per_game_url_current, season,
                             str(week),
                             this_week_date_str,
                             'penalties_opponent_penalties_per_game')
        poppg_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalties_per_Game',
                                 season: 'Current_Season_Penalties_Opponent_Penalties_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Penalties_Opponent_Penalties_per_Game',
                                 'Last 3': 'Last_3_Penalties_Opponent_Penalties_per_Game',
                                 'Last 1': 'Last_1_Penalties_Opponent_Penalties_per_Game',
                                 'Home': 'At_Home_Penalties_Opponent_Penalties_per_Game',
                                 'Away': 'Away_Turnovers_Penalties_Opponent_Penalties_per_Game'
                                 }, inplace=True)
        poppg_df['Team'] = poppg_df['Team'].str.strip()
        if season == '2010':
            poppg_df['Rank_Penalties_Opponent_Penalties_per_Game'] = poppg_df.index + 1
        poppg_df = poppg_df.replace('--', np.nan)
        poppg_df = poppg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_opponent_penalty_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalty-yards-per-game' \
                                                                + '?date=' \
                                                                + this_week_date_str
        popypg_df = main_hist(penalties_opponent_penalty_yards_per_game_url_current, season,
                              str(week),
                              this_week_date_str,
                              'penalties_opponent_penalty_yards_per_game')
        popypg_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalty_Yards_per_Game',
                                  season: 'Current_Season_Penalties_Opponent_Penalty_Yards_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Penalties_Opponent_Penalty_Yards_per_Game',
                                  'Last 3': 'Last_3_Penalties_Opponent_Penalty_Yards_per_Game',
                                  'Last 1': 'Last_1_Penalties_Opponent_Penalty_Yards_per_Game',
                                  'Home': 'At_Home_Penalties_Opponent_Penalty_Yards_per_Game',
                                  'Away': 'Away_Turnovers_Penalties_Opponent_Penalty_Yards_per_Game'
                                  }, inplace=True)
        popypg_df['Team'] = popypg_df['Team'].str.strip()
        if season == '2010':
            popypg_df['Rank_Penalties_Opponent_Penalty_Yards_per_Game'] = popypg_df.index + 1
        popypg_df = popypg_df.replace('--', np.nan)
        popypg_df = popypg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_opponent_penalty_first_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalty-first-downs-per-game' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
        popfdpg_df = main_hist(penalties_opponent_penalty_first_down_per_game_url_current, season,
                               str(week),
                               this_week_date_str,
                               'penalties_opponent_penalty_first_downs_per_game')
        popfdpg_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                   season: 'Current_Season_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                   'Last 3': 'Last_3_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                   'Last 1': 'Last_1_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                   'Home': 'At_Home_Penalties_Opponent_Penalty_First_Downs_per_Game',
                                   'Away': 'Away_Turnovers_Penalties_Opponent_Penalty_First_Downs_per_Game'
                                   }, inplace=True)
        popfdpg_df['Team'] = popfdpg_df['Team'].str.strip()
        if season == '2010':
            popfdpg_df['Rank_Penalties_Opponent_Penalty_First_Downs_per_Game'] = popfdpg_df.index + 1
        popfdpg_df = popfdpg_df.replace('--', np.nan)
        popfdpg_df = popfdpg_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_penalty_yards_per_penalty_url_current = 'https://www.teamrankings.com/college-football/stat/penalty-yards-per-penalty' \
                                                          + '?date=' \
                                                          + this_week_date_str
        poptpp_df = main_hist(penalties_penalty_yards_per_penalty_url_current, season,
                              str(week),
                              this_week_date_str,
                              'penalties_penalty_yards_per_penalty')
        poptpp_df.rename(columns={'Rank': 'Rank_Penalties_Penalty_Yards_per_Penalty',
                                  season: 'Current_Season_Penalties_Penalty_Yards_per_Penalty',
                                  str(int(
                                      season) - 1): 'Previous_Season_Penalties_Penalty_Yards_per_Penalty',
                                  'Last 3': 'Last_3_Penalties_Penalty_Yards_per_Penalty',
                                  'Last 1': 'Last_1_Penalties_Penalty_Yards_per_Penalty',
                                  'Home': 'At_Home_Penalties_Penalty_Yards_per_Penalty',
                                  'Away': 'Away_Turnovers_Penalties_Penalty_Yards_per_Penalty'
                                  }, inplace=True)
        poptpp_df['Team'] = poptpp_df['Team'].str.strip()
        if season == '2010':
            poptpp_df['Rank_Penalties_Penalty_Yards_per_Penalty'] = poptpp_df.index + 1
        poptpp_df = poptpp_df.replace('--', np.nan)
        poptpp_df = poptpp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_penalties_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/penalties-per-play' \
                                                   + '?date=' \
                                                   + this_week_date_str
        pppp_df = main_hist(penalties_penalties_per_play_url_current, season,
                            str(week),
                            this_week_date_str,
                            'penalties_penalties_per_play')
        pppp_df.rename(columns={'Rank': 'Rank_Penalties_Penalties_per_Play',
                                season: 'Current_Season_Penalties_Penalties_per_Play',
                                str(int(
                                    season) - 1): 'Previous_Season_Penalties_Penalties_per_Play',
                                'Last 3': 'Last_3_Penalties_Penalties_per_Play',
                                'Last 1': 'Last_1_Penalties_Penalties_per_Play',
                                'Home': 'At_Home_Penalties_Penalties_per_Play',
                                'Away': 'Away_Turnovers_Penalties_Penalties_per_Play'
                                }, inplace=True)
        pppp_df['Team'] = pppp_df['Team'].str.strip()
        if season == '2010':
            pppp_df['Rank_Penalties_Penalties_per_Play'] = pppp_df.index + 1
        pppp_df = pppp_df.replace('--', np.nan)
        pppp_df = pppp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_opponent_penalty_yards_per_penalty_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalty-yards-per-penalty' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
        poppypp_df = main_hist(penalties_opponent_penalty_yards_per_penalty_url_current, season,
                               str(week),
                               this_week_date_str,
                               'penalties_opponent_penalty_yards_per_penalty')
        poppypp_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                   season: 'Current_Season_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                   str(int(season) - 1): 'Previous_Season_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                   'Last 3': 'Last_3_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                   'Last 1': 'Last_1_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                   'Home': 'At_Home_Penalties_Opponent_Penalty_Yards_per_Penalty',
                                   'Away': 'Away_Turnovers_Penalties_Opponent_Penalty_Yards_per_Penalty'
                                   }, inplace=True)
        poppypp_df['Team'] = poppypp_df['Team'].str.strip()
        if season == '2010':
            poppypp_df['Rank_Penalties_Opponent_Penalty_Yards_per_Penalty'] = poppypp_df.index + 1
        poppypp_df = poppypp_df.replace('--', np.nan)
        poppypp_df = poppypp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        penalties_opponent_penalties_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-penalties-per-play' \
                                                            + '?date=' \
                                                            + this_week_date_str
        popppp_df = main_hist(penalties_opponent_penalty_yards_per_penalty_url_current, season,
                              str(week),
                              this_week_date_str,
                              'penalties_opponent_penalties_per_play')
        popppp_df.rename(columns={'Rank': 'Rank_Penalties_Opponent_Penalties_per_Play',
                                  season: 'Current_Season_Penalties_Opponent_Penalties_per_Play',
                                  str(int(
                                      season) - 1): 'Previous_Season_Penalties_Opponent_Penalties_per_Play',
                                  'Last 3': 'Last_3_Penalties_Opponent_Penalties_per_Play',
                                  'Last 1': 'Last_1_Penalties_Opponent_Penalties_per_Play',
                                  'Home': 'At_Home_Penalties_Opponent_Penalties_per_Play',
                                  'Away': 'Away_Turnovers_Penalties_Opponent_Penalties_per_Play'
                                  }, inplace=True)
        popppp_df['Team'] = popppp_df['Team'].str.strip()
        if season == '2010':
            popppp_df['Rank_Penalties_Opponent_Penalties_per_Play'] = popppp_df.index + 1
        popppp_df = popppp_df.replace('--', np.nan)
        popppp_df = popppp_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        predictive_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/predictive-by-other' \
                                               + '?date=' \
                                               + this_week_date_str
        ppr_df = main_hist(predictive_power_ranking_url_current, season, str(week), this_week_date_str,
                           'predictive_power_ranking')
        ppr_df.rename(columns={'Rank': 'Rank_Predictive_Power_Ranking',
                               'Rating': 'Rating_Predictive_Power_Ranking',
                               'Hi': 'Hi_Predictive_Power_Ranking',
                               'Low': 'Low_Predictive_Power_Ranking',
                               'Last': 'Last_Predictive_Power_Ranking'
                               }, inplace=True)
        ppr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        ppr_df['Team'] = ppr_df['Team'].str.strip()
        if season == '2010':
            ppr_df['Rank_Predictive_Power_Ranking'] = ppr_df.index + 1
        ppr_df = ppr_df.replace('--', np.nan)
        ppr_df = ppr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        home_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/home-by-other' \
                                         + '?date=' \
                                         + this_week_date_str
        hpr_df = main_hist(home_power_ranking_url_current, season, str(week), this_week_date_str,
                           'home_power_ranking')
        hpr_df.rename(columns={'Rank': 'Rank_Home_Power_Ranking',
                               'Rating': 'Rating_Home_Power_Ranking',
                               'Hi': 'Hi_Home_Power_Ranking',
                               'Low': 'Low_Home_Power_Ranking',
                               'Last': 'Last_Home_Power_Ranking'
                               }, inplace=True)
        hpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        hpr_df['Team'] = hpr_df['Team'].str.strip()
        if season == '2010':
            hpr_df['Rank_Home_Power_Ranking'] = hpr_df.index + 1
        hpr_df = hpr_df.replace('--', np.nan)
        hpr_df = hpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        away_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/away-by-other' \
                                         + '?date=' \
                                         + this_week_date_str
        apr_df = main_hist(away_power_ranking_url_current, season, str(week), this_week_date_str,
                           'away_power_ranking')
        apr_df.rename(columns={'Rank': 'Rank_Away_Power_Ranking',
                               'Rating': 'Rating_Away_Power_Ranking',
                               'Hi': 'Hi_Away_Power_Ranking',
                               'Low': 'Low_Away_Power_Ranking',
                               'Last': 'Last_Away_Power_Ranking'
                               }, inplace=True)
        apr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        apr_df['Team'] = apr_df['Team'].str.strip()
        if season == '2010':
            apr_df['Rank_Away_Power_Ranking'] = apr_df.index + 1
        apr_df = apr_df.replace('--', np.nan)
        apr_df = apr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        neutral_site_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/neutral-by-other' \
                                                 + '?date=' \
                                                 + this_week_date_str
        nspr_df = main_hist(neutral_site_power_ranking_url_current, season, str(week), this_week_date_str,
                            'neutral_site_power_ranking')
        nspr_df.rename(columns={'Rank': 'Rank_Neutral_Site_Power_Ranking',
                                'Rating': 'Rating_Neutral_Site_Power_Ranking',
                                'Hi': 'Hi_Neutral_Site_Power_Ranking',
                                'Low': 'Low_Neutral_Site_Power_Ranking',
                                'Last': 'Last_Neutral_Site_Power_Ranking'
                                }, inplace=True)
        nspr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        nspr_df['Team'] = nspr_df['Team'].str.strip()
        if season == '2010':
            nspr_df['Rank_Neutral_Site_Power_Ranking'] = nspr_df.index + 1
        nspr_df = nspr_df.replace('--', np.nan)
        nspr_df = nspr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        home_advantage_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/home-adv-by-other' \
                                                   + '?date=' \
                                                   + this_week_date_str
        hapr_df = main_hist(home_advantage_power_ranking_url_current, season, str(week), this_week_date_str,
                            'home_advantage_power_ranking')
        hapr_df.rename(columns={'Rank': 'Rank_Home_Advantage_Power_Ranking',
                                'Rating': 'Rating_Home_Advantage_Power_Ranking',
                                'Hi': 'Hi_Home_Advantage_Power_Ranking',
                                'Low': 'Low_Home_Advantage_Power_Ranking',
                                'Last': 'Last_Home_Advantage_Power_Ranking'
                                }, inplace=True)
        hapr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        hapr_df['Team'] = hapr_df['Team'].str.strip()
        if season == '2010':
            hapr_df['Rank_Home_Advantage_Power_Ranking'] = hapr_df.index + 1
        hapr_df = hapr_df.replace('--', np.nan)
        hapr_df = hapr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/schedule-strength-by-other' \
                                                         + '?date=' \
                                                         + this_week_date_str
        sospr_df = main_hist(strength_of_schedule_power_ranking_url_current, season, str(week), this_week_date_str,
                             'strength_of_schedule_power_ranking')
        sospr_df.rename(columns={'Rank': 'Rank_Strength_of_Schedule_Power_Ranking',
                                 'Rating': 'Rating_Strength_of_Schedule_Power_Ranking',
                                 'Hi': 'Hi_Strength_of_Schedule_Power_Ranking',
                                 'Low': 'Low_Strength_of_Schedule_Power_Ranking',
                                 'Last': 'Last_Strength_of_Schedule_Power_Ranking'
                                 }, inplace=True)
        # sospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        sospr_df['Team'] = sospr_df['Team'].str.strip()
        if season == '2010':
            sospr_df['Rank_Strength_of_Schedule_Power_Ranking'] = sospr_df.index + 1
        sospr_df = sospr_df.replace('--', np.nan)
        sospr_df = sospr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        future_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/future-sos-by-other' \
                                                                + '?date=' \
                                                                + this_week_date_str
        fsospr_df = main_hist(future_strength_of_schedule_power_ranking_url_current, season, str(week),
                              this_week_date_str, 'future_strength_of_schedule_power_ranking')
        fsospr_df.rename(columns={'Rank': 'Rank_Future_Strength_of_Schedule_Power_Ranking',
                                  'Rating': 'Rating_Future_Strength_of_Schedule_Power_Ranking',
                                  'Hi': 'Hi_Future_Strength_of_Schedule_Power_Ranking',
                                  'Low': 'Low_Future_Strength_of_Schedule_Power_Ranking',
                                  'Last': 'Last_Future_Strength_of_Schedule_Power_Ranking'
                                  }, inplace=True)
        # fsospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        fsospr_df['Team'] = fsospr_df['Team'].str.strip()
        if season == '2010':
            fsospr_df['Rank_Future_Strength_of_Schedule_Power_Ranking'] = fsospr_df.index + 1
        fsospr_df = fsospr_df.replace('--', np.nan)
        fsospr_df = fsospr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        season_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/season-sos-by-other' \
                                                                + '?date=' \
                                                                + this_week_date_str
        ssospr_df = main_hist(season_strength_of_schedule_power_ranking_url_current, season, str(week),
                              this_week_date_str, 'season_strength_of_schedule_power_ranking')
        ssospr_df.rename(columns={'Rank': 'Rank_Season_Strength_of_Schedule_Power_Ranking',
                                  'Rating': 'Rating_Season_Strength_of_Schedule_Power_Ranking',
                                  'Hi': 'Hi_Season_Strength_of_Schedule_Power_Ranking',
                                  'Low': 'Low_Season_Strength_of_Schedule_Power_Ranking',
                                  'Last': 'Last_Season_Strength_of_Schedule_Power_Ranking'
                                  }, inplace=True)
        # ssospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        ssospr_df['Team'] = ssospr_df['Team'].str.strip()
        if season == '2010':
            ssospr_df['Rank_Season_Strength_of_Schedule_Power_Ranking'] = ssospr_df.index + 1
        ssospr_df = ssospr_df.replace('--', np.nan)
        ssospr_df = ssospr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        strength_of_schedule_power_ranking_basic_method_url_current = 'https://www.teamrankings.com/college-football/ranking/sos-basic-by-other' \
                                                                      + '?date=' \
                                                                      + this_week_date_str
        sosprbm_df = main_hist(strength_of_schedule_power_ranking_basic_method_url_current, season, str(week),
                               this_week_date_str, 'strength_of_schedule_power_ranking_basic_method')
        sosprbm_df.rename(columns={'Rank': 'Rank_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                   'Rating': 'Rating_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                   'Hi': 'Hi_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                   'Low': 'Low_Strength_of_Schedule_Power_Ranking_Basic_Method',
                                   'Last': 'Last_Strength_of_Schedule_Power_Ranking_Basic_Method'
                                   }, inplace=True)
        # sosprbm_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        sosprbm_df['Team'] = sosprbm_df['Team'].str.strip()
        if season == '2010':
            sosprbm_df['Rank_Strength_of_Schedule_Power_Ranking_Basic_Method'] = sosprbm_df.index + 1
        sosprbm_df = sosprbm_df.replace('--', np.nan)
        sosprbm_df = sosprbm_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        in_conference_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/in-conference-sos-by-other' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
        icsospr_df = main_hist(in_conference_strength_of_schedule_power_ranking_url_current, season, str(week),
                               this_week_date_str, 'in_conference_strength_of_schedule_power_ranking')
        icsospr_df.rename(columns={'Rank': 'Rank_In_Conference_Strength_of_Schedule_Power_Ranking',
                                   'Rating': 'Rating_In_Conference_Strength_of_Schedule_Power_Ranking',
                                   'Hi': 'Hi_In_Conference_Strength_of_Schedule_Power_Ranking',
                                   'Low': 'Low_In_Conference_Strength_of_Schedule_Power_Ranking',
                                   'Last': 'Last_In-Conference_Strength_of_Schedule_Power_Ranking'
                                   }, inplace=True)
        # icsospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        icsospr_df['Team'] = icsospr_df['Team'].str.strip()
        if season == '2010':
            icsospr_df['Rank_In_Conference_Strength_of_Schedule_Power_Ranking'] = icsospr_df.index + 1
        icsospr_df = icsospr_df.replace('--', np.nan)
        icsospr_df = icsospr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        nonconference_strength_of_schedule_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/non-conference-sos-by-other' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
        ncsospr_df = main_hist(nonconference_strength_of_schedule_power_ranking_url_current, season, str(week),
                               this_week_date_str, 'nonconference_strength_of_schedule_power_ranking')
        ncsospr_df.rename(columns={'Rank': 'Rank_Nonconference_Strength_of_Schedule_Power_Ranking',
                                   'Rating': 'Rating_Nonconference_Strength_of_Schedule_Power_Ranking',
                                   'Hi': 'Hi_Nonconference_Strength_of_Schedule_Power_Ranking',
                                   'Low': 'Low_Nonconference_Strength_of_Schedule_Power_Ranking',
                                   'Last': 'Last_Nonconference_Strength_of_Schedule_Power_Ranking'
                                   }, inplace=True)
        # ncsospr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        ncsospr_df['Team'] = ncsospr_df['Team'].str.strip()
        if season == '2010':
            ncsospr_df['Rank_Nonconference_Strength_of_Schedule_Power_Ranking'] = ncsospr_df.index + 1
        ncsospr_df = ncsospr_df.replace('--', np.nan)
        ncsospr_df = ncsospr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        last_10_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/last-10-games-by-other' \
                                            + '?date=' \
                                            + this_week_date_str
        l10_df = main_hist(last_10_power_ranking_url_current, season, str(week),
                           this_week_date_str, 'last_10_power_ranking')
        l10_df.rename(columns={'Rank': 'Rank_Last_10_Power_Ranking',
                               'Rating': 'Rating_Last_10_Power_Ranking',
                               'Hi': 'Hi_Last_10_Power_Ranking',
                               'Low': 'Low_Last_10_Power_Ranking',
                               'Last': 'Last_Last_10_Power_Ranking'
                               }, inplace=True)
        # l10_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        l10_df['Team'] = l10_df['Team'].str.strip()
        if season == '2010':
            l10_df['Rank_Last_10_Power_Ranking'] = l10_df.index + 1
        l10_df = l10_df.replace('--', np.nan)
        l10_df = l10_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        last_5_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/last-5-games-by-other' \
                                           + '?date=' \
                                           + this_week_date_str
        l5_df = main_hist(last_5_power_ranking_url_current, season, str(week),
                          this_week_date_str, 'last_5_power_ranking')
        l5_df.rename(columns={'Rank': 'Rank_Last_5_Power_Ranking',
                              'Rating': 'Rating_Last_5_Power_Ranking',
                              'Hi': 'Hi_Last_5_Power_Ranking',
                              'Low': 'Low_Last_5_Power_Ranking',
                              'Last': 'Last_Last_5_Power_Ranking'
                              }, inplace=True)
        # l5_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        l5_df['Team'] = l5_df['Team'].str.strip()
        if season == '2010':
            l5_df['Rank_Last_5_Power_Ranking'] = l5_df.index + 1
        l5_df = l5_df.replace('--', np.nan)
        l5_df = l5_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        in_conference_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/in-conference-by-other' \
                                                  + '?date=' \
                                                  + this_week_date_str
        icpr_df = main_hist(in_conference_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'in_conference_power_ranking')
        icpr_df.rename(columns={'Rank': 'In_Conference_Power_Ranking',
                                'Rating': 'Rating_In-Conference_Power_Ranking',
                                'Hi': 'Hi_In-Conference_Power_Ranking',
                                'Low': 'Low_In_Conference_Power_Ranking',
                                'Last': 'Last_In-Conference_Power_Ranking'
                                }, inplace=True)
        # icpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        icpr_df['Team'] = icpr_df['Team'].str.strip()
        if season == '2010':
            icpr_df['In_Conference_Power_Ranking'] = icpr_df.index + 1
        icpr_df = icpr_df.replace('--', np.nan)
        icpr_df = icpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        nonconference_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/non-conference-by-other' \
                                                  + '?date=' \
                                                  + this_week_date_str
        ncpr_df = main_hist(nonconference_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'nononference_power_ranking')
        ncpr_df.rename(columns={'Rank': 'Nonconference_Power_Ranking',
                                'Rating': 'Rating_Nonconference_Power_Ranking',
                                'Hi': 'Hi_Nonconference_Power_Ranking',
                                'Low': 'Low_Nonconference_Power_Ranking',
                                'Last': 'Last_Nonconference_Power_Ranking'
                                }, inplace=True)
        # ncpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        ncpr_df['Team'] = ncpr_df['Team'].str.strip()
        if season == '2010':
            ncpr_df['Nonconference_Power_Ranking'] = ncpr_df.index + 1
        ncpr_df = ncpr_df.replace('--', np.nan)
        ncpr_df = ncpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        luck_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/luck-by-other' \
                                         + '?date=' \
                                         + this_week_date_str
        lpr_df = main_hist(luck_power_ranking_url_current, season, str(week),
                           this_week_date_str, 'luck_power_ranking')
        lpr_df.rename(columns={'Rank': 'Luck_Power_Ranking',
                               'Rating': 'Rating_Luck_Power_Ranking',
                               'Hi': 'Hi_Luck_Power_Ranking',
                               'Low': 'Low_Luck_Power_Ranking',
                               'Last': 'Last_Luck_Power_Ranking'
                               }, inplace=True)
        lpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        lpr_df['Team'] = lpr_df['Team'].str.strip()
        if season == '2010':
            lpr_df['Luck_Power_Ranking'] = lpr_df.index + 1
        lpr_df = lpr_df.replace('--', np.nan)
        lpr_df = lpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        consistency_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/consistency-by-other' \
                                                + '?date=' \
                                                + this_week_date_str
        cpr_df = main_hist(consistency_power_ranking_url_current, season, str(week),
                           this_week_date_str, 'consistency_power_ranking')
        cpr_df.rename(columns={'Rank': 'Consistency_Power_Ranking',
                               'Rating': 'Rating_Consistency_Power_Ranking',
                               'Hi': 'Hi_Consistency_Power_Ranking',
                               'Low': 'Low_Consistency_Power_Ranking',
                               'Last': 'Last_Consistency_Power_Ranking'
                               }, inplace=True)
        cpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        cpr_df['Team'] = cpr_df['Team'].str.strip()
        if season == '2010':
            cpr_df['Consistency_Power_Ranking'] = cpr_df.index + 1
        cpr_df = cpr_df.replace('--', np.nan)
        cpr_df = cpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        versus_teams_1_thru_10_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-1-10-by-other' \
                                                           + '?date=' \
                                                           + this_week_date_str
        vt1_df = main_hist(versus_teams_1_thru_10_power_ranking_url_current, season, str(week),
                           this_week_date_str, 'versus_teams_1_thru_10_power_ranking')
        vt1_df.rename(columns={'Rank': 'Versus_Teams_1_Thru_10_Power_Ranking',
                               'Rating': 'Rating_Versus_Teams_1_Thru_10_Power_Ranking',
                               'Hi': 'Hi_Versus_Teams_1_Thru_10_Power_Ranking',
                               'Low': 'Low_Versus_Teams_1_Thru_10_Power_Ranking',
                               'Last': 'Last_Versus_Teams_1_Thru_10_Power_Ranking'
                               }, inplace=True)
        # vt1_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        vt1_df['Team'] = vt1_df['Team'].str.strip()
        if season == '2010':
            vt1_df['Versus_Teams_1_Thru_10_Power_Ranking'] = vt1_df.index + 1
        vt1_df = vt1_df.replace('--', np.nan)
        vt1_df = vt1_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        versus_teams_11_thru_25_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-11-25-by-other' \
                                                            + '?date=' \
                                                            + this_week_date_str
        vt11_df = main_hist(versus_teams_11_thru_25_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'versus_teams_11_thru_25_power_ranking')
        vt11_df.rename(columns={'Rank': 'Versus_Teams_11_Thru_25_Power_Ranking',
                                'Rating': 'Rating_Versus_Teams_11_Thru_25_Power_Ranking',
                                'Hi': 'Hi_Versus_Teams_11_Thru_25_Power_Ranking',
                                'Low': 'Low_Versus_Teams_11_Thru_25_Power_Ranking',
                                'Last': 'Last_Versus_Teams_11_Thru_25_Power_Ranking'
                                }, inplace=True)
        # vt11_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        vt11_df['Team'] = vt11_df['Team'].str.strip()
        if season == '2010':
            vt11_df['Versus_Teams_11_Thru_25_Power_Ranking'] = vt11_df.index + 1
        vt11_df = vt11_df.replace('--', np.nan)
        vt11_df = vt11_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        versus_teams_26_thru_40_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-26-40-by-other' \
                                                            + '?date=' \
                                                            + this_week_date_str
        vt26_df = main_hist(versus_teams_26_thru_40_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'versus_teams_26_thru_40_power_ranking')
        vt26_df.rename(columns={'Rank': 'Versus_Teams_26_Thru_40_Power_Ranking',
                                'Rating': 'Rating_Versus_Teams_26_Thru_40_Power_Ranking',
                                'Hi': 'Hi_Versus_Teams_26_Thru_40_Power_Ranking',
                                'Low': 'Low_Versus_Teams_26_Thru_40_Power_Ranking',
                                'Last': 'Last_Versus_Teams_26_Thru_40_Power_Ranking'
                                }, inplace=True)
        # vt26_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        vt26_df['Team'] = vt26_df['Team'].str.strip()
        if season == '2010':
            vt26_df['Versus_Teams_26_Thru_40_Power_Ranking'] = vt26_df.index + 1
        vt26_df = vt26_df.replace('--', np.nan)
        vt26_df = vt26_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        versus_teams_41_thru_75_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-41-75-by-other' \
                                                            + '?date=' \
                                                            + this_week_date_str
        vt41_df = main_hist(versus_teams_41_thru_75_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'versus_teams_41_thru_75_power_ranking')
        vt41_df.rename(columns={'Rank': 'Versus_Teams_41_Thru_75_Power_Ranking',
                                'Rating': 'Rating_Versus_Teams_41_Thru_75_Power_Ranking',
                                'Hi': 'Hi_Versus_Teams_41_Thru_75_Power_Ranking',
                                'Low': 'Low_Versus_Teams_41_Thru_75_Power_Ranking',
                                'Last': 'Last_Versus_Teams_41_Thru_75_Power_Ranking'
                                }, inplace=True)
        # vt41_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        vt41_df['Team'] = vt41_df['Team'].str.strip()
        if season == '2010':
            vt41_df['Versus_Teams_41_Thru_75_Power_Ranking'] = vt41_df.index + 1
        vt41_df = vt41_df.replace('--', np.nan)
        vt41_df = vt41_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        versus_teams_76_thru_120_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/vs-76-120-by-other' \
                                                             + '?date=' \
                                                             + this_week_date_str
        vt76_df = main_hist(versus_teams_76_thru_120_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'versus_teams_76_thru_120_power_ranking')
        vt76_df.rename(columns={'Rank': 'Versus_Teams_76_Thru_120_Power_Ranking',
                                'Rating': 'Rating_Versus_Teams_76_Thru_120_Power_Ranking',
                                'Hi': 'Hi_Versus_Teams_76_Thru_120_Power_Ranking',
                                'Low': 'Low_Versus_Teams_76_Thru_120_Power_Ranking',
                                'Last': 'Last_Versus_Teams_76_Thru_120_Power_Ranking'
                                }, inplace=True)
        # vt76_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        vt76_df['Team'] = vt76_df['Team'].str.strip()
        if season == '2010':
            vt76_df['Versus_Teams_76_Thru_120_Power_Ranking'] = vt76_df.index + 1
        vt76_df = vt76_df.replace('--', np.nan)
        vt76_df = vt76_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        first_half_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/first-half-by-other' \
                                               + '?date=' \
                                               + this_week_date_str
        fhpr_df = main_hist(first_half_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'first_half_power_ranking')
        fhpr_df.rename(columns={'Rank': 'First_Half_Power_Ranking',
                                'Rating': 'Rating_First_Half_Power_Ranking',
                                'Hi': 'Hi_First_Half_Power_Ranking',
                                'Low': 'Low_First_Half_Power_Ranking',
                                'Last': 'Last_First_Half_Power_Ranking'
                                }, inplace=True)
        # fhpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        fhpr_df['Team'] = fhpr_df['Team'].str.strip()
        if season == '2010':
            fhpr_df['First_Half_Power_Ranking'] = fhpr_df.index + 1
        fhpr_df = fhpr_df.replace('--', np.nan)
        fhpr_df = fhpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        second_half_power_ranking_url_current = 'https://www.teamrankings.com/college-football/ranking/second-half-by-other' \
                                                + '?date=' \
                                                + this_week_date_str
        shpr_df = main_hist(second_half_power_ranking_url_current, season, str(week),
                            this_week_date_str, 'second_half_power_ranking')
        shpr_df.rename(columns={'Rank': 'Second_Half_Power_Ranking',
                                'Rating': 'Rating_Second_Half_Power_Ranking',
                                'Hi': 'Hi_Second_Half_Power_Ranking',
                                'Low': 'Low_Second_Half_Power_Ranking',
                                'Last': 'Last_Second_Half_Power_Ranking'
                                }, inplace=True)
        # shpr_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        shpr_df['Team'] = shpr_df['Team'].str.strip()
        if season == '2010':
            shpr_df['Second_Half_Power_Ranking'] = shpr_df.index + 1
        shpr_df = shpr_df.replace('--', np.nan)
        shpr_df = shpr_df.apply(pd.to_numeric, errors='ignore')
        time.sleep(random.uniform(0.2, 2))

        this_week_date = this_week_date + datetime.timedelta(days=7)
        this_week_date_str = this_week_date.strftime("%Y-%m-%d")

        this_week_df = pd.merge(so_df, tasm_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, typp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, typm_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, typ_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, typpm_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, trs_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, trsp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, trspp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tot_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, top_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tops_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fiq_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sq_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tq_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fq_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ot_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fh_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sh_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fiqtp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sqtp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tqtp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fqtp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fhtp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, shtp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toppg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toypp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, totdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, totdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tofdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tofdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toatp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toatpp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, totdcp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tofdcp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toppp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toppos_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rorapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rorypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rorypra_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rorpp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, roryp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, popapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pocpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poipg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pocp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, popypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poqspg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poqsp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poapr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ppoppp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, popyp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poyppa_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poypc_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stofgapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stofgmpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stofgcp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stopapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stogpypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdoppg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdoypp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdoppp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdoasm_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdorzsapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdorzspg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdorzsp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdoppfga_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdootpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sdooppg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdoypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdoppg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdofdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdotdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdotdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdofodpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdofdcpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdoatop_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdptopp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdotdcp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdofdcp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdoppp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tdoppos_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rdorapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rdorypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rdorfdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rdoypra_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rdorpp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, rdoryp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdopapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdocpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdoipg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdocp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdopypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdofdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdoatpr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdtsp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdoppp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdopyp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdspg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdoyppa_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pdoypc_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stdofgapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stdofgmpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stdopapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, stdogpypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, titpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tfpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tflpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tfnlpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tgpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ttmpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tipg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tofpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toflpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tofnlpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tttapg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, totmpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, titp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tfrp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tgfrp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ttfrp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, toitp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tofrp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tofrp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, totfrp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pppg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ppypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ppfdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poppg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, popypg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, popfdpg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poptpp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, pppp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, poppypp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, popppp_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ppr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, hpr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, apr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, nspr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, hapr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sospr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fsospr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ssospr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sosprbm_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, icsospr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ncsospr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, l10_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, l5_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, icpr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ncpr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, lpr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, cpr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, vt1_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, vt11_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, vt26_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, vt41_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, vt76_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, fhpr_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, shpr_df, on=['Team', 'Season', 'Week'], how='outer')

        # this_week_df = rearrange_columns(this_week_df)
        # season_df = pd.concat([season_df, this_week_df])
        # master_df = pd.concat([master_df, this_week_df])

        save_file = 'Scraped_TR_Data_Season_' + season + '_Week_' + week
        try:
            datascraper.save_df(this_week_df, save_dir, save_file)
            print('{} saved successfully.'.format(save_file))
            print('File successfully saved at {}.'.format(save_dir))
        except:
            print('I don\'t think the file saved, you should double check.')
