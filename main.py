from package import *
import sys
import os
import datetime
import time
import pandas as pd
import numpy as np


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
            save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/' + this_season
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
    run_type = 'historic season'    # This is used to tell the below code which mode we want to run in
    # run_type = 'current week'

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
        for season in ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']:
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
                season_start_date = datetime.date(int(season), 9, 27)
            elif (season == '2011') | (season == '2016'):
                season_start_date = datetime.date(int(season), 9, 26)
            elif (season == '2012') | (season == '2018'):
                season_start_date = datetime.date(int(season), 9, 24)
            elif (season == '2013') | (season == '2019'):
                season_start_date = datetime.date(int(season), 9, 23)
            elif season == '2014':
                season_start_date = datetime.date(int(season), 9, 22)
            elif season == '2015':
                season_start_date = datetime.date(int(season), 9, 28)
            elif season == '2017':
                season_start_date = datetime.date(int(season), 9, 25)

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
            for week in range(5, 15):
                """
                # For each week of the current season, we will build a url string for each of the tables we want to
                # scrape using the current value of 'this_week_date_str'. The 'this_week_date_str' variable will 
                # start at 2010-9-26  and will be updated at the bottom of this loop to add 7 days so that each time
                # we go through the loop it will scrape the next weeks table.
                """
                scoring_offense_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-game'\
                    + '?date='\
                    + this_week_date_str
                so_df = main_hist(scoring_offense_url_current, season, str(week), this_week_date_str, 'scoring_offense')
                so_df.rename(columns={'Rank': 'Rank_Scoring_Offense',
                                      season: 'Current_Season_Scoring_Offense',
                                      str(int(season) - 1): 'Previous_Season_Scoring_Offense',
                                      'Last 3': 'Last 3_Scoring_Offense',
                                      'Last 1': 'Last 1_Scoring_Offense',
                                      'Home': 'At_Home_Scoring_Offense',
                                      'Away': 'Away_Scoring_Offense'
                                      }, inplace=True)
                so_df['Team'] = so_df['Team'].str.strip()
                if season == '2010':
                    so_df['Rank_Scoring_Offense'] = so_df.index + 1
                so_df = so_df.replace('--', np.nan)
                so_df = so_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_average_scoring_margin_url_current = 'https://www.teamrankings.com/college-football/stat/average-scoring-margin' \
                    + '?date=' \
                    + this_week_date_str
                tasm_df = main_hist(team_average_scoring_margin_url_current, season, str(week), this_week_date_str, 'team_average_scoring_margin')
                tasm_df.rename(columns={'Rank': 'Rank_Team_Average_Scoring_Margin',
                                      season: 'Current_Season_Team_Scoring_Average_Margin',
                                      str(int(season) - 1): 'Previous_Season_Team_Average_Scoring_Margin',
                                      'Last 3': 'Last 3_Team_Average_Scoring_Margin',
                                      'Last 1': 'Last 1_Team_Average_Scoring_Margin',
                                      'Home': 'At_Home_Team_Average_Scoring_Margin',
                                      'Away': 'Away_Team_Average_Scoring_Margin'
                                      }, inplace=True)
                tasm_df['Team'] = tasm_df['Team'].str.strip()
                if season == '2010':
                    tasm_df['Rank_Team_Average_Scoring_Margin'] = tasm_df.index + 1
                tasm_df = tasm_df.replace('--', np.nan)
                tasm_df = tasm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point' \
                    + '?date=' \
                    + this_week_date_str
                typp_df = main_hist(team_yards_per_point_url_current, season, str(week), this_week_date_str, 'team_yards_per_point')
                typp_df.rename(columns={'Rank': 'Rank_Team_Yards_Per_Point',
                                      season: 'Current_Season_Team_Yards_per_Point',
                                      str(int(season) - 1): 'Previous_Season_Team_yards_per_Point',
                                      'Last 3': 'Last 3_Team_Yards_per_Point',
                                      'Last 1': 'Last 1_Team_Yards_per_Point',
                                      'Home': 'At_Home_Team_Yards_per_Point',
                                      'Away': 'Away_Team_Yards_per_Point'
                                      }, inplace=True)
                typp_df['Team'] = typp_df['Team'].str.strip()
                if season == '2010':
                    typp_df['Rank_Team_Yards_per_Point'] = typp_df.index + 1
                typp_df = typp_df.replace('--', np.nan)
                typp_df = typp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_yards_per_point_margin_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point-margin' \
                    + '?date=' \
                    + this_week_date_str
                typm_df = main_hist(team_yards_per_point_margin_url_current, season, str(week), this_week_date_str, 'team_yards_per_point_margin')
                typm_df.rename(columns={'Rank': 'Rank_Team_Yards_per_Point_Margin',
                                      season: 'Current_Season_Team_Yards_per_Point_Margin',
                                      str(int(season) - 1): 'Previous_Season_Team_yards_per_Point_Margin',
                                      'Last 3': 'Last 3_Team_Yards_per_Point_Margin',
                                      'Last 1': 'Last 1_Team_Yards_per_Point_Margin',
                                      'Home': 'At_Home_Team_Yards_per_Point_Margin',
                                      'Away': 'Away_Team_Yards_per_Point_Margin'
                                      }, inplace=True)
                typm_df['Team'] = typm_df['Team'].str.strip()
                if season == '2010':
                    typm_df['Rank_Team_Yards_per_Point_Margin'] = typm_df.index + 1
                typm_df = typm_df.replace('--', np.nan)
                typm_df = typm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_points_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play' \
                    + '?date=' \
                    + this_week_date_str
                typ_df = main_hist(team_points_per_play_url_current, season, str(week), this_week_date_str, 'team_points_per_play')
                typ_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play',
                                      season: 'Current_Season_Team_Points_per_Play',
                                      str(int(season) - 1): 'Previous_Season_Team_Points_per_Play',
                                      'Last 3': 'Last 3_Team_Points_per_Play',
                                      'Last 1': 'Last 1_Team_Points_per_Play',
                                      'Home': 'At_Home_Team_Points_per_Play',
                                      'Away': 'Away_Team_Points_per_Play'
                                      }, inplace=True)
                typ_df['Team'] = typ_df['Team'].str.strip()
                if season == '2010':
                    typ_df['Rank_Team_Points_per_Play'] = typ_df.index + 1
                typ_df = typ_df.replace('--', np.nan)
                typ_df = typ_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_points_per_play_margin_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play-margin' \
                    + '?date=' \
                    + this_week_date_str
                typpm_df = main_hist(team_points_per_play_margin_url_current, season, str(week), this_week_date_str, 'team_points_per_play_margin')
                typpm_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play_Margin',
                                      season: 'Current_Season_Team_Points_per_Play_Margin',
                                      str(int(season) - 1): 'Previous_Season_Team_Points_per_Play_Margin',
                                      'Last 3': 'Last 3_Team_Points_per_Play_Margin',
                                      'Last 1': 'Last 1_Team_Points_per_Play_Margin',
                                      'Home': 'At_Home_Team_Points_per_Play_Margin',
                                      'Away': 'Away_Team_Points_per_Play_Margin'
                                      }, inplace=True)
                typpm_df['Team'] = typpm_df['Team'].str.strip()
                if season == '2010':
                    typpm_df['Rank_Team_Points_per_Play_Margin'] = typpm_df.index + 1
                typpm_df = typpm_df.replace('--', np.nan)
                typpm_df = typpm_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_red_zone_scoring_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-attempts-per-game' \
                    + '?date=' \
                    + this_week_date_str
                trs_df = main_hist(team_red_zone_scoring_attempts_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_red_zone_scoring_attempts_per_game')
                trs_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Attempts_per_Game',
                                      season: 'Current_Season_Team_Red-Zone_Scoring_Attempts_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scoring_Attempts_per_Game',
                                      'Last 3': 'Last 3_Team_Red_Zone_Scoring_Attempts_per_Game',
                                      'Last 1': 'Last 1_Team_Red_Zone_Scoring_Attempts_per_Game',
                                      'Home': 'At_Home_Team_Red_Zone_Scoring_Attempts_per_Game',
                                      'Away': 'Away_Team_Red_Zone-Scoring-Attempts_per_Game'
                                      }, inplace=True)
                trs_df['Team'] = trs_df['Team'].str.strip()
                if season == '2010':
                    trs_df['Rank_Team_Red-Zone_Scoring_Attempts_per_Game'] = trs_df.index + 1
                trs_df = trs_df.replace('--', np.nan)
                trs_df = trs_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_red_zone_scores_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scores-per-game' \
                    + '?date=' \
                    + this_week_date_str
                trsp_df = main_hist(team_red_zone_scores_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_red_zone_scores_per_game')
                trsp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scores_per_Game',
                                      season: 'Current_Season_Team_Red-Zone_Scores_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scores_per_Game',
                                      'Last 3': 'Last 3_Team_Red_Zone_Scores_per_Game',
                                      'Last 1': 'Last 1_Team_Red_Zone_Scores_per_Game',
                                      'Home': 'At_Home_Team_Red_Zone_Scores_per_Game',
                                      'Away': 'Away_Team_Red_Zone-Scores_per_Game'
                                      }, inplace=True)
                trsp_df['Team'] = trsp_df['Team'].str.strip()
                if season == '2010':
                    trsp_df['Rank_Team_Red-Zone_Scores_per_Game'] = trsp_df.index + 1
                tssp_df = trsp_df.replace('--', np.nan)
                trsp_df = trsp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_red_zone_scoring_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-pct' \
                    + '?date=' \
                    + this_week_date_str
                trspp_df = main_hist(team_red_zone_scoring_percentage_url_current, season, str(week), this_week_date_str,
                                  'team_red_zone_scoring_percentage')
                trspp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Percentage',
                                      season: 'Current_Season_Team_Red-Zone_Scoring_Percentage',
                                      str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scoring_Percentage',
                                      'Last 3': 'Last 3_Team_Red_Zone_Scoring_Percentage',
                                      'Last 1': 'Last 1_Team_Red_Zone_Scoring_Percentage',
                                      'Home': 'At_Home_Team_Red_Zone_Scoring_Percentage',
                                      'Away': 'Away_Team_Red_Zone-Scoring_Percentage'
                                      }, inplace=True)
                trspp_df['Team'] = trspp_df['Team'].str.strip()
                if season == '2010':
                    trspp_df['Rank_Team_Red-Zone_Scoring_Percentage'] = trspp_df.index + 1
                trspp_df = trspp_df.replace('--', np.nan)
                trspp_df = trspp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_offensive_touchdowns_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-touchdowns-per-game' \
                    + '?date=' \
                    + this_week_date_str
                tot_df = main_hist(team_offensive_touchdowns_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_offensive_touchdowns_per_game')
                tot_df.rename(columns={'Rank': 'Rank_Team_Offensive_Touchdowns_per_Game',
                                      season: 'Current_Season_Team_Offensive_Touchdowns_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Offensive_Touchdowns_per_Game',
                                      'Last 3': 'Last 3_Team_Offensive_Touchdowns_per_Game',
                                      'Last 1': 'Last 1_Team_Offensive_Touchdowns_per_Game',
                                      'Home': 'At_Home_Team_Offensive_Touchdowns_per_Game',
                                      'Away': 'Away_Team_Offensive_Touchdowns_per_Game'
                                      }, inplace=True)
                tot_df['Team'] = tot_df['Team'].str.strip()
                if season == '2010':
                    tot_df['Rank_Team_Offensive_Touchdowns_per_Game'] = tot_df.index + 1
                tot_df = tot_df.replace('--', np.nan)
                tot_df = tot_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_offensive_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                top_df = main_hist(team_offensive_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_offensive_points_per_game')
                top_df.rename(columns={'Rank': 'Rank_Team_Offensive_Points_per_Game',
                                      season: 'Current_Season_Team_Offensive_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Offensive_Points_per_Game',
                                      'Last 3': 'Last 3_Team_Offensive_Points_per_Game',
                                      'Last 1': 'Last 1_Team_Offensive_Points_per_Game',
                                      'Home': 'At_Home_Team_Offensive_Points_per_Game',
                                      'Away': 'Away_Team_Offensive_Points_per_Game'
                                      }, inplace=True)
                top_df['Team'] = top_df['Team'].str.strip()
                if season == '2010':
                    top_df['Rank_Team_Offensive_Points_per_Game'] = top_df.index + 1
                top_df = top_df.replace('--', np.nan)
                top_df = top_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_offensive_point_share_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-point-share-pct' \
                    + '?date=' \
                    + this_week_date_str
                tops_df = main_hist(team_offensive_point_share_percentage_url_current, season, str(week), this_week_date_str,
                                  'team_offensive_point_share_percentage')
                tops_df.rename(columns={'Rank': 'Rank_Team_Offensive_Point_Share_Percentage',
                                      season: 'Current_Season_Team_Offensive_Point_Share_Percentage',
                                      str(int(season) - 1): 'Previous_Season_Team_Offensive_Point_Share_Percentage',
                                      'Last 3': 'Last 3_Team_Offensive_Point_Share_Percentage',
                                      'Last 1': 'Last 1_Team_Offensive_Point_Share_Percentage',
                                      'Home': 'At_Home_Team_Offensive_Point_Share_Percentage',
                                      'Away': 'Away_Team_Offensive_Point_Share_Percentage'
                                      }, inplace=True)
                tops_df['Team'] = tops_df['Team'].str.strip()
                if season == '2010':
                    tops_df['Rank_Team_Offensive_Point_Share_Percentage'] = tops_df.index + 1
                tops_df = tops_df.replace('--', np.nan)
                tops_df = tops_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_first_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                fiq_df = main_hist(team_first_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                                    'team_first_quarter_points_per_game')
                fiq_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Points_per_Game',
                                        season: 'Current_Season_Team_First_Quarter_Points_per_Game',
                                        str(int(season) - 1): 'Previous_Season_Team_First_Quarter_Points_per_Game',
                                        'Last 3': 'Last 3_Team_First_Quarter_Points_per_Game',
                                        'Last 1': 'Last 1_Team_First_Quarter_Points_per_Game',
                                        'Home': 'At_Home_Team_First_Quarter_Points_per_Game',
                                        'Away': 'Away_Team_First_Quarter_Points_per_Game'
                                        }, inplace=True)
                fiq_df['Team'] = fiq_df['Team'].str.strip()
                if season == '2010':
                    fiq_df['Rank_Team_First_Quarter_Points_per_Game'] = fiq_df.index + 1
                fiq_df = fiq_df.replace('--', np.nan)
                fiq_df = fiq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_second_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                sq_df = main_hist(team_second_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_second_quarter_points_per_game')
                sq_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Points_per_Game',
                                      season: 'Current_Season_Team_Second_Quarter_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Second_Quarter_Points_per_Game',
                                      'Last 3': 'Last 3_Team_Second_Quarter_Points_per_Game',
                                      'Last 1': 'Last 1_Team_Second_Quarter_Points_per_Game',
                                      'Home': 'At_Home_Team_Second_Quarter_Points_per_Game',
                                      'Away': 'Away_Team_Second_Quarter_Points_per_Game'
                                      }, inplace=True)
                sq_df['Team'] = sq_df['Team'].str.strip()
                if season == '2010':
                    sq_df['Rank_Team_Second_Quarter_Points_per_Game'] = sq_df.index + 1
                sq_df = sq_df.replace('--', np.nan)
                sq_df = sq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_third_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/3rd-quarter-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                tq_df = main_hist(team_third_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_third_quarter_points_per_game')
                tq_df.rename(columns={'Rank': 'Rank_Team_Third_Quarter_Points_per_Game',
                                      season: 'Current_Season_Team_Third_Quarter_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Third_Quarter_Points_per_Game',
                                      'Last 3': 'Last 3_Team_Third_Quarter_Points_per_Game',
                                      'Last 1': 'Last 1_Team_Third_Quarter_Points_per_Game',
                                      'Home': 'At_Home_Team_Third_Quarter_Points_per_Game',
                                      'Away': 'Away_Team_Third_Quarter_Points_per_Game'
                                      }, inplace=True)
                tq_df['Team'] = tq_df['Team'].str.strip()
                if season == '2010':
                    tq_df['Rank_Team_Third_Quarter_Points_per_Game'] = tq_df.index + 1
                tq_df = tq_df.replace('--', np.nan)
                tq_df = tq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_fourth_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                fq_df = main_hist(team_fourth_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_fourth_quarter_points_per_game')
                fq_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Points_per_Game',
                                      season: 'Current_Season_Team_Fourth_Quarter_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Fourth_Quarter_Points_per_Game',
                                      'Last 3': 'Last 3_Team_Fourth_Quarter_Points_per_Game',
                                      'Last 1': 'Last 1_Team_Fourth_Quarter_Points_per_Game',
                                      'Home': 'At_Home_Team_Fourth_Quarter_Points_per_Game',
                                      'Away': 'Away_Team_Fourth_Quarter_Points_per_Game'
                                      }, inplace=True)
                fq_df['Team'] = fq_df['Team'].str.strip()
                if season == '2010':
                    fq_df['Rank_Team_Fourth_Quarter_Points_per_Game'] = fq_df.index + 1
                fq_df = fq_df.replace('--', np.nan)
                fq_df = fq_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_overtime_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/overtime-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                ot_df = main_hist(team_overtime_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_overtime_points_per_game')
                ot_df.rename(columns={'Rank': 'Rank_Overtime_Points_per_Game',
                                      season: 'Current_Season_Team_Overtime_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Overtime_Points_per_Game',
                                      'Last 3': 'Last 3_Team_Overtime_Points_per_Game',
                                      'Last 1': 'Last 1_Team_Overtime_Points_per_Game',
                                      'Home': 'At_Home_Team_Overtime_Points_per_Game',
                                      'Away': 'Away_Team_Overtime_Points_per_Game'
                                      }, inplace=True)
                ot_df['Team'] = ot_df['Team'].str.strip()
                if season == '2010':
                    ot_df['Rank_Team_Overtime_Points_per_Game'] = fq_df.index + 1
                ot_df = ot_df.replace('--', np.nan)
                ot_df = ot_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_first_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                fh_df = main_hist(team_first_half_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_first_half_points_per_game')
                fh_df.rename(columns={'Rank': 'Rank_First_Half_Points_per_Game',
                                      season: 'Current_Season_Team_First-Half_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_First-Half_Points_per_Game',
                                      'Last 3': 'Last 3_Team_First-Half_Points_per_Game',
                                      'Last 1': 'Last 1_Team_First-Half_Points_per_Game',
                                      'Home': 'At_Home_Team_First_Half_Points_per_Game',
                                      'Away': 'Away_Team_First-Half_Points_per_Game'
                                      }, inplace=True)
                fh_df['Team'] = fh_df['Team'].str.strip()
                if season == '2010':
                    fh_df['Rank_Team_First_Half_Points_per_Game'] = fh_df.index + 1
                fh_df = fh_df.replace('--', np.nan)
                fh_df = fh_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_second_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-points-per-game' \
                    + '?date=' \
                    + this_week_date_str
                sh_df = main_hist(team_second_half_points_per_game_url_current, season, str(week), this_week_date_str,
                                  'team_second_half_points_per_game')
                sh_df.rename(columns={'Rank': 'Rank_Second_Half_Points_per_Game',
                                      season: 'Current_Season_Team_Second-Half_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Team_Second-Half_Points_per_Game',
                                      'Last 3': 'Last 3_Team_Second-Half_Points_per_Game',
                                      'Last 1': 'Last 1_Team_Second-Half_Points_per_Game',
                                      'Home': 'At_Home_Team_Second_Half_Points_per_Game',
                                      'Away': 'Away_Team_Second-Half_Points_per_Game'
                                      }, inplace=True)
                sh_df['Team'] = sh_df['Team'].str.strip()
                if season == '2010':
                    sh_df['Rank_Team_Second_Half_Points_per_Game'] = fh_df.index + 1
                sh_df = sh_df.replace('--', np.nan)
                sh_df = sh_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_first_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-time-of-possession-share-pct' \
                    + '?date=' \
                    + this_week_date_str
                fiqtp_df = main_hist(team_first_quarter_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                                  'team_first_quarter_time_of_possession_share_percent')
                fiqtp_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                      season: 'Current_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                      str(int(season) - 1): 'Previous_Season_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                      'Last 3': 'Last 3_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                      'Last 1': 'Last 1_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                      'Home': 'At_Home_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                      'Away': 'Away_Team_First_Quarter_Time_of_Possession_Share_Percent'
                                      }, inplace=True)
                fiqtp_df['Team'] = fiqtp_df['Team'].str.strip()
                if season == '2010':
                    fiqtp_df['Rank_Team_First_Quarter_Time_of_Possession_Share_Percent'] = fiqtp_df.index + 1
                fiqtp_df = fiqtp_df.replace('--', np.nan)
                fiqtp_df = fiqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_second_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-time-of-possession-share-pct' \
                    + '?date=' \
                    + this_week_date_str
                sqtp_df = main_hist(team_second_quarter_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                                    'team_second_quarter_time_of_possession_share_percent')
                sqtp_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last 3_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last 1_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Second_Quarter_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                sqtp_df['Team'] = sqtp_df['Team'].str.strip()
                if season == '2010':
                    sqtp_df['Rank_Team_Second_Quarter_Time_of_Possession_Share_Percent'] = sqtp_df.index + 1
                sqtp_df = sqtp_df.replace('--', np.nan)
                sqtp_df = sqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

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
                                        'Last 3': 'Last 3_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last 1_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Third_Quarter_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                tqtp_df['Team'] = tqtp_df['Team'].str.strip()
                if season == '2010':
                    tqtp_df['Rank_Team_Third_Quarter_Time_of_Possession_Share_Percent'] = tqtp_df.index + 1
                tqtp_df = tqtp_df.replace('--', np.nan)
                tqtp_df = tqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_fourth_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-time-of-possession-share-pct' \
                    + '?date=' \
                    + this_week_date_str
                fqtp_df = main_hist(team_fourth_quarter_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                                    'team_fourth_quarter_time_of_possession_share_percent')
                fqtp_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last 3_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last 1_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Fourth_Quarter_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                fqtp_df['Team'] = fqtp_df['Team'].str.strip()
                if season == '2010':
                    fqtp_df['Rank_Team_Fourth_Quarter_Time_of_Possession_Share_Percent'] = fqtp_df.index + 1
                fqtp_df = fqtp_df.replace('--', np.nan)
                fqtp_df = fqtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_first_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-time-of-possession-share-pct' \
                    + '?date=' \
                    + this_week_date_str
                fhtp_df = main_hist(team_first_half_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                                    'team_first_half_time_of_possession_share_percent')
                fhtp_df.rename(columns={'Rank': 'Rank_Team_First_Half_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_First_Half_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last 3_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last 1_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_First_Half_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_First_Half_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                fhtp_df['Team'] = fhtp_df['Team'].str.strip()
                if season == '2010':
                    fhtp_df['Rank_Team_First_Half_Time_of_Possession_Share_Percent'] = fhtp_df.index + 1
                fhtp_df = fhtp_df.replace('--', np.nan)
                fhtp_df = fhtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                team_second_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-time-of-possession-share-pct' \
                    + '?date=' \
                    + this_week_date_str
                shtp_df = main_hist(team_second_half_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                                    'team_second_half_time_of_possession_share_percent')
                shtp_df.rename(columns={'Rank': 'Rank_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        season: 'Current_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        str(int(
                                            season) - 1): 'Previous_Season_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Last 3': 'Last 3_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Last 1': 'Last 1_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Home': 'At_Home_Team_Second_Half_Time_of_Possession_Share_Percent',
                                        'Away': 'Away_Team_Second_Half_Time_of_Possession_Share_Percent'
                                        }, inplace=True)
                shtp_df['Team'] = shtp_df['Team'].str.strip()
                if season == '2010':
                    shtp_df['Rank_Team_Second_Half_Time_of_Possession_Share_Percent'] = shtp_df.index + 1
                shtp_df = shtp_df.replace('--', np.nan)
                shtp_df = shtp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-game'\
                    + '?date='\
                    + this_week_date_str
                toypg_df = main_hist(total_offense_yards_per_game_url_current, season, str(week), this_week_date_str, 'total_offense_yards_per_game')
                toypg_df.rename(columns={'Rank': 'Rank_Total_Offense_yards_per_game',
                                      season: 'Current_Season_Total_Offense_Yards_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Game',
                                      'Last 3': 'Last 3_Total_Offense_Yards_per_Game',
                                      'Last 1': 'Last 1_Total_Offense_Yards_per_Game',
                                      'Home': 'At_Home_Total_Offense_Yards_per_Game',
                                      'Away': 'Away_Total_Offense_Yards_per_Game'
                                      }, inplace=True)
                toypg_df['Team'] = toypg_df['Team'].str.strip()
                if season == '2010':
                    toypg_df['Rank_Total_Offense_Yards_per_Game'] = toypg_df.index + 1
                toypg_df = toypg_df.replace('--', np.nan)
                toypg_df = toypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_plays_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/plays-per-game' \
                    + '?date=' \
                    + this_week_date_str
                toppg_df = main_hist(total_offense_plays_per_game_url_current, season, str(week), this_week_date_str, 'total_offense_plays_per_game')
                toppg_df.rename(columns={'Rank': 'Rank_Total_Offense_Plays_per_Game',
                                         season: 'Current_Season_Total_Offense_Plays_per_Game',
                                         str(int(season) - 1): 'Previous_Season_Total_Offense_Plays_per_Game',
                                         'Last 3': 'Last 3_Total_Offense_Plays_per_Game',
                                         'Last 1': 'Last 1_Total_Offense_Plays_per_Game',
                                         'Home': 'At_Home_Total_Offense_Plays_per_Game',
                                         'Away': 'Away_Total_Offense_Plays_per_Game'
                                         }, inplace=True)
                toppg_df['Team'] = toppg_df['Team'].str.strip()
                if season == '2010':
                    toppg_df['Rank_Total_Offense_Plays_per_Play'] = toppg_df.index + 1
                toppg_df = toppg_df.replace('--', np.nan)
                toppg_df = toppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_yards_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-play' \
                    + '?date=' \
                    + this_week_date_str
                toypp_df = main_hist(total_offense_yards_per_play_url_current, season, str(week), this_week_date_str,
                                     'total_offense_yards_per_play')
                toypp_df.rename(columns={'Rank': 'Rank_Total_Offense_Yards_per_Play',
                                         season: 'Current_Season_Total_Offense_Yards_per_Play',
                                         str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Play',
                                         'Last 3': 'Last 3_Total_Offense_Yards_per_Play',
                                         'Last 1': 'Last 1_Total_Offense_Yards_per_Play',
                                         'Home': 'At_Home_Total_Offense_Yards_per_Play',
                                         'Away': 'Away_Total_Offense_Yards_per_Play'
                                         }, inplace=True)
                toypp_df['Team'] = toypp_df['Team'].str.strip()
                if season == '2010':
                    toypp_df['Rank_Total_Offense_Yards_per_Play'] = toypp_df.index + 1
                toypp_df = toypp_df.replace('--', np.nan)
                toypp_df = toypp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_third_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-downs-per-game' \
                    + '?date=' \
                    + this_week_date_str
                totdpg_df = main_hist(total_offense_third_down_per_game_url_current, season, str(week), this_week_date_str,
                                     'total_offense_third_down_per_game')
                totdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_per_Game',
                                         season: 'Current_Season_Total_Offense_Third_Down_per_Game',
                                         str(int(season) - 1): 'Previous_Season_Total_Offense_Third_Down_per_Game',
                                         'Last 3': 'Last 3_Total_Offense_Third_Down_per_Game',
                                         'Last 1': 'Last 1_Total_Offense_Third_Down_per_Game',
                                         'Home': 'At_Home_Total_Offense_Third_Down_per_Game',
                                         'Away': 'Away_Total_Offense_Third-Down-per_Game'
                                         }, inplace=True)
                totdpg_df['Team'] = totdpg_df['Team'].str.strip()
                if season == '2010':
                    totdpg_df['Rank_Total_Offense_Third_Down_per_Game'] = totdpg_df.index + 1
                totdpg_df = totdpg_df.replace('--', np.nan)
                totdpg_df = totdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_third_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversions-per-game' \
                    + '?date=' \
                    + this_week_date_str
                totdcpg_df = main_hist(total_offense_third_down_conversions_per_game_url_current, season, str(week), this_week_date_str,
                                      'total_offense_third_down_conversions_per_game')
                totdcpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_Conversions_per_Game',
                                          season: 'Current_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                          str(int(season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                          'Last 3': 'Last 3_Total_Offense_Third_Down_Conversions_per_Game',
                                          'Last 1': 'Last 1_Total_Offense_Third_Down_Conversions_per_Game',
                                          'Home': 'At_Home_Total_Offense_Third_Down_Conversions_per_Game',
                                          'Away': 'Away_Total_Offense_Third-Down-Conversions_per_Game'
                                          }, inplace=True)
                totdcpg_df['Team'] = totdcpg_df['Team'].str.strip()
                if season == '2010':
                    totdcpg_df['Rank_Total_Offense_Third_Down_Conversions_per_Game'] = totdcpg_df.index + 1
                totdcpg_df = totdcpg_df.replace('--', np.nan)
                totdcpg_df = totdcpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_fourth_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-downs-per-game' \
                    + '?date=' \
                    + this_week_date_str
                tofdpg_df = main_hist(total_offense_fourth_down_per_game_url_current, season, str(week), this_week_date_str,
                                      'total_offense_fourth_down_per_game')
                tofdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_per_Game',
                                          season: 'Current_Season_Total_Offense_Fourth_Down_per_Game',
                                          str(int(season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_per_Game',
                                          'Last 3': 'Last 3_Total_Offense_Fourth_Down_per_Game',
                                          'Last 1': 'Last 1_Total_Offense_Fourth_Down_per_Game',
                                          'Home': 'At_Home_Total_Offense_Fourth_Down_per_Game',
                                          'Away': 'Away_Total_Offense_Fourth-Down_per_Game'
                                          }, inplace=True)
                tofdpg_df['Team'] = tofdpg_df['Team'].str.strip()
                if season == '2010':
                    tofdpg_df['Rank_Total_Offense_Fourth_Down_per_Game'] = tofdpg_df.index + 1
                tofdpg_df = tofdpg_df.replace('--', np.nan)
                tofdpg_df = tofdpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_fourth_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-down-conversions-per-game' \
                    + '?date=' \
                    + this_week_date_str
                tofdcpg_df = main_hist(total_offense_fourth_down_conversions_per_game_url_current, season, str(week), this_week_date_str,
                                       'total_offense_fourth_down_conversions_per_game')
                tofdcpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           season: 'Current_Season_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Last 3': 'Last 3_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Last 1': 'Last 1_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Home': 'At_Home_Total_Offense_Fourth_Down_Conversions_per_Game',
                                           'Away': 'Away_Total_Offense_Fourth_Down-Conversions_per_Game'
                                           }, inplace=True)
                tofdcpg_df['Team'] = tofdcpg_df['Team'].str.strip()
                if season == '2010':
                    tofdcpg_df['Rank_Total_Offense_Fourth_Down_Conversions_per_Game'] = tofdcpg_df.index + 1
                tofdcpg_df = tofdcpg_df.replace('--', np.nan)
                tofdcpg_df = tofdcpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_average_time_of_possession_url_current = 'https://www.teamrankings.com/college-football/stat/average-time-of-possession-net-of-ot' \
                    + '?date=' \
                    + this_week_date_str
                toatp_df = main_hist(total_offense_average_time_of_possession_url_current, season, str(week), this_week_date_str,
                                       'total_offense_average_time_of_possession')
                toatp_df.rename(columns={'Rank': 'Rank_Total_Offense_Average_Time_of_Possession',
                                           season: 'Current_Season_Total_Offense_Average_Time_of_Possession',
                                           str(int(
                                               season) - 1): 'Previous_Season_Total_Offense_Average_Time_of_Possession',
                                           'Last 3': 'Last 3_Total_Offense_Average_Time_of_Possession',
                                           'Last 1': 'Last 1_Total_Offense_Average_Time_of_Possession',
                                           'Home': 'At_Home_Total_Offense_Average_Time_of_Possession',
                                           'Away': 'Away_Total_Offense_Average_Time_of_Possession'
                                           }, inplace=True)
                toatp_df['Team'] = toatp_df['Team'].str.strip()
                if season == '2010':
                    toatp_df['Rank_Total_Offense_Average_Time_of_Possession'] = toatp_df.index + 1
                toatp_df = toatp_df.replace('--', np.nan)
                toatp_df = toatp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_average_time_of_possession_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/time-of-possession-pct-net-of-ot' \
                    + '?date=' \
                    + this_week_date_str
                toatpp_df = main_hist(total_offense_average_time_of_possession_percentage_url_current, season, str(week), this_week_date_str,
                                     'total_offense_average_time_of_possession_percentage')
                toatpp_df.rename(columns={'Rank': 'Rank_Total_Offense_Average_Time_of_Possession_Percentage',
                                         season: 'Current_Season_Total_Offense_Average_Time_of_Possession_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Total_Offense_Average_Time_of_Possession_Percentage',
                                         'Last 3': 'Last 3_Total_Offense_Average_Time_of_Possession_Percentage',
                                         'Last 1': 'Last 1_Total_Offense_Average_Time_of_Possession_Percentage',
                                         'Home': 'At_Home_Total_Offense_Average_Time_of_Possession_Percentage',
                                         'Away': 'Away_Total_Offense_Average_Time_of_Possession_Percentage'
                                         }, inplace=True)
                toatpp_df['Team'] = toatpp_df['Team'].str.strip()
                if season == '2010':
                    toatpp_df['Rank_Total_Offense_Average_Time_of_Possession_Percentage'] = toatpp_df.index + 1
                toatpp_df = toatpp_df.replace('--', np.nan)
                toatpp_df = toatpp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_third_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversion-pct' \
                    + '?date=' \
                    + this_week_date_str
                totdcp_df = main_hist(total_offense_third_down_conversion_percent_url_current, season,
                                      str(week), this_week_date_str,
                                      'total_offense_third_down_conversion_percent')
                totdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down-Conversion_Percent',
                                          season: 'Current_Season_Total_Offense_Third_Down-Conversion_Percent',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversion_Percent',
                                          'Last 3': 'Last 3_Total_Offense_Third_Down_Conversion_Percent',
                                          'Last 1': 'Last 1_Total_Offense_Third_Down_Conversion_Percent',
                                          'Home': 'At_Home_Total_Offense_Third_Down_Conversion_Percent',
                                          'Away': 'Away_Total_Offense_Third_Down_Conversion_Percent'
                                          }, inplace=True)
                totdcp_df['Team'] = totdcp_df['Team'].str.strip()
                if season == '2010':
                    totdcp_df['Rank_Total_Offense_Third_Down_Conversion_Percent'] = totdcp_df.index + 1
                totdcp_df = totdcp_df.replace('--', np.nan)
                totdcp_df = totdcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_fourth_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-down-conversion-pct' \
                    + '?date=' \
                    + this_week_date_str
                tofdcp_df = main_hist(total_offense_fourth_down_conversion_percent_url_current, season, str(week), this_week_date_str,
                                      'total_offense_fourth_down_conversion_percent')
                tofdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down-Conversion_Percent',
                                          season: 'Current_Season_Total_Offense_Fourth_Down-Conversion_Percent',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Last 3': 'Last 3_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Last 1': 'Last 1_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Home': 'At_Home_Total_Offense_Fourth_Down_Conversion_Percent',
                                          'Away': 'Away_Total_Offense_Fourth_Down_Conversion_Percent'
                                          }, inplace=True)
                tofdcp_df['Team'] = tofdcp_df['Team'].str.strip()
                if season == '2010':
                    tofdcp_df['Rank_Total_Offense_Fourth_Down_Conversion_Percent'] = tofdcp_df.index + 1
                tofdcp_df = tofdcp_df.replace('--', np.nan)
                tofdcp_df = tofdcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_punts_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-play' \
                    + '?date=' \
                    + this_week_date_str
                toppp_df = main_hist(total_offense_punts_per_play_url_current, season, str(week), this_week_date_str,
                                      'total_offense_punts_per_play')
                toppp_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Play',
                                          season: 'Current_Season_Total_Offense_Punts_per_Play',
                                          str(int(
                                              season) - 1): 'Previous_Season_Total_Offense_Punts_per_Play',
                                          'Last 3': 'Last 3_Total_Offense_Punts_per_Play',
                                          'Last 1': 'Last 1_Total_Offense_Punts_per_Play',
                                          'Home': 'At_Home_Total_Offense_Punts_per_Play',
                                          'Away': 'Away_Total_Offense_Punts_per_Play'
                                          }, inplace=True)
                toppp_df['Team'] = toppp_df['Team'].str.strip()
                if season == '2010':
                    toppp_df['Rank_Total_Offense_Punts_per_Play'] = toppp_df.index + 1
                toppp_df = toppp_df.replace('--', np.nan)
                toppp_df = toppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_offense_punts_per_offensive_score_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-offensive-score' \
                    + '?date=' \
                    + this_week_date_str
                toppos_df = main_hist(total_offense_punts_per_offensive_score_url_current, season, str(week), this_week_date_str,
                                     'total_offense_punts_per_offensive score')
                toppos_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Offensive_Score',
                                         season: 'Current_Season_Total_Offense_Punts_per_Offensive_Score',
                                         str(int(
                                             season) - 1): 'Previous_Season_Total_Offense_Punts_per_Offensive_Score',
                                         'Last 3': 'Last 3_Total_Offense_Punts_per_Offensive_Score',
                                         'Last 1': 'Last 1_Total_Offense_Punts_per_Offensive_Score',
                                         'Home': 'At_Home_Total_Offense_Punts_per_Offensive_Score',
                                         'Away': 'Away_Total_Offense_Punts_per_Offensive_Score'
                                         }, inplace=True)
                toppos_df['Team'] = toppos_df['Team'].str.strip()
                if season == '2010':
                    toppos_df['Rank_Total_Offense_Punts_per_Offensive_Score'] = toppos_df.index + 1
                toppos_df = toppos_df.replace('--', np.nan)
                toppos_df = toppos_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                rushing_offense_rushing_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-attempts-per-game' \
                    + '?date=' \
                    + this_week_date_str
                rorapg_df = main_hist(rushing_offense_rushing_attempts_per_game_url_current, season, str(week), this_week_date_str,
                                      'rushing_offense_rushing_attempts_per_game')
                rorapg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Attempts_per_Game',
                                          season: 'Current_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Last 3': 'Last 3_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Last 1': 'Last 1_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Home': 'At_Home_Rushing_Offense_Rushing_Attempts_per_Game',
                                          'Away': 'Away_Rushing_Offense_Rushing_Attempts_per_Game'
                                          }, inplace=True)
                rorapg_df['Team'] = rorapg_df['Team'].str.strip()
                if season == '2010':
                    rorapg_df['Rank_Rushing_Offense_Rushing_Attempts_per_Game'] = rorapg_df.index + 1
                rorapg_df = rorapg_df.replace('--', np.nan)
                rorapg_df = rorapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                rushing_offense_rushing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-per-game' \
                    + '?date=' \
                    + this_week_date_str
                rorypg_df = main_hist(rushing_offense_rushing_yards_per_game_url_current, season, str(week), this_week_date_str,
                                      'rushing_offense_rushing_yards_per_game')
                rorypg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Game',
                                          season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Last 3': 'Last 3_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Last 1': 'Last 1_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Game',
                                          'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Game'
                                          }, inplace=True)
                rorypg_df['Team'] = rorypg_df['Team'].str.strip()
                if season == '2010':
                    rorypg_df['Rank_Rushing_Offense_Rushing_Yards_per_Game'] = rorypg_df.index + 1
                rorypg_df = rorypg_df.replace('--', np.nan)
                rorypg_df = rorypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                rushing_offense_rushing_yards_per_rush_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-rush-attempt' \
                    + '?date=' \
                    + this_week_date_str
                rorypra_df = main_hist(rushing_offense_rushing_yards_per_rush_attempt_url_current, season, str(week), this_week_date_str,
                                      'rushing_offense_rushing_yards_per_rush_attempt')
                rorypra_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                          season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                          str(int(
                                              season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                          'Last 3': 'Last 3_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                          'Last 1': 'Last 1_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                          'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                          'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Rush_Attempt'
                                          }, inplace=True)
                rorypra_df['Team'] = rorypra_df['Team'].str.strip()
                if season == '2010':
                    rorypra_df['Rank_Rushing_Offense_Rushing_Yards_per_Rush_Attempt'] = rorypra_df.index + 1
                rorypra_df = rorypra_df.replace('--', np.nan)
                rorypra_df = rorypra_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                rushing_offense_rushing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-play-pct' \
                    + '?date=' \
                    + this_week_date_str
                rorpp_df = main_hist(rushing_offense_rushing_play_percentage_url_current, season, str(week), this_week_date_str,
                                       'rushing_offense_rushing_play_percentage')
                rorpp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Play_Percentage',
                                           season: 'Current_Season_Rushing_Offense_Rushing_Play_Percentage',
                                           str(int(
                                               season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Play_Percentage',
                                           'Last 3': 'Last 3_Rushing_Offense_Rushing_Play_Percentage',
                                           'Last 1': 'Last 1_Rushing_Offense_Rushing_Play_Percentage',
                                           'Home': 'At_Home_Rushing_Offense_Rushing_Play_Percentage',
                                           'Away': 'Away_Rushing_Offense_Rushing_Play_Percentage'
                                           }, inplace=True)
                rorpp_df['Team'] = rorpp_df['Team'].str.strip()
                if season == '2010':
                    rorpp_df['Rank_Rushing_Offense_Rushing_Play_Percentage'] = rorpp_df.index + 1
                rorpp_df = rorpp_df.replace('--', np.nan)
                rorpp_df = rorpp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                rushing_offense_rushing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-pct' \
                    + '?date=' \
                    + this_week_date_str
                roryp_df = main_hist(rushing_offense_rushing_yards_percentage_url_current, season, str(week), this_week_date_str,
                                     'rushing_offense_rushing_yards_percentage')
                roryp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_Percentage',
                                         season: 'Current_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Last 3': 'Last 3_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Last 1': 'Last 1_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Home': 'At_Home_Rushing_Offense_Rushing_Yards_Percentage',
                                         'Away': 'Away_Rushing_Offense_Rushing_Yards_Percentage'
                                         }, inplace=True)
                roryp_df['Team'] = roryp_df['Team'].str.strip()
                if season == '2010':
                    roryp_df['Rank_Rushing_Offense_Rushing_Yards_Percentage'] = roryp_df.index + 1
                roryp_df = roryp_df.replace('--', np.nan)
                roryp_df = roryp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_pass_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/pass-attempts-per-game' \
                    + '?date=' \
                    + this_week_date_str
                popapg_df = main_hist(passing_offense_pass_attempts_per_game_url_current, season, str(week), this_week_date_str,
                                     'passing_offense_pass_attempts_per_game')
                popapg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Pass_Attempts_per_Game',
                                         season: 'Current_Season_Passing_Offense_Pass_Attempts_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Pass_Attempts_per_Game',
                                         'Last 3': 'Last 3_Passing_Offense_Pass_Attempts_per_Game',
                                         'Last 1': 'Last 1_Passing_Offense_Pass_Attempts_per_Game',
                                         'Home': 'At_Home_Passing_Offense_Pass_Attempts_per_Game',
                                         'Away': 'Away_Passing_Offense_Pass_Attempts_per_Game'
                                         }, inplace=True)
                popapg_df['Team'] = popapg_df['Team'].str.strip()
                if season == '2010':
                    popapg_df['Rank_Passing_Offense_Pass_Attempts_per_Game'] = popapg_df.index + 1
                popapg_df = popapg_df.replace('--', np.nan)
                popapg_df = popapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_completions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/completions-per-game' \
                    + '?date=' \
                    + this_week_date_str
                pocpg_df = main_hist(passing_offense_completions_per_game_url_current, season, str(week), this_week_date_str,
                                      'passing_offense_completions_per_game')
                pocpg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completions_per_Game',
                                          season: 'Current_Season_Passing_Offense_Completions_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_Completions_per_Game',
                                          'Last 3': 'Last 3_Passing_Offense_Completions_per_Game',
                                          'Last 1': 'Last 1_Passing_Offense_Completions_per_Game',
                                          'Home': 'At_Home_Passing_Offense_Completions_per_Game',
                                          'Away': 'Away_Passing_Offense_Completions_per_Game'
                                          }, inplace=True)
                pocpg_df['Team'] = pocpg_df['Team'].str.strip()
                if season == '2010':
                    pocpg_df['Rank_Passing_Offense_Completions_per_Game'] = pocpg_df.index + 1
                pocpg_df = pocpg_df.replace('--', np.nan)
                pocpg_df = pocpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_incompletions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/incompletions-per-game' \
                    + '?date=' \
                    + this_week_date_str
                poipg_df = main_hist(passing_offense_incompletions_per_game_url_current, season, str(week), this_week_date_str,
                                     'passing_offense_incompletions_per_game')
                poipg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Incompletions_per_Game',
                                         season: 'Current_Season_Passing_Offense_Incompletions_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Incompletions_per_Game',
                                         'Last 3': 'Last 3_Passing_Offense_Incompletions_per_Game',
                                         'Last 1': 'Last 1_Passing_Offense_Incompletions_per_Game',
                                         'Home': 'At_Home_Passing_Offense_Incompletions_per_Game',
                                         'Away': 'Away_Passing_Offense_Incompletions_per_Game'
                                         }, inplace=True)
                poipg_df['Team'] = poipg_df['Team'].str.strip()
                if season == '2010':
                    poipg_df['Rank_Passing_Offense_Incompletions_per_Game'] = poipg_df.index + 1
                poipg_df = poipg_df.replace('--', np.nan)
                poipg_df = poipg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_completion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/completion-pct' \
                    + '?date=' \
                    + this_week_date_str
                pocp_df = main_hist(passing_offense_completion_percentage_url_current, season, str(week), this_week_date_str,
                                     'passing_offense_completion_percentage')
                pocp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completion_Percentage',
                                         season: 'Current_Season_Passing_Offense_Completion_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Completion_Percentage',
                                         'Last 3': 'Last 3_Passing_Offense_Completion_Percentage',
                                         'Last 1': 'Last 1_Passing_Offense_Completion_Percentage',
                                         'Home': 'At_Home_Passing_Offense_Completion_Percentage',
                                         'Away': 'Away_Passing_Offense_Completion_Percentage'
                                         }, inplace=True)
                pocp_df['Team'] = pocp_df['Team'].str.strip()
                if season == '2010':
                    pocp_df['Rank_Passing_Offense_Completion_Percentage'] = pocp_df.index + 1
                pocp_df = pocp_df.replace('--', np.nan)
                pocp_df = pocp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_passing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-per-game' \
                    + '?date=' \
                    + this_week_date_str
                popypg_df = main_hist(passing_offense_passing_yards_per_game_url_current, season, str(week), this_week_date_str,
                                    'passing_offense_passing_yards_per_game')
                popypg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_per_Game',
                                        season: 'Current_Season_Passing_Offense_Passing_Yards_per_Game',
                                        str(int(
                                            season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_per_Game',
                                        'Last 3': 'Last 3_Passing_Offense_Passing_Yards_per_Game',
                                        'Last 1': 'Last 1_Passing_Offense_Passing_Yards_per_Game',
                                        'Home': 'At_Home_Passing_Offense_Passing_Yards_per_Game',
                                        'Away': 'Away_Passing_Offense_Passing_Yards_per_Game'
                                        }, inplace=True)
                popypg_df['Team'] = popypg_df['Team'].str.strip()
                if season == '2010':
                    popypg_df['Rank_Passing_Offense_Passing_Yards_per_Game'] = popypg_df.index + 1
                popypg_df = popypg_df.replace('--', np.nan)
                popypg_df = popypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_qb_sacked_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-per-game' \
                    + '?date=' \
                    + this_week_date_str
                poqspg_df = main_hist(passing_offense_qb_sacked_per_game_url_current, season, str(week), this_week_date_str,
                                      'passing_offense_qb_sacked_per_game')
                poqspg_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_per_Game',
                                          season: 'Current_Season_Passing_Offense_QB_Sacked_per_Game',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_per_Game',
                                          'Last 3': 'Last 3_Passing_Offense_QB_Sacked_per_Game',
                                          'Last 1': 'Last 1_Passing_Offense_QB_Sacked_per_Game',
                                          'Home': 'At_Home_Passing_Offense_QB_Sacked_per_Game',
                                          'Away': 'Away_Passing_Offense_QB_Sacked_per_Game'
                                          }, inplace=True)
                poqspg_df['Team'] = poqspg_df['Team'].str.strip()
                if season == '2010':
                    poqspg_df['Rank_Passing_Offense_QB_Sacked_per_Game'] = poqspg_df.index + 1
                poqspg_df = poqspg_df.replace('--', np.nan)
                poqspg_df = poqspg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_qb_sacked_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-pct' \
                    + '?date=' \
                    + this_week_date_str
                poqsp_df = main_hist(passing_offense_qb_sacked_percentage_url_current, season, str(week), this_week_date_str,
                                      'passing_offense_qb_sacked_percentage')
                poqsp_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_Percentage',
                                          season: 'Current_Season_Passing_Offense_QB_Sacked_Percentage',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_Percentage',
                                          'Last 3': 'Last 3_Passing_Offense_QB_Sacked_Percentage',
                                          'Last 1': 'Last 1_Passing_Offense_QB_Sacked_Percentage',
                                          'Home': 'At_Home_Passing_Offense_QB_Sacked_Percentage',
                                          'Away': 'Away_Passing_Offense_QB_Sacked_Percentage'
                                          }, inplace=True)
                poqsp_df['Team'] = poqsp_df['Team'].str.strip()
                if season == '2010':
                    poqsp_df['Rank_Passing_Offense_QB_Sacked_Percentage'] = poqsp_df.index + 1
                poqsp_df = poqsp_df.replace('--', np.nan)
                poqsp_df = poqsp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_average_passer_rating_url_current = 'https://www.teamrankings.com/college-football/stat/average-team-passer-rating' \
                    + '?date=' \
                    + this_week_date_str
                poapr_df = main_hist(passing_offense_average_passer_rating_url_current, season, str(week), this_week_date_str,
                                     'passing_offense_average_passer_rating')
                poapr_df.rename(columns={'Rank': 'Rank_Passing_Offense_Average_Passer_Rating',
                                         season: 'Current_Season_Passing_Offense_Average_Passer_Rating',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Average_Passer_Rating',
                                         'Last 3': 'Last 3_Passing_Offense_Average_Passer_Rating',
                                         'Last 1': 'Last 1_Passing_Offense_Average_Passer_Rating',
                                         'Home': 'At_Home_Passing_Offense_Average_Passer_Rating',
                                         'Away': 'Away_Passing_Offense_Average_Passer_Rating'
                                         }, inplace=True)
                poapr_df['Team'] = poapr_df['Team'].str.strip()
                if season == '2010':
                    poapr_df['Rank_Passing_Offense_Average_Passer_Rating'] = poapr_df.index + 1
                poapr_df = poapr_df.replace('--', np.nan)
                poqpr_df = poapr_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_passing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-play-pct' \
                    + '?date=' \
                    + this_week_date_str
                poppp_df = main_hist(passing_offense_passing_play_percentage_url_current, season, str(week), this_week_date_str,
                                     'passing_offense_passing_play_percentage')
                poppp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Play_Percentage',
                                         season: 'Current_Season_Passing_Offense_Passing_Play_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Passing_Play_Percentage',
                                         'Last 3': 'Last 3_Passing_Offense_Passing_Play_Percentage',
                                         'Last 1': 'Last 1_Passing_Offense_Passing_Play_Percentage',
                                         'Home': 'At_Home_Passing_Offense_Passing_Play_Percentage',
                                         'Away': 'Away_Passing_Offense_Passing_Play_Percentage'
                                         }, inplace=True)
                poppp_df['Team'] = poppp_df['Team'].str.strip()
                if season == '2010':
                    poppp_df['Rank_Passing_Offense_Passing_Play_Percentage'] = poppp_df.index + 1
                poppp_df = poppp_df.replace('--', np.nan)
                poppp_df = poppp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_passing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-pct' \
                    + '?date=' \
                    + this_week_date_str
                popyp_df = main_hist(passing_offense_passing_yards_percentage_url_current, season, str(week), this_week_date_str,
                                     'passing_offense_passing_yards_percentage')
                popyp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_Percentage',
                                         season: 'Current_Season_Passing_Offense_Passing_Yards_Percentage',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_Percentage',
                                         'Last 3': 'Last 3_Passing_Offense_Passing_Yards_Percentage',
                                         'Last 1': 'Last 1_Passing_Offense_Passing_Yards_Percentage',
                                         'Home': 'At_Home_Passing_Offense_Passing_Yards_Percentage',
                                         'Away': 'Away_Passing_Offense_Passing_Yards_Percentage'
                                         }, inplace=True)
                popyp_df['Team'] = popyp_df['Team'].str.strip()
                if season == '2010':
                    popyp_df['Rank_Passing_Offense_Passing_Yards_Percentage'] = popyp_df.index + 1
                popyp_df = popyp_df.replace('--', np.nan)
                popyp_df = popyp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_yards_per_pass_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-pass-attempt' \
                    + '?date=' \
                    + this_week_date_str
                poyppa_df = main_hist(passing_offense_yards_per_pass_attempt_url_current, season, str(week), this_week_date_str,
                                     'passing_offense_yards_per_pass_attempt')
                poyppa_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Pass_Attempt',
                                         season: 'Current_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                         str(int(
                                             season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                         'Last 3': 'Last 3_Passing_Offense_Yards_per_Pass_Attempt',
                                         'Last 1': 'Last 1_Passing_Offense_Yards_per_Pass_Attempt',
                                         'Home': 'At_Home_Passing_Offense_Yards_per_Pass_Attempt',
                                         'Away': 'Away_Passing_Offense_Yards_per_Pass_Attempt'
                                         }, inplace=True)
                poyppa_df['Team'] = poyppa_df['Team'].str.strip()
                if season == '2010':
                    poyppa_df['Rank_Passing_Offense_Yards_per_Pass_Attempt'] = poyppa_df.index + 1
                poyppa_df = poyppa_df.replace('--', np.nan)
                poyppa_df = poyppa_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                passing_offense_yards_per_completion_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-completion' \
                    + '?date=' \
                    + this_week_date_str
                poypc_df = main_hist(passing_offense_yards_per_completion_url_current, season, str(week), this_week_date_str,
                                      'passing_offense_yards_per_completion')
                poypc_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Completion',
                                          season: 'Current_Season_Passing_Offense_Yards_per_Completion',
                                          str(int(
                                              season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Completion',
                                          'Last 3': 'Last 3_Passing_Offense_Yards_per_Completion',
                                          'Last 1': 'Last 1_Passing_Offense_Yards_per_Completion',
                                          'Home': 'At_Home_Passing_Offense_Yards_per_Completion',
                                          'Away': 'Away_Passing_Offense_Yards_per_Completion'
                                          }, inplace=True)
                poypc_df['Team'] = poypc_df['Team'].str.strip()
                if season == '2010':
                    poypc_df['Rank_Passing_Offense_Yards_per_Completion'] = poypc_df.index + 1
                poypc_df = poypc_df.replace('--', np.nan)
                poypc_df = poypc_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                special_teams_offense_field_goal_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/field-goal-attempts-per-game' \
                    + '?date=' \
                    + this_week_date_str
                stofgapg_df = main_hist(special_teams_offense_field_goal_attempts_per_game_url_current, season, str(week), this_week_date_str,
                                     'special_teams_offense_field_goal_attempts_per_game')
                stofgapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                         season: 'Current_Season_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                         str(int(
                                             season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                         'Last 3': 'Last 3_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                         'Last 1': 'Last 1_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                         'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                         'Away': 'Away_Special_Teams_Offense_Field_Goal_Attempts_per_Game'
                                         }, inplace=True)
                stofgapg_df['Team'] = stofgapg_df['Team'].str.strip()
                if season == '2010':
                    stofgapg_df['Rank_Special_Teams_Offense_Field_Goal_Attempts_per_Game'] = stofgapg_df.index + 1
                stofgapg_df = stofgapg_df.replace('--', np.nan)
                stofgapg_df = stofgapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                special_teams_offense_field_goals_made_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/field-goals-made-per-game' \
                    + '?date=' \
                    + this_week_date_str
                stofgmpg_df = main_hist(special_teams_offense_field_goals_made_per_game_url_current, season, str(week), this_week_date_str,
                                        'special_teams_offense_field_goals_made_per_game')
                stofgmpg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            season: 'Current_Season_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            str(int(
                                                season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Last 3': 'Last 3_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Last 1': 'Last 1_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Home': 'At_Home_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                            'Away': 'Away_Special_Teams_Offense_Field_Goals_Made_per_Game'
                                            }, inplace=True)
                stofgmpg_df['Team'] = stofgmpg_df['Team'].str.strip()
                if season == '2010':
                    stofgmpg_df['Rank_Special_Teams_Offense_Field_Goals_Made_per_Game'] = stofgmpg_df.index + 1
                stofgmpg_df = stofgmpg_df.replace('--', np.nan)
                stofgmpg_df = stofgmpg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                special_teams_offense_field_goal_conversion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/field-goal-conversion-pct' \
                    + '?date=' \
                    + this_week_date_str
                stofgcp_df = main_hist(special_teams_offense_field_goal_conversion_percentage_url_current, season, str(week), this_week_date_str,
                                        'special_teams_offense_field_goal_conversion_percentage')
                stofgcp_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                            season: 'Current_Season_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                            str(int(
                                                season) - 1): 'Previous_Season_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                            'Last 3': 'Last 3_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                            'Last 1': 'Last 1_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                            'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                            'Away': 'Away_Special_Teams_Offense_Field_Goal_Conversion_Percentage'
                                            }, inplace=True)
                stofgcp_df['Team'] = stofgcp_df['Team'].str.strip()
                if season == '2010':
                    stofgcp_df['Rank_Special_Teams_Offense_Field_Goal_Conversion_Percentage'] = stofgcp_df.index + 1
                stofgcp_df = stofgcp_df.replace('--', np.nan)
                stofgcp_df = stofgcp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                special_teams_offense_punt_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/punt-attempts-per-game' \
                    + '?date=' \
                    + this_week_date_str
                stopapg_df = main_hist(special_teams_offense_punt_attempts_per_game_url_current, season, str(week), this_week_date_str,
                                       'special_teams_offense_punt_attempts_per_game')
                stopapg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           season: 'Current_Season_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Last 3': 'Last 3_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Last 1': 'Last 1_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Home': 'At_Home_Special_Teams_Offense_Punt_Attempts_per_Game',
                                           'Away': 'Away_Special_Teams_Offense_Punt_Attempts_per_Game'
                                           }, inplace=True)
                stopapg_df['Team'] = stopapg_df['Team'].str.strip()
                if season == '2010':
                    stopapg_df['Rank_Special_Teams_Offense_Punt_Attempts_per_Game'] = stopapg_df.index + 1
                stopapg_df = stopapg_df.replace('--', np.nan)
                stopapg_df = stopapg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                special_teams_offense_gross_punt_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/gross-punt-yards-per-game' \
                    + '?date=' \
                    + this_week_date_str
                stogpypg_df = main_hist(special_teams_offense_gross_punt_yards_per_game_url_current, season, str(week), this_week_date_str,
                                       'special_teams_offense_gross_punt_yards_per_game')
                stogpypg_df.rename(columns={'Rank': 'Rank_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                           season: 'Current_Season_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                           str(int(
                                               season) - 1): 'Previous_Season_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                           'Last 3': 'Last 3_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                           'Last 1': 'Last 1_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                           'Home': 'At_Home_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                           'Away': 'Away_Special_Teams_Offense_Gross_Punt_Yards_per_Game'
                                           }, inplace=True)
                stogpypg_df['Team'] = stogpypg_df['Team'].str.strip()
                if season == '2010':
                    stogpypg_df['Rank_Special_Teams_Offense_Gross_Punt_Yards_per_Game'] = stogpypg_df.index + 1
                stogpypg_df = stogpypg_df.replace('--', np.nan)
                stogpypg_df = stogpypg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                scoring_defense_opponent_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-game'\
                    + '?date='\
                    + this_week_date_str
                sdoppg_df = main_hist(scoring_defense_opponent_points_per_game_url_current, season, str(week), this_week_date_str, 'scoring_defense_opponent_points_per_game')
                sdoppg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Game',
                                      season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Game',
                                      str(int(season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Game',
                                      'Last 3': 'Last 3_Scoring_Defense_Opponent_Points_per_Game',
                                      'Last 1': 'Last 1_Scoring_Defense_Opponent_Points_per_Game',
                                      'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Game',
                                      'Away': 'Away_Scoring_Defense_Opponent_Points_per_Game'
                                      }, inplace=True)
                sdoppg_df['Team'] = sdoppg_df['Team'].str.strip()
                if season == '2010':
                    sdoppg_df['Rank_Scoring_Defense_Opponent_Points_per_Game'] = sdoppg_df.index + 1
                sdoppg_df = sdoppg_df.replace('--', np.nan)
                sdoppg_df = sdoppg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                scoring_defense_opp_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/opp-yards-per-point' \
                    + '?date=' \
                    + this_week_date_str
                sdoypp_df = main_hist(scoring_defense_opp_yards_per_point_url_current, season, str(week),
                                      this_week_date_str, 'scoring_defense_opp_yards_per_point')
                sdoypp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opp_Yards_per_Point',
                                          season: 'Current_Season_Scoring_Defense_Opp_Yards_per_Point',
                                          str(int(
                                              season) - 1): 'Previous_Season_Scoring_Defense_Opp_Yards_per_Point',
                                          'Last 3': 'Last 3_Scoring_Defense_Opp_Yards_per_Point',
                                          'Last 1': 'Last 1_Scoring_Defense_Opp_Yards_per_Point',
                                          'Home': 'At_Home_Scoring_Defense_Opp_Yards_per_Point',
                                          'Away': 'Away_Scoring_Defense_Opp_Yards_per_Point'
                                          }, inplace=True)
                sdoypp_df['Team'] = sdoypp_df['Team'].str.strip()
                if season == '2010':
                    sdoypp_df['Rank_Scoring_Defense_Opp_Yards_per_Point'] = sdoypp_df.index + 1
                sdoypp_df = sdoypp_df.replace('--', np.nan)
                sdoypp_df = sdoypp_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                total_defense_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-game'\
                    + '?date='\
                    + this_week_date_str
                td_df = main_hist(total_defense_url_current, season, str(week), this_week_date_str, 'total_defense')
                td_df.rename(columns={'Rank': 'Rank_Total_Defense',
                                      season: 'Current_Season_Total_Defense',
                                      str(int(season) - 1): 'Previous_Season_Total_Defense',
                                      'Last 3': 'Last 3_Total_Defense',
                                      'Last 1': 'Last 1_Total_Defense',
                                      'Home': 'At_Home_Total_Defense',
                                      'Away': 'Away_Total_Defense'
                                      }, inplace=True)
                td_df['Team'] = td_df['Team'].str.strip()
                if season == '2010':
                    td_df['Rank_Total_Defense'] = td_df.index + 1
                td_df = td_df.replace('--', np.nan)
                td_df = td_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                turnovers_given_url_current = 'https://www.teamrankings.com/college-football/stat/giveaways-per-game'\
                    + '?date='\
                    + this_week_date_str
                tg_df = main_hist(turnovers_given_url_current, season, str(week), this_week_date_str, 'turnovers_given')
                tg_df.rename(columns={'Rank': 'Rank_Turnovers_Given',
                                      season: 'Current_Season_Turnovers_Given',
                                      str(int(season) - 1): 'Previous_Season_Turnovers_Given',
                                      'Last 3': 'Last 3_Turnovers_Given',
                                      'Last 1': 'Last 1_Turnovers_Given',
                                      'Home': 'At_Home_Turnovers_Given',
                                      'Away': 'Away_Turnovers_Given'
                                      }, inplace=True)
                tg_df['Team'] = tg_df['Team'].str.strip()
                if season == '2010':
                    tg_df['Rank_Turnovers_Given'] = tg_df.index + 1
                tg_df = tg_df.replace('--', np.nan)
                tg_df = tg_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                turnovers_taken_url_current = 'https://www.teamrankings.com/college-football/stat/takeaways-per-game'\
                    + '?date='\
                    + this_week_date_str
                tt_df = main_hist(turnovers_taken_url_current, season, str(week), this_week_date_str, 'turnovers_taken')
                tt_df.rename(columns={'Rank': 'Rank_Turnovers_Taken',
                                      season: 'Current_Season_Turnovers_Taken',
                                      str(int(season) - 1): 'Previous_Season_Turnovers_Taken',
                                      'Last 3': 'Last 3_Turnovers_Taken',
                                      'Last 1': 'Last 1_Turnovers_Taken',
                                      'Home': 'At_Home_Turnovers_Taken',
                                      'Away': 'Away_Turnovers_Taken'
                                      }, inplace=True)
                tt_df['Team'] = tt_df['Team'].str.strip()
                if season == '2010':
                    tt_df['Rank_Turnovers_Taken'] = tt_df.index + 1
                tt_df = tt_df.replace('--', np.nan)
                tt_df = tt_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                last_5_url_current = 'https://www.teamrankings.com/college-football/ranking/last-5-games-by-other'\
                    + '?date='\
                    + this_week_date_str
                l5_df = main_hist(last_5_url_current, season, str(week), this_week_date_str, 'last_5')
                l5_df.rename(columns={'Rank': 'Rank_Last_5',
                                      'Rating': 'Rating_Last_5',
                                      'Hi': 'Hi_Last_5',
                                      'Low': 'Low_Last_5',
                                      'Last': 'Last_Last_5'
                                      }, inplace=True)
                l5_df['Team'] = l5_df['Team'].str.strip()
                if season == '2010':
                    l5_df['Rank_Last_5'] = l5_df.index + 1
                l5_df = l5_df.replace('--', np.nan)
                l5_df = l5_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                neutral_site_url_current = 'https://www.teamrankings.com/college-football/ranking/neutral-by-other'\
                    + '?date='\
                    + this_week_date_str
                ns_df = main_hist(neutral_site_url_current, season, str(week), this_week_date_str, 'neutral_site')
                ns_df.rename(columns={'Rank': 'Rank_Neutral_Site',
                                      'Rating': 'Rating_Neutral_Site',
                                      'Hi': 'Hi_Neutral_Site',
                                      'Low': 'Low_Neutral_Site',
                                      'Last': 'Last_Neutral_Site'
                                      }, inplace=True)
                ns_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
                ns_df['Team'] = ns_df['Team'].str.strip()
                if season == '2010':
                    ns_df['Rank_Neutral_Site'] = ns_df.index + 1
                ns_df = ns_df.replace('--', np.nan)
                ns_df = ns_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                sos_url_current = 'https://www.teamrankings.com/college-football/ranking/schedule-strength-by-other'\
                    + '?date='\
                    + this_week_date_str
                sos_df = main_hist(sos_url_current, season, str(week), this_week_date_str, 'sos')
                sos_df.rename(columns={'Rank': 'Rank_SoS',
                                       'Rating': 'Rating_SoS',
                                       'Hi': 'Hi_SoS',
                                       'Low': 'Low_SoS',
                                       'Last': 'Last_SoS'
                                       }, inplace=True)
                sos_df['Team'] = sos_df['Team'].str.strip()
                if season == '2010':
                    sos_df['Rank_SoS'] = sos_df.index + 1
                sos_df = sos_df.replace('--', np.nan)
                sos_df = sos_df.apply(pd.to_numeric, errors='ignore')

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
                this_week_df = pd.merge(this_week_df, poppp_df, on=['Team', 'Season', 'Week'], how='outer')
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
                this_week_df = pd.merge(this_week_df, td_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tg_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, tt_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, l5_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, ns_df, on=['Team', 'Season', 'Week'], how='outer')
                this_week_df = pd.merge(this_week_df, sos_df, on=['Team', 'Season', 'Week'], how='outer')

                this_week_df = rearrange_columns(this_week_df)
                season_df = pd.concat([season_df, this_week_df])
                master_df = pd.concat([master_df, this_week_df])

                time.sleep(3)

            save_dir = 'C:\PythonPrograms'
            save_file = 'Scraped_TR_Data_Combined_' + season
            try:
                datascraper.save_df(season_df, save_dir, save_file)
                print('{} saved successfully.'.format(save_file))
                print('File successfully saved at {}.'.format(save_dir))
            except:
                print('I don\'t think the file saved, you should double check.')

        save_dir = 'C:\PythonPrograms'
        save_file = 'Scraped_TR_Data_Combined_ALL'
        try:
            datascraper.save_df(master_df, save_dir, save_file)
            print('{} saved successfully.'.format(save_file))
            print('File successfully saved at {}.'.format(save_dir))
        except:
            print('I don\'t think the file saved, you should double check.')

    else:
        season = 'Current'
        week = '14'
        season_start_date = datetime.date(2021, 12, 20)

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

        scoring_offense_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-game' \
                                      + '?date=' \
                                      + this_week_date_str
        so_df = main_hist(scoring_offense_url_current, season, str(week), this_week_date_str, 'scoring_offense')
        so_df.rename(columns={'Rank': 'Rank_Scoring_Offense',
                              season: 'Current_Season_Scoring_Offense',
                              str(int(season) - 1): 'Previous_Season_Scoring_Offense',
                              'Last 3': 'Last 3_Scoring_Offense',
                              'Last 1': 'Last 1_Scoring_Offense',
                              'Home': 'At_Home_Scoring_Offense',
                              'Away': 'Away_Scoring_Offense'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_average_scoring_margin_url_current = 'https://www.teamrankings.com/college-football/stat/average-scoring-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        tasm_df = main_hist(team_average_scoring_margin_url_current, season, str(week), this_week_date_str, 'team_average_scoring_margin')
        tasm_df.rename(columns={'Rank': 'Rank_Team_Average_Scoring_Margin',
                              season: 'Current_Season_Team_Average_Scoring_Margin',
                              str(int(season) - 1): 'Previous_Season_Team_Average_Scoring_Margin',
                              'Last 3': 'Last 3_Team_Average_Scoring_Margin',
                              'Last 1': 'Last 1_Team_Average_Scoring_Margin',
                              'Home': 'At_Home_Team_Average_Scoring_Margin',
                              'Away': 'Away_Team_Average_Scoring_Margin'
                              }, inplace=True)
        tasm_df['Team'] = tasm_df['Team'].str.strip()
        time.sleep(1)

        team_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point' \
                                           + '?date=' \
                                           + this_week_date_str
        typp_df = main_hist(team_yards_per_point_url_current, season, str(week), this_week_date_str, 'team_yards_per_point')
        typp_df.rename(columns={'Rank': 'Rank_Team_Yards_per_Point',
                              season: 'Current_Season_Team_Yards_per_Point',
                              str(int(season) - 1): 'Previous_Season_Team_yards_per_Point',
                              'Last 3': 'Last 3_Team_Yards_per_Point',
                              'Last 1': 'Last 1_Team_Yards_per_Point',
                              'Home': 'At_Home_Team_Yards_per_Point',
                              'Away': 'Away_Team_Yards_per_Point'
                              }, inplace=True)
        typp_df['Team'] = typp_df['Team'].str.strip()
        time.sleep(1)

        team_yards_per_point_margin_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        typm_df = main_hist(team_yards_per_point_margin_url_current, season, str(week), this_week_date_str,
                          'team_yards_per_point_margin')
        typm_df.rename(columns={'Rank': 'Rank_Team_Yards_Per_Point_Margin',
                              season: 'Current_Season_Team_Yards_per_Point_Margin',
                              str(int(season) - 1): 'Previous_Season_Team_yards_per_Point_Margin',
                              'Last 3': 'Last 3_Team_Yards_per_Point_Margin',
                              'Last 1': 'Last 1_Team_Yards_per_Point_Margin',
                              'Home': 'At_Home_Team_Yards_per_Point_Nargin',
                              'Away': 'Away_Team_Yards_per_Point_Margin'
                              }, inplace=True)
        typm_df['Team'] = typm_df['Team'].str.strip()
        time.sleep(1)

        team_points_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play' \
                                           + '?date=' \
                                           + this_week_date_str
        typ_df = main_hist(team_points_per_play_url_current, season, str(week), this_week_date_str, 'team_points_per_play')
        typ_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play',
                              season: 'Current_Season_Team_Points_per_Play',
                              str(int(season) - 1): 'Previous_Season_Team_Points_per_Play',
                              'Last 3': 'Last 3_Team_Points_per_Play',
                              'Last 1': 'Last 1_Team_Points_per_Play',
                              'Home': 'At_Home_Team_Points_per_Play',
                              'Away': 'Away_Team_Points_per_Play'
                              }, inplace=True)
        typ_df['Team'] = typ_df['Team'].str.strip()
        time.sleep(1)

        team_points_per_play_margin_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        typpm_df = main_hist(team_points_per_play_margin_url_current, season, str(week), this_week_date_str, 'team_points_per_play_margin')
        typpm_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play_Margin',
                              season: 'Current_Season_Team_Points_per_Play_Margin',
                              str(int(season) - 1): 'Previous_Season_Team_Points_per_Play_Margin',
                              'Last 3': 'Last 3_Team_Points_per_Play_Margin',
                              'Last 1': 'Last 1_Team_Points_per_Play_Margin',
                              'Home': 'At_Home_Team_Points_per_Play_Margin',
                              'Away': 'Away_Team_Points_per_Play_Margin'
                              }, inplace=True)
        typpm_df['Team'] = typpm_df['Team'].str.strip()
        time.sleep(1)

        team_red_zone_scoring_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-attempts-per-game' \
                                                              + '?date=' \
                                                              + this_week_date_str
        trs_df = main_hist(team_red_zone_scoring_attempts_per_game_url_current, season, str(week), this_week_date_str,
                          'team_red_zone_scoring_attempts_per_game')
        trs_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Attempts_per_Game',
                              season: 'Current_Season_Team_Red-Zone_Scoring_Attempts_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scoring_Attempts_per_Game',
                              'Last 3': 'Last 3_Team_Red_Zone_Scoring_Attempts_per_Game',
                              'Last 1': 'Last 1_Team_Red_Zone_Scoring_Attempts_per_Game',
                              'Home': 'At_Home_Team_Red_Zone_Scoring_Attempts_per_Game',
                              'Away': 'Away_Team_Red_Zone-Scoring-Attempts_per_Game'
                              }, inplace=True)
        trs_df['Team'] = trs_df['Team'].str.strip()
        time.sleep(1)

        team_red_zone_scores_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scores-per-game' \
                                                    + '?date=' \
                                                    + this_week_date_str
        trsp_df = main_hist(team_red_zone_scores_per_game_url_current, season, str(week), this_week_date_str,
                          'team_red_zone_scores_per_game')
        trsp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scores_per_Game',
                              season: 'Current_Season_Team_Red-Zone_Scores_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scores_per_Game',
                              'Last 3': 'Last 3_Team_Red_Zone_Scores_per_Game',
                              'Last 1': 'Last 1_Team_Red_Zone_Scores_per_Game',
                              'Home': 'At_Home_Team_Red_Zone_Scores_per_Game',
                              'Away': 'Away_Team_Red_Zone-Scores_per_Game'
                              }, inplace=True)
        trsp_df['Team'] = trsp_df['Team'].str.strip()
        time.sleep(1)

        team_red_zone_scoring_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-pct' \
                                                       + '?date=' \
                                                       + this_week_date_str
        trspp_df = main_hist(team_red_zone_scoring_percentage_url_current, season, str(week), this_week_date_str,
                          'team_red_zone_scoring_percentage')
        trspp_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Percentage',
                              season: 'Current_Season_Team_Red-Zone_Scoring_Percentage',
                              str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scoring_Percentage',
                              'Last 3': 'Last 3_Team_Red_Zone_Scoring_Percentage',
                              'Last 1': 'Last 1_Team_Red_Zone_Scoring_Percentage',
                              'Home': 'At_Home_Team_Red_Zone_Scoring_Percentage',
                              'Away': 'Away_Team_Red_Zone-Scoring_Percentage'
                              }, inplace=True)
        trspp_df['Team'] = trspp_df['Team'].str.strip()
        time.sleep(1)

        team_offensive_touchdowns_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-touchdowns-per-game' \
                                                     + '?date=' \
                                                     + this_week_date_str
        tot_df = main_hist(team_offensive_touchdowns_per_game_url_current, season, str(week), this_week_date_str,
                      'team_offensive_touchdowns_per_game')
        tot_df.rename(columns={'Rank': 'Rank_Team_Offensive_Touchdowns_per_Game',
                          season: 'Current_Season_Team_Offensive_Touchdowns_per_Game',
                          str(int(season) - 1): 'Previous_Season_Team_Offensive_Touchdowns_per_Game',
                          'Last 3': 'Last 3_Team_Offensive_Touchdowns_per_Game',
                          'Last 1': 'Last 1_Team_Offensive_Touchdowns_per_Game',
                          'Home': 'At_Home_Team_Offensive_Touchdowns_per_Game',
                          'Away': 'Away_Team_Offensive_Touchdowns_per_Game'
                          }, inplace=True)
        tot_df['Team'] = tot_df['Team'].str.strip()
        time.sleep(1)

        team_offensive_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-points-per-game' \
                                                     + '?date=' \
                                                     + this_week_date_str
        top_df = main_hist(team_offensive_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_offensive_points_per_game')
        top_df.rename(columns={'Rank': 'Rank_Team_Offensive_Points_per_Game',
                              season: 'Current_Season_Team_Offensive_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Offensive_Points_per_Game',
                              'Last 3': 'Last 3_Team_Offensive_Points_per_Game',
                              'Last 1': 'Last 1_Team_Offensive_Points_per_Game',
                              'Home': 'At_Home_Team_Offensive_Points_per_Game',
                              'Away': 'Away_Team_Offensive_Points_per_Game'
                              }, inplace=True)
        top_df['Team'] = top_df['Team'].str.strip()
        time.sleep(1)

        team_offensive_point_share_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-point-share-pct' \
                                                           + '?date=' \
                                                           + this_week_date_str
        tops_df = main_hist(team_offensive_point_share_percentage_url_current, season, str(week), this_week_date_str,
                          'team_offensive_point_share_percentage')
        tops_df.rename(columns={'Rank': 'Rank_Team_Offensive_Point_Share_Percentage',
                              season: 'Current_Season_Team_Offensive_Point_Share_Percentage',
                              str(int(season) - 1): 'Previous_Season_Team_Offensive_Point_Share_Percentage',
                              'Last 3': 'Last 3_Team_Offensive_Point_Share_Percentage',
                              'Last 1': 'Last 1_Team_Offensive_Point_Share_Percentage',
                              'Home': 'At_Home_Team_Offensive_Point_Share_Percentage',
                              'Away': 'Away_Team_Offensive_Point_Share_Percentage'
                              }, inplace=True)
        tops_df['Team'] = tops_df['Team'].str.strip()
        time.sleep(1)

        team_first_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-points-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        fiq_df = main_hist(team_first_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_first_quarter_points_per_game')
        fiq_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Points_per_Game',
                              season: 'Current_Season_Team_First_Quarter_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_First_Quarter_Points_per_Game',
                              'Last 3': 'Last 3_Team_First_Quarter_Points_per_Game',
                              'Last 1': 'Last 1_Team_First_Quarter_Points_per_Game',
                              'Home': 'At_Home_Team_First_Quarter_Points_per_Game',
                              'Away': 'Away_Team_First_Quarter_Points_per_Game'
                              }, inplace=True)
        fiq_df['Team'] = fiq_df['Team'].str.strip()
        time.sleep(1)

        team_second_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-points-per-game' \
                                                          + '?date=' \
                                                          + this_week_date_str
        sq_df = main_hist(team_second_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_second_quarter_points_per_game')
        sq_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Points_per_Game',
                              season: 'Current_Season_Team_Second_Quarter_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Second_Quarter_Points_per_Game',
                              'Last 3': 'Last 3_Team_Second_Quarter_Points_per_Game',
                              'Last 1': 'Last 1_Team_Second_Quarter_Points_per_Game',
                              'Home': 'At_Home_Team_Second_Quarter_Points_per_Game',
                              'Away': 'Away_Team_Second_Quarter_Points_per_Game'
                              }, inplace=True)
        sq_df['Team'] = sq_df['Team'].str.strip()
        time.sleep(1)

        team_third_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/3rd-quarter-points-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        tq_df = main_hist(team_third_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_third_quarter_points_per_game')
        tq_df.rename(columns={'Rank': 'Rank_Team_Third_Quarter_Points_per_Game',
                              season: 'Current_Season_Team_Third_Quarter_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Third_Quarter_Points_per_Game',
                              'Last 3': 'Last 3_Team_Third_Quarter_Points_per_Game',
                              'Last 1': 'Last 1_Team_Third_Quarter_Points_per_Game',
                              'Home': 'At_Home_Team_Third_Quarter_Points_per_Game',
                              'Away': 'Away_Team_Third_Quarter_Points_per_Game'
                              }, inplace=True)
        tq_df['Team'] = tq_df['Team'].str.strip()
        time.sleep(1)

        team_fourth_quarter_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-points-per-game' \
                                                          + '?date=' \
                                                          + this_week_date_str
        fq_df = main_hist(team_fourth_quarter_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_fourth_quarter_points_per_game')
        fq_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Points_per_Game',
                              season: 'Current_Season_Team_Fourth_Quarter_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Fourth_Quarter_Points_per_Game',
                              'Last 3': 'Last 3_Team_Fourth_Quarter_Points_per_Game',
                              'Last 1': 'Last 1_Team_Fourth_Quarter_Points_per_Game',
                              'Home': 'At_Home_Team_Fourth_Quarter_Points_per_Game',
                              'Away': 'Away_Team_Fourth_Quarter_Points_per_Game'
                              }, inplace=True)
        fq_df['Team'] = fq_df['Team'].str.strip()
        time.sleep(1)

        team_overtime_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/overtime-points-per-game' \
                                                    + '?date=' \
                                                    + this_week_date_str
        ot_df = main_hist(team_overtime_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_overtime_points_per_game')
        ot_df.rename(columns={'Rank': 'Rank_Overtime_Points_per_Game',
                              season: 'Current_Season_Team_Overtime_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Overtime_Points_per_Game',
                              'Last 3': 'Last 3_Team_Overtime_Points_per_Game',
                              'Last 1': 'Last 1_Team_Overtime_Points_per_Game',
                              'Home': 'At_Home_Team_Overtime_Points_per_Game',
                              'Away': 'Away_Team_Overtime_Points_per_Game'
                              }, inplace=True)
        ot_df['Team'] = ot_df['Team'].str.strip()
        time.sleep(1)

        team_first_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-points-per-game' \
                                                      + '?date=' \
                                                      + this_week_date_str
        fh_df = main_hist(team_first_half_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_first_half_points_per_game')
        fh_df.rename(columns={'Rank': 'Rank_First_Half_Points_per_Game',
                              season: 'Current_Season_Team_First-Half_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_First-Half_Points_per_Game',
                              'Last 3': 'Last 3_Team_First-Half_Points_per_Game',
                              'Last 1': 'Last 1_Team_First-Half_Points_per_Game',
                              'Home': 'At_Home_Team_First_Half_Points_per_Game',
                              'Away': 'Away_Team_First-Half_Points_per_Game'
                              }, inplace=True)
        fh_df['Team'] = fh_df['Team'].str.strip()
        time.sleep(1)

        team_second_half_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-points-per-game' \
                                                       + '?date=' \
                                                       + this_week_date_str
        sh_df = main_hist(team_second_half_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_second_half_points_per_game')
        sh_df.rename(columns={'Rank': 'Rank_Second_Half_Points_per_Game',
                              season: 'Current_Season_Team_Second-Half_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Second-Half_Points_per_Game',
                              'Last 3': 'Last 3_Team_Second-Half_Points_per_Game',
                              'Last 1': 'Last 1_Team_Second-Half_Points_per_Game',
                              'Home': 'At_Home_Team_Second_Half_Points_per_Game',
                              'Away': 'Away_Team_Second-Half_Points_per_Game'
                              }, inplace=True)
        sh_df['Team'] = sh_df['Team'].str.strip()
        time.sleep(1)

        team_first_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-quarter-time-of-possession-share-pct' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
        fiqtp_df = main_hist(team_first_quarter_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                            'team_first_quarter_time_of_possession_share_percent')
        fiqtp_df.rename(columns={'Rank': 'Rank_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last 3_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last 1_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_First_Quarter_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_First_Quarter_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        fiqtp_df['Team'] = fiqtp_df['Team'].str.strip()
        time.sleep(1)

        team_second_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-quarter-time-of-possession-share-pct' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
        sqtp_df = main_hist(team_second_quarter_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                            'team_second_quarter_time_of_possession_share_percent')
        sqtp_df.rename(columns={'Rank': 'Rank_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last 3_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last 1_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Second_Quarter_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Second_Quarter_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        sqtp_df['Team'] = sqtp_df['Team'].str.strip()
        time.sleep(1)

        team_third_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/3rd-quarter-time-of-possession-share-pct' \
                                                                          + '?date=' \
                                                                          + this_week_date_str
        tqtp_df = main_hist(team_third_quarter_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                            'team_third_quarter_time_of_possession_share_percent')
        tqtp_df.rename(columns={'Rank': 'Rank_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last 3_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last 1_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Third_Quarter_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Third_Quarter_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        tqtp_df['Team'] = tqtp_df['Team'].str.strip()
        time.sleep(1)

        team_fourth_quarter_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/4th-quarter-time-of-possession-share-pct' \
                                                                           + '?date=' \
                                                                           + this_week_date_str
        fqtp_df = main_hist(team_fourth_quarter_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                            'team_fourth_quarter_time_of_possession_share_percent')
        fqtp_df.rename(columns={'Rank': 'Rank_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last 3_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last 1_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Fourth_Quarter_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Fourth_Quarter_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        fqtp_df['Team'] = fqtp_df['Team'].str.strip()
        time.sleep(1)

        team_first_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/1st-half-time-of-possession-share-pct' \
                                                                       + '?date=' \
                                                                       + this_week_date_str
        fhtp_df = main_hist(team_first_half_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                            'team_first_half_time_of_possession_share_percent')
        fhtp_df.rename(columns={'Rank': 'Rank_Team_First_Half_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_First_Half_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last 3_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last 1_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_First_Half_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_First_Half_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        fhtp_df['Team'] = fhtp_df['Team'].str.strip()
        time.sleep(1)

        team_second_half_time_of_possession_share_percent_url_current = 'https://www.teamrankings.com/college-football/stat/2nd-half-time-of-possession-share-pct' \
                                                                        + '?date=' \
                                                                        + this_week_date_str
        shtp_df = main_hist(team_second_half_time_of_possession_share_percent_url_current, season, str(week), this_week_date_str,
                            'team_second_half_time_of_possession_share_percent')
        shtp_df.rename(columns={'Rank': 'Rank_Team_Second_Half_Time_of_Possession_Share_Percent',
                                season: 'Current_Team_Second_Half_Time_of_Possession_Share_Percent',
                                str(int(
                                    season) - 1): 'Previous_Season_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Last 3': 'Last 3_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Last 1': 'Last 1_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Home': 'At_Home_Team_Second_Half_Time_of_Possession_Share_Percent',
                                'Away': 'Away_Team_Second_Half_Time_of_Possession_Share_Percent'
                                }, inplace=True)
        shtp_df['Team'] = shtp_df['Team'].str.strip()
        time.sleep(1)

        total_offense_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-game' \
                                    + '?date=' \
                                    + this_week_date_str
        toypg_df = main_hist(total_offense_yards_per_game_url_current, season, str(week), this_week_date_str, 'total_offense_yards_per_game')
        toypg_df.rename(columns={'Rank': 'Rank_Total_Offense_Yards_per_Game',
                              season: 'Current_Season_Total_Offense_Yards_per_Game',
                              str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Game',
                              'Last 3': 'Last 3_Total_Offense_Yards_per_Game',
                              'Last 1': 'Last 1_Total_Offense_Yards_per_Game',
                              'Home': 'At_Home_Total_Offense_Yards_per_Game',
                              'Away': 'Away_Total_Offense_Yards_per_Game'
                              }, inplace=True)
        toypg_df['Team'] = toypg_df['Team'].str.strip()
        time.sleep(1)

        total_offense_plays_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/plays-per-game' \
                                                   + '?date=' \
                                                   + this_week_date_str
        toppg_df = main_hist(total_offense_plays_per_game_url_current, season, str(week), this_week_date_str,
                             'total_offense_plays_per_game')
        toppg_df.rename(columns={'Rank': 'Rank_Total_Offense_Plays_per_Game',
                                 season: 'Current_Season_Total_Offense_Plays_per_Game',
                                 str(int(season) - 1): 'Previous_Season_Total_Offense_Plays_per_Game',
                                 'Last 3': 'Last 3_Total_Offense_Plays_per_Game',
                                 'Last 1': 'Last 1_Total_Offense_Plays_per_Game',
                                 'Home': 'At_Home_Total_Offense_Plays_per_Game',
                                 'Away': 'Away_Total_Offense_Plays_per_Game'
                                 }, inplace=True)
        toppg_df['Team'] = toppg_df['Team'].str.strip()
        time.sleep(1)

        total_offense_yards_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-play' \
                                                   + '?date=' \
                                                   + this_week_date_str
        toypp_df = main_hist(total_offense_yards_per_play_url_current, season, str(week), this_week_date_str,
                             'total_offense_yards_per_play')
        toypp_df.rename(columns={'Rank': 'Rank_Total_Offense_Yards_per_Play',
                                 season: 'Current_Season_Total_Offense_Yards_per_Play',
                                 str(int(season) - 1): 'Previous_Season_Total_Offense_Yards_per_Play',
                                 'Last 3': 'Last 3_Total_Offense_Yards_per_Play',
                                 'Last 1': 'Last 1_Total_Offense_Yards_per_Play',
                                 'Home': 'At_Home_Total_Offense_Yards_per_Play',
                                 'Away': 'Away_Total_Offense_Yards_per_Play'
                                 }, inplace=True)
        toypp_df['Team'] = toypp_df['Team'].str.strip()
        time.sleep(1)

        total_offense_third_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-downs-per-game' \
                                                        + '?date=' \
                                                        + this_week_date_str
        totdpg_df = main_hist(total_offense_third_down_per_game_url_current, season, str(week), this_week_date_str,
                              'total_offense_third_down_per_game')
        totdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_per_Game',
                                  season: 'Current_Season_Total_Offense_Third_Down_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Total_Offense_Third_Down_per_Game',
                                  'Last 3': 'Last 3_Total_Offense_Third_Down_per_Game',
                                  'Last 1': 'Last 1_Total_Offense_Third_Down_per_Game',
                                  'Home': 'At_Home_Total_Offense_Third_Down_per_Game',
                                  'Away': 'Away_Total_Offense_Third-Down-per_Game'
                                  }, inplace=True)
        totdpg_df['Team'] = totdpg_df['Team'].str.strip()
        time.sleep(1)

        total_offense_third_down_conversions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversions-per-game' \
                                                                    + '?date=' \
                                                                    + this_week_date_str
        totdcpg_df = main_hist(total_offense_third_down_conversions_per_game_url_current, season, str(week), this_week_date_str,
                               'total_offense_third_down_conversions_per_game')
        totdcpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down_Conversions_per_Game',
                                   season: 'Current_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                   str(int(
                                       season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Last 3': 'Last 3_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Last 1': 'Last 1_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Home': 'At_Home_Total_Offense_Third_Down_Conversions_per_Game',
                                   'Away': 'Away_Total_Offense_Third-Down-Conversions_per_Game'
                                   }, inplace=True)
        totdcpg_df['Team'] = totdcpg_df['Team'].str.strip()
        time.sleep(1)

        total_offense_fourth_down_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-downs-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        tofdpg_df = main_hist(total_offense_fourth_down_per_game_url_current, season, str(week),
                              this_week_date_str,
                              'total_offense_fourth_down_per_game')
        tofdpg_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down_per_Game',
                                  season: 'Current_Season_Total_Offense_Fourth_Down_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_per_Game',
                                  'Last 3': 'Last 3_Total_Offense_Fourth_Down_per_Game',
                                  'Last 1': 'Last 1_Total_Offense_Fourth_Down_per_Game',
                                  'Home': 'At_Home_Total_Offense_Fourth_Down_per_Game',
                                  'Away': 'Away_Total_Offense_Fourth-Down-per_Game'
                                  }, inplace=True)
        tofdpg_df['Team'] = tofdpg_df['Team'].str.strip()
        time.sleep(1)

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
                                   'Last 3': 'Last 3_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   'Last 1': 'Last 1_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   'Home': 'At_Home_Total_Offense_Fourth_Down_Conversions_per_Game',
                                   'Away': 'Away_Total_Offense_Fourth_Down-Conversions_per_Game'
                                   }, inplace=True)
        tofdcpg_df['Team'] = tofdcpg_df['Team'].str.strip()
        time.sleep(2)

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
                                 'Last 3': 'Last 3_Total_Offense_Average_Time_of_Possession',
                                 'Last 1': 'Last 1_Total_Offense_Average_Time_of_Possession',
                                 'Home': 'At_Home_Total_Offense_Average_Time_of_Possession',
                                 'Away': 'Away_Total_Offense_Average_Time_of_Possession'
                                 }, inplace=True)
        toatp_df['Team'] = toatp_df['Team'].str.strip()
        time.sleep(1)

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
                                  'Last 3': 'Last 3_Total_Offense_Average_Time_of_Possession_Percentage',
                                  'Last 1': 'Last 1_Total_Offense_Average_Time_of_Possession_Percentage',
                                  'Home': 'At_Home_Total_Offense_Average_Time_of_Possession_Percentage',
                                  'Away': 'Away_Total_Offense_Average_Time_of_Possession_Percentage'
                                  }, inplace=True)
        toatpp_df['Team'] = toatpp_df['Team'].str.strip()
        time.sleep(1)

        total_offense_third_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/third-down-conversion-pct' \
                                                                  + '?date=' \
                                                                  + this_week_date_str
        totdcp_df = main_hist(total_offense_third_down_conversion_percent_url_current, season, str(week), this_week_date_str,
                              'total_offense_third_down_conversion_percent')
        totdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Third_Down-Conversion_Percent',
                                  season: 'Current_Season_Total_Offense_Third_Down-Conversion_Percent',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Offense_Third_Down_Conversion_Percent',
                                  'Last 3': 'Last 3_Total_Offense_Third_Down_Conversion_Percent',
                                  'Last 1': 'Last 1_Total_Offense_Third_Down_Conversion_Percent',
                                  'Home': 'At_Home_Total_Offense_Third_Down_Conversion_Percent',
                                  'Away': 'Away_Total_Offense_Third_Down_Conversion_Percent'
                                  }, inplace=True)
        totdcp_df['Team'] = totdcp_df['Team'].str.strip()
        time.sleep(1)

        total_offense_fourth_down_conversion_percent_url_current = 'https://www.teamrankings.com/college-football/stat/fourth-down-conversion-pct' \
                                                                   + '?date=' \
                                                                   + this_week_date_str
        tofdcp_df = main_hist(total_offense_fourth_down_conversion_percent_url_current, season, str(week), this_week_date_str,
                              'total_offense_fourth_down_conversion_percent')
        tofdcp_df.rename(columns={'Rank': 'Rank_Total_Offense_Fourth_Down-Conversion_Percent',
                                  season: 'Current_Season_Total_Offense_Fourth_Down-Conversion_Percent',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Last 3': 'Last 3_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Last 1': 'Last 1_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Home': 'At_Home_Total_Offense_Fourth_Down_Conversion_Percent',
                                  'Away': 'Away_Total_Offense_Fourth_Down_Conversion_Percent'
                                  }, inplace=True)
        tofdcp_df['Team'] = tofdcp_df['Team'].str.strip()
        time.sleep(1)

        total_offense_punts_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-play' \
                                                   + '?date=' \
                                                   + this_week_date_str
        toppp_df = main_hist(total_offense_punts_per_play_url_current, season, str(week), this_week_date_str,
                             'total_offense_punts_per_play')
        toppp_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Play',
                                 season: 'Current_Season_Total_Offense_Punts_per_Play',
                                 str(int(
                                     season) - 1): 'Previous_Season_Total_Offense_Punts_per_Play',
                                 'Last 3': 'Last 3_Total_Offense_Punts_per_Play',
                                 'Last 1': 'Last 1_Total_Offense_Punts_per_Play',
                                 'Home': 'At_Home_Total_Offense_Punts_per_Play',
                                 'Away': 'Away_Total_Offense_Punts_per_Play'
                                 }, inplace=True)
        toppp_df['Team'] = toppp_df['Team'].str.strip()
        time.sleep(1)

        total_offense_punts_per_offensive_score_url_current = 'https://www.teamrankings.com/college-football/stat/punts-per-offensive-score' \
                                                              + '?date=' \
                                                              + this_week_date_str
        toppos_df = main_hist(total_offense_punts_per_offensive_score_url_current, season, str(week), this_week_date_str,
                              'total_offense_punts_per_offensive score')
        toppos_df.rename(columns={'Rank': 'Rank_Total_Offense_Punts_per_Offensive_Score',
                                  season: 'Current_Season_Total_Offense_Punts_per_Offensive_Score',
                                  str(int(
                                      season) - 1): 'Previous_Season_Total_Offense_Punts_per_Offensive_Score',
                                  'Last 3': 'Last 3_Total_Offense_Punts_per_Offensive_Score',
                                  'Last 1': 'Last 1_Total_Offense_Punts_per_Offensive_Score',
                                  'Home': 'At_Home_Total_Offense_Punts_per_Offensive_Score',
                                  'Away': 'Away_Total_Offense_Punts_per_Offensive_Score'
                                  }, inplace=True)
        toppos_df['Team'] = toppos_df['Team'].str.strip()
        time.sleep(1)

        rushing_offense_rushing_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-attempts-per-game' \
                                                                + '?date=' \
                                                                + this_week_date_str
        rorapg_df = main_hist(rushing_offense_rushing_attempts_per_game_url_current, season, str(week), this_week_date_str,
                              'rushing_offense_rushing_attempts_per_game')
        rorapg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Attempts_per_Game',
                                  season: 'Current_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Last 3': 'Last 3_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Last 1': 'Last 1_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Home': 'At_Home_Rushing_Offense_Rushing_Attempts_per_Game',
                                  'Away': 'Away_Rushing_Offense_Rushing_Attempts_per_Game'
                                  }, inplace=True)
        rorapg_df['Team'] = rorapg_df['Team'].str.strip()
        time.sleep(1)

        rushing_offense_rushing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        rorypg_df = main_hist(rushing_offense_rushing_yards_per_game_url_current, season, str(week), this_week_date_str,
                              'rushing_offense_rushing_yards_per_game')
        rorypg_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Game',
                                  season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Last 3': 'Last 3_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Last 1': 'Last 1_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Game',
                                  'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Game'
                                  }, inplace=True)
        rorypg_df['Team'] = rorypg_df['Team'].str.strip()
        time.sleep(1)

        rushing_offense_rushing_yards_per_rush_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-rush-attempt' \
                                                                     + '?date=' \
                                                                     + this_week_date_str
        rorypra_df = main_hist(rushing_offense_rushing_yards_per_rush_attempt_url_current, season, str(week), this_week_date_str,
                               'rushing_offense_rushing_yards_per_rush_attempt')
        rorypra_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   season: 'Current_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   str(int(
                                       season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Last 3': 'Last 3_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Last 1': 'Last 1_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Home': 'At_Home_Rushing_Offense_Rushing_Yards_per_Rush_Attempt',
                                   'Away': 'Away_Rushing_Offense_Rushing_Yards_per_Rush_Attempt'
                                   }, inplace=True)
        rorypra_df['Team'] = rorypra_df['Team'].str.strip()
        time.sleep(1)

        rushing_offense_rushing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-play-pct' \
                                                              + '?date=' \
                                                              + this_week_date_str
        rorpp_df = main_hist(rushing_offense_rushing_play_percentage_url_current, season, str(week), this_week_date_str,
                             'rushing_offense_rushing_play_percentage')
        rorpp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Play_Percentage',
                                 season: 'Current_Season_Rushing_Offense_Rushing_Play_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Play_Percentage',
                                 'Last 3': 'Last 3_Rushing_Offense_Rushing_Play_Percentage',
                                 'Last 1': 'Last 1_Rushing_Offense_Rushing_Play_Percentage',
                                 'Home': 'At_Home_Rushing_Offense_Rushing_Play_Percentage',
                                 'Away': 'Away_Rushing_Offense_Rushing_Play_Percentage'
                                 }, inplace=True)
        rorpp_df['Team'] = rorpp_df['Team'].str.strip()
        time.sleep(1)

        rushing_offense_rushing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/rushing-yards-pct' \
                                                               + '?date=' \
                                                               + this_week_date_str
        roryp_df = main_hist(rushing_offense_rushing_yards_percentage_url_current, season, str(week), this_week_date_str,
                             'rushing_offense_rushing_yards_percentage')
        roryp_df.rename(columns={'Rank': 'Rank_Rushing_Offense_Rushing_Yards_Percentage',
                                 season: 'Current_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Last 3': 'Last 3_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Last 1': 'Last 1_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Home': 'At_Home_Rushing_Offense_Rushing_Yards_Percentage',
                                 'Away': 'Away_Rushing_Offense_Rushing_Yards_Percentage'
                                 }, inplace=True)
        roryp_df['Team'] = roryp_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_pass_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/pass-attempts-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        popapg_df = main_hist(passing_offense_pass_attempts_per_game_url_current, season, str(week), this_week_date_str,
                              'passing_offense_pass_attempts_per_game')
        popapg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Pass_Attempts_per_Game',
                                  season: 'Current_Season_Passing_Offense_Pass_Attempts_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_Pass_Attempts_per_Game',
                                  'Last 3': 'Last 3_Passing_Offense_Pass_Attempts_per_Game',
                                  'Last 1': 'Last 1_Passing_Offense_Pass_Attempts_per_Game',
                                  'Home': 'At_Home_Passing_Offense_Pass_Attempts_per_Game',
                                  'Away': 'Away_Passing_Offense_Pass_Attempts_per_Game'
                                  }, inplace=True)
        popapg_df['Team'] = popapg_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_completions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/completions-per-game' \
                                                           + '?date=' \
                                                           + this_week_date_str
        pocpg_df = main_hist(passing_offense_completions_per_game_url_current, season, str(week), this_week_date_str,
                             'passing_offense_completions_per_game')
        pocpg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completions_per_Game',
                                 season: 'Current_Season_Passing_Offense_Completions_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Completions_per_Game',
                                 'Last 3': 'Last 3_Passing_Offense_Completions_per_Game',
                                 'Last 1': 'Last 1_Passing_Offense_Completions_per_Game',
                                 'Home': 'At_Home_Passing_Offense_Completions_per_Game',
                                 'Away': 'Away_Passing_Offense_Completions_per_Game'
                                 }, inplace=True)
        pocpg_df['Team'] = pocpg_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_incompletions_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/incompletions-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        poipg_df = main_hist(passing_offense_incompletions_per_game_url_current, season, str(week), this_week_date_str,
                             'passing_offense_incompletions_per_game')
        poipg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Incompletions_per_Game',
                                 season: 'Current_Season_Passing_Offense_Incompletions_per_Game',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Incompletions_per_Game',
                                 'Last 3': 'Last 3_Passing_Offense_Incompletions_per_Game',
                                 'Last 1': 'Last 1_Passing_Offense_Incompletions_per_Game',
                                 'Home': 'At_Home_Passing_Offense_Incompletions_per_Game',
                                 'Away': 'Away_Passing_Offense_Incompletions_per_Game'
                                 }, inplace=True)
        poipg_df['Team'] = poipg_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_completion_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/completion-pct' \
                                                            + '?date=' \
                                                            + this_week_date_str
        pocp_df = main_hist(passing_offense_completion_percentage_url_current, season, str(week), this_week_date_str,
                            'passing_offense_completion_percentage')
        pocp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Completion_Percentage',
                                season: 'Current_Season_Passing_Offense_Completion_Percentage',
                                str(int(
                                    season) - 1): 'Previous_Season_Passing_Offense_Completion_Percentage',
                                'Last 3': 'Last 3_Passing_Offense_Completion_Percentage',
                                'Last 1': 'Last 1_Passing_Offense_Completion_Percentage',
                                'Home': 'At_Home_Passing_Offense_Completion_Percentage',
                                'Away': 'Away_Passing_Offense_Completion_Percentage'
                                }, inplace=True)
        pocp_df['Team'] = pocp_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_passing_yards_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-per-game' \
                                                             + '?date=' \
                                                             + this_week_date_str
        popypg_df = main_hist(passing_offense_passing_yards_per_game_url_current, season, str(week), this_week_date_str,
                              'passing_offense_passing_yards_per_game')
        popypg_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_per_Game',
                                  season: 'Current_Season_Passing_Offense_Passing_Yards_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_per_Game',
                                  'Last 3': 'Last 3_Passing_Offense_Passing_Yards_per_Game',
                                  'Last 1': 'Last 1_Passing_Offense_Passing_Yards_per_Game',
                                  'Home': 'At_Home_Passing_Offense_Passing_Yards_per_Game',
                                  'Away': 'Away_Passing_Offense_Passing_Yards_per_Game'
                                  }, inplace=True)
        popypg_df['Team'] = popypg_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_qb_sacked_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-per-game' \
                                                         + '?date=' \
                                                         + this_week_date_str
        poqspg_df = main_hist(passing_offense_qb_sacked_per_game_url_current, season, str(week), this_week_date_str,
                              'passing_offense_qb_sacked_per_game')
        poqspg_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_per_Game',
                                  season: 'Current_Season_Passing_Offense_QB_Sacked_per_Game',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_per_Game',
                                  'Last 3': 'Last 3_Passing_Offense_QB_Sacked_per_Game',
                                  'Last 1': 'Last 1_Passing_Offense_QB_Sacked_per_Game',
                                  'Home': 'At_Home_Passing_Offense_QB_Sacked_per_Game',
                                  'Away': 'Away_Passing_Offense_QB_Sacked_per_Game'
                                  }, inplace=True)
        poqspg_df['Team'] = poqspg_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_qb_sacked_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/qb-sacked-pct' \
                                                           + '?date=' \
                                                           + this_week_date_str
        poqsp_df = main_hist(passing_offense_qb_sacked_percentage_url_current, season, str(week), this_week_date_str,
                             'passing_offense_qb_sacked_percentage')
        poqsp_df.rename(columns={'Rank': 'Rank_Passing_Offense_QB_Sacked_Percentage',
                                 season: 'Current_Season_Passing_Offense_QB_Sacked_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_QB_Sacked_Percentage',
                                 'Last 3': 'Last 3_Passing_Offense_QB_Sacked_Percentage',
                                 'Last 1': 'Last 1_Passing_Offense_QB_Sacked_Percentage',
                                 'Home': 'At_Home_Passing_Offense_QB_Sacked_Percentage',
                                 'Away': 'Away_Passing_Offense_QB_Sacked_Percentage'
                                 }, inplace=True)
        poqsp_df['Team'] = poqsp_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_average_passer_rating_url_current = 'https://www.teamrankings.com/college-football/stat/average-team-passer-rating' \
                                                            + '?date=' \
                                                            + this_week_date_str
        poapr_df = main_hist(passing_offense_average_passer_rating_url_current, season, str(week), this_week_date_str,
                             'passing_offense_average_passer_rating')
        poapr_df.rename(columns={'Rank': 'Rank_Passing_Offense_Average_Passer_Rating',
                                 season: 'Current_Season_Passing_Offense_Average_Passer_Rating',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Average_Passer_Rating',
                                 'Last 3': 'Last 3_Passing_Offense_Average_Passer_Rating',
                                 'Last 1': 'Last 1_Passing_Offense_Average_Passer_Rating',
                                 'Home': 'At_Home_Passing_Offense_Average_Passer_Rating',
                                 'Away': 'Away_Passing_Offense_Average_Passer_Rating'
                                 }, inplace=True)
        poapr_df['Team'] = poapr_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_passing_play_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-play-pct' \
                                                              + '?date=' \
                                                              + this_week_date_str
        poppp_df = main_hist(passing_offense_passing_play_percentage_url_current, season, str(week), this_week_date_str,
                             'passing_offense_passing_play_percentage')
        poppp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Play_Percentage',
                                 season: 'Current_Season_Passing_Offense_Passing_Play_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Passing_Play_Percentage',
                                 'Last 3': 'Last 3_Passing_Offense_Passing_Play_Percentage',
                                 'Last 1': 'Last 1_Passing_Offense_Passing_Play_Percentage',
                                 'Home': 'At_Home_Passing_Offense_Passing_Play_Percentage',
                                 'Away': 'Away_Passing_Offense_Passing_Play_Percentage'
                                 }, inplace=True)
        poppp_df['Team'] = poppp_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_passing_yards_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/passing-yards-pct' \
                                                               + '?date=' \
                                                               + this_week_date_str
        popyp_df = main_hist(passing_offense_passing_yards_percentage_url_current, season, str(week), this_week_date_str,
                             'passing_offense_passing_yards_percentage')
        popyp_df.rename(columns={'Rank': 'Rank_Passing_Offense_Passing_Yards_Percentage',
                                 season: 'Current_Season_Passing_Offense_Passing_Yards_Percentage',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Passing_Yards_Percentage',
                                 'Last 3': 'Last 3_Passing_Offense_Passing_Yards_Percentage',
                                 'Last 1': 'Last 1_Passing_Offense_Passing_Yards_Percentage',
                                 'Home': 'At_Home_Passing_Offense_Passing_Yards_Percentage',
                                 'Away': 'Away_Passing_Offense_Passing_Yards_Percentage'
                                 }, inplace=True)
        popyp_df['Team'] = popyp_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_yards_per_pass_attempt_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-pass-attempt' \
                                                         + '?date=' \
                                                         + this_week_date_str
        poyppa_df = main_hist(passing_offense_yards_per_pass_attempt_url_current, season, str(week), this_week_date_str,
                              'passing_offense_yards_per_pass_attempt')
        poyppa_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Pass_Attempt',
                                  season: 'Current_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                  str(int(
                                      season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Last 3': 'Last 3_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Last 1': 'Last 1_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Home': 'At_Home_Passing_Offense_Yards_per_Pass_Attempt',
                                  'Away': 'Away_Passing_Offense_Yards_per_Pass_Attempt'
                                  }, inplace=True)
        poyppa_df['Team'] = poyppa_df['Team'].str.strip()
        time.sleep(1)

        passing_offense_yards_per_completion_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-completion' \
                                                       + '?date=' \
                                                       + this_week_date_str
        poypc_df = main_hist(passing_offense_yards_per_completion_url_current, season, str(week), this_week_date_str,
                             'passing_offense_yards_per_completion')
        poypc_df.rename(columns={'Rank': 'Rank_Passing_Offense_Yards_per_Completion',
                                 season: 'Current_Season_Passing_Offense_Yards_per_Completion',
                                 str(int(
                                     season) - 1): 'Previous_Season_Passing_Offense_Yards_per_Completion',
                                 'Last 3': 'Last 3_Passing_Offense_Yards_per_Completion',
                                 'Last 1': 'Last 1_Passing_Offense_Yards_per_Completion',
                                 'Home': 'At_Home_Passing_Offense_Yards_per_Completion',
                                 'Away': 'Away_Passing_Offense_Yards_per_Completion'
                                 }, inplace=True)
        poypc_df['Team'] = poypc_df['Team'].str.strip()
        time.sleep(1)

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
                                    'Last 3': 'Last 3_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    'Last 1': 'Last 1_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Attempts_per_Game',
                                    'Away': 'Away_Special_Teams_Offense_Field_Goal_Attempts_per_Game'
                                    }, inplace=True)
        stofgapg_df['Team'] = stofgapg_df['Team'].str.strip()
        time.sleep(1)

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
                                    'Last 3': 'Last 3_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    'Last 1': 'Last 1_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    'Home': 'At_Home_Special_Teams_Offense_Field_Goals_Made_per_Game',
                                    'Away': 'Away_Special_Teams_Offense_Field_Goals_Made_per_Game'
                                    }, inplace=True)
        stofgmpg_df['Team'] = stofgmpg_df['Team'].str.strip()
        time.sleep(1)

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
                                    'Last 3': 'Last 3_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                    'Last 1': 'Last 1_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                    'Home': 'At_Home_Special_Teams_Offense_Field_Goal_Conversion_Percentage',
                                    'Away': 'Away_Special_Teams_Offense_Field_Goal_Conversion_Percentage'
                                    }, inplace=True)
        stofgcp_df['Team'] = stofgcp_df['Team'].str.strip()
        time.sleep(1)

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
                                   'Last 3': 'Last 3_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   'Last 1': 'Last 1_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   'Home': 'At_Home_Special_Teams_Offense_Punt_Attempts_per_Game',
                                   'Away': 'Away_Special_Teams_Offense_Punt_Attempts_per_Game'
                                   }, inplace=True)
        stopapg_df['Team'] = stopapg_df['Team'].str.strip()
        time.sleep(1)

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
                                    'Last 3': 'Last 3_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    'Last 1': 'Last 1_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    'Home': 'At_Home_Special_Teams_Offense_Gross_Punt_Yards_per_Game',
                                    'Away': 'Away_Special_Teams_Offense_Gross_Punt_Yards_per_Game'
                                    }, inplace=True)
        stogpypg_df['Team'] = stogpypg_df['Team'].str.strip()
        time.sleep(1)

        scoring_defense_opponent_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-game' \
                                                               + '?date=' \
                                                               + this_week_date_str
        sdoppg_df = main_hist(scoring_defense_opponent_points_per_game_url_current, season, str(week),
                              this_week_date_str, 'scoring_defense_opponent_points_per_game')
        sdoppg_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opponent_Points_per_Game',
                                  season: 'Current_Season_Scoring_Defense_Opponent_Points_per_Game',
                                  str(int(season) - 1): 'Previous_Season_Scoring_Defense_Opponent_Points_per_Game',
                                  'Last 3': 'Last 3_Scoring_Defense_Opponent_Points_per_Game',
                                  'Last 1': 'Last 1_Scoring_Defense_Opponent_Points_per_Game',
                                  'Home': 'At_Home_Scoring_Defense_Opponent_Points_per_Game',
                                  'Away': 'Away_Scoring_Defense_Opponent_Points_per_Game'
                                  }, inplace=True)
        sdoppg_df['Team'] = sdoppg_df['Team'].str.strip()
        time.sleep(1)

        scoring_defense_opp_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/opp-yards-per-point' \
                                                          + '?date=' \
                                                          + this_week_date_str
        sdoypp_df = main_hist(scoring_defense_opp_yards_per_point_url_current, season, str(week),
                              this_week_date_str, 'scoring_defense_opp_yards_per_point')
        sdoypp_df.rename(columns={'Rank': 'Rank_Scoring_Defense_Opp_Yards_per_Point',
                                  season: 'Current_Season_Scoring_Defense_Opp_Yards_per_Point',
                                  str(int(
                                      season) - 1): 'Previous_Season_Scoring_Defense_Opp_Yards_per_Point',
                                  'Last 3': 'Last 3_Scoring_Defense_Opp_Yards_per_Point',
                                  'Last 1': 'Last 1_Scoring_Defense_Opp_Yards_per_Point',
                                  'Home': 'At_Home_Scoring_Defense_Opp_Yards_per_Point',
                                  'Away': 'Away_Scoring_Defense_Opp_Yards_per_Point'
                                  }, inplace=True)
        sdoypp_df['Team'] = sdoypp_df['Team'].str.strip()
        time.sleep(1)

        total_defense_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-game' \
                                    + '?date=' \
                                    + this_week_date_str
        td_df = main_hist(total_defense_url_current, season, str(week), this_week_date_str, 'total_defense')
        td_df.rename(columns={'Rank': 'Rank_Total_Defense',
                              season: 'Current_Season_Total_Defense',
                              str(int(season) - 1): 'Previous_Season_Total_Defense',
                              'Last 3': 'Last 3_Total_Defense',
                              'Last 1': 'Last 1_Total_Defense',
                              'Home': 'At_Home_Total_Defense',
                              'Away': 'Away_Total_Defense'
                              }, inplace=True)
        td_df['Team'] = td_df['Team'].str.strip()
        time.sleep(1)

        turnovers_given_url_current = 'https://www.teamrankings.com/college-football/stat/giveaways-per-game' \
                                      + '?date=' \
                                      + this_week_date_str
        tg_df = main_hist(turnovers_given_url_current, season, str(week), this_week_date_str, 'turnovers_given')
        tg_df.rename(columns={'Rank': 'Rank_Turnovers_Given',
                              season: 'Current_Season_Turnovers_Given',
                              str(int(season) - 1): 'Previous_Season_Turnovers_Given',
                              'Last 3': 'Last 3_Turnovers_Given',
                              'Last 1': 'Last 1_Turnovers_Given',
                              'Home': 'At_Home_Turnovers_Given',
                              'Away': 'Away_Turnovers_Given'
                              }, inplace=True)
        tg_df['Team'] = tg_df['Team'].str.strip()
        time.sleep(1)

        turnovers_taken_url_current = 'https://www.teamrankings.com/college-football/stat/takeaways-per-game' \
                                      + '?date=' \
                                      + this_week_date_str
        tt_df = main_hist(turnovers_taken_url_current, season, str(week), this_week_date_str, 'turnovers_taken')
        tt_df.rename(columns={'Rank': 'Rank_Turnovers_Taken',
                              season: 'Current_Season_Turnovers_Taken',
                              str(int(season) - 1): 'Previous_Season_Turnovers_Taken',
                              'Last 3': 'Last 3_Turnovers_Taken',
                              'Last 1': 'Last 1_Turnovers_Taken',
                              'Home': 'At_Home_Turnovers_Taken',
                              'Away': 'Away_Turnovers_Taken'
                              }, inplace=True)
        tt_df['Team'] = tt_df['Team'].str.strip()
        time.sleep(1)

        last_5_url_current = 'https://www.teamrankings.com/college-football/ranking/last-5-games-by-other' \
                             + '?date=' \
                             + this_week_date_str
        l5_df = main_hist(last_5_url_current, season, str(week), this_week_date_str, 'last_5')
        l5_df.rename(columns={'Rank': 'Rank_Last_5',
                              'Rating': 'Rating_Last_5',
                              'Hi': 'Hi_Last_5',
                              'Low': 'Low_Last_5',
                              'Last': 'Last_Last_5'
                              }, inplace=True)
        l5_df['Team'] = l5_df['Team'].str.strip()
        time.sleep(1)

        neutral_site_url_current = 'https://www.teamrankings.com/college-football/ranking/neutral-by-other' \
                                   + '?date=' \
                                   + this_week_date_str
        ns_df = main_hist(neutral_site_url_current, season, str(week), this_week_date_str, 'neutral_site')
        ns_df.rename(columns={'Rank': 'Rank_Neutral_Site',
                              'Rating': 'Rating_Neutral_Site',
                              'Hi': 'Hi_Neutral_Site',
                              'Low': 'Low_Neutral_Site',
                              'Last': 'Last_Neutral_Site'
                              }, inplace=True)
        ns_df.drop(['v 1-10', 'v 11-25', 'v 26-40'], axis=1, inplace=True)
        ns_df['Team'] = ns_df['Team'].str.strip()
        time.sleep(1)

        sos_url_current = 'https://www.teamrankings.com/college-football/ranking/schedule-strength-by-other' \
                          + '?date=' \
                          + this_week_date_str
        sos_df = main_hist(sos_url_current, season, str(week), this_week_date_str, 'sos')
        sos_df.rename(columns={'Rank': 'Rank_SoS',
                               'Rating': 'Rating_SoS',
                               'Hi': 'Hi_SoS',
                               'Low': 'Low_SoS',
                               'Last': 'Last_SoS'
                               }, inplace=True)
        sos_df['Team'] = sos_df['Team'].str.strip()

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
        this_week_df = pd.merge(this_week_df, poppp_df, on=['Team', 'Season', 'Week'], how='outer')
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
        this_week_df = pd.merge(this_week_df, td_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tg_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, tt_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, l5_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, ns_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sos_df, on=['Team', 'Season', 'Week'], how='outer')

        this_week_df = rearrange_columns(this_week_df)
        season_df = pd.concat([season_df, this_week_df])
        master_df = pd.concat([master_df, this_week_df])

        save_dir = 'C:\PythonPrograms'
        save_file = 'Scraped_TR_Data_Season_' + season + '_Week_' + week
        try:
            datascraper.save_df(season_df, save_dir, save_file)
            print('{} saved successfully.'.format(save_file))
            print('File successfully saved at {}.'.format(save_dir))
        except:
            print('I don\'t think the file saved, you should double check.')
