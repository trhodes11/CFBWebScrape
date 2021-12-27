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
                                      'Home': 'At_Home_Team_Yards_per_Point_Nargin',
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
                trsp_df = main_hist(team_red_zone_scores_per_game_url_current, season, str(week),
                                  this_week_date_str,
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

                total_offense_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-game'\
                    + '?date='\
                    + this_week_date_str
                to_df = main_hist(total_offense_url_current, season, str(week), this_week_date_str, 'total_offense')
                to_df.rename(columns={'Rank': 'Rank_Total_Offense',
                                      season: 'Current_Season_Total_Offense',
                                      str(int(season) - 1): 'Previous_Season_Total_Offense',
                                      'Last 3': 'Last 3_Total_Offense',
                                      'Last 1': 'Last 1_Total_Offense',
                                      'Home': 'At_Home_Total_Offense',
                                      'Away': 'Away_Total_Offense'
                                      }, inplace=True)
                to_df['Team'] = to_df['Team'].str.strip()
                if season == '2010':
                    to_df['Rank_Total_Offense'] = to_df.index + 1
                to_df = to_df.replace('--', np.nan)
                to_df = to_df.apply(pd.to_numeric, errors='ignore')
                time.sleep(2)

                scoring_defense_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-game'\
                    + '?date='\
                    + this_week_date_str
                sd_df = main_hist(scoring_defense_url_current, season, str(week), this_week_date_str, 'scoring_defense')
                sd_df.rename(columns={'Rank': 'Rank_Scoring_Defense',
                                      season: 'Current_Season_Scoring_Defense',
                                      str(int(season) - 1): 'Previous_Season_Scoring_Defense',
                                      'Last 3': 'Last 3_Scoring_Defense',
                                      'Last 1': 'Last 1_Scoring_Defense',
                                      'Home': 'At_Home_Scoring_Defense',
                                      'Away': 'Away_Scoring_Defense'
                                      }, inplace=True)
                sd_df['Team'] = sd_df['Team'].str.strip()
                if season == '2010':
                    sd_df['Rank_Scoring_Defense'] = sd_df.index + 1
                sd_df = sd_df.replace('--', np.nan)
                sd_df = sd_df.apply(pd.to_numeric, errors='ignore')
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
                this_week_df = pd.merge(this_week_df, sd_df, on=['Team', 'Season', 'Week'], how='outer')
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
        so_df = main_hist(team_average_scoring_margin_url_current, season, str(week), this_week_date_str, 'team_average_scoring_margin')
        so_df.rename(columns={'Rank': 'Rank_Team_Average_Scoring_Margin',
                              season: 'Current_Season_Team_Average_Scoring_Margin',
                              str(int(season) - 1): 'Previous_Season_Team_Average_Scoring_Margin',
                              'Last 3': 'Last 3_Team_Average_Scoring_Margin',
                              'Last 1': 'Last 1_Team_Average_Scoring_Margin',
                              'Home': 'At_Home_Team_Average_Scoring_Margin',
                              'Away': 'Away_Team_Average_Scoring_Margin'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_yards_per_point_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point' \
                                           + '?date=' \
                                           + this_week_date_str
        so_df = main_hist(team_yards_per_point_url_current, season, str(week), this_week_date_str, 'team_yards_per_point')
        so_df.rename(columns={'Rank': 'Rank_Team_Yards_per_Point',
                              season: 'Current_Season_Team_Yards_per_Point',
                              str(int(season) - 1): 'Previous_Season_Team_yards_per_Point',
                              'Last 3': 'Last 3_Team_Yards_per_Point',
                              'Last 1': 'Last 1_Team_Yards_per_Point',
                              'Home': 'At_Home_Team_Yards_per_Point',
                              'Away': 'Away_Team_Yards_per_Point'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_yards_per_point_margin_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-point-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        so_df = main_hist(team_yards_per_point_margin_url_current, season, str(week), this_week_date_str,
                          'team_yards_per_point_margin')
        so_df.rename(columns={'Rank': 'Rank_Team_Yards_Per_Point_Margin',
                              season: 'Current_Season_Team_Yards_per_Point_Margin',
                              str(int(season) - 1): 'Previous_Season_Team_yards_per_Point_Margin',
                              'Last 3': 'Last 3_Team_Yards_per_Point_Margin',
                              'Last 1': 'Last 1_Team_Yards_per_Point_Margin',
                              'Home': 'At_Home_Team_Yards_per_Point_Nargin',
                              'Away': 'Away_Team_Yards_per_Point_Margin'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_points_per_play_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play' \
                                           + '?date=' \
                                           + this_week_date_str
        so_df = main_hist(team_points_per_play_url_current, season, str(week), this_week_date_str, 'team_points_per_play')
        so_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play',
                              season: 'Current_Season_Team_Points_per_Play',
                              str(int(season) - 1): 'Previous_Season_Team_Points_per_Play',
                              'Last 3': 'Last 3_Team_Points_per_Play',
                              'Last 1': 'Last 1_Team_Points_per_Play',
                              'Home': 'At_Home_Team_Points_per_Play',
                              'Away': 'Away_Team_Points_per_Play'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_points_per_play_margin_url_current = 'https://www.teamrankings.com/college-football/stat/points-per-play-margin' \
                                                  + '?date=' \
                                                  + this_week_date_str
        so_df = main_hist(team_points_per_play_margin_url_current, season, str(week), this_week_date_str, 'team_points_per_play_margin')
        so_df.rename(columns={'Rank': 'Rank_Team_Points_per_Play_Margin',
                              season: 'Current_Season_Team_Points_per_Play_Margin',
                              str(int(season) - 1): 'Previous_Season_Team_Points_per_Play_Margin',
                              'Last 3': 'Last 3_Team_Points_per_Play_Margin',
                              'Last 1': 'Last 1_Team_Points_per_Play_Margin',
                              'Home': 'At_Home_Team_Points_per_Play_Margin',
                              'Away': 'Away_Team_Points_per_Play_Margin'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_red_zone_scoring_attempts_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-attempts-per-game' \
                                                              + '?date=' \
                                                              + this_week_date_str
        so_df = main_hist(team_red_zone_scoring_attempts_per_game_url_current, season, str(week), this_week_date_str,
                          'team_red_zone_scoring_attempts_per_game')
        so_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Attempts_per_Game',
                              season: 'Current_Season_Team_Red-Zone_Scoring_Attempts_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scoring_Attempts_per_Game',
                              'Last 3': 'Last 3_Team_Red_Zone_Scoring_Attempts_per_Game',
                              'Last 1': 'Last 1_Team_Red_Zone_Scoring_Attempts_per_Game',
                              'Home': 'At_Home_Team_Red_Zone_Scoring_Attempts_per_Game',
                              'Away': 'Away_Team_Red_Zone-Scoring-Attempts_per_Game'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_red_zone_scores_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scores-per-game' \
                                                    + '?date=' \
                                                    + this_week_date_str
        so_df = main_hist(team_red_zone_scores_per_game_url_current, season, str(week), this_week_date_str,
                          'team_red_zone_scores_per_game')
        so_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scores_per_Game',
                              season: 'Current_Season_Team_Red-Zone_Scores_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scores_per_Game',
                              'Last 3': 'Last 3_Team_Red_Zone_Scores_per_Game',
                              'Last 1': 'Last 1_Team_Red_Zone_Scores_per_Game',
                              'Home': 'At_Home_Team_Red_Zone_Scores_per_Game',
                              'Away': 'Away_Team_Red_Zone-Scores_per_Game'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_red_zone_scoring_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/red-zone-scoring-pct' \
                                                       + '?date=' \
                                                       + this_week_date_str
        so_df = main_hist(team_red_zone_scoring_percentage_url_current, season, str(week), this_week_date_str,
                          'team_red_zone_scoring_percentage')
        so_df.rename(columns={'Rank': 'Rank_Team_Red_Zone_Scoring_Percentage',
                              season: 'Current_Season_Team_Red-Zone_Scoring_Percentage',
                              str(int(season) - 1): 'Previous_Season_Team_Red-Zone_Scoring_Percentage',
                              'Last 3': 'Last 3_Team_Red_Zone_Scoring_Percentage',
                              'Last 1': 'Last 1_Team_Red_Zone_Scoring_Percentage',
                              'Home': 'At_Home_Team_Red_Zone_Scoring_Percentage',
                              'Away': 'Away_Team_Red_Zone-Scoring_Percentage'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_offensive_touchdowns_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-touchdowns-per-game' \
                                                     + '?date=' \
                                                     + this_week_date_str
        so_df = main_hist(team_offensive_touchdowns_per_game_url_current, season, str(week), this_week_date_str,
                      'team_offensive_touchdowns_per_game')
        so_df.rename(columns={'Rank': 'Rank_Team_Offensive_Touchdowns_per_Game',
                          season: 'Current_Season_Team_Offensive_Touchdowns_per_Game',
                          str(int(season) - 1): 'Previous_Season_Team_Offensive_Touchdowns_per_Game',
                          'Last 3': 'Last 3_Team_Offensive_Touchdowns_per_Game',
                          'Last 1': 'Last 1_Team_Offensive_Touchdowns_per_Game',
                          'Home': 'At_Home_Team_Offensive_Touchdowns_per_Game',
                          'Away': 'Away_Team_Offensive_Touchdowns_per_Game'
                          }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        team_offensive_points_per_game_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-points-per-game' \
                                                     + '?date=' \
                                                     + this_week_date_str
        so_df = main_hist(team_offensive_points_per_game_url_current, season, str(week), this_week_date_str,
                          'team_offensive_points_per_game')
        so_df.rename(columns={'Rank': 'Rank_Team_Offensive_Points_per_Game',
                              season: 'Current_Season_Team_Offensive_Points_per_Game',
                              str(int(season) - 1): 'Previous_Season_Team_Offensive_Points_per_Game',
                              'Last 3': 'Last 3_Team_Offensive_Points_per_Game',
                              'Last 1': 'Last 1_Team_Offensive_Points_per_Game',
                              'Home': 'At_Home_Team_Offensive_Points_per_Game',
                              'Away': 'Away_Team_Offensive_Points_per_Game'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        eam_offensive_point_share_percentage_url_current = 'https://www.teamrankings.com/college-football/stat/offensive-point-share-pct' \
                                                           + '?date=' \
                                                           + this_week_date_str
        so_df = main_hist(team_offensive_point_share_percentage_url_current, season, str(week), this_week_date_str,
                          'team_offensive_point_share_percentage')
        so_df.rename(columns={'Rank': 'Rank_Team_Offensive_Point_Share_Percentage',
                              season: 'Current_Season_Team_Offensive_Point_Share_Percentage',
                              str(int(season) - 1): 'Previous_Season_Team_Offensive_Point_Share_Percentage',
                              'Last 3': 'Last 3_Team_Offensive_Point_Share_Percentage',
                              'Last 1': 'Last 1_Team_Offensive_Point_Share_Percentage',
                              'Home': 'At_Home_Team_Offensive_Point_Share_Percentage',
                              'Away': 'Away_Team_Offensive_Point_Share_Percentage'
                              }, inplace=True)
        so_df['Team'] = so_df['Team'].str.strip()
        time.sleep(1)

        total_offense_url_current = 'https://www.teamrankings.com/college-football/stat/yards-per-game' \
                                    + '?date=' \
                                    + this_week_date_str
        to_df = main_hist(total_offense_url_current, season, str(week), this_week_date_str, 'total_offense')
        to_df.rename(columns={'Rank': 'Rank_Total_Offense',
                              season: 'Current_Season_Total_Offense',
                              str(int(season) - 1): 'Previous_Season_Total_Offense',
                              'Last 3': 'Last 3_Total_Offense',
                              'Last 1': 'Last 1_Total_Offense',
                              'Home': 'At_Home_Total_Offense',
                              'Away': 'Away_Total_Offense'
                              }, inplace=True)
        to_df['Team'] = to_df['Team'].str.strip()
        time.sleep(1)

        scoring_defense_url_current = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-game' \
                                      + '?date=' \
                                      + this_week_date_str
        sd_df = main_hist(scoring_defense_url_current, season, str(week), this_week_date_str, 'scoring_defense')
        sd_df.rename(columns={'Rank': 'Rank_Scoring_Defense',
                              season: 'Current_Season_Scoring_Defense',
                              str(int(season) - 1): 'Previous_Season_Scoring_Defense',
                              'Last 3': 'Last 3_Scoring_Defense',
                              'Last 1': 'Last 1_Scoring_Defense',
                              'Home': 'At_Home_Scoring_Defense',
                              'Away': 'Away_Scoring_Defense'
                              }, inplace=True)
        sd_df['Team'] = sd_df['Team'].str.strip()
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

        this_week_df = pd.merge(so_df, to_df, on=['Team', 'Season', 'Week'], how='outer')
        this_week_df = pd.merge(this_week_df, sd_df, on=['Team', 'Season', 'Week'], how='outer')
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
