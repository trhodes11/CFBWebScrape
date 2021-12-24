from package import *
import sys
import os
import datetime
import time
import pandas as pd


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
    # This try statement is a check to see whether the program was started using the command line/terminal or using an 
    # editor (like pycharm). We will always be starting it from pycharm, so the try statement will always fail and 
    # skip down into the except code
    """
    try:
        main_hist(sys.argv[1])
    except IndexError as e:
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
                    time.sleep(1)

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
                    time.sleep(1)

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
                    time.sleep(1)

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
                    time.sleep(1)

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
                    time.sleep(1)

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
                    time.sleep(1)

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
                    time.sleep(1)

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
                    time.sleep(1)

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

                    this_week_date = this_week_date + datetime.timedelta(days=7)
                    this_week_date_str = this_week_date.strftime("%Y-%m-%d")

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

                save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/Combined'
                save_file = 'Scraped_TR_Data_Combined_' + season
                try:
                    datascraper.save_df(season_df, save_dir, save_file)
                    print('{} saved successfully.'.format(save_file))
                    print('File successfully saved at {}.'.format(save_dir))
                except:
                    print('I don\'t think the file saved, you should double check.')

                time.sleep(3)

            save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/Combined'
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

            save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/Current'
            save_file = 'Scraped_TR_Data_Season_' + season + '_Week_' + week
            try:
                datascraper.save_df(season_df, save_dir, save_file)
                print('{} saved successfully.'.format(save_file))
                print('File successfully saved at {}.'.format(save_dir))
            except:
                print('I don\'t think the file saved, you should double check.')
