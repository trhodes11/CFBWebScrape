from package import *
import pandas as pd
import numpy as np

Week = 5
Season = 2021

def pred_request():
    f = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/Scraped_TR_Data_Season_' + str(Season) + '_Week_' + str(Week) + '.csv'
    current_week_scraped = pd.read_csv(f)

    f = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/Historic_Games.csv'
    historic_games = pd.read_csv(f)

    df_historical_games = historic_games[['home_team',
                                          'venue',
                                          'home_conference']].drop_duplicates(subset=['home_team'])

    df_historical_games.loc[df_historical_games['home_team'] == "Appalachian State", 'home_team'] = "App State"
    df_historical_games.loc[df_historical_games['home_team'] == "Arizona State", 'home_team'] = "Arizona St"
    df_historical_games.loc[df_historical_games['home_team'] == "Arkansas State", 'home_team'] = "Arkansas St"
    df_historical_games.loc[df_historical_games['home_team'] == "Boston College", 'home_team'] = "Boston Col"
    df_historical_games.loc[df_historical_games['home_team'] == "Bowling Green", 'home_team'] = "Bowling Grn"
    df_historical_games.loc[df_historical_games['home_team'] == "UCF", 'home_team'] = "Central FL"
    df_historical_games.loc[df_historical_games['home_team'] == "Central Michigan", 'home_team'] = "Central Mich"
    df_historical_games.loc[df_historical_games['home_team'] == "Coastal Carolina", 'home_team'] = "Coastal Car"
    df_historical_games.loc[df_historical_games['home_team'] == "Colorado State", 'home_team'] = "Colorado St"
    df_historical_games.loc[df_historical_games['home_team'] == "East Carolina", 'home_team'] = "E Carolina"
    df_historical_games.loc[df_historical_games['home_team'] == "Eastern Michigan", 'home_team'] = "E Michigan"
    df_historical_games.loc[df_historical_games['home_team'] == "Florida Atlantic", 'home_team'] = "Fla Atlantic"
    df_historical_games.loc[df_historical_games['home_team'] == "Florida International", 'home_team'] = "Florida Intl"
    df_historical_games.loc[df_historical_games['home_team'] == "Florida State", 'home_team'] = "Florida St"
    df_historical_games.loc[df_historical_games['home_team'] == "Fresno State", 'home_team'] = "Fresno St"
    df_historical_games.loc[df_historical_games['home_team'] == "Georgia Southern", 'home_team'] = "GA Southern"
    df_historical_games.loc[df_historical_games['home_team'] == "Georgia Tech", 'home_team'] = "GA Tech"
    df_historical_games.loc[df_historical_games['home_team'] == "Hawai'i", 'home_team'] = "Hawaii"
    df_historical_games.loc[df_historical_games['home_team'] == "Kansas State", 'home_team'] = "Kansas St"
    df_historical_games.loc[df_historical_games['home_team'] == "Lafayette", 'home_team'] = "LA Lafayette"
    df_historical_games.loc[df_historical_games['home_team'] == "Louisiana Monroe", 'home_team'] = "LA Monroe"
    df_historical_games.loc[df_historical_games['home_team'] == "Louisiana Tech", 'home_team'] = "LA Tech"
    df_historical_games.loc[df_historical_games['home_team'] == "Miami", 'home_team'] = "Miami (FL)"
    df_historical_games.loc[df_historical_games['home_team'] == "Michigan State", 'home_team'] = "Michigan St"
    df_historical_games.loc[df_historical_games['home_team'] == "Middle Tennessee", 'home_team'] = "Middle Tenn"
    df_historical_games.loc[df_historical_games['home_team'] == "Mississippi State", 'home_team'] = "Miss State"
    df_historical_games.loc[df_historical_games['home_team'] == "Ole Miss", 'home_team'] = "Mississippi"
    df_historical_games.loc[df_historical_games['home_team'] == "North Carolina", 'home_team'] = "N Carolina"
    df_historical_games.loc[df_historical_games['home_team'] == "New Mexico State", 'home_team'] = "N Mex State"
    df_historical_games.loc[df_historical_games['home_team'] == "Oklahoma State", 'home_team'] = "Oklahoma St"
    df_historical_games.loc[df_historical_games['home_team'] == "Oregon State", 'home_team'] = "Oregon St"
    df_historical_games.loc[df_historical_games['home_team'] == "South Alabama", 'home_team'] = "S Alabama"
    df_historical_games.loc[df_historical_games['home_team'] == "South Carolina", 'home_team'] = "S Carolina"
    df_historical_games.loc[df_historical_games['home_team'] == "South Florida", 'home_team'] = "S Florida"
    df_historical_games.loc[df_historical_games['home_team'] == "SMU", 'home_team'] = "S Methodist"
    df_historical_games.loc[df_historical_games['home_team'] == "Southern Mississippi", 'home_team'] = "S Mississippi"
    df_historical_games.loc[df_historical_games['home_team'] == "San Diego State", 'home_team'] = "San Diego St"
    df_historical_games.loc[df_historical_games['home_team'] == "San Jose State", 'home_team'] = "San Jose St"
    df_historical_games.loc[df_historical_games['home_team'] == "TCU", 'home_team'] = "TX Christian"
    df_historical_games.loc[df_historical_games['home_team'] == "UTEP", 'home_team'] = "TX El Paso"
    df_historical_games.loc[df_historical_games['home_team'] == "UT San Antonio", 'home_team'] = "TX-San Ant"
    df_historical_games.loc[df_historical_games['home_team'] == "UMass", 'home_team'] = "U Mass"
    df_historical_games.loc[df_historical_games['home_team'] == "Virginia Tech", 'home_team'] = "VA Tech"
    df_historical_games.loc[df_historical_games['home_team'] == "Western Kentucky", 'home_team'] = "W Kentucky"
    df_historical_games.loc[df_historical_games['home_team'] == "Western Michigan", 'home_team'] = "W Michigan"
    df_historical_games.loc[df_historical_games['home_team'] == "West Virginia", 'home_team'] = "W Virginia"
    df_historical_games.loc[df_historical_games['home_team'] == "Washington State", 'home_team'] = "Wash State"

    df_historical_games.loc[df_historical_games['home_team'] == "LA Lafayette", 'home_conference'] = "Sun Belt"
    df_historical_games.loc[df_historical_games['home_team'] == "GA Southern", 'home_conference'] = "Sun Belt"
    df_historical_games.loc[df_historical_games['home_team'] == "Liberty", 'home_conference'] = "FBS Independents"
    df_historical_games.loc[df_historical_games['home_team'] == "Old Dominion", 'home_conference'] = "Conference USA"
    df_historical_games.loc[df_historical_games['home_team'] == "App State", 'home_conference'] = "Sun Belt"
    df_historical_games.loc[df_historical_games['home_team'] == "Idaho", 'home_conference'] = "Big Sky"
    df_historical_games.loc[df_historical_games['home_team'] == "Coastal Car", 'home_conference'] = "Sun Belt"
    df_historical_games.loc[df_historical_games['home_team'] == "Georgia State", 'home_conference'] = "Sun Belt"

    f = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/Teams.csv'
    teams = pd.read_csv(f)

    df_teams_home = teams
    df_teams_home['home_team'] = df_teams_home['School']
    df_teams_home['home_city'] = df_teams_home['City']
    df_teams_home['home_state'] = df_teams_home['State']

    df_teams_home.loc[
        df_teams_home['home_team'] == "Appalachian State", 'home_team'] = "App State"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Arizona State", 'home_team'] = "Arizona St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Arkansas State", 'home_team'] = "Arkansas St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Boston College", 'home_team'] = "Boston Col"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Bowling Green", 'home_team'] = "Bowling Grn"
    df_teams_home.loc[
        df_teams_home['home_team'] == "UCF", 'home_team'] = "Central FL"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Central Michigan", 'home_team'] = "Central Mich"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Coastal Carolina", 'home_team'] = "Coastal Car"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Colorado State", 'home_team'] = "Colorado St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "East Carolina", 'home_team'] = "E Carolina"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Eastern Michigan", 'home_team'] = "E Michigan"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Florida Atlantic", 'home_team'] = "Fla Atlantic"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Florida International", 'home_team'] = "Florida Intl"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Florida State", 'home_team'] = "Florida St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Fresno State", 'home_team'] = "Fresno St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Georgia Southern", 'home_team'] = "GA Southern"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Georgia Tech", 'home_team'] = "GA Tech"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Hawai'i", 'home_team'] = "Hawaii"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Kansas State", 'home_team'] = "Kansas St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Louisiana", 'home_team'] = "LA Lafayette"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Louisiana Monroe", 'home_team'] = "LA Monroe"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Louisiana Tech", 'home_team'] = "LA Tech"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Miami", 'home_team'] = "Miami (FL)"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Michigan State", 'home_team'] = "Michigan St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Middle Tennessee", 'home_team'] = "Middle Tenn"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Mississippi State", 'home_team'] = "Miss State"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Ole Miss", 'home_team'] = "Mississippi"
    df_teams_home.loc[
        df_teams_home['home_team'] == "North Carolina", 'home_team'] = "N Carolina"
    df_teams_home.loc[
        df_teams_home['home_team'] == "New Mexico State", 'home_team'] = "N Mex State"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Northern Illinois", 'home_team'] = "N Illinois"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Oklahoma State", 'home_team'] = "Oklahoma St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Oregon State", 'home_team'] = "Oregon St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "South Alabama", 'home_team'] = "S Alabama"
    df_teams_home.loc[
        df_teams_home['home_team'] == "South Carolina", 'home_team'] = "S Carolina"
    df_teams_home.loc[
        df_teams_home['home_team'] == "South Florida", 'home_team'] = "S Florida"
    df_teams_home.loc[
        df_teams_home['home_team'] == "SMU", 'home_team'] = "S Methodist"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Southern Mississippi", 'home_team'] = "S Mississippi"
    df_teams_home.loc[
        df_teams_home['home_team'] == "San Diego State", 'home_team'] = "San Diego St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "San Jose State", 'home_team'] = "San Jose St"
    df_teams_home.loc[
        df_teams_home['home_team'] == "TCU", 'home_team'] = "TX Christian"
    df_teams_home.loc[
        df_teams_home['home_team'] == "UTEP", 'home_team'] = "TX El Paso"
    df_teams_home.loc[
        df_teams_home['home_team'] == "UT San Antonio", 'home_team'] = "TX-San Ant"
    df_teams_home.loc[
        df_teams_home['home_team'] == "UMass", 'home_team'] = "U Mass"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Virginia Tech", 'home_team'] = "VA Tech"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Western Kentucky", 'home_team'] = "W Kentucky"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Western Michigan", 'home_team'] = "W Michigan"
    df_teams_home.loc[
        df_teams_home['home_team'] == "West Virginia", 'home_team'] = "W Virginia"
    df_teams_home.loc[
        df_teams_home['home_team'] == "Washington State", 'home_team'] = "Wash State"

    df_teams = pd.merge(df_historical_games,
                        df_teams_home[['home_team', 'home_city', 'home_state']],
                        on=['home_team'],
                        how='left')

    f = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/Schedule v3 2001.csv'
    df_schedule = pd.read_csv(f)
    df_schedule = df_schedule.rename({'Home': 'home_team'}, axis=1)
    df_schedule = df_schedule.rename({'Away': 'away_team'}, axis=1)

    df_schedule = df_schedule[df_schedule['Week'] == Week]

    df_schedule = pd.merge(df_schedule,
                           df_teams,
                           on=['home_team'],
                           how='left')

    df_teams = df_teams.rename({'home_team': 'away_team'}, axis=1)
    df_teams = df_teams.rename({'home_conference': 'away_conference'}, axis=1)
    df_teams = df_teams.rename({'home_city': 'away_city'}, axis=1)
    df_teams = df_teams.rename({'home_state': 'away_state'}, axis=1)

    df_schedule = pd.merge(df_schedule,
                           df_teams.drop(['venue'], 1),
                           on=['away_team'],
                           how='left')

    comparison_column = np.where(df_schedule['home_conference'] == df_schedule['away_conference'], True, False)
    df_schedule['conference_game'] = comparison_column

    df_schedule = df_schedule.rename({'Date': 'start_date'}, axis=1)
    df_schedule['Season'] = pd.DatetimeIndex(df_schedule['start_date']).year

    df_schedule['score_diff'] = np.nan

    """
        # Append 'Home_' to each column name so that we can distinguish home team data from away team data
        """
    home_team_scraped_TR = current_week_scraped
    for col_name in home_team_scraped_TR:
        if (col_name != 'Season') & (col_name != 'Week') & (col_name != 'Team'):
            home_team_scraped_TR = home_team_scraped_TR.rename({col_name: 'Home_' + col_name}, axis=1)
    """
    # Append 'Away_' to each column name so that we can distinguish home team data from away team data
    """
    away_team_scraped_TR = current_week_scraped
    for col_name in away_team_scraped_TR:
        if (col_name != 'Season') & (col_name != 'Week') & (col_name != 'Team'):
            away_team_scraped_TR = away_team_scraped_TR.rename({col_name: 'Away_' + col_name}, axis=1)
    home_team_scraped_TR = home_team_scraped_TR.rename({'Team': 'home_team'}, axis=1)
    away_team_scraped_TR = away_team_scraped_TR.rename({'Team': 'away_team'}, axis=1)

    df_schedule = pd.merge(df_schedule,
                           home_team_scraped_TR,
                           on=['home_team', 'Week', 'Season'],
                           how='left')
    df_schedule = pd.merge(df_schedule,
                           away_team_scraped_TR,
                           on=['away_team', 'Week', 'Season'],
                           how='left')

    return df_schedule


if __name__ == '__main__':
    predDF = pred_request()
    save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/Prediction/'
    save_file = 'Prediction_Request_Dataset_Week_' + str(Week)
    try:
        datascraper.save_df(predDF, save_dir, save_file)
        print('{} saved successfully.'.format(save_file))
        print('File successfully saved at {}.'.format(save_dir))
    except:
        print('I don\'t think the file saved, you should double check.')
