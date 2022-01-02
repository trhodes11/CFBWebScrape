from package import *
import pandas as pd
import numpy as np


def build_model_df():
    desired_width = 320
    pd.set_option('display.width', desired_width)
    pd.set_option('display.max_columns', 60)

    """
    # Open the scraped 10 year history dataset and save as a dataframe called Scraped_TR_data
    """
    f = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/scraped_data/Combined/Final_Combined_Dataset.csv'
    Scraped_TR_data = pd.read_csv(f)

    """
    # Open the csv file with all games from the past 10 years and save as a dataframe called Historic_Games
    """
    f = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/Historic_Games.csv'
    Historic_Games = pd.read_csv(f)

    """
    # Open the csv file that maps Teams to locations and save as a dataframe called Teams
    """
    f = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/Teams.csv'
    Teams = pd.read_csv(f)

    """
    # Append 'Home_' to each column name so that we can distinguish home team data from away team data
    """
    home_team_scraped_TR = Scraped_TR_data
    for col_name in home_team_scraped_TR:
        if (col_name != 'Season') & (col_name != 'Week') & (col_name != 'Team'):
            home_team_scraped_TR = home_team_scraped_TR.rename({col_name: 'Home_' + col_name}, axis=1)
    """
    # Append 'Away_' to each column name so that we can distinguish home team data from away team data
    """
    away_team_scraped_TR = Scraped_TR_data
    for col_name in away_team_scraped_TR:
        if (col_name != 'Season') & (col_name != 'Week') & (col_name != 'Team'):
            away_team_scraped_TR = away_team_scraped_TR.rename({col_name: 'Away_' + col_name}, axis=1)

    """
    # Build a list of the team names as they appear in the scraped data. This will be the official naming convention
    """
    TR_Teams = ['Colorado St', 'Georgia State', 'TX Christian', 'Kent State', 'S Mississippi', 'Central Mich',
                'S Alabama', 'Middle Tenn', 'Central FL', 'Northwestern', 'Florida Intl', 'Oregon St', 'Connecticut',
                'Washington', 'Miss State', 'Vanderbilt', 'Pittsburgh', 'San Diego St', 'Fla Atlantic', 'W Virginia',
                'Iowa State', 'Nebraska', 'LSU', 'Arizona St', 'Air Force', 'Penn State', 'Wyoming', 'Old Dominion',
                'Wisconsin', 'Boston Col', 'Coastal Car', 'GA Southern', 'North Texas', 'Michigan St', 'S Methodist',
                'Oklahoma St', 'Bowling Grn', 'Wake Forest', 'Texas State', 'N Mex State', 'Mississippi', 'Arkansas St',
                'Memphis','San Jose St','NC State','TX-San Ant','Boise State','Baylor','E Michigan','Oregon','Arkansas',
                'Temple','E Carolina','Louisville','W Kentucky','Miami (FL)','N Illinois','Ohio State','Texas Tech',
                'Notre Dame','New Mexico','Florida St','Miami (OH)','Ball State','N Carolina','Cincinnati','California',
                'TX El Paso','S Carolina','W Michigan','Utah State','BYU','Michigan','Virginia','Navy','LA Lafayette',
                'Arizona','Marshall','Minnesota','App State','Kansas St','Texas A&M','LA Monroe','Fresno St',
                'S Florida','Tennessee','Charlotte','Iowa','Wash State','Florida','Indiana','VA Tech','Hawaii',
                'Oklahoma','Stanford','Missouri','Kentucky','Syracuse','Colorado','Duke','Illinois','Maryland','Rice',
                'GA Tech','LA Tech','Rutgers','Georgia','Alabama','Houston','Buffalo','Liberty','Clemson','Auburn',
                'Nevada','Toledo','Kansas','Purdue','Tulane','U Mass','Texas','UNLV','Akron','Tulsa','Idaho','Utah',
                'UAB','Ohio','UCLA','Army','Troy','USC']

    df_historical_games = Historic_Games
    """
    # 1. Create a column in the historic game dataframe so we know what season each game comes from
    # 2. Filter the dataframe to keep only games that are from weeks 5-14
    # 3. Rename the column week to Week so that all dataframes have a consistent column name
    """
    df_historical_games['Season'] = pd.DatetimeIndex(df_historical_games['start_date']).year
    df_historical_games = df_historical_games[df_historical_games['week'] >= 5]
    df_historical_games = df_historical_games.rename({'week': 'Week'}, axis=1)

    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Appalachian State", 'home_team'] = "App State"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Arizona State", 'home_team'] = "Arizona St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Arkansas State", 'home_team'] = "Arkansas St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Boston College", 'home_team'] = "Boston Col"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Bowling Green", 'home_team'] = "Bowling Grn"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "UCF", 'home_team'] = "Central FL"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Central Michigan", 'home_team'] = "Central Mich"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Coastal Carolina", 'home_team'] = "Coastal Car"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Colorado State", 'home_team'] = "Colorado St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "East Carolina", 'home_team'] = "E Carolina"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Eastern Michigan", 'home_team'] = "E Michigan"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Florida Atlantic", 'home_team'] = "Fla Atlantic"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Florida International", 'home_team'] = "Florida Intl"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Florida State", 'home_team'] = "Florida St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Fresno State", 'home_team'] = "Fresno St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Georgia Southern", 'home_team'] = "GA Southern"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Georgia Tech", 'home_team'] = "GA Tech"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Hawai'i", 'home_team'] = "Hawaii"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Kansas State", 'home_team'] = "Kansas St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Lafayette", 'home_team'] = "LA Lafayette"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Louisiana Monroe", 'home_team'] = "LA Monroe"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Louisiana Tech", 'home_team'] = "LA Tech"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Miami", 'home_team'] = "Miami (FL)"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Michigan State", 'home_team'] = "Michigan St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Middle Tennessee", 'home_team'] = "Middle Tenn"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Mississippi State", 'home_team'] = "Miss State"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Ole Miss", 'home_team'] = "Mississippi"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "North Carolina", 'home_team'] = "N Carolina"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "New Mexico State", 'home_team'] = "N Mex State"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Oklahoma State", 'home_team'] = "Oklahoma St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Oregon State", 'home_team'] = "Oregon St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "South Alabama", 'home_team'] = "S Alabama"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "South Carolina", 'home_team'] = "S Carolina"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "South Florida", 'home_team'] = "S Florida"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "SMU", 'home_team'] = "S Methodist"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Southern Mississippi", 'home_team'] = "S Mississippi"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "San Diego State", 'home_team'] = "San Diego St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "San Jose State", 'home_team'] = "San Jose St"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "TCU", 'home_team'] = "TX Christian"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "UTEP", 'home_team'] = "TX El Paso"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "UT San Antonio", 'home_team'] = "TX-San Ant"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "UMass", 'home_team'] = "U Mass"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Virginia Tech", 'home_team'] = "VA Tech"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Western Kentucky", 'home_team'] = "W Kentucky"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Western Michigan", 'home_team'] = "W Michigan"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "West Virginia", 'home_team'] = "W Virginia"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Washington State", 'home_team'] = "Wash State"

    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Appalachian State", 'away_team'] = "App State"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Arizona State", 'away_team'] = "Arizona St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Arkansas State", 'away_team'] = "Arkansas St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Boston College", 'away_team'] = "Boston Col"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Bowling Green", 'away_team'] = "Bowling Grn"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "UCF", 'away_team'] = "Central FL"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Central Michigan", 'away_team'] = "Central Mich"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Coastal Carolina", 'away_team'] = "Coastal Car"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Colorado State", 'away_team'] = "Colorado St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "East Carolina", 'away_team'] = "E Carolina"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Eastern Michigan", 'away_team'] = "E Michigan"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Florida Atlantic", 'away_team'] = "Fla Atlantic"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Florida International", 'away_team'] = "Florida Intl"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Florida State", 'away_team'] = "Florida St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Fresno State", 'away_team'] = "Fresno St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Georgia Southern", 'away_team'] = "GA Southern"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Georgia Tech", 'away_team'] = "GA Tech"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Hawai'i", 'away_team'] = "Hawaii"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Kansas State", 'away_team'] = "Kansas St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Lafayette", 'away_team'] = "LA Lafayette"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Louisiana Monroe", 'away_team'] = "LA Monroe"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Louisiana Tech", 'away_team'] = "LA Tech"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Miami", 'away_team'] = "Miami (FL)"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Michigan State", 'away_team'] = "Michigan St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Middle Tennessee", 'away_team'] = "Middle Tenn"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Mississippi State", 'away_team'] = "Miss State"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Ole Miss", 'away_team'] = "Mississippi"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "North Carolina", 'away_team'] = "N Carolina"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "New Mexico State", 'away_team'] = "N Mex State"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Oklahoma State", 'away_team'] = "Oklahoma St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Oregon State", 'away_team'] = "Oregon St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "South Alabama", 'away_team'] = "S Alabama"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "South Carolina", 'away_team'] = "S Carolina"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "South Florida", 'away_team'] = "S Florida"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "SMU", 'away_team'] = "S Methodist"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Southern Mississippi", 'away_team'] = "S Mississippi"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "San Diego State", 'away_team'] = "San Diego St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "San Jose State", 'away_team'] = "San Jose St"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "TCU", 'away_team'] = "TX Christian"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "UTEP", 'away_team'] = "TX El Paso"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "UT San Antonio", 'away_team'] = "TX-San Ant"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "UMass", 'away_team'] = "U Mass"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Virginia Tech", 'away_team'] = "VA Tech"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Western Kentucky", 'away_team'] = "W Kentucky"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Western Michigan", 'away_team'] = "W Michigan"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "West Virginia", 'away_team'] = "W Virginia"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Washington State", 'away_team'] = "Wash State"

    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "LA Lafayette", 'home_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "GA Southern", 'home_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Liberty", 'home_conference'] = "FBS Independents"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Old Dominion", 'home_conference'] = "Conference USA"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "App State", 'home_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Idaho", 'home_conference'] = "Big Sky"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Coastal Car", 'home_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['home_team'] == "Georgia State", 'home_conference'] = "Sun Belt"

    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "LA Lafayette", 'away_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "GA Southern", 'away_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Liberty", 'away_conference'] = "FBS Independents"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Old Dominion", 'away_conference'] = "Conference USA"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "App State", 'away_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Idaho", 'away_conference'] = "Big Sky"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Coastal Car", 'away_conference'] = "Sun Belt"
    df_historical_games = df_historical_games.loc[df_historical_games['away_team'] == "Georgia State", 'away_conference'] = "Sun Belt"

    """
    # Within the df_teams dataframe, use the existing columns called 'School', 'City', and 'State' to create columns
    # called 'home_team', 'home_city', 'home_state'
    """
    df_teams = Teams
    df_teams['home_team'] = df_teams['School']
    df_teams['home_city'] = df_teams['City']
    df_teams['home_state'] = df_teams['State']

    """
    # Join together the dataframes df_historical_games and df_teams on the home_team column. This will add the City and
    # State columns to the df_joined dataframe (similar to doing a vlookup in Excel)
    """
    df_joined = pd.merge(df_historical_games,
                         df_teams[['home_team', 'home_city', 'home_state']],
                         on=['home_team'],
                         how='left')

    """
    # Create a column called 'conference_game' by comparing the conferences of the home and away teams;
    # If they are in the same conference, conference_game column will be True; otherwise False
    """
    comparison_column = np.where(df_joined['home_conference'] == df_joined['away_conference'], True, False)
    df_joined['conference_game'] = comparison_column

    df_joined['score_diff'] = df_joined['home_points'] - df_joined['away_points']

    df_joined = df_joined.drop(['home_points', 'away_points'], 1)

    boolean_series = df_historical_games.home_team.isin(TR_Teams) & df_historical_games.away_team.isin(TR_Teams)
    df_historical_games = df_historical_games[boolean_series]

    """
    df_joined = df_joined.join(home_team_scraped_TR.withColumnRenamed('Team', 'home_team'),
                                ['home_team', 'Week', 'Season'],
                                how='left')
    df_joined = df_joined.join(away_team_scraped_TR.withColumnRenamed('Team', 'away_team'),
                                ['away_team', 'Week', 'Season'],
                                how='left')
    """
    return Historic_Games


if __name__ == '__main__':
    modelDF = build_model_df()
    save_dir = '/Users/staceyrhodes/PycharmProjects/TeamRankingsWebScraper/Model/'
    save_file = 'Model_Dataset'
    try:
        datascraper.save_df(modelDF, save_dir, save_file)
        print('{} saved successfully.'.format(save_file))
        print('File successfully saved at {}.'.format(save_dir))
    except:
        print('I don\'t think the file saved, you should double check.')