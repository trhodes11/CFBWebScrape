def cleaned_cpr_2022(Historic_Games, Teams, Scraped_TR_data):
    Scraped_TR_data = Scraped_TR_data

    home_team_scraped_TR = Scraped_TR_data
    for col_name in home_team_scraped_TR.columns:
        if((col_name != 'Season') & (col_name != 'Week') & (col_name != 'Team')):
            home_team_scraped_TR =home_team_scraped_TR.withColumnRenamed(col_name, "Home_" + col_name)
    away_team_scraped_TR = Scraped_TR_data
    for col_name in away_team_scraped_TR.columns:
        if((col_name != 'Season') & (col_name != 'Week') & (col_name != 'Team')):
            away_team_scraped_TR =away_team_scraped_TR.withColumnRenamed(col_name, "Away_" + col_name)

    TR_Teams = ['Colorado St','Georgia State','TX Christian','Kent State','S Mississippi','Central Mich','S Alabama','Middle Tenn','Central FL','Northwestern','Florida Intl','Oregon St','Connecticut','Washington','Miss State','Vanderbilt','Pittsburgh','San Diego St','Fla Atlantic','W Virginia','Iowa State','Nebraska','LSU','Arizona St','Air Force','Penn State','Wyoming','Old Dominion','Wisconsin','Boston Col','Coastal Car','GA Southern','North Texas','Michigan St','S Methodist','Oklahoma St','Bowling Grn','Wake Forest','Texas State','N Mex State','Mississippi','Arkansas St','Memphis','San Jose St','NC State','TX-San Ant','Boise State','Baylor','E Michigan','Oregon','Arkansas','Temple','E Carolina','Louisville','W Kentucky','Miami (FL)','N Illinois','Ohio State','Texas Tech','Notre Dame','New Mexico','Florida St','Miami (OH)','Ball State','N Carolina','Cincinnati','California','TX El Paso','S Carolina','W Michigan','Utah State','BYU','Michigan','Virginia','Navy','LA Lafayette','Arizona','Marshall','Minnesota','App State','Kansas St','Texas A&M','LA Monroe','Fresno St','S Florida','Tennessee','Charlotte','Iowa','Wash State','Florida','Indiana','VA Tech','Hawaii','Oklahoma','Stanford','Missouri','Kentucky','Syracuse','Colorado','Duke','Illinois','Maryland','Rice','GA Tech','LA Tech','Rutgers','Georgia','Alabama','Houston','Buffalo','Liberty','Clemson','Auburn','Nevada','Toledo','Kansas','Purdue','Tulane','U Mass','Texas','UNLV','Akron','Tulsa','Idaho','Utah','UAB','Ohio','UCLA','Army','Troy','USC']

    df_historical_games = Historic_Games \
        .withColumn('Season',F.year(F.col('start_date'))) \
        .filter(F.col('week') >= 5) \
        .withColumnRenamed('week', 'Week')

    df_historical_games = df_historical_games.filter((df_historical_games.home_team != 'Yale') | (df_historical_games.home_team != 'Norfolk State'))

    df_teams = Teams.withColumn('home_team', F.col('School')) \
        .withColumn('home_city', F.col('City')) \
        .withColumn('home_state', F.col('State'))

    df_joined = df_historical_games.join(df_teams.select('home_team', 'home_city', 'home_state'),
                        ['home_team'],
                        how='left')

    df_joined = df_joined.withColumn('season', F.year(df_joined.start_date))

    df_joined = df_joined.withColumn('conference_game', F.when(F.col('home_conference') == F.col('away_conference'), F.lit('true')).otherwise(F.lit('false'))) \
        .withColumn('score_diff', F.col('home_points') - F.col('away_points')) \
        .drop('season', 'home_points', 'away_points')

    df_joined = df_joined.withColumn('Season',F.year(df_joined.start_date))

    df_joined = df_joined.withColumn('home_team',
                                    when(F.col('home_team') == 'Appalachian State', F.lit('App State'))
                                    .when(F.col('home_team') == 'Arizona State', F.lit('Arizona St'))
                                    .when(F.col('home_team') == 'Arkansas State', F.lit('Arkansas St'))
                                    .when(F.col('home_team') == 'Boston College', F.lit('Boston Col'))
                                    .when(F.col('home_team') == 'Bowling Green', F.lit('Bowling Grn'))
                                    .when(F.col('home_team') == 'UCF', F.lit('Central FL'))
                                    .when(F.col('home_team') == 'Central Michigan', F.lit('Central Mich'))
                                    .when(F.col('home_team') == 'Coastal Carolina', F.lit('Coastal Car'))
                                    .when(F.col('home_team') == 'Colorado State', F.lit('Colorado St'))
                                    .when(F.col('home_team') == 'East Carolina', F.lit('E Carolina'))
                                    .when(F.col('home_team') == 'Eastern Michigan', F.lit('E Michigan'))
                                    .when(F.col('home_team') == 'Florida Atlantic', F.lit('Fla Atlantic'))
                                    .when(F.col('home_team') == 'Florida International', F.lit('Florida Intl'))
                                    .when(F.col('home_team') == 'Florida State', F.lit('Florida St'))
                                    .when(F.col('home_team') == 'Fresno State', F.lit('Fresno St'))
                                    .when(F.col('home_team') == 'Georgia Southern', F.lit('GA Southern'))
                                    .when(F.col('home_team') == 'Georgia Tech', F.lit('GA Tech'))
                                    .when(F.col('home_team') == "Hawai'i", F.lit('Hawaii'))
                                    .when(F.col('home_team') == 'Kansas State', F.lit('Kansas St'))
                                    .when(F.col('home_team') == 'Lafayette', F.lit('LA Lafayette'))
                                    .when(F.col('home_team') == 'Louisiana Monroe', F.lit('LA Monroe'))
                                    .when(F.col('home_team') == 'Louisiana Tech', F.lit('LA Tech'))
                                    .when(F.col('home_team') == 'Miami', F.lit('Miami (FL)'))
                                    .when(F.col('home_team') == 'Michigan State', F.lit('Michigan St'))
                                    .when(F.col('home_team') == 'Middle Tennessee', F.lit('Middle Tenn'))
                                    .when(F.col('home_team') == 'Mississippi State', F.lit('Miss State'))
                                    .when(F.col('home_team') == 'Ole Miss', F.lit('Mississippi'))
                                    .when(F.col('home_team') == 'North Carolina', F.lit('N Carolina'))
                                    .when(F.col('home_team') == 'New Mexico State', F.lit('N Mex State'))
                                    .when(F.col('home_team') == 'Oklahoma State', F.lit('Oklahoma St'))
                                    .when(F.col('home_team') == 'Oregon State', F.lit('Oregon St'))
                                    .when(F.col('home_team') == 'South Alabama', F.lit('S Alabama'))
                                    .when(F.col('home_team') == 'South Carolina', F.lit('S Carolina'))
                                    .when(F.col('home_team') == 'South Florida', F.lit('S Florida'))
                                    .when(F.col('home_team') == 'SMU', F.lit('S Methodist'))
                                    .when(F.col('home_team') == 'Southern Mississippi', F.lit('S Mississippi'))
                                    .when(F.col('home_team') == 'San Diego State', F.lit('San Diego St'))
                                    .when(F.col('home_team') == 'San Jose State', F.lit('San Jose St'))
                                    .when(F.col('home_team') == 'TCU', F.lit('TX Christian'))
                                    .when(F.col('home_team') == 'UTEP', F.lit('TX El Paso'))
                                    .when(F.col('home_team') == 'UT San Antonio', F.lit('TX-San Ant'))
                                    .when(F.col('home_team') == 'UMass', F.lit('U Mass'))
                                    .when(F.col('home_team') == 'Virginia Tech', F.lit('VA Tech'))
                                    .when(F.col('home_team') == 'Western Kentucky', F.lit('W Kentucky'))
                                    .when(F.col('home_team') == 'Western Michigan', F.lit('W Michigan'))
                                    .when(F.col('home_team') == 'West Virginia', F.lit('W Virginia'))
                                    .when(F.col('home_team') == 'Washington State', F.lit('Wash State'))
                                    .otherwise(F.col('home_team')))
    df_joined = df_joined.withColumn('away_team',
                                    when(F.col('away_team') == 'Appalachian State', F.lit('App State'))
                                    .when(F.col('away_team') == 'Arizona State', F.lit('Arizona St'))
                                    .when(F.col('away_team') == 'Arkansas State', F.lit('Arkansas St'))
                                    .when(F.col('away_team') == 'Boston College', F.lit('Boston Col'))
                                    .when(F.col('away_team') == 'Bowling Green', F.lit('Bowling Grn'))
                                    .when(F.col('away_team') == 'UCF', F.lit('Central FL'))
                                    .when(F.col('away_team') == 'Central Michigan', F.lit('Central Mich'))
                                    .when(F.col('away_team') == 'Coastal Carolina', F.lit('Coastal Car'))
                                    .when(F.col('away_team') == 'Colorado State', F.lit('Colorado St'))
                                    .when(F.col('away_team') == 'East Carolina', F.lit('E Carolina'))
                                    .when(F.col('away_team') == 'Eastern Michigan', F.lit('E Michigan'))
                                    .when(F.col('away_team') == 'Florida Atlantic', F.lit('Fla Atlantic'))
                                    .when(F.col('away_team') == 'Florida International', F.lit('Florida Intl'))
                                    .when(F.col('away_team') == 'Florida State', F.lit('Florida St'))
                                    .when(F.col('away_team') == 'Fresno State', F.lit('Fresno St'))
                                    .when(F.col('away_team') == 'Georgia Southern', F.lit('GA Southern'))
                                    .when(F.col('away_team') == 'Georgia Tech', F.lit('GA Tech'))
                                    .when(F.col('away_team') == "Hawai'i", F.lit('Hawaii'))
                                    .when(F.col('away_team') == 'Kansas State', F.lit('Kansas St'))
                                    .when(F.col('away_team') == 'Lafayette', F.lit('LA Lafayette'))
                                    .when(F.col('away_team') == 'Louisiana Monroe', F.lit('LA Monroe'))
                                    .when(F.col('away_team') == 'Louisiana Tech', F.lit('LA Tech'))
                                    .when(F.col('away_team') == 'Miami', F.lit('Miami (FL)'))
                                    .when(F.col('away_team') == 'Michigan State', F.lit('Michigan St'))
                                    .when(F.col('away_team') == 'Middle Tennessee', F.lit('Middle Tenn'))
                                    .when(F.col('away_team') == 'Mississippi State', F.lit('Miss State'))
                                    .when(F.col('away_team') == 'Ole Miss', F.lit('Mississippi'))
                                    .when(F.col('away_team') == 'North Carolina', F.lit('N Carolina'))
                                    .when(F.col('away_team') == 'New Mexico State', F.lit('N Mex State'))
                                    .when(F.col('away_team') == 'Oklahoma State', F.lit('Oklahoma St'))
                                    .when(F.col('away_team') == 'Oregon State', F.lit('Oregon St'))
                                    .when(F.col('away_team') == 'South Alabama', F.lit('S Alabama'))
                                    .when(F.col('away_team') == 'South Carolina', F.lit('S Carolina'))
                                    .when(F.col('away_team') == 'South Florida', F.lit('S Florida'))
                                    .when(F.col('away_team') == 'SMU', F.lit('S Methodist'))
                                    .when(F.col('away_team') == 'Southern Mississippi', F.lit('S Mississippi'))
                                    .when(F.col('away_team') == 'San Diego State', F.lit('San Diego St'))
                                    .when(F.col('away_team') == 'San Jose State', F.lit('San Jose St'))
                                    .when(F.col('away_team') == 'TCU', F.lit('TX Christian'))
                                    .when(F.col('away_team') == 'UTEP', F.lit('TX El Paso'))
                                    .when(F.col('away_team') == 'UT San Antonio', F.lit('TX-San Ant'))
                                    .when(F.col('away_team') == 'UMass', F.lit('U Mass'))
                                    .when(F.col('away_team') == 'Virginia Tech', F.lit('VA Tech'))
                                    .when(F.col('away_team') == 'Western Kentucky', F.lit('W Kentucky'))
                                    .when(F.col('away_team') == 'Western Michigan', F.lit('W Michigan'))
                                    .when(F.col('away_team') == 'West Virginia', F.lit('W Virginia'))
                                    .when(F.col('away_team') == 'Washington State', F.lit('Wash State'))
                                    .otherwise(F.col('away_team')))

    df_joined = df_joined.withColumn('home_conference',
                                    when(F.col('home_team') == 'LA Lafayette', F.lit('Sun Belt'))
                                    .when(F.col('home_team') == 'GA Southern', F.lit('Sun Belt'))
                                    .when(F.col('home_team') == 'Liberty', F.lit('FBS Independents'))
                                    .when(F.col('home_team') == 'Old Dominion', F.lit('Conference USA'))
                                    .when(F.col('home_team') == 'App State', F.lit('Sun Belt'))
                                    .when(F.col('home_team') == 'Idaho', F.lit('Big Sky'))
                                    .when(F.col('home_team') == 'Coastal Car', F.lit('Sun Belt'))
                                    .when(F.col('home_team') == 'Georgia State', F.lit('Sun Belt'))
                                    .otherwise(F.col('home_conference')))

    df_joined = df_joined.withColumn('away_conference',
                                    when(F.col('away_team') == 'LA Lafayette', F.lit('Sun Belt'))
                                    .when(F.col('away_team') == 'GA Southern', F.lit('Sun Belt'))
                                    .when(F.col('away_team') == 'Liberty', F.lit('FBS Independents'))
                                    .when(F.col('away_team') == 'Old Dominion', F.lit('Conference USA'))
                                    .when(F.col('away_team') == 'App State', F.lit('Sun Belt'))
                                    .when(F.col('away_team') == 'Idaho', F.lit('Big Sky'))
                                    .when(F.col('away_team') == 'Coastal Car', F.lit('Sun Belt'))
                                    .when(F.col('away_team') == 'Georgia State', F.lit('Sun Belt'))
                                    .otherwise(F.col('away_conference')))

    df_joined = df_joined.filter((F.col('home_team').isin(TR_Teams)) & (F.col('away_team').isin(TR_Teams)))
    df_joined = df_joined.filter((F.col('Week') > 4) & (F.col('Week') < 15))

    df_joined = df_joined.join(home_team_scraped_TR.withColumnRenamed('Team', 'home_team'),
                                ['home_team', 'Week', 'Season'],
                                how='left')
    df_joined = df_joined.join(away_team_scraped_TR.withColumnRenamed('Team', 'away_team'),
                                ['away_team', 'Week', 'Season'],
                                how='left')

    return(df_joined)
    
