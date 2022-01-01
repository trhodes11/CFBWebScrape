def pred_request(cleaned_cpr_v2, Schedule_v2, OFFENSIVE_FEI_RATINGS_2021_WK_13, DEFENSIVE_FEI_RATINGS_2021_WK_13, ST_FEI_RATINGS_2021_WK_13):

    df_teams = cleaned_cpr_v2.select('home_team',
                                     'venue',
                                     'home_conference',
                                     'home_city',
                                     'home_state').drop_duplicates(subset=['home_team'])

    df_schedule = Schedule_v2 \
        .withColumnRenamed('Home', 'home_team') \
        .withColumnRenamed('Away', 'away_team') \
        .withColumn('home_team',
                    F.when(F.col('home_team') == 'UConn', F.lit('Connecticut')).otherwise(F.col('home_team'))) \
        .withColumn('away_team',
                    F.when(F.col('away_team') == 'UConn', F.lit('Connecticut')).otherwise(F.col('away_team'))) \
        .withColumn('home_team',
                    F.when(F.col('home_team') == 'UTSA', F.lit('UT San Antonio')).otherwise(F.col('home_team'))) \
        .withColumn('away_team',
                    F.when(F.col('away_team') == 'UTSA', F.lit('UT San Antonio')).otherwise(F.col('away_team')))

    df_offense = OFFENSIVE_FEI_RATINGS_2021_WK_13 \
        .withColumn('Team', F.when(F.col('Team') == 'Hawaii', F.lit('Hawai\'i')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'UTSA', F.lit('UT San Antonio')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'UL Monroe', F.lit('Louisiana Monroe')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'Massachusetts', F.lit('UMass')).otherwise(F.col('Team'))) \
        .withColumn('Season', F.lit('2021'))

    df_defense = DEFENSIVE_FEI_RATINGS_2021_WK_13 \
        .withColumn('Team', F.when(F.col('Team') == 'Hawaii', F.lit('Hawai\'i')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'UTSA', F.lit('UT San Antonio')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'UL Monroe', F.lit('Louisiana Monroe')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'Massachusetts', F.lit('UMass')).otherwise(F.col('Team'))) \
        .withColumn('Season', F.lit('2021'))

    df_ST = ST_FEI_RATINGS_2021_WK_13 \
        .withColumn('Team', F.when(F.col('Team') == 'Hawaii', F.lit('Hawai\'i')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'UTSA', F.lit('UT San Antonio')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'UL Monroe', F.lit('Louisiana Monroe')).otherwise(F.col('Team'))) \
        .withColumn('Team', F.when(F.col('Team') == 'Massachusetts', F.lit('UMass')).otherwise(F.col('Team'))) \
        .withColumn('Season', F.lit('2021'))


    df_schedule = df_schedule.filter(F.col('Week') == 'Week 13')

    df_schedule = df_schedule.join(df_teams, 
                                  ['home_team'],
                                  how='left')

    df_teams = df_teams \
        .withColumnRenamed('home_team', 'away_team') \
        .withColumnRenamed('home_conference', 'away_conference')

    df_schedule = df_schedule.join(df_teams.select('away_team', 'away_conference'),
                                    ['away_team'],
                                    how='left')

    df_schedule = df_schedule.withColumn('conference_game', F.when(F.col('home_conference') == F.col('away_conference'), F.lit('true')).otherwise(F.lit('false')))

    df_schedule = df_schedule.withColumn("Week", F.expr("substring(Week, 6, length(Week))"))

    df_schedule = df_schedule.withColumn('Season', F.lit('2021'))

    df_schedule = df_schedule.withColumnRenamed('Date', 'start_date')

    df_offense_home = df_offense
    df_offense_away = df_offense

    df_offense_home = df_offense_home.select([F.col(col_name).alias('home_' + col_name) for col_name in df_offense_home.columns]) \
        .withColumnRenamed('home_Team', 'home_team') \
        .withColumnRenamed('home_Season', 'Season') \
        .withColumnRenamed('home_Rk', 'home_O_Rk')

    df_offense_away = df_offense_away.select([F.col(col_name).alias('away_' + col_name) for col_name in df_offense_away.columns]) \
            .withColumnRenamed('away_Team', 'away_team') \
        .withColumnRenamed('away_Season', 'Season') \
        .withColumnRenamed('away_Rk', 'away_O_Rk')

    df_schedule = df_schedule.join(df_offense_home,
                        ['Season', 'home_team'],
                        how='left')

    df_schedule = df_schedule.join(df_offense_away,
                        ['Season', 'away_team'],
                        how='left')

    
    df_defense_home = df_defense
    df_defense_away = df_defense

    df_defense_home = df_defense_home.select([F.col(col_name).alias('home_' + col_name) for col_name in df_defense_home.columns]) \
        .withColumnRenamed('home_Team', 'home_team') \
        .withColumnRenamed('home_Season', 'Season') \
        .withColumnRenamed('home_Rk', 'home_D_Rk')

    df_defense_away = df_defense_away.select([F.col(col_name).alias('away_' + col_name) for col_name in df_defense_away.columns]) \
            .withColumnRenamed('away_Team', 'away_team') \
        .withColumnRenamed('away_Season', 'Season') \
        .withColumnRenamed('away_Rk', 'away_D_Rk')

    df_schedule = df_schedule.join(df_defense_home,
                        ['Season', 'home_team'],
                        how='left')

    df_schedule = df_schedule.join(df_defense_away,
                        ['Season', 'away_team'],
                        how='left')


    df_ST_home = df_ST
    df_ST_away = df_ST

    df_ST_home = df_ST_home.select([F.col(col_name).alias('home_' + col_name) for col_name in df_ST_home.columns]) \
        .withColumnRenamed('home_Team', 'home_team') \
        .withColumnRenamed('home_Season', 'Season') \
        .withColumnRenamed('home_Rk', 'home_ST_Rk')

    df_ST_away = df_ST_away.select([F.col(col_name).alias('away_' + col_name) for col_name in df_ST_away.columns]) \
            .withColumnRenamed('away_Team', 'away_team') \
        .withColumnRenamed('away_Season', 'Season') \
        .withColumnRenamed('away_Rk', 'away_ST_Rk')

    df_schedule = df_schedule.join(df_ST_home,
                        ['Season', 'home_team'],
                        how='left')

    df_schedule = df_schedule.join(df_ST_away,
                        ['Season', 'away_team'],
                        how='left')

    df_schedule = df_schedule.withColumnRenamed('Week', 'week')

    return(df_schedule)
