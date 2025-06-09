import os
import json
import pymysql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password=os.getenv('mysql_password'),
        db='TFT',
        charset='utf8'
    )

def save_challenger_users():
    conn = get_db_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute("select puuid from challenger_users")
    db_users_df = pd.DataFrame(cur.fetchall())

    challenger_user_df = pd.read_csv('challenger_users.csv')

    # 강등당한 챌린저 유저 데이터 삭제
    for index, row in db_users_df.iterrows():
        if row['puuid'] not in challenger_user_df['puuid'].values:
            query = "delete from challenger_users where puuid = %s"
            cur.execute(query, (row['puuid']))

    # 승급한 챌린저 유저 데이터 저장
    for index, row in challenger_user_df.iterrows():
        query = "insert ignore into challenger_users (summonerId, puuid, leaguePoints, `rank`, wins, losses) values (%s, %s, %s, %s, %s, %s) on duplicate key update leaguePoints = %s, `rank` = %s, wins = %s, losses = %s"
        cur.execute(query, (row['summonerId'],row['puuid'], row['leaguePoints'], row['rank'], row['wins'], row['losses'], row['leaguePoints'], row['rank'], row['wins'], row['losses']))

    conn.commit()
    conn.close()

def save_match_details():
    conn = get_db_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute("select match_id from challenger_match_details")
    db_match_ids_df = pd.DataFrame(cur.fetchall())

    match_ids_df = pd.read_csv('challenger_match_details.csv')

    for index, row in db_match_ids_df.iterrows():
        if row['match_id'] not in match_ids_df['match_id'].values:
            query = "delete from challenger_match_details where match_id = %s"
            cur.execute(query, (row['match_id']))

    for index, row in match_ids_df.iterrows():
        query = "insert ignore into challenger_match_details (match_id, game_datetime, game_length, game_version, queue_id, tft_set_number) values (%s, %s, %s, %s, %s, %s) on duplicate key update game_datetime = %s, game_length = %s, game_version = %s, queue_id = %s, tft_set_number = %s"
        cur.execute(query, (row['match_id'], row['game_datetime'], row['game_length'], row['game_version'], row['queue_id'], row['tft_set_number'], row['game_datetime'], row['game_length'], row['game_version'], row['queue_id'], row['tft_set_number']))
        
    conn.commit()
    conn.close()

def save_participants():
    conn = get_db_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute("select puuid from challenger_match_participants")
    db_match_participants_df = pd.DataFrame(cur.fetchall())

    match_participants_df = pd.read_csv('challenger_match_participants.csv')

    for index, row in db_match_participants_df.iterrows():
        if row['puuid'] not in match_participants_df['puuid'].values:
            query = "delete from challenger_match_participants where puuid = %s and match_id = %s"
            cur.execute(query, (row['puuid'], row['match_id']))

    for index, row in match_participants_df.iterrows():
        query = "insert ignore into challenger_match_participants (match_id, puuid, placement, level, gold_left, last_round, players_eliminated, total_damage_to_players) values (%s, %s, %s, %s, %s, %s, %s, %s)"
        cur.execute(query, (row['match_id'], row['puuid'], row['placement'], row['level'], row['gold_left'], row['last_round'], row['players_eliminated'], row['total_damage_to_players']))
    conn.commit()
    conn.close()

def save_traits():
    conn = get_db_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute("select match_id, puuid from challenger_match_participants")
    db_traits_df = pd.DataFrame(cur.fetchall())

    traits_df = pd.read_csv('challenger_match_participants.csv')

    for index, row in db_traits_df.iterrows():
        if row['match_id'] not in traits_df['match_id'].values:
            query = "delete from participant_traits where match_id = %s and puuid = %s"
            cur.execute(query, (row['match_id'], row['puuid']))

    for index, row in traits_df.iterrows():
        traits_list = eval(row['traits'])
        traits_json = json.dumps(traits_list)
        query = "insert ignore into participant_traits (match_id, puuid, traits) values (%s, %s, %s)"
        cur.execute(query, (row['match_id'], row['puuid'], traits_json))
    conn.commit()
    conn.close()

def save_units():
    conn = get_db_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute("select match_id, puuid from challenger_match_participants")
    db_units_df = pd.DataFrame(cur.fetchall())

    units_df = pd.read_csv('challenger_match_participants.csv')

    for index, row in db_units_df.iterrows():
        if row['match_id'] not in units_df['match_id'].values:
            query = "delete from participant_units where match_id = %s and puuid = %s"
            cur.execute(query, (row['match_id'], row['puuid']))

    for index, row in units_df.iterrows():
        units_list = eval(row['units'])
        units_json = json.dumps(units_list)
        query = "insert ignore into participant_units (match_id, puuid, units) values (%s, %s, %s)"
        cur.execute(query, (row['match_id'], row['puuid'], units_json))
    conn.commit()
    conn.close()

def save_to_database():
    save_challenger_users()
    save_match_details()
    save_participants()
    save_traits()
    save_units() 