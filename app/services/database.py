# tft/app/services/database.py

import os
import json
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('MYSQL_HOST')
DB_USER = os.getenv('MYSQL_USER')
DB_PASSWORD = os.getenv('MYSQL_PASSWORD')
DB_NAME = os.getenv('MYSQL_DATABASE')

def get_db_connection():
    """
    MySQL 데이터베이스 연결 객체를 반환합니다.
    """
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            # multi_statements=True  # <-- 이 라인을 다시 삭제합니다.
        )
        print("DEBUG: Successfully connected to MySQL database.")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def save_challenger_player(player_data: dict):
    """
    단일 챌린저 플레이어 데이터를 데이터베이스에 저장합니다.
    PUUID가 이미 존재하면 REPLACE INTO를 사용하여 업데이트합니다.
    """
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False

        cursor = conn.cursor()

        sql = """
        REPLACE INTO challenger_players 
            (puuid, summoner_name, league_points, wins, losses)
        VALUES 
            (%s, %s, %s, %s, %s);
        """
        
        values = (
            player_data.get('puuid'),
            #player_data.get('summonerId'),
            player_data.get('summonerName'),
            #player_data.get('summonerLevel'),
            player_data.get('leaguePoints'),
            player_data.get('wins'),
            player_data.get('losses')
        )
        
        cursor.execute(sql, values)
        conn.commit()
        print(f"DEBUG: Saved/Updated player '{player_data.get('summonerName')}' (PUUID: {player_data.get('puuid')[:10]}...).")
        return True

    except mysql.connector.Error as err:
        print(f"Error saving player data to MySQL: {err}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def fetch_all_challenger_players():
    """
    데이터베이스에서 모든 챌린저 플레이어 데이터를 가져옵니다.
    """
    conn = None
    players = []
    try:
        conn = get_db_connection()
        if conn is None:
            return []

        cursor = conn.cursor(dictionary=True) # dictionary=True로 설정하여 결과를 딕셔너리 형태로 받습니다.
        sql = "SELECT puuid, summoner_id, summoner_name, summoner_level, league_points, wins, losses, last_updated FROM challenger_players ORDER BY league_points DESC;"
        cursor.execute(sql)
        players = cursor.fetchall() # 모든 결과 가져오기
        print(f"DEBUG: Fetched {len(players)} players from database.")
        return players

    except mysql.connector.Error as err:
        print(f"Error fetching player data from MySQL: {err}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def save_match_details_to_db(match_details):
    """
    match_id 리스트를 challenger_match_ids 테이블에 저장합니다.
    이미 존재하는 match_id는 무시하고, 새로 추가합니다.
    """
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False

        cursor = conn.cursor()

        sql = """
        INSERT IGNORE INTO challenger_match_details 
        (match_id, game_datetime, game_length, game_version, queue_id, tft_set_number, tft_set_core_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            game_datetime=VALUES(game_datetime),
            game_length=VALUES(game_length),
            game_version=VALUES(game_version),
            queue_id=VALUES(queue_id),
            tft_set_number=VALUES(tft_set_number);
        """

        for match in match_details:
            cursor.execute(sql, (
                match['match_id'],
                match['game_datetime'],
                match['game_length'],
                match['game_version'],
                match['queue_id'],
                match['tft_set_number'],
                match['tft_set_core_name']
            ))
        conn.commit()
        print(f"DEBUG: Saved/Updated {len(match_details)} match details to DB.")
        return True

    except Exception as err:
        print(f"Error saving match ids to MySQL: {err}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def save_participant_details_to_db(participant_details):
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False

        cursor = conn.cursor()
        sql = """
            INSERT IGNORE INTO challenger_match_participants
            (match_id, puuid, placement, level, gold_left, last_round, players_eliminated, total_damage_to_players, traits, units)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                placement=VALUES(placement),
                level=VALUES(level),
                gold_left=VALUES(gold_left),
                last_round=VALUES(last_round),
                players_eliminated=VALUES(players_eliminated),
                total_damage_to_players=VALUES(total_damage_to_players),
                traits=VALUES(traits),
                units=VALUES(units)
        """
        for p in participant_details:
            cursor.execute(sql, (
                p['match_id'],
                p['puuid'],
                p['placement'],
                p['level'],
                p['gold_left'],
                p['last_round'],
                p['players_eliminated'],
                p['total_damage_to_players'],
                json.dumps(p['traits']),
                json.dumps(p['units'])
            ))
        conn.commit()
        print(f"DEBUG: Saved/Updated {len(participant_details)} participant details to DB.")
        return True
    
    except Exception as err:
        print(f"Error saving match ids to MySQL: {err}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def close_db_connection(conn):
    if conn and conn.is_connected():
        conn.close()