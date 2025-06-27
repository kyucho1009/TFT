# tft/app/data_collector.py

import os
import asyncio
import aiohttp
import datetime
import sys # sys 모듈 임포트
from dotenv import load_dotenv

from .services.database import save_challenger_player, save_match_details_to_db, save_participant_details_to_db
from .services.riot_api import fetch_challenger_data, fetch_account_info_by_puuid, fetch_summoner_details_by_puuid, fetch_match_ids, fetch_match_detail

load_dotenv()

"""
Riot API에서 챌린저 리그 데이터를 가져와
각 플레이어의 상세 정보를 수집하고 데이터베이스에 저장하는 비동기 함수입니다.
진행률을 표시합니다.
"""
async def collect_challenger_summoner_names():
    async with aiohttp.ClientSession() as session:
        print("1. Fetching Challenger League data...")
        challenger_league = await fetch_challenger_data(session)

        if not challenger_league or not challenger_league.get('entries'):
            print("Failed to fetch challenger league data or no entries found.")
            return []

        print(f"   Successfully fetched {len(challenger_league['entries'])} challenger entries.")
        entries_to_process = challenger_league['entries'] 
        total_entries = len(entries_to_process) # 전체 엔트리 수
        
        print(f"2. Fetching summoner and account details for {total_entries} entries...")
        
        account_tasks = []
        for entry in entries_to_process:
            puuid = entry.get('puuid')
            if puuid:
                account_tasks.append(fetch_account_info_by_puuid(session, puuid))
        
        chunk_size = 50
        account_results = []
        for i in range(0, len(account_tasks), chunk_size):
            chunk = account_tasks[i:i+chunk_size]
            chunk_results = await asyncio.gather(*chunk, return_exceptions=True)
            account_results.extend(chunk_results)
            print(f"   [Account] 진행률: {min(i+chunk_size, total_entries)}/{total_entries}")
            # API 요청 제한을 위한 대기 (0.5초)
            await asyncio.sleep(0.5)

        challenger_summoners = []        
        print(f"3. Saving to DB...")
        for i, entry in enumerate(entries_to_process):
            account_info = account_results[i]
            if isinstance(account_info, Exception) or account_info is None:
                pass
            else:
                player_data = {
                    "puuid": entry.get('puuid'),
                    "summonerName": account_info.get('gameName', '') + '#' + account_info.get('tagLine', ''),
                    "leaguePoints": entry.get('leaguePoints'),
                    "wins": entry.get('wins'),
                    "losses": entry.get('losses'),
                }
                challenger_summoners.append(player_data)
                save_challenger_player(player_data)
            if (i+1) % 10 == 0 or (i+1) == total_entries:
                print(f"   [DB] 진행률: {i+1}/{total_entries}")

    print(f"3. Collected {len(challenger_summoners)} summoner names with details.")
    return challenger_summoners

async def collect_challenger_match_id(challenger_summoners: list):
    async with aiohttp.ClientSession() as session:
        print("\n1. Fetching Challenger Summoner Match ID...")
        match_id_tasks = []
        for summoner in challenger_summoners:
            puuid = summoner.get('puuid')
            if puuid:
                match_id_tasks.append(fetch_match_ids(session, puuid))

        chunk_size = 50
        match_id_results = []
        for i in range(0, len(match_id_tasks), chunk_size):
            chunk = match_id_tasks[i:i+chunk_size]
            chunk_results = await asyncio.gather(*chunk, return_exceptions=True)
            match_id_results.extend(chunk_results)
            print(f"   [MatchID] 진행률: {min(i+chunk_size, len(match_id_tasks))}/{len(match_id_tasks)}")
            await asyncio.sleep(0.5)  # API 제한을 고려한 대기
        
        match_ids = []
        for match_id in match_id_results:
            if isinstance(match_id, Exception):
                print(f"Error fetching matches: {str(match_id)}")
                continue
            if isinstance(match_id, list):
                match_ids.extend(match_id)
            else:
                print(f"Unexpected result type: {type(match_id)}")
                
        unique_match_ids = list(set(match_ids))
        print(f"Collected {len(unique_match_ids)} match ids.")
        
        '''
        print(f"2. Saving to DB...")
        save_match_ids_to_db(unique_match_ids)
        '''
        return unique_match_ids

async def collect_challenger_match_details(match_ids: list):
    async with aiohttp.ClientSession() as session:
        print("\n1. Fetching Challenger Match Details...")

        chunk_size = 50
        detail_tasks = []
        for match_id in match_ids:
            detail_tasks.append(fetch_match_detail(session, match_id))

        match_details = []
        participant_details = []
        for i in range(0,len(detail_tasks),chunk_size):
            chunk = detail_tasks[i:i + chunk_size]
            chunk_results = await asyncio.gather(*chunk,return_exceptions=True)
            print(f"   [MatchDetail] 진행률: {min(i+chunk_size, len(detail_tasks))}/{len(detail_tasks)}")
            await asyncio.sleep(0.5)  # API 제한을 고려한 대기

            for match_id, result in zip(match_ids[i:i + chunk_size], chunk_results):
                if isinstance(result, Exception):
                    print(f"Error fetching match details for match_id {match_id}: {str(result)}")
                    continue
                else:
                    match_detail = result
                    match_info = {
                        'match_id': match_id,
                        'game_datetime': datetime.datetime.fromtimestamp(match_detail['info']['game_datetime'] / 1000),
                        'game_length': datetime.timedelta(seconds = match_detail['info']['game_length']),
                        'game_version': match_detail['info']['game_version'],
                        'queue_id': match_detail['info']['queue_id'],
                        'tft_set_number': match_detail['info']['tft_set_number'],
                        'tft_set_core_name': match_detail['info']['tft_set_core_name']
                    }
                    match_details.append(match_info)

                for participant in match_detail['info']['participants']:
                    participant_info = {
                            'match_id': match_id,
                            'puuid': participant['puuid'],
                            'placement': participant['placement'],
                            'level': participant['level'],
                            'gold_left': participant['gold_left'],
                            'last_round': participant['last_round'],
                            'players_eliminated': participant['players_eliminated'],
                            'total_damage_to_players': participant['total_damage_to_players'],
                            'traits': participant['traits'],
                            'units': participant['units']
                        }
                    participant_details.append(participant_info)            
        
        print(f"3. Saving to DB...")

        save_match_details_to_db(match_details)
        #print("match_details가 DB에 저장되었습니다.")
        save_participant_details_to_db(participant_details)

        return match_details, participant_details

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    collected_data = asyncio.run(collect_challenger_summoner_names())
    
    collected_unique_match_ids = asyncio.run(collect_challenger_match_id(collected_data))
    
    collected_challenger_match_details = asyncio.run(collect_challenger_match_details(collected_unique_match_ids))
    # print("\n--- Collected Challenger Summoner Data ---")
    # # 모든 데이터를 출력하는 것은 너무 길 수 있으므로, 상위 10명만 예시로 출력
    # for i, summoner in enumerate(collected_data[:10]):
    #     name_to_display = summoner.get('summonerName', '이름 없음')
    #     lp_to_display = summoner.get('leaguePoints', 'N/A')
    #     puuid_to_display = summoner.get('puuid', 'N/A')
    #     print(f"  {i+1}. Name: {name_to_display}, LP: {lp_to_display}, PUUID: {puuid_to_display[:10]}...")
    # if len(collected_data) > 10:
    #     print(f"  ... and {len(collected_data) - 10} more players.")