import os
import time
import asyncio
import aiohttp
import datetime
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('api_key')

request_header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com",
    "X-Riot-Token": api_key
}

base_url = "https://kr.api.riotgames.com/tft/"
match_base_url = "https://asia.api.riotgames.com/tft/match/v1/"
account_base_url = "https://asia.api.riotgames.com/riot/account/v1/accounts/"

# 챌린저 유저 데이터 가져오는 함수
async def fetch_challenger_data(session: aiohttp.ClientSession):
    url = base_url + 'league/v1/challenger'
    while True:
        async with session.get(url, headers=request_header) as response:
            if response.status == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', 1))
                await asyncio.sleep(retry_after)
                continue
            return await response.json()

# 매치 아이디 가져오는 함수
async def fetch_match_ids(session: aiohttp.ClientSession, puuid: str, count: int = 10):
    url = f"{match_base_url}matches/by-puuid/{puuid}/ids?count={count}"
    while True:
        async with session.get(url, headers=request_header) as response:
            if response.status == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                await asyncio.sleep(retry_after)
                continue
            if response.status == 200:
                return await response.json()
            
# 매치 상세 데이터 가져오는 함수
async def fetch_match_detail(session: aiohttp.ClientSession, match_id: str):
    url = f"{match_base_url}matches/{match_id}"
    while True:
        async with session.get(url, headers=request_header) as response:
            if response.status == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                await asyncio.sleep(retry_after)
                continue
            return await response.json()
        
# 유저 이름 가져오는 함수
async def fetch_user_name(session: aiohttp.ClientSession, puuid: str):
    url = f"{account_base_url}by-puuid/{puuid}"
    while True:
        async with session.get(url, headers=request_header) as response:
            if response.status == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                await asyncio.sleep(retry_after)
                continue
            return await response.json()

# 이름으로 유저 검색
async def search_by_name(session: aiohttp.ClientSession, gameName: str, tagLine:str):
    url = f"{account_base_url}by-riot-id/{gameName}/{tagLine}"
    while True:
        async with session.get(url, headers=request_header) as response:
            if response.status == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                await asyncio.sleep(retry_after)
                continue
            return await response.json()
        
# 챌린저 데이터 DataFrame 생성
async def process_challenger_data():
    async with aiohttp.ClientSession() as session:
        challenger  = await fetch_challenger_data(session)
        challenger_df = pd.DataFrame(challenger['entries'])
        challenger_df.to_csv('src/challenger_users.csv', index=False)
        return challenger_df

async def process_match_ids():
    async with aiohttp.ClientSession() as session:
        # 챌린저 데이터 가져오기
        challenger_df = await process_challenger_data()
        
        # 모든 매치 ID를 저장할 리스트
        all_match_ids = []
        
        # 병렬 처리를 위한 태스크 생성
        tasks = []
        for puuid in challenger_df['puuid'][:100]:
            tasks.append(fetch_match_ids(session, puuid))
        
        # 모든 태스크 동시 실행
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 처리
        for result in results:
            if isinstance(result, Exception):
                print(f"Error fetching matches: {str(result)}")
                continue
            all_match_ids.extend(result)
        
        # 중복 제거
        unique_match_ids = list(set(all_match_ids))
        
        # DataFrame 생성 및 저장
        match_ids_df = pd.DataFrame(unique_match_ids, columns=['match_id'])
        match_ids_df.to_csv('challenger_match_ids.csv', index=False)
        
        print(f"수집된 고유 매치 ID 개수: {len(unique_match_ids)}")
        return match_ids_df

async def process_match_details():
    async with aiohttp.ClientSession() as session:
        # 저장된 매치 ID 파일 읽기
        match_ids_df = pd.read_csv('src/challenger_match_ids.csv')
        match_ids = match_ids_df['match_id'].tolist()
        
        # 매치 상세 정보를 저장할 리스트
        match_details = []
        
        # 참가자 정보를 저장할 리스트
        participant_details = []

        # 병렬 처리를 위한 태스크 생성
        tasks = []
        for match_id in match_ids:
            tasks.append(fetch_match_detail(session, match_id))
        
        # 모든 태스크 동시 실행 (최대 50개씩)
        chunk_size = 50  # 청크 크기를 50으로 증가
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            results = await asyncio.gather(*chunk, return_exceptions=True)
            
            for match_id, result in zip(match_ids[i:i + chunk_size], results):
                if isinstance(result, Exception):
                    print(f"Error fetching match details for match_id {match_id}: {str(result)}")
                    continue
                
                try:
                    match_detail = result
                    # 필요한 정보 추출
                    match_info = {
                        'match_id': match_id,
                        'game_datetime': datetime.datetime.fromtimestamp(match_detail['info']['game_datetime'] / 1000),
                        'game_length': match_detail['info']['game_length'],
                        'game_version': match_detail['info']['game_version'],
                        'queue_id': match_detail['info']['queue_id'],
                        'tft_set_number': match_detail['info']['tft_set_number'],
                    }
                    match_details.append(match_info)
                    
                    # 참가자 정보 추출
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
                    
                except Exception as e:
                    print(f"Error processing match details for match_id {match_id}: {str(e)}")
                    continue
            
            # API 요청 제한을 위한 대기 (0.5초로 단축)
            await asyncio.sleep(0.5)
            
            # 진행 상황 출력
            print(f"진행률: {min(i + chunk_size, len(match_ids))}/{len(match_ids)}")
        
        # DataFrame 생성 및 저장
        match_details_df = pd.DataFrame(match_details)
        match_details_df.to_csv('src/challenger_match_details.csv', index=False)
        
        # 참가자 정보 DataFrame 생성 및 저장
        participant_details_df = pd.DataFrame(participant_details)
        participant_details_df.to_csv('src/challenger_match_participants.csv', index=False)

        print(f"수집된 매치 상세 정보 개수: {len(match_details)}")
        print(f"수집된 매치 참가자 정보 개수: {len(participant_details)}")
        return match_details_df

async def main():
    start_time = time.time()
    
    # 챌린저 데이터 수집
    challenger_df = await process_challenger_data()
    print(f"챌린저 데이터 수집 완료: {time.time() - start_time:.2f}초")
    
    # 매치 ID 수집
    match_ids_df = await process_match_ids()
    print(f"매치 ID 수집 완료: {time.time() - start_time:.2f}초")
    
    # 매치 상세 정보 수집
    match_details_df = await process_match_details()
    print(f"매치 상세 정보 수집 완료: {time.time() - start_time:.2f}초")
    
    print(f"전체 실행 시간: {time.time() - start_time:.2f}초")