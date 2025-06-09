import os
import asyncio
import aiohttp
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
account_base_url = "https://asia.api.riotgames.com/riot/account/v1/accounts/by-puuid/"

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
        
# 챌린저 데이터 DataFrame 생성
async def process_challenger_data():
    async with aiohttp.ClientSession() as session:
        challenger  = await fetch_challenger_data(session)
        challenger_df = pd.DataFrame(challenger['entries'])
        challenger_df.to_csv('challenger_users.csv', index=False)
        return challenger_df

def a():
    print("a")