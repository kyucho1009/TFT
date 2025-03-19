import numpy as np
import pandas as pd
import requests
import time
import json 
from pandas import json_normalize

api_key = 'RGAPI-14e0eba5-cb2e-4f37-a5e9-9aac0aa79b7c'

request_header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com",
    "X-Riot-Token": api_key
}

base_url = "https://kr.api.riotgames.com/tft/"

challenger_url = base_url + 'league/v1/challenger'
challenger = requests.get(challenger_url, headers=request_header).json()

# url 응답상태 확인 코드
# print(challenger.status_code)

challenger_df = json_normalize(challenger['entries'])
print(challenger_df)