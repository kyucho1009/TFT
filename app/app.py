import os
import asyncio
from flask import Flask, render_template
from services.database import get_db_connection, fetch_all_challenger_players

app = Flask(__name__)

@app.route('/')
async def main():
    return render_template('test.html')

@app.route('/test_rank')
async def index():
    """
    메인 페이지를 렌더링하고 챌린저 플레이어 목록을 표시합니다.
    """    
    # 모든 챌린저 플레이어 데이터를 가져옵니다.
    players = await fetch_all_challenger_players_async()
    
    # index.html 템플릿에 데이터를 전달하여 렌더링합니다.
    return render_template('test_rank.html', players=players)

async def fetch_all_challenger_players_async():
    return fetch_all_challenger_players()


if __name__ == '__main__':
    # Windows에서 asyncio 오류 방지
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    app.run(debug=True) # 개발 모드 실행