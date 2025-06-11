from flask import Flask, render_template, request
from services import get_db_connection, search_by_name, fetch_match_ids, fetch_match_detail
import pymysql.cursors
import asyncio
import aiohttp

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/test')
def test():
    return render_template('test.html')

@app.route('/ranking')
def ranking():
    conn = get_db_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    # 챌린저 유저들의 랭킹 정보를 가져옵니다
    query = """
    SELECT puuid, leaguePoints, `rank`, wins, losses, 
           (wins + losses) as total_games,
           ROUND((wins / (wins + losses)) * 100, 1) as win_rate
    FROM challenger_users 
    ORDER BY leaguePoints DESC, `rank` ASC
    """
    cur.execute(query)
    users = cur.fetchall()
    
    conn.close()
    return render_template('ranking.html', users=users)

@app.route('/search')
def search():
    summoner = request.args.get('q', '')
    if not summoner:
        return render_template('index.html', error="소환사명을 입력해주세요.")
    
    if '#' not in summoner:
        return render_template('index.html', error="소환사명과 태그를 모두 입력해주세요. (예: Hide on bush#KR1)")
    
    game_name, tag_line = summoner.split('#', 1)
    
    async def get_summoner_and_matches():
        async with aiohttp.ClientSession() as session:
            try:
                # 소환사 정보 가져오기
                summoner_info = await search_by_name(session, game_name, tag_line)
                print(f"API 응답: {summoner_info}")  # API 응답 출력
                if not summoner_info:
                    return None, None
                
                puuid = summoner_info.get('puuid')
                if not puuid:
                    return None, None
                
                # 최근 5개 경기 정보 가져오기
                match_ids = await fetch_match_ids(session, puuid, count=5)
                if not match_ids:
                    return summoner_info, []
                
                # 각 경기의 상세 정보 가져오기
                match_details = []
                for match_id in match_ids:
                    match_detail = await fetch_match_detail(session, match_id)
                    if match_detail:
                        # 해당 소환사의 경기 정보만 필터링
                        for participant in match_detail['info']['participants']:
                            if participant['puuid'] == puuid:
                                match_details.append({
                                    'match_id': match_id,
                                    'placement': participant['placement'],
                                    'level': participant['level'],
                                    'gold_left': participant['gold_left'],
                                    'last_round': participant['last_round'],
                                    'traits': participant['traits'],
                                    'units': participant['units']
                                })
                                break
                
                return summoner_info, match_details
                
            except Exception as e:
                print(f"Error: {str(e)}")
                return None, None
    
    summoner_info, match_details = asyncio.run(get_summoner_and_matches())
    if not summoner_info:
        return render_template('index.html', error="소환사를 찾을 수 없습니다.")
    
    return render_template('index.html', 
                         query=summoner, 
                         summoner_info=summoner_info,
                         game_name=game_name,
                         tag_line=tag_line,
                         match_details=match_details)


if __name__ == '__main__':
    app.run(debug=True)