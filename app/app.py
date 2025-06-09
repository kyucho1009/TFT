import pymysql.cursors
from flask import Flask, render_template
import pandas as pd
import pymysql
import os
from dotenv import load_dotenv
from tests import test

# .env 파일 로드
load_dotenv()

app = Flask(__name__)

@app.route('/') 
def home():
   return render_template('index.html')

@app.route('/rank')
def rank():
    # 데이터베이스 연결
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password=os.getenv('mysql_password'),
        db='TFT',
        charset='utf8',
        cursorclass=pymysql.cursors.DictCursor
    )
    cur =conn.cursor()
    query = "SELECT * FROM challenger_users ORDER BY leaguePoints DESC"
    cur.execute(query)
    users = cur.fetchall()
    conn.close()
    return render_template('rank.html', users=users)

if __name__ == '__main__' :   
  app.run(debug=True)