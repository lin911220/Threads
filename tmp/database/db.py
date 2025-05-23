# 資料庫處理 : 連接，測試，關閉 

import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

# connect to database
def connect_to_db():
    try:
        conn = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print('DB connection successful.')
        return conn
    except Exception as e:
        print(f"DB connection error {e}")
        return None

# test if the database connection is successful
def test_db_connection(conn=None):
    if conn:
        print('DB connection successful.')
    else:
        print('DB is not connected.')
        conn = connect_to_db()
        if conn:
            print('test successful.')
        else:
            print('Failed to connect to the database.')

# close database connection
def close_db_connection(conn=None):
    if conn:
        try:
            conn.close()
            print('DB close successful.')
        except Exception as e:
            print(f'DB close error {e}')
