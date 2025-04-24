# app/models/detector.py

import pymysql
import torch
from transformers import BertTokenizer, BertForSequenceClassification
import os
from dotenv import load_dotenv
import boto3

# 載入 .env
load_dotenv()

def download_model_from_s3(bucket_name, model_key, local_file_path):
    # 初始化 S3 客戶端
    s3_client = boto3.client('s3')
    try:
        # 下載模型檔案
        s3_client.download_file(bucket_name, model_key, local_file_path)
        print(f"Model downloaded successfully: {local_file_path}")
    except Exception as e:
        print(f"Error downloading model from S3: {e}")

# 初始化模型與 tokenizer（只載入一次）
model = None
tokenizer = None

def load_model():
    """
    載入 BERT 模型與 tokenizer
    """
    global model, tokenizer

    model_path = os.path.join(os.path.dirname(__file__), './Model')
    if not os.path.exists(model_path):
        # 假設你的 S3 桶名稱是 'misogyny-models'，模型檔案的 key 是 'Chinese_misogyny_detection_model_best'
        download_model_from_s3('misogyny-models', 'Chinese_misogyny_detection_model_0321latest', model_path)
    model = BertForSequenceClassification.from_pretrained(model_path)
    tokenizer = BertTokenizer.from_pretrained(model_path)
    print("✅ BERT 模型載入完成！")



def connect_to_db():
    """
    建立資料庫連線
    """
    try:
        conn = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("✅ 資料庫連接成功！")
        return conn
    except pymysql.MySQLError as e:
        print(f"資料庫連接錯誤: {e}")
        return None

def close_db_connection(conn):
    """
    關閉資料庫連線
    """
    if conn and conn.open:
        conn.close()
        print("🔌 資料庫連接已關閉。")

def predict_label(text):
    """
    使用模型預測文本的標籤與置信度
    """
    if text is None:
        return None, None

    cleaned_text = text.lower().strip()
    inputs = tokenizer(cleaned_text, return_tensors="pt", padding=True, truncation=True, max_length=128)

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
        label = torch.argmax(probs, dim=1).item()
        confidence = probs[0][label].item()

    return label, confidence

def predict_and_update(text, post_id, table_name):
    """
    對一筆資料進行預測並更新資料庫
    """
    label, confidence = predict_label(text)
    if label is None:
        print(f"⚠ 跳過 ID {post_id}，因為文本為空。")
        return

    print(f"\n📌 ID: {post_id}")
    print(f"文字: {text}")
    print(f"預測結果: {'厭女（1）' if label == 1 else '非厭女（0）'}，置信度: {confidence:.4f}")

    conn = connect_to_db()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE {table_name}
                    SET is_misogyny = %s, confidence = %s
                    WHERE id = %s
                """, (label, confidence, post_id))
                conn.commit()
        finally:
            close_db_connection(conn)


# 執行全部的 function
def process_posts():
    """
    自動處理資料庫中尚未預測的 posts 和 replies
    """
    conn = connect_to_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            # posts
            cursor.execute("SELECT id, post_text FROM posts WHERE is_misogyny IS NULL")
            posts = cursor.fetchall()
            for post in posts:
                predict_and_update(post['post_text'], post['id'], 'posts')

            # replies
            cursor.execute("SELECT id, reply_text FROM replies WHERE is_misogyny IS NULL")
            replies = cursor.fetchall()
            for reply in replies:
                predict_and_update(reply['reply_text'], reply['id'], 'replies')

        print("✅ 所有資料已預測並更新完畢！")
    finally:
        close_db_connection(conn)


def get_post_stats_and_misogynistic_texts(username):
    # 連接到MySQL數據庫
    connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    try:
        with connection.cursor() as cursor:
            # 獲取該用戶的總帖子數和厌女帖子的數量
            cursor.execute("""
                SELECT 
                    SUM(total) AS total_posts,
                    SUM(misogynistic) AS misogynistic_posts
                FROM (
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN is_misogyny = TRUE THEN 1 ELSE 0 END) AS misogynistic
                    FROM posts WHERE username = %s
                    UNION ALL
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN is_misogyny = TRUE THEN 1 ELSE 0 END) AS misogynistic
                    FROM replies WHERE username = %s
                ) AS combined;
            """, (username, username))
            stats = cursor.fetchone()

            # 獲取所有符合厌女標準的帖子文本和回覆文本
            cursor.execute("""
                SELECT post_text AS text FROM posts 
                WHERE username = %s AND is_misogyny = TRUE
                UNION ALL
                SELECT reply_text AS text FROM replies
                WHERE username = %s AND is_misogyny = TRUE;
            """, (username, username))
            posts = cursor.fetchall()

            # 返回統計數據和符合條件的帖子文本
            return stats, posts

    finally:
        # 關閉數據庫連接
        connection.close()
