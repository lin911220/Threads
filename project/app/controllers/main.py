from flask import Blueprint, render_template, request
from app.threads.crawler import scrape_profile
from app.models.detector import load_model, connect_to_db, close_db_connection, process_posts , get_post_stats_and_misogynistic_texts
from dotenv import load_dotenv
load_dotenv()

main_bp = Blueprint('main', __name__)

# 啟動時載入 BERT 模型與資料庫連線
load_model()

@main_bp.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        username = request.form.get('username')

        if not username:
            return render_template('index.html', error="請輸入帳號")

        conn = connect_to_db()
        # 第一步：爬蟲爬資料進資料庫
        scrape_profile(username)

        # 第二步：執行模型預測
        process_posts()

        # 第三步：統計並取得厭女文內容（貼文 + 留言）
        stats, misogynistic_texts = get_post_stats_and_misogynistic_texts(username)

        return render_template(
            'index.html',
            username=username,
            stats=stats,
            posts=misogynistic_texts  # 每個 post 是 dict: {'text': ...}
        )

    return render_template('index.html')
