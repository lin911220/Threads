# crawl

import json
from typing import Dict
import jmespath
from parsel import Selector
from playwright.sync_api import sync_playwright
from nested_lookup import nested_lookup
import pymysql
import os

# 測試資料庫連接
def test_db_connection():
    try:
        conn = connect_to_db()  # 嘗試連接資料庫
        print("資料庫連接測試成功！")
    except Exception as e:
        print(f"資料庫連接測試失敗！錯誤: {e}")

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

# 2. 儲存資料到資料庫
def save_to_db(user_data: dict, threads_data: list):
    """Store the scraped profile and threads in the database."""
    conn = connect_to_db()
    cursor = conn.cursor()

    # 儲存使用者資料
    try:
        user_query = """
            INSERT INTO profiles (username, full_name, bio, followers, url)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            user_query, (
                user_data["username"], 
                user_data["full_name"], 
                user_data["bio"], 
                user_data["followers"], 
                user_data["url"]
            )
        )
    except pymysql.MySQLError as e:
        if e.args[0] == 1062:  # Duplicate entry error
            print(f"Warning: User {user_data['username']} already exists, skipping insert.")
        else:
            raise  # Raise other errors

    # 儲存每個貼文資料
    for thread in threads_data:
        try:
            post_query = """
                INSERT INTO posts (username, post_id, post_text, post_url, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """
            cursor.execute(
                post_query, (
                    thread["username"], 
                    thread["code"], 
                    thread["text"], 
                    thread["url"]
                )
            )
        except pymysql.MySQLError as e:
            if e.args[0] == 1062:  # Duplicate entry error
                print(f"Warning: Post with post_id {thread['code']} already exists, skipping insert.")
            else:
                raise  # Raise other errors

        # 儲存回覆資料
        for reply in thread.get("replies", []):
            try:
                reply_query = """
                    INSERT INTO replies (post_id, username, reply_id, reply_text, reply_url, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """
                cursor.execute(
                    reply_query, (
                        thread["code"],  # 這裡應該是回覆所對應的貼文 ID
                        thread["username"], 
                        reply["code"],  # 這是回覆的 ID
                        reply["text"], 
                        reply["url"]
                    )
                )
            except pymysql.MySQLError as e:
                if e.args[0] == 1062:  # Duplicate entry error
                    print(f"Warning: Reply with reply_id {reply['code']} already exists, skipping insert.")
                else:
                    raise  # Raise other errors

    # 提交資料並關閉連線
    conn.commit()
    cursor.close()
    conn.close()


# 3. 解析 Profile 資料
def parse_profile(data: Dict) -> Dict:
    result = jmespath.search(
        """{
        is_private: text_post_app_is_private,
        is_verified: is_verified,
        profile_pic: hd_profile_pic_versions[-1].url,
        username: username,
        full_name: full_name,
        bio: biography,
        bio_links: bio_links[].url,
        followers: follower_count
    }""",
        data,
    )
    result["url"] = f"https://www.threads.net/@{result['username']}"
    return result



# 4. 解析 Thread 資料
def parse_thread(data: Dict) -> Dict:
    result = jmespath.search(
        """{
        text: post.caption.text,
        published_on: post.taken_at,
        id: post.id,
        pk: post.pk,
        code: post.code,
        username: post.user.username,
        user_pic: post.user.profile_pic_url,
        user_verified: post.user.is_verified,
        user_pk: post.user.pk,
        user_id: post.user.id,
        has_audio: post.has_audio,
        reply_count: view_replies_cta_string,
        like_count: post.like_count,
        images: post.carousel_media[].image_versions2.candidates[1].url,
        image_count: post.carousel_media_count,
        videos: post.video_versions[].url
    }""",
        data,
    )
    result["videos"] = list(set(result["videos"] or []))
    if result["reply_count"] and type(result["reply_count"]) != int:
        result["reply_count"] = int(result["reply_count"].split(" ")[0])
    result["url"] = f"https://www.threads.net/@{result['username']}/post/{result['code']}"
    
    # 保留必要欄位
    keys_to_keep = ["text", "code", "username", "url"]
    filtered_result = {key: result[key] for key in keys_to_keep if key in result}
    return filtered_result


# 5. 爬取 Thread 資料
def scrape_thread(url: str, expected_code: str, context=None) -> dict:
    if context is None:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()
            result = _scrape_thread(page, url, expected_code)
            browser.close()
            return result
    else:
        page = context.new_page()
        return _scrape_thread(page, url, expected_code)


def _scrape_thread(page, url: str, expected_code: str) -> dict:
    print(f"Debug: Visiting URL: {url}")
    page.goto(url)
    page.wait_for_selector("[data-pressable-container=true]")
    selector = Selector(page.content())
    hidden_datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
    for hidden_dataset in hidden_datasets:
        if '"ScheduledServerJS"' not in hidden_dataset or "thread_items" not in hidden_dataset:
            continue
        data = json.loads(hidden_dataset)
        thread_items = nested_lookup("thread_items", data)
        if not thread_items:
            continue
        threads = [parse_thread(t) for thread in thread_items for t in thread]
        
        main_thread = None
        replies = []
        for thread in threads:
            if thread.get("code") == expected_code:
                main_thread = thread
            else:
                replies.append(thread)
        
        if main_thread:
            print(f"Debug: Main thread text: {main_thread['text']}")
            print(f"Debug: Replies count: {len(replies)}")
            return {
                "thread": main_thread,
                "replies": replies,
            }
        else:
            print(f"Debug: No thread found with code {expected_code}")
    raise ValueError("could not find thread data in page")

# 6. 爬取 Profile 資料
def scrape_profile(username: str) -> dict:
    parsed = {
        "user": {},
        "threads": [],
    }
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()
        profile_url = f"https://www.threads.net/@{username}"
        print(f"Debug: Visiting profile URL: {profile_url}")
        page.goto(profile_url)
        page.wait_for_selector("[data-pressable-container=true]")
        selector = Selector(page.content())
        hidden_datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
        for hidden_dataset in hidden_datasets:
            if '"ScheduledServerJS"' not in hidden_dataset:
                continue
            is_profile = 'follower_count' in hidden_dataset
            is_threads = 'thread_items' in hidden_dataset
            if not is_profile and not is_threads:
                continue
            data = json.loads(hidden_dataset)
            if is_profile:
                user_data = nested_lookup('user', data)
                parsed['user'] = parse_profile(user_data[0])
            if is_threads:
                thread_items = nested_lookup('thread_items', data)
                threads = [parse_thread(t) for thread in thread_items for t in thread]
                parsed['threads'].extend(threads)
        
        # 取得每篇貼文的回覆
        for thread in parsed['threads']:
            thread_code = thread["code"]
            thread_url = f"https://www.threads.net/t/{thread_code}/"
            print(f"Debug: Scraping thread URL: {thread_url}")
            try:
                thread_data = scrape_thread(thread_url, thread_code, context=context)
                thread["replies"] = thread_data["replies"]
            except Exception as e:
                print(f"Error scraping replies for {thread_code}: {e}")
                thread["replies"] = []
        
        browser.close()
    
    # 儲存資料到資料庫
    save_to_db(parsed["user"], parsed["threads"])
    return parsed


if __name__ == "__main__":
    print("開始測試資料庫連接...")
    test_db_connection()  # 測試資料庫連接
    print("根據 Threads 使用者名稱抓取所有貼文與回覆")
    username = input("your_username_here : ")
    user_data = scrape_profile(username)
    print("完成！")
