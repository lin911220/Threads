from flask import Flask, request, jsonify,  abort
from playwright.sync_api import sync_playwright
from nested_lookup import nested_lookup
from parsel import Selector
from typing import Dict
import jmespath
import pymysql
import json
from database.db import connect_to_db

app = Flask(__name__)

# ===== 資料處理與資料庫儲存 =====

def save_to_db(user_data: dict, posts_data: list, replies_data: list):
    conn = connect_to_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO profiles (username, full_name, bio, followers, url)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_data["username"], user_data["full_name"], user_data["bio"], user_data["followers"], user_data["url"]))
    except pymysql.MySQLError as e:
        if e.args[0] != 1062:
            raise

    for post in posts_data:
        if not post.get("text"):
            continue
        try:
            cursor.execute("""
                INSERT INTO posts (username, post_id, post_text, post_url, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (post["username"], post["code"], post["text"], post["url"]))
        except pymysql.MySQLError as e:
            if e.args[0] != 1062:
                raise

    for reply in replies_data:
        if not reply.get("text"):
            continue
        try:
            cursor.execute("""
                INSERT INTO replies (post_id, username, reply_id, reply_text, reply_url, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (reply.get("post_id", reply["code"]), reply["username"], reply["code"], reply["text"], reply["url"]))
        except pymysql.MySQLError as e:
            if e.args[0] != 1062:
                raise

    conn.commit()
    cursor.close()
    conn.close()

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
    }""", data)
    result["url"] = f"https://www.threads.net/@{result['username']}"
    return result

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
    }""", data)
    result["videos"] = list(set(result["videos"] or []))
    if result["reply_count"] and type(result["reply_count"]) != int:
        result["reply_count"] = int(result["reply_count"].split(" ")[0])
    result["url"] = f"https://www.threads.net/@{result['username']}/post/{result['code']}"
    keys_to_keep = ["text", "code", "username", "url"]
    return {k: result[k] for k in keys_to_keep if k in result}

# ===== Flask API 入口點 =====

@app.route("/scrape", methods=["POST"])
def scrape_profile_api():
    data = request.get_json()
    if not data or "username" not in data:
        return jsonify({"status": "error", "detail": "請提供 username"}), 400

    username = data["username"]
    parsed = {"user": {}, "threads": [], "replies": []}

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()

            # Threads 首頁
            profile_url = f"https://www.threads.net/@{username}"
            print(f"🔍 Visiting: {profile_url}")
            try:
                page.goto(profile_url)
                page.wait_for_selector("[data-pressable-container=true]", timeout=8000)
            except Exception as e:
                print("⚠️ 首頁錯誤:", e)
                return jsonify({"status": "no_posts"})

            selector = Selector(page.content())
            hidden_datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
            for hidden_dataset in hidden_datasets:
                if 'ScheduledServerJS' not in hidden_dataset:
                    continue
                data_json = json.loads(hidden_dataset)
                if 'follower_count' in hidden_dataset:
                    user_data = nested_lookup('user', data_json)
                    if user_data:
                        parsed['user'] = parse_profile(user_data[0])
                if 'thread_items' in hidden_dataset:
                    thread_items = nested_lookup('thread_items', data_json)
                    threads = [parse_thread(t) for thread in thread_items for t in thread]
                    parsed['threads'].extend(threads)

            if not parsed['threads']:
                return jsonify({"status": "no_posts"})

            # Threads 回覆
            replies_url = f"https://www.threads.net/@{username}/replies"
            print(f"🔍 Visiting: {replies_url}")
            try:
                page.goto(replies_url)
                page.wait_for_selector("[data-pressable-container=true]", timeout=8000)
                selector = Selector(page.content())
                hidden_datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
                for hidden_dataset in hidden_datasets:
                    if 'ScheduledServerJS' not in hidden_dataset or 'thread_items' not in hidden_dataset:
                        continue
                    data_json = json.loads(hidden_dataset)
                    thread_items = nested_lookup('thread_items', data_json)
                    if thread_items:
                        replies = [parse_thread(t) for thread in thread_items for t in thread]
                        own_replies = [r for r in replies if r["username"] == username]
                        for r in own_replies:
                            r['post_id'] = r['code']
                        parsed['replies'].extend(own_replies)
            except Exception as e:
                print("⚠️ 回覆錯誤:", e)

            browser.close()

        save_to_db(parsed["user"], parsed["threads"], parsed["replies"])
        return jsonify({"status": "done", "user": parsed["user"], "thread_count": len(parsed["threads"]), "reply_count": len(parsed["replies"])})

    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
