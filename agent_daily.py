import os
import requests
import pandas as pd
from datetime import datetime, timezone

# Tokens de entorno
TIKTOK_TOKEN = os.getenv("TIKTOK_TOKEN")
FB_TOKEN = os.getenv("FB_TOKEN")  # Para Instagram/Facebook Graph API
ZAPIER_WEBHOOK = os.getenv("ZAPIER_WEBHOOK")

# Listado de cuentas
TIKTOK_USERS = ["usuario1"]
IG_USER_IDS = ["IG_USER_ID"]
FB_PAGE_IDS = ["FB_PAGE_ID"]

def fetch_tiktok_videos(username):
    url = "https://open-api.tiktok.com/video/list/"
    headers = {"Authorization": f"Bearer {TIKTOK_TOKEN}", "Content-Type": "application/json"}
    payload = {"username": username, "count": 20}

    try:
        res = requests.post(url, json=payload, headers=headers, timeout=30)
        res.raise_for_status()
        return res.json()
    except Exception:
        return {"videos": []}

def fetch_instagram_media(user_id):
    base = f"https://graph.facebook.com/v17.0/{user_id}/media"
    params = {
        "access_token": FB_TOKEN,
        "fields": "id,caption,media_type,timestamp"
    }
    try:
        r = requests.get(base, params=params, timeout=30)
        r.raise_for_status()
        data = r.json().get("data", [])
    except Exception:
        return []

    results = []
    for item in data:
        media_id = item["id"]
        ins_url = f"https://graph.facebook.com/v17.0/{media_id}/insights"
        ins_params = {
            "metric": "impressions,reach,engagement,video_views",
            "access_token": FB_TOKEN
        }

        try:
            ins = requests.get(ins_url, params=ins_params, timeout=30)
            ins.raise_for_status()
            insights = ins.json()
        except:
            insights = {"data": []}

        results.append({"media": item, "insights": insights})

    return results

def fetch_facebook_videos(page_id):
    url = f"https://graph.facebook.com/v17.0/{page_id}/videos"
    params = {
        "access_token": FB_TOKEN,
        "fields": "id,title,description,length,created_time,insights.metric(total_video_impressions,total_video_views)"
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json().get("data", [])
    except:
        return []

def analyze_and_rank(collected):
    rows = []
    for acc in collected:
        posts = acc.get("posts", [])
        total_views = sum([p.get("views", 0) for p in posts])
        n = len(posts)
        avg_views = total_views / n if n > 0 else 0

        rows.append({
            "account": acc["account"],
            "total_views": total_views,
            "avg_views": avg_views,
            "n_posts": n
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df, rows
    df = df.sort_values("total_views", ascending=False)
    return df, rows

def generate_prompts(acc_summary):
    keyword = "producto"
    top_hashtag = "#oferta"

    prompt1 = (
        f"Genera 5 captions estilo '{acc_summary['account']}' utilizando la keyword "
        f"'{keyword}' y el hashtag '{top_hashtag}'. Deben ser cortos, con CTA y en tono comercial."
    )

    prompt2 = (
        f"Genera 6 ideas de video de 15 segundos con hook inicial persuasivo, "
        f"similares a los videos más virales de la cuenta '{acc_summary['account']}'."
    )

    return [prompt1, prompt2]

def send_to_zapier(payload):
    try:
        r = requests.post(ZAPIER_WEBHOOK, json=payload, timeout=15)
        r.raise_for_status()
        return True
    except:
        return False

def main():
    collected = []

    # TikTok
    for user in TIKTOK_USERS:
        data = fetch_tiktok_videos(user)
        posts = [{"views": v.get("play_count", 0), "caption": v.get("description", "")}
                 for v in data.get("videos", [])]
        collected.append({"account": f"tiktok:{user}", "posts": posts})

    # Instagram
    for uid in IG_USER_IDS:
        media = fetch_instagram_media(uid)
        posts = []
        for item in media:
            insights = item["insights"].get("data", [])
            views = 0
            for metric in insights:
                if metric.get("name") in ("video_views", "impressions"):
                    try:
                        views = int(metric["values"][0]["value"])
                    except:
                        pass
            posts.append({"views": views, "caption": item["media"].get("caption", "")})
        collected.append({"account": f"instagram:{uid}", "posts": posts})

    # Facebook
    for pid in FB_PAGE_IDS:
        videos = fetch_facebook_videos(pid)
        posts = []
        for v in videos:
            insights = v.get("insights", {}).get("data", [])
            views = 0
            for metric in insights:
                if metric.get("name") in ("total_video_views", "total_video_impressions"):
                    try:
                        views = int(metric["values"][0]["value"])
                    except:
                        pass
            posts.append({"views": views, "caption": v.get("description", "")})
        collected.append({"account": f"facebook:{pid}", "posts": posts})

    df, rows = analyze_and_rank(collected)

    if df.empty:
        print("No se encontraron datos para analizar.")
        return

    top_acc = df.iloc[0].to_dict()
    prompts = generate_prompts(top_acc)

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ranking": rows,
        "top_account": top_acc,
        "prompts": prompts
    }

    send_to_zapier(payload)

if __name__ == "__main__":
    main()
