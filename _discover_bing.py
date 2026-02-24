import requests
import re
import time
import json
from datetime import datetime
from pathlib import Path

queries = [
    "site:facebook.com Kanchipuram food restaurant",
    "site:facebook.com Kancheepuram food",
    "site:facebook.com Kanchipuram biryani",
    "site:facebook.com Kanchipuram tiffin",
    "site:facebook.com Kanchipuram hotel food",
]

pages = set()
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

skip = {
    "sharer", "share", "login", "dialog", "pages", "groups", "profile",
    "pg", "permalink", "photo", "video", "events", "tr", "plugins", "hashtag",
    "watch", "marketplace", "gaming", "help", "policies", "about", "home",
    "notifications", "messages", "friends", "search", "ads", "business",
}

for q in queries:
    try:
        time.sleep(3)
        r = requests.get("https://www.bing.com/search", params={"q": q}, headers=headers, timeout=15)
        print(f"Bing {r.status_code} -> {q[:60]}")
        if r.status_code == 200:
            hits = re.findall(r'facebook\.com/([a-zA-Z0-9_.]{3,60})(?:["/\?]|$)', r.text)
            for h in hits:
                if h not in skip and not h.startswith("p/"):
                    pages.add(h)
    except Exception as e:
        print(f"Error: {e}")

print(f"\nDiscovered {len(pages)} candidate pages:")
for p in sorted(pages):
    print(f"  {p}")

# Write queue file
if pages:
    queue = {
        "location": "Kanchipuram",
        "category": "food",
        "entity_type": "page",
        "total": len(pages),
        "page_names": sorted(pages),
        "completed": [],
        "failed": {},
        "current_index": 0,
        "created_at": datetime.now().isoformat(),
        "source": "bing_search"
    }
    out = Path("data/queue") / f"Kanchipuram_food_page_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(queue, f, indent=2)
    print(f"\nQueue saved: {out}")
else:
    print("\nNo pages found via Bing, will use manual seed list")
