# 📘 Facebook Page & Group Scraper

> Part of **[ScrapeClaw](https://www.scrapeclaw.cc/)** — a suite of production-ready, agentic social media scrapers for Instagram, YouTube, X/Twitter, and Facebook. Built with Python & Playwright. No API keys required.

[![ScrapeClaw](https://img.shields.io/badge/ScrapeClaw-Visit%20Site-blue?style=flat-square)](https://www.scrapeclaw.cc/)
[![ClawHub](https://img.shields.io/badge/ClawHub-View%20Skill-green?style=flat-square)](https://clawhub.ai/ArulmozhiV/facebook-scraper)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-PayPal-yellow?style=flat-square&logo=paypal)](https://www.paypal.com/paypalme/arulmozhivelu)

---

## What Is This?

A browser-based Facebook scraper that discovers and extracts structured data from **public Facebook pages** and **groups** — without any official API. It uses Playwright for full browser automation with built-in anti-detection, fingerprinting, and human behavior simulation to scrape at scale reliably.

Two-phase workflow:
1. **Discovery** — Find Facebook pages/groups by location and category via Google Custom Search
2. **Scraping** — Extract full profile data, stats, posts, and media using a real browser session

---

## Features

| Feature | Description |
|---------|-------------|
| 🔍 **Discovery** | Find pages/groups by city and category automatically |
| 🌐 **Browser Simulation** | Full Playwright browser — renders JavaScript, handles logins |
| 🛡️ **Anti-Detection** | Browser fingerprinting, stealth scripts, human behavior simulation |
| 📊 **Rich Data** | Page/group info, follower counts, contacts, posts, engagement stats |
| 🖼️ **Media Download** | Profile pics and cover photos saved locally |
| 💾 **Flexible Export** | JSON and CSV output formats |
| 🔄 **Resume Support** | Checkpoint-based resume for interrupted sessions |
| ⚡ **Smart Filtering** | Auto-skip private groups, low-like pages, deactivated profiles |
| 🔁 **Session Reuse** | Saves login state to skip re-login on subsequent runs |
| 🌍 **Residential Proxy** | Built-in proxy manager supporting 4 major providers |

---

## Installation

```bash
# Clone the repository
git clone https://github.com/Scrapeclaw/facebook-scraper.git
cd facebook-scraper

# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### Environment Setup

Create a `.env` file in the project root:

```env
# Facebook credentials (required)
FACEBOOK_EMAIL=your_email@example.com
FACEBOOK_PASSWORD=your_password

# Google Custom Search API (optional, for discovery)
GOOGLE_API_KEY=your_google_api_key
GOOGLE_SEARCH_ENGINE_ID=your_search_engine_id

# Residential proxy (optional — see Proxy section below)
PROXY_ENABLED=false
PROXY_PROVIDER=brightdata
PROXY_USERNAME=your_proxy_user
PROXY_PASSWORD=your_proxy_pass
PROXY_COUNTRY=us
PROXY_STICKY=true
```

---

## Usage

### Discover Pages/Groups

```bash
# Discover restaurant pages in Miami
python main.py discover --location "Miami" --category "restaurant" --type page

# Discover fitness groups in New York
python main.py discover --location "New York" --category "fitness" --type group

# Return JSON output (for agent integration)
python main.py discover --location "Miami" --category "restaurant" --output json
```

### Scrape

```bash
# Scrape a single page by name
python main.py scrape --page-name examplebusiness

# Scrape a single group
python main.py scrape --page-name miamirunners --type group

# Scrape from a discovery queue file
python main.py scrape data/queue/Miami_restaurant_page_20260220.json

# Run headless
python main.py scrape --page-name examplebusiness --headless
```

### Manage & Export

```bash
# List available queue files
python main.py list

# Export all scraped data to JSON + CSV
python main.py export --format both
```

---

## Output Data

Each scraped page/group is saved to `data/output/{page_name}.json`:

```json
{
  "page_name": "example_business",
  "display_name": "Example Business",
  "entity_type": "page",
  "category": "Restaurant",
  "about": "Family-owned Italian restaurant since 1985",
  "followers": 45000,
  "page_likes": 42000,
  "location": "Miami, FL",
  "phone": "+1-555-0123",
  "email": "info@example.com",
  "website": "https://example.com",
  "hours": "Mon-Sat 11AM-10PM",
  "is_verified": false,
  "page_tier": "mid",
  "profile_pic_local": "thumbnails/example_business/profile_abc123.jpg",
  "cover_photo_local": "thumbnails/example_business/cover_def456.jpg",
  "recent_posts": [
    {"post_url": "...", "reactions": 320, "comments": 45, "shares": 12}
  ],
  "scrape_timestamp": "2026-02-20T14:30:00"
}
```

### Page Tiers

| Tier | Likes / Members |
|------|----------------|
| nano | < 1,000 |
| micro | 1,000 – 10,000 |
| mid | 10,000 – 100,000 |
| macro | 100,000 – 1M |
| mega | > 1,000,000 |

---

## 🌐 Residential Proxy (Recommended for Scale)

Running long scraping sessions without a residential proxy will get your IP blocked. The built-in proxy manager handles rotation, sticky sessions, and country targeting automatically.

### Why Use a Residential Proxy?

- ✅ Avoid IP bans — residential IPs look like real users to Facebook
- ✅ Rotate IPs automatically on every request or session
- ✅ Sticky sessions — keep the same IP during a login session
- ✅ Geo-target by country for locale-accurate content
- ✅ 95%+ success rates vs ~30% with datacenter proxies

### Recommended Providers

We have affiliate partnerships with the following providers. Using these links supports this project at no extra cost to you:

| Provider | Highlights | Sign Up |
|----------|-----------|---------|
| **Bright Data** | World's largest network, 72M+ IPs, enterprise-grade | 👉 [**Get Bright Data**](https://get.brightdata.com/o1kpd2da8iv4) |
| **IProyal** | Pay-as-you-go, 195+ countries, no traffic expiry | 👉 [**Get IProyal**](https://iproyal.com/?r=ScrapeClaw) |
| **Storm Proxies** | Fast & reliable, developer-friendly API, competitive pricing | 👉 [**Get Storm Proxies**](https://stormproxies.com/clients/aff/go/scrapeclaw) |
| **NetNut** | ISP-grade network, 52M+ IPs, direct connectivity | 👉 [**Get NetNut**](https://netnut.io?ref=mwrlzwv) |

> These are affiliate links. We may earn a commission at no extra cost to you.

### Enabling the Proxy

**Option 1 — Environment variables (recommended):**

```bash
export PROXY_ENABLED=true
export PROXY_PROVIDER=brightdata        # brightdata | iproyal | stormproxies | netnut | custom
export PROXY_USERNAME=your_proxy_user
export PROXY_PASSWORD=your_proxy_pass
export PROXY_COUNTRY=us                 # optional
export PROXY_STICKY=true                # keeps same IP per session
```

**Option 2 — `config/scraper_config.json`:**

```json
{
  "proxy": {
    "enabled": true,
    "provider": "brightdata",
    "country": "us",
    "sticky": true,
    "sticky_ttl_minutes": 10
  }
}
```

Set credentials via env vars (`PROXY_USERNAME`, `PROXY_PASSWORD`) — never hardcode them in the config file.

### Provider Host/Port Reference

| Provider | Host | Port |
|----------|------|------|
| Bright Data | `brd.superproxy.io` | `22225` |
| IProyal | `proxy.iproyal.com` | `12321` |
| Storm Proxies | `rotating.stormproxies.com` | `9999` |
| NetNut | `gw-resi.netnut.io` | `5959` |

Once configured, the scraper uses the proxy automatically — no extra flags needed. The log confirms it:

```
INFO - Proxy enabled: <ProxyManager provider=brightdata enabled host=brd.superproxy.io:22225>
INFO - Browser using proxy: brightdata → brd.superproxy.io:22225
```

---

## Configuration Reference

Edit `config/scraper_config.json` to customise behaviour:

```json
{
  "proxy": {
    "enabled": false,
    "provider": "netnut",
    "country": "",
    "sticky": true,
    "sticky_ttl_minutes": 10
  },
  "google_search": {
    "enabled": true,
    "api_key": "",
    "search_engine_id": "",
    "queries_per_location": 3
  },
  "scraper": {
    "headless": false,
    "min_likes": 1000,
    "download_thumbnails": true,
    "max_thumbnails": 6,
    "delay_between_profiles": [5, 10],
    "timeout": 60000
  }
}
```

---

## Project Structure

```
facebook-scraper/
├── main.py               # CLI entry point
├── scraper.py            # Playwright browser scraper
├── discovery.py          # Google-based page/group discovery
├── anti_detection.py     # Fingerprinting & stealth
├── proxy_manager.py      # Residential proxy integration
├── config/
│   └── scraper_config.json
├── data/
│   ├── output/           # Scraped JSON files
│   ├── queue/            # Discovery queue files
│   └── sessions/         # Saved browser sessions
└── thumbnails/           # Downloaded profile & cover images
```

---

## Part of ScrapeClaw

This scraper is one of several tools in the **[ScrapeClaw](https://www.scrapeclaw.cc/)** collection:

| Scraper | Description | Links |
|---------|-------------|-------|
| 📘 **Facebook** | Pages, groups, posts & engagement data | [GitHub](https://github.com/Scrapeclaw/facebook-scraper) · [ClawHub](https://clawhub.ai/ArulmozhiV/facebook-scraper) |
| 📸 **Instagram** | Profiles, posts, media & follower counts | [GitHub](https://github.com/Scrapeclaw/instagram-scraper) · [ClawHub](https://clawhub.ai/ArulmozhiV/instagram-scraper) |
| 🎥 **YouTube** | Channels, subscribers & video metadata | [GitHub](https://github.com/Scrapeclaw/youtube-scrapper) · [ClawHub](https://clawhub.ai/ArulmozhiV/youtube-scrapper) |
| 🐦 **X / Twitter** | Tweets, profiles & engagement metrics | [GitHub](https://github.com/Scrapeclaw/twitter-scraper) · [ClawHub](https://clawhub.ai/ArulmozhiV/x-twitter-scraper) |

All scrapers share the same anti-detection foundation, proxy support, and JSON/CSV export pipeline.

---

## ☕ Support This Project

If this tool saves you time or helps your workflow, consider buying me a coffee — it keeps the project maintained and new scrapers coming!

[![Buy Me a Coffee via PayPal](https://img.shields.io/badge/☕%20Buy%20Me%20a%20Coffee-PayPal-blue?style=for-the-badge&logo=paypal)](https://www.paypal.com/paypalme/arulmozhivelu)

👉 **[paypal.me/arulmozhivelu](https://www.paypal.com/paypalme/arulmozhivelu)**

---

## Disclaimer

This tool is intended for scraping **publicly available** data only. Always comply with Facebook's Terms of Service and your local data privacy regulations. The author is not responsible for any misuse.

---

*Built by [ScrapeClaw](https://www.scrapeclaw.cc/) · [View all scrapers](https://www.scrapeclaw.cc/#scrapers)*
