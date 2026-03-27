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

## 🚀 ScrapeClaw Customised Solutions

> **We build, you own.** No per-credit fees. Stop renting data — own your entire scraping infrastructure.

ScrapeClaw offers two commercial offerings for teams and businesses that need more than open-source:

### 📦 Tailored Datasets

Get pre-scraped or on-demand datasets built around your exact industry, platform, or niche — delivered ready for analysis.

- Industry-specific social media datasets on demand
- Custom extraction logic ("Skills") for your use case
- One-time delivery or recurring data feeds
- Output in CSV, JSON, or direct database delivery

👉 [**Request a Dataset →**](https://www.scrapeclaw.cc/)

### 🏗️ Private Infrastructure Setup ★ High Value

We deploy a turnkey ScrapeClaw system on your own servers — you own 100% of the infrastructure and the data.

- 🔒 **Privacy & Compliance** — data never leaves your network, ideal for FinTech & Health
- 🤖 **Self-Healing Agents** — AI-powered scrapers that adapt when sites change
- 💸 **Slash API Costs** — stop paying $1–5 per 1K requests; scrape 1M rows at flat infra cost
- Includes **1 month of managed maintenance & support**

👉 [**Book a Strategy Call →**](https://www.scrapeclaw.cc/)

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
