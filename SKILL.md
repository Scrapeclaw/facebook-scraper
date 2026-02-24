````skill
# Facebook Page & Group Scraper

A browser-based Facebook page and group discovery and scraping tool.

```yaml
---
name: facebook-scraper
description: Discover and scrape Facebook pages and public groups from your browser.
emoji: 📘
version: 1.0.0
author: influenza
tags:
  - facebook
  - scraping
  - social-media
  - page-discovery
  - group-discovery
  - business-pages
metadata:
  clawdbot:
    requires:
      bins:
        - python3
        - chromium

    config:
      stateDirs:
        - data/output
        - data/queue
        - thumbnails
      outputFormats:
        - json
        - csv
---
```

## Overview

This skill provides a two-phase Facebook scraping system:

1. **Page/Group Discovery**  
2. **Browser Scraping** 

## Features

- 🔍  - Discover Facebook pages and groups by location and category
- 🌐  - Full browser simulation for accurate scraping
- 🛡️  - Browser fingerprinting, human behavior simulation, and stealth scripts
- 📊  - Page/group info, stats, images, and engagement data
- 💾  - JSON/CSV export with downloaded thumbnails
- 🔄  - Resume interrupted scraping sessions
- ⚡  - Auto-skip private groups, low-like pages, empty profiles
- 📂  - Supports pages, groups, and public profiles via --type flag

#### Getting Google API Credentials (Optional)

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable "Custom Search API"
4. Create API credentials → API Key
5. Go to [Programmable Search Engine](https://programmablesearchengine.google.com/)
6. Create a search engine with `facebook.com` as the site to search
7. Copy the Search Engine ID

## Usage

### Agent Tool Interface

For OpenClaw agent integration, the skill provides JSON output:

```bash
# Discover Facebook pages (returns JSON)
discover --location "Miami" --category "restaurant" --type page --output json

# Discover Facebook groups (returns JSON)
discover --location "New York" --category "fitness" --type group --output json

# Scrape single page (returns JSON)
scrape --page-name examplebusiness --output json

# Scrape single group (returns JSON)
scrape --page-name examplegroup --type group --output json
```

## Output Data

### Page/Group Data Structure

```json
{
  "page_name": "example_business",
  "display_name": "Example Business",
  "entity_type": "page",
  "category": "Restaurant",
  "subcategory": "Italian Restaurant",
  "about": "Family-owned Italian restaurant since 1985",
  "followers": 45000,
  "page_likes": 42000,
  "location": "Miami, FL",
  "address": "123 Main St, Miami, FL 33101",
  "phone": "+1-555-0123",
  "email": "info@example.com",
  "website": "https://example.com",
  "hours": "Mon-Sat 11AM-10PM",
  "is_verified": false,
  "page_tier": "mid",
  "profile_pic_local": "thumbnails/example_business/profile_abc123.jpg",
  "cover_photo_local": "thumbnails/example_business/cover_def456.jpg",
  "recent_posts": [
    {"post_url": "https://facebook.com/example_business/posts/123", "reactions": 320, "comments": 45, "shares": 12}
  ],
  "scrape_timestamp": "2026-02-20T14:30:00"
}
```

### Group Data Structure

```json
{
  "page_name": "example_group",
  "display_name": "Miami Fitness Community",
  "entity_type": "group",
  "about": "A community for fitness enthusiasts in Miami",
  "members": 15000,
  "privacy": "Public",
  "posts_per_day": 25,
  "location": "Miami",
  "page_tier": "mid",
  "profile_pic_local": "thumbnails/example_group/profile_abc123.jpg",
  "cover_photo_local": "thumbnails/example_group/cover_def456.jpg",
  "scrape_timestamp": "2026-02-20T14:30:00"
}
```

### Page Tiers

| Tier  | Likes/Members Range |
|-------|---------------------|
| nano  | < 1,000             |
| micro | 1,000 - 10,000      |
| mid   | 10,000 - 100,000    |
| macro | 100,000 - 1M        |
| mega  | > 1,000,000         |

### File Outputs

- **Queue files**: `data/queue/{location}_{category}_{type}_{timestamp}.json`
- **Scraped data**: `data/output/{page_name}.json`
- **Thumbnails**: `thumbnails/{page_name}/profile_*.jpg`, `thumbnails/{page_name}/cover_*.jpg`
- **Export files**: `data/export_{timestamp}.json`, `data/export_{timestamp}.csv`

## Configuration

Edit `config/scraper_config.json`:

```json
{
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
    "max_thumbnails": 6
  },
  "cities": ["New York", "Los Angeles", "Miami", "Chicago"],
  "categories": ["restaurant", "retail", "fitness", "real-estate", "healthcare", "beauty"]
}
```

## Filters Applied

The scraper automatically filters out:

- ❌ Private groups
- ❌ Pages with < 1,000 likes (configurable)
- ❌ Deactivated or removed pages
- ❌ Non-existent pages/groups
- ❌ Already scraped entries (deduplication)

## Troubleshooting

### Login Issues

- Ensure credentials are correct
- Handle verification codes when prompted
- Wait if rate limited (the script will auto-retry)

### No Pages Discovered

- Check Google API key and quota
- Verify Search Engine ID is configured for facebook.com
- Try different location/category combinations

### Rate Limiting

- Reduce scraping speed (increase delays)
- Use multiple Facebook accounts
- Run during off-peak hours

````
