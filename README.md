# FreeFootyDataConsolidation

```python
import asyncio
from playwright.async_api import async_playwright
from aiohttp import ClientSession
from siteScrapers import extract_main
from dfTransforms import transform_main
from db import load_main

async def main() -> None:
    """Runs pipeline webscraping extraction, site categorical linkage transforms, and loads into an instance of PostgreSQL"""
    async with async_playwright() as p:
        async with ClientSession() as session:
            browser = await p.chromium.launch(headless=False)
            all_clubs_df, all_players_df = await extract_main(browser, session)
            df_pipeline = await transform_main(all_players_df, all_clubs_df, session)
            load_main(df_pipeline.player_match_df, df_pipeline.club_match_df)

if __name__ == "__main__":
    asyncio.run(main())
```

## 1. Introduction and Project Motivation
As analysis of soccer/football data has become more and more popular, a variety of websites have emerged as prime resources for free, high quality data. However,
each website has its own strengths and weaknesses, and thus individually they are not as comprehensive as they could be collectively. To consolidate them together
requires extracting every team and player ID from each website as each provider has its own respective way of uniquely identifying data objects. Given the
international nature of football this presented the following challenges to maintaining data accuracy/uniformity:
- *Multiple players have the same exact name*
- *Some websites include accented characters while others simply provide the ASCII equivalents*
- *Variation in what name players are listed under, especially for players who are predominantly identified by a nickname*

## 2. Pipeline
To address the challenges mentioned above, the following batch ETL pipeline was implemented to effectively consolidate this data at scale.
### 2.1 Web Scraping w/ aiohttp and async Playwright to extract comprehensive player and team pandas dataframes
- [**Site classes to define how data is uniquely structured on each**](https://github.com/emarrow40/FreeFootyDataConsolidation/blob/main/sites.py)
- [**Web scraping classes to define how each site should be scraped based on header and cookie requirements**](https://github.com/emarrow40/FreeFootyDataConsolidation/blob/main/siteScrapers.py)
### 2.2 Processing dataframes with pandas to map player names and teams accross sites
- [**Splitting comprehensive dataframes by site to map names**](https://github.com/emarrow40/FreeFootyDataConsolidation/blob/main/dfTransforms.py)
- [**Utilized fuzzy matching and wikipedia API to link naming discrepancies**](https://github.com/emarrow40/FreeFootyDataConsolidation/blob/main/nameMatches.py)
### 2.3 Loading linked players and teams into a local instance of PostgreSQL
- [**Through psycopg2, created player and team tables for each site linked relationally with foreign keys**](https://github.com/emarrow40/FreeFootyDataConsolidation/blob/main/db.py)

## 3. Future Steps
I hope to use the stored IDs to implement an API that can be used to drive future analyses and ML models in an efficient, accurate, and comprehensive manner. Also,
I will be keeping my eye out for new websites that offer new data that is not available through the ones that I have included so far.
