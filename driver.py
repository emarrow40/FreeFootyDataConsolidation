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

            



