import json
import pandas as pd
from itertools import chain
import asyncio
from aiohttp import ClientSession
from playwright.async_api import Browser, Page
from sites import Fbref, FotMob, Soccerment, Under, Who, Sofa, Tm, Cap
from siteHeaders import set_headers
from idObjects import ClubData, PlayerData
from typing import Union

class PlaywrightOnly:
    """Facilitates scraping of sites where content is dynamically loaded
    
    Applicable to:
    understat
    fbref
    """
    def __init__(self, site: Union[Under, Fbref], browser: Browser) -> None:
        self.site = site
        self.browser = browser
        self.context = None

    async def get_club_table(self, league, page: Page) -> list[ClubData]:
        league_url = self.site.get_league_url(league)
        await page.goto(league_url)
        await page.locator(self.site.club_table).wait_for()
        return self.site.process_club_table(await page.inner_html(self.site.club_table))
        
    async def get_player_table(self, club: ClubData, page: Page) -> list[PlayerData]:
        await page.goto(club.url)
        await page.locator(self.site.player_table).wait_for()
        return self.site.process_player_table(await page.inner_html(self.site.player_table), club.name)

    def idObjects_to_df(self, idObjects: Union[list[ClubData], list[PlayerData]]) -> pd.DataFrame:
        return pd.DataFrame(map(lambda x: x.__dict__, idObjects)) 

    async def main(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.context = await self.browser.new_context()
        page = await self.context.new_page()
        clubs = [await self.get_club_table(league, page) for league in self.site.leagues.values()]
        all_clubs = list(chain(*clubs))
        club_df = self.idObjects_to_df(all_clubs)
        players = [await self.get_player_table(club, page) for club in all_clubs]
        await self.context.close()
        all_players = list(chain(*players))
        player_df = self.idObjects_to_df(all_players)
        return club_df, player_df

class PlaywrightOnlyCap:
    """Facilitates scraping of capology where content is dynamically loaded
    
    Applicable to just capology as its content loads much slower than the other website
    yet the way the data is structured has a workaround where all teams and players within a league
    are available within a single page
    """
    def __init__(self, site: Cap, browser: Browser) -> None:
        self.site = site
        self.browser = browser
        self.context = None

    async def get_club_table(self, league, page: Page) -> list[ClubData]:
        league_url = self.site.get_league_url(league)
        await page.goto(league_url)
        await page.locator(self.site.club_table).wait_for()
        return self.site.process_club_table(await page.inner_html(self.site.club_table))

    async def get_player_table(self, page: Page) -> list[PlayerData]:
        await page.locator(self.site.player_table).first.wait_for()
        await page.locator('a.dropdown-item:has-text("All")').first.click()
        await page.locator('div.float-right.pagination').first.wait_for(state='hidden')
        return self.site.process_player_table(await page.inner_html(self.site.player_table))

    async def get_league_data(self, league, page: Page) -> tuple[list[ClubData], list[PlayerData]]:
        league_clubs = await self.get_club_table(league, page)
        league_players = await self.get_player_table(page)
        return league_clubs, league_players

    def idObjects_to_df(self, idObjects: Union[list[ClubData], list[PlayerData]]) -> pd.DataFrame:
        return pd.DataFrame(map(lambda x: x.__dict__, idObjects))

    async def main(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.context = await self.browser.new_context()
        page = await self.context.new_page()
        leagues = [await self.get_league_data(league, page) for league in self.site.leagues.values()]
        await self.context.close()
        clubs = map(lambda x: x[0], leagues)
        all_clubs = list(chain(*clubs))
        club_df = self.idObjects_to_df(all_clubs)
        players = map(lambda x: x[1], leagues)
        all_players = list(chain(*players))
        player_df = self.idObjects_to_df(all_players)
        return club_df, player_df

class AiohttpOnlyJson:
    """Facilitates scraping of sites where content can be extracted through
    only http requests that return JSON data

    Applicable to:
    sofascore
    transfermrkt
    """
    def __init__(self, site: Union[Sofa, Tm], session: ClientSession) -> None:
        self.site = site
        self.session = session

    async def get_club_json(self, league) -> list[ClubData]:
        league_url = self.site.get_league_url(league)
        print(league_url)
        async with self.session.get(league_url, headers=self.site.headers) as r:
            r.raise_for_status()
            club_json = json.loads(await r.read())
            return self.site.process_club_json(club_json)

    async def get_player_json(self, club: ClubData) -> list[PlayerData]:
        club_api_url = self.site.club_api_url(club)
        print(club_api_url)
        async with self.session.get(club_api_url, headers=self.site.headers) as r:
            r.raise_for_status()
            player_json = json.loads(await r.read())
            return self.site.process_player_json(player_json, club.name)

    def idObjects_to_df(self, idObjects: Union[list[ClubData], list[PlayerData]]) -> pd.DataFrame:
        return pd.DataFrame(map(lambda x: x.__dict__, idObjects))

    async def main(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        clubs = [await self.get_club_json(league) for league in self.site.leagues.values()]
        all_clubs = list(chain(*clubs))
        club_df = self.idObjects_to_df(all_clubs)
        players = [await self.get_player_json(club) for club in all_clubs]
        all_players = list(chain(*players))
        player_df = self.idObjects_to_df(all_players)
        return club_df, player_df

class AiohttpOnlyHtml:
    """Facilitates scraping of sites where content can be extracted through
    only http requests that return HTML

    Applicable to:
    soccerment
    """
    def __init__(self, site: Soccerment, session: ClientSession) -> None:
        self.site = site
        self.session = session

    async def get_club_html(self, league) -> list[ClubData]:
        league_url = self.site.get_league_url(league)
        async with self.session.get(league_url, headers=self.site.headers) as r:
            r.raise_for_status()
            club_html = await r.text()
            return self.site.process_club_html(club_html)

    async def get_player_html(self, club: ClubData) -> list[PlayerData]:
        async with self.session.get(club.url, headers=self.site.headers) as r:
            r.raise_for_status()
            player_html = await r.text()
            return self.site.process_player_html(player_html)

    def idObjects_to_df(self, idObjects: Union[list[ClubData], list[PlayerData]]) -> pd.DataFrame:
        return pd.DataFrame(map(lambda x: x.__dict__, idObjects))

    async def main(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        clubs = [await self.get_club_html(league) for league in self.site.leagues.values()]
        all_clubs = list(chain(*clubs))
        club_df = self.idObjects_to_df(all_clubs)
        players = [await self.get_player_html(club) for club in all_clubs]
        all_players = list(chain(*players))
        player_df = self.idObjects_to_df(all_players)
        return club_df, player_df

class PlaywrightAiohttp:
    """Faciliates scraping of site where http requests can be used to extract
    data after the site cookies are obtained through browser automation

    Applicable to:
    whoscored
    fotmob
    """
    def __init__(self, site: Union[Who, FotMob], session: ClientSession, browser: Browser):
        self.site = site
        self.session = session
        self.browser = browser
        self.context = None

    async def get_cookies(self) -> str:
        cookies = await self.context.cookies()
        r_cookies = filter(lambda x: self.site.cookie_keyword in x['name'] or x['name'] in self.site.cookie_filter, cookies)
        cookie_header = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in r_cookies]).strip()
        return cookie_header

    async def get_headers(self, club: ClubData) -> dict:
        cookie_header = await self.get_cookies()
        return set_headers(
            cookie = cookie_header,
            referer = club.url,
        )

    async def get_club_table(self, league, page: Page) -> list[ClubData]:
        league_url = self.site.get_league_url(league)
        await page.goto(league_url)
        await page.locator(self.site.club_table).wait_for()
        return self.site.process_club_table(await page.inner_html(self.site.club_table), league)

    async def get_player_json(self, club: ClubData) -> list[PlayerData]:
        club_api_url = self.site.club_api_url(club)
        headers = await self.get_headers(club)
        async with self.session.get(club_api_url, headers=headers) as r:
            r.raise_for_status()
            player_json = json.loads(await r.read())
            return self.site.process_player_json(player_json, club)

    def idObjects_to_df(self, idObjects: Union[list[ClubData], list[PlayerData]]) -> pd.DataFrame:
        return pd.DataFrame(map(lambda x: x.__dict__, idObjects))

    async def main(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.context = await self.browser.new_context()
        page = await self.context.new_page()
        clubs = [await self.get_club_table(league, page) for league in self.site.leagues.values()]
        all_clubs = list(chain(*clubs))
        club_df = self.idObjects_to_df(all_clubs)
        players = [await self.get_player_json(club) for club in all_clubs]
        await self.context.close()
        all_players = list(chain(*players))
        player_df = self.idObjects_to_df(all_players)
        return club_df, player_df

async def extract_main(browser: Browser, session: ClientSession) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extracts player and team data from each site and stores them in comprehensive dataframes"""
    site_tasks = [
        AiohttpOnlyJson(Sofa(), session).main(),
        AiohttpOnlyJson(Tm(), session).main(),
        AiohttpOnlyHtml(Soccerment(), session).main(),
        PlaywrightOnly(Under(), browser).main(),
        PlaywrightOnly(Fbref(), browser).main(),
        PlaywrightOnlyCap(Cap(), browser).main(),
        PlaywrightAiohttp(Who(), session, browser).main(),
        PlaywrightAiohttp(FotMob(), session, browser).main(),
    ]
    df_tuples = await asyncio.gather(*site_tasks)
    all_clubs_df = pd.concat((df_tuple[0] for df_tuple in df_tuples))
    all_players_df = pd.concat((df_tuple[1] for df_tuple in df_tuples))
    return all_clubs_df, all_players_df

        
        

        
        
            
        
