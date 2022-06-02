from aiohttp import ClientSession
import asyncio
import json
from unidecode import unidecode
from bs4 import BeautifulSoup
from thefuzz import fuzz, process
import pandas as pd

class PlayerMatchesBySite:
    """Facilitates inter-site name matching to appropriately link naming discrepancies for players on a given team"""
    def __init__(self, players_by_site: list[pd.DataFrame], team_name: str, session: ClientSession) -> None:
        self.players_by_site = players_by_site
        self.team_name = team_name
        self.session = session
        self.site_names = ['transfermrkt', 'sofascore', 'fbref', 'understat', 'whoscored', 'soccerment', 'capology']
        self.fm_names = self.players_by_site[-1] # fotmob designated as primary name in matching functions
        self.sites_to_match = {site_name: players for site_name, players in zip(self.site_names, self.players_by_site[:-1])}
        self.match_rows = [self.match_row_template(fm_name) for fm_name in self.fm_names]
        self.full_matches = []

    def match_row_template(self, fm_name: str) -> dict:
        """Creates dict to store match results and account for sites that still don't have a match"""
        match_row = {site_name: None for site_name in self.site_names}
        match_row['fotmob'] = fm_name
        return match_row

    def sites_remaining(self, match_row: dict) -> dict:
        """After a given match stage, determines the sites for a given player that still don't have a match"""
        return {key: self.sites_to_match[key] for key, val in match_row.items() if val is None}

    def store_full_matches(self) -> None:
        """After a given match stage, stores match rows where a match was found for each site"""
        for match_row in self.match_rows:
            if len(self.sites_remaining(match_row).keys()) == 0:
                self.full_matches.append(match_row)

    def remaining_match_rows(self) -> None:
        """After a given match stage, determines the players that still have sites remaining"""
        self.match_rows = list(filter(lambda match_row: match_row not in self.full_matches, self.match_rows))

    def same_name(self, match_row: dict) -> dict:
        """Stores matches that have the same name"""
        name_to_match = match_row['fotmob']
        for site_name, players in self.sites_to_match.items():
            if name_to_match in players:
                self.sites_to_match[site_name] = players[players != name_to_match]
                match_row[site_name] = name_to_match
        return match_row

    def fuzzy_match(self, match_row: dict) -> dict:
        """Stores matches that have the highest fuzzy match ratio"""
        name_to_match = match_row['fotmob']
        for site_name, players in self.sites_remaining(match_row).items():
            match = process.extractOne(name_to_match, players, score_cutoff=80)
            if match:
                self.sites_to_match[site_name] = players[players != match[0]]
                match_row[site_name] = match[0]
        return match_row

    def common_lastname(self, match_row: dict) -> dict:
        """Stores matches where there is only one player that has same lastname"""
        last_name = match_row['fotmob'].split()[-1]
        for site_name, players in self.sites_remaining(match_row).items():
            common_lastname = list(filter(lambda x: x.split()[-1] == last_name, players))
            if len(common_lastname) == 1:
                self.sites_to_match[site_name] = players[players != common_lastname[0]]
                match_row[site_name] = common_lastname[0]
        return match_row

    async def wiki_search(self, match_row: dict) -> str:
        """Makes GET request to wiki API to extract key for the player page"""
        name_team = match_row['fotmob'] + ' ' + self.team_name
        search_url = f'https://en.wikipedia.org/w/rest.php/v1/search/page?q={name_team}&limit=1'
        r_search = await self.session.get(search_url)
        r_search.raise_for_status()
        search_json = json.loads(await r_search.read())
        if search_json['pages']:
            return search_json['pages'][0]['key']

    async def wiki_names(self, match_row: dict) -> list:
        """Using player page key, makes GET request to wiki API to extract alternative names"""
        page_key = await self.wiki_search(match_row)
        if page_key:
            parse_url = f'https://en.wikipedia.org/api/rest_v1/page/summary/{page_key}'
            r_parse = await self.session.get(parse_url)
            r_parse.raise_for_status()
            parse_json = json.loads(await r_parse.read())
            parse_html = parse_json['extract_html']
            soup = BeautifulSoup(parse_html, 'lxml')
            alt_names = [bold_name.text for bold_name in soup.find_all('b')]
            alt_names_processed = [unidecode(alt_name.lower()) for alt_name in alt_names]
            return [alt_name for alt_name in alt_names_processed if fuzz.partial_ratio(alt_name, match_row['fotmob']) != 100]

    async def wiki_name_match(self, match_row: dict) -> dict:
        """Stores matches that have the highest fuzzy match ratio w/ wiki alternative names"""
        alt_names = await self.wiki_names(match_row)
        if alt_names:
            for site_name, players in self.sites_remaining(match_row).items():
                for alt_name in alt_names:
                    match = process.extractOne(alt_name, players, score_cutoff=80)
                    if match:
                        self.sites_to_match[site_name] = players[players != match[0]]
                        match_row[site_name] = match[0]
        return match_row

    def add_team_name(self, match_row: dict) -> dict:
        """Adds team name to match row"""
        match_row['team'] = self.team_name
        return match_row

    def match_stage_sync(self, func) -> None:
        """Runs a synchronous match stage and the match stage helpers"""
        current_match_rows = self.match_rows
        self.match_rows = [func(match_row) for match_row in current_match_rows]
        self.store_full_matches()
        self.remaining_match_rows()

    def apply_match_stages_sync(self, match_funcs) -> None:
        """Applies synchronous match stages to match rows"""
        for match_function in match_funcs:
            if self.match_rows:
                self.match_stage_sync(match_function)

    async def match_stage_async(self, func) -> None:
        """Runs an asynchronous match stages and the match stage helpers"""
        current_match_rows = self.match_rows
        match_tasks = [func(match_row) for match_row in current_match_rows]
        self.match_rows = await asyncio.gather(*match_tasks)
        self.store_full_matches()
        self.remaining_match_rows()

    async def apply_match_stages_async(self, match_funcs) -> None:
        """Applies asynchronous match stages to match rows"""
        for match_function in match_funcs:
            if self.match_rows:
                await self.match_stage_async(match_function)

    async def main(self) -> pd.DataFrame:
        """Designates sync and async match stages and runs them to produce a Match DataFrame"""
        sync_match_funcs = [
            self.same_name, 
            self.fuzzy_match, 
            self.common_lastname,
        ]
        async_match_funcs = [
            self.wiki_name_match
        ]
        self.apply_match_stages_sync(sync_match_funcs)
        await self.apply_match_stages_async(async_match_funcs)
        rows_with_team = [self.add_team_name(match_row) for match_row in self.full_matches]
        return pd.DataFrame(rows_with_team)