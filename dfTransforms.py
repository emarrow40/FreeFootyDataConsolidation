from unidecode import unidecode
import re
import asyncio
import pandas as pd
import numpy as np
from nameMatches import PlayerMatchesBySite, ClubMatchesBySite

class PipeBase:
    """Pipeline base class, stores class variables accessible to child classes to modify"""
    team_match_map = {}
    player_match_map = {}
    sites = [
        'transfermarkt',
        'sofascore',
        'fbref',
        'understat',
        'whoscored',
        'soccerment',
        'capology',
        'fotmob',
    ]

"""
Each child of PipeBase represents a different stage of the pipeline.
Aside from the initial loading of the DataFrames, each child has a run function to execute the stage.
The pipeline stages either create or modify class variables within a PipeBase instance.
"""

class LoadDataFrames(PipeBase):
    """Loads player and club dataframes generated by the site scrapers
    
    Creates:
    player_df
    club_df
    """
    def load_dfs(self, player_df: pd.DataFrame, club_df: pd.DataFrame) -> None:
        PipeBase.player_df = player_df.drop_duplicates()
        PipeBase.club_df = club_df.drop_duplicates()
    
class FormatNames(PipeBase):
    """Removes accents from player name characters
    
    Modifies:
    player_df
    """
    def player_name(self, name) -> str:
        return re.sub(r'[^a-z\s]', ' ', unidecode(str(name).lower()))

    def run(self) -> None:
        PipeBase.player_df['processedName'] = PipeBase.player_df.name.apply(self.player_name)

class SplitBySite(PipeBase):
    """Splits main player and team dataframes by site

    Creates:
    site_dfs -- player_df split by site
    site_dfs_clubs -- club_df split by site
    """
    def split_df(self, df: pd.DataFrame) -> list[pd.DataFrame]:
        return [df[df['site'] == site] for site in PipeBase.sites]

    def filter_duplicate_names(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df.name.duplicated(keep=False) == False]

    def run(self) -> None:
        PipeBase.site_dfs = self.split_df(PipeBase.player_df)
        PipeBase.site_dfs_clubs = self.split_df(PipeBase.club_df)

class ClubNameMatches(PipeBase):
    """Finds cross-site matches for clubs with naming discrepancies and stores team name links accross sites

    Creates:
    club_match_df

    Modifies:
    team_match_map
    site_dfs
    site_dfs_clubs
    """
    def get_club_match_df(self) -> pd.DataFrame:
        club_names_by_site = [df['name'].values for df in PipeBase.site_dfs_clubs]
        return ClubMatchesBySite(club_names_by_site).main()

    def match_cols(self) -> list:
        return PipeBase.club_match_df.columns

    def get_row_matches(self, row) -> None:
        cols = self.match_cols()
        for col in cols:
            PipeBase.team_match_map[row[col]] = row['sofascore']

    def map_team_matches(self, team) -> str:
        return PipeBase.team_match_map[team]

    def apply_map_to_sitedfs(self) -> None:
        sites = PipeBase.site_dfs
        for site in sites:
            site['team'] = site.team.apply(self.map_team_matches)
        PipeBase.site_dfs = sites

    def apply_map_to_clubdf(self) -> None:
        sites = PipeBase.site_dfs_clubs
        for site in sites:
            site['namematch_index'] = site.name.apply(self.map_team_matches)
        PipeBase.site_dfs_clubs = sites

    def run(self) -> None:
        PipeBase.club_match_df = self.get_club_match_df()
        df_c = PipeBase.club_match_df.copy()
        df_c.apply(self.get_row_matches, axis=1)
        self.apply_map_to_sitedfs()
        self.apply_map_to_clubdf()

class ClubSiteJoin(PipeBase):
    """Joins site_dfs_clubs by namematch_index based on team_match_map

    Creates:
    club_match_df
    """
    def club_join(self, sites: list[pd.DataFrame]) -> pd.DataFrame:
        join_df = sites[0]
        for site in sites[1:]:
            join_df = join_df.merge(site, on='namematch_index', suffixes=(None, f'_{site.site.iat[1]}'))
        return join_df

    def run(self) -> None:
        PipeBase.club_match_df = (self.club_join(PipeBase.site_dfs_clubs)                            
                            .drop('namematch_index', axis=1)
                            .rename(columns={col: col + '_transfermrkt' for col in ['name', 'id', 'url', 'site']})
                            .reset_index(drop=True))

class SplitSitesByClub(PipeBase):
    """Determines which players in each site df have naming discrepancies and splits them by team name
    to prepare them for the name matching stage

    Creates:
    players_left_by_site
    players_by_club
    """
    def split_site_dfs(self, sites: list[pd.DataFrame]) -> dict[str, list]:
        splits_by_club = {}
        for team in PipeBase.club_match_df.name_sofascore.unique():
            sites_split_by_club = [site[site.team == team].processedName.values for site in sites]
            splits_by_club[team] = sites_split_by_club
        return splits_by_club

    def run(self) -> None:
        PipeBase.players_left_by_site = PipeBase.site_dfs
        PipeBase.players_by_club = self.split_site_dfs(PipeBase.players_left_by_site)

class PlayerNameMatches(PipeBase):
    """Finds cross-site matches for players with naming discrepancies

    Creates:
    player_name_matches_df

    Modifies:
    player_match_map
    """
    async def get_player_match_df(self, session) -> None:
        matches_by_club = (PlayerMatchesBySite(players, club, session) for club, players in PipeBase.players_by_club.items())
        match_result_tasks = [club.main() for club in matches_by_club]
        match_results = await asyncio.gather(*match_result_tasks)
        PipeBase.player_name_matches_df = pd.concat(match_results)

    def match_cols(self) -> list:
        return list(filter(lambda x: x != 'team', PipeBase.player_name_matches_df.columns))

    def get_name_matches(self, row) -> None:
        cols = self.match_cols()
        for col in cols:
            PipeBase.player_match_map[row[col] + '---' + row['team']] = row['fotmob'] + '---' + row['team']

    async def run(self, session) -> None:
        await self.get_player_match_df(session)
        df_c = PipeBase.player_name_matches_df.copy()
        df_c.apply(self.get_name_matches, axis=1)

class PlayerSiteJoin(PipeBase):
    """Joins remaining players based on results of name matches stage and adds them to player_match_df

    Creates:
    player_match_df
    """
    def get_namematch_index(self, row):
        try:
            return PipeBase.player_match_map[row['processedName'] + '---' + row['team']]
        except KeyError:
            return np.nan

    def set_namematch_col(self, site: pd.DataFrame) -> pd.DataFrame:
        site['processedName'] = site.apply(self.get_namematch_index, axis=1)
        return site.dropna(subset='processedName')

    def second_join(self, sites: list[pd.DataFrame]) -> pd.DataFrame:
        join_df = sites[0]
        for site in sites[1:]:
            join_df = join_df.merge(site, on='processedName', suffixes=(None, f'_{site.site.iat[1]}'))
        final_join_result = join_df.rename(columns={'processedName': 'namematch_index'})
        return final_join_result

    def run(self) -> None:
        sites_to_join = [self.set_namematch_col(site) for site in PipeBase.players_left_by_site]
        PipeBase.player_match_df = (self.second_join(sites_to_join)
                            .drop('namematch_index', axis=1)
                            .rename(columns={col: col + '_transfermrkt' for col in ['name', 'id', 'team', 'url', 'site']})) # with the way join is set up, transfermrkt info only one that doesn't have site suffix

async def transform_main(player_df, club_df, session) -> PipeBase:
    """Creates instance of PipeBase, runs transformation child classes, and returns object w/ match dataframes"""
    pipe = PipeBase()
    LoadDataFrames().load_dfs(player_df, club_df)
    FormatNames().run()
    SplitBySite().run()
    ClubNameMatches().run()
    ClubSiteJoin().run()
    SplitSitesByClub().run()
    await PlayerNameMatches().run(session)
    PlayerSiteJoin().run()
    return pipe



    




