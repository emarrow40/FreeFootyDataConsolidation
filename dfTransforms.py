from unidecode import unidecode
import re
import asyncio
import pandas as pd
import numpy as np
from nameMatches import PlayerMatchesBySite

class PipeBase:
    team_match_map = {}
    player_match_map = {}
    sites = [
            'transfermarkt',
            'sofascore',
            'fbref',
            'understat',
            'whoscored',
            'soccerment',
            'fotmob',   
    ]

class LoadPlayerDataFrame(PipeBase):
    def load_dfs(self, player_df, club_df):
        PipeBase.player_df = player_df.drop_duplicates()
        PipeBase.club_df = club_df.drop_duplicates()
    
class FormatNames(PipeBase):
    def player_name(self, name):
        return re.sub(r'[^a-z\s]', ' ', unidecode(str(name).lower()))

    def run(self):
        PipeBase.player_df['processedName'] = PipeBase.player_df.name.apply(self.player_name)

class SplitBySite(PipeBase):
    def split_df(self, df):
        return [df[df['site'] == site] for site in PipeBase.sites]

    def filter_duplicate_names(self, df):
        return df[df.name.duplicated(keep=False) == False]

    def run(self):
        PipeBase.site_dfs = self.split_df(PipeBase.player_df)
        PipeBase.site_dfs_nodups = [self.filter_duplicate_names(site_df) for site_df in PipeBase.site_dfs]
        PipeBase.site_dfs_clubs = self.split_df(PipeBase.club_df)

class InitialSiteJoin(PipeBase):
    def initial_join(self, sites):
        join_df = sites[0]
        for site in sites[1:]:
            join_df = join_df.merge(site, on='processedName', suffixes=(None, f'_{site.site.iat[1]}'))
        final_join_result = join_df.rename(columns={'processedName': 'namematch_index'})
        return final_join_result

    def run(self):
        PipeBase.player_match_df = self.initial_join(PipeBase.site_dfs_nodups)

class ClubNameMatches(PipeBase):
    def match_cols(self):
        return list(filter(lambda x: x.startswith('team'), PipeBase.player_match_df.columns))

    def get_row_matches(self, row):
        cols = self.match_cols()
        for col in cols:
            PipeBase.team_match_map[row[col]] = row['team_sofascore']

    def map_team_matches(self, team):
        return PipeBase.team_match_map[team]

    def apply_map_to_matchdf(self):
        cols = self.match_cols()
        for col in cols:
            PipeBase.player_match_df[col] = PipeBase.player_match_df[col].apply(self.map_team_matches)

    def apply_map_to_sitedfs(self):
        sites = PipeBase.site_dfs
        for site in sites:
            site['team'] = site.team.apply(self.map_team_matches)
        PipeBase.site_dfs = sites

    def apply_map_to_clubdf(self):
        sites = PipeBase.site_dfs_clubs
        for site in sites:
            site['namematch_index'] = site.name.apply(self.map_team_matches)
        PipeBase.site_dfs_clubs = sites

    def run(self):
        df_c = PipeBase.player_match_df.copy()
        df_c.apply(self.get_row_matches, axis=1)
        self.apply_map_to_matchdf()
        self.apply_map_to_sitedfs()
        self.apply_map_to_clubdf()

class ClubSiteJoin(PipeBase):
    def club_join(self, sites) -> pd.DataFrame:
        join_df = sites[0]
        for site in sites[1:]:
            join_df = join_df.merge(site, on='namematch_index', suffixes=(None, f'_{site.site.iat[1]}'))
        return join_df

    def run(self):
        PipeBase.club_match_df = (self.club_join(PipeBase.site_dfs_clubs)                            
                            .drop('namematch_index', axis=1)
                            .rename(columns={col: col + '_transfermrkt' for col in ['name', 'id', 'url', 'site']})
                            .reset_index(drop=True))

class SplitSitesByClub(PipeBase):
    def players_left_by_site(self, sites):
        return [site[site.processedName.isin(PipeBase.player_match_df.namematch_index) == False] for site in sites]

    def split_site_dfs(self, sites):
        splits_by_club = {}
        for team in PipeBase.player_match_df.team.unique():
            sites_split_by_club = [site[site.team == team].processedName.values for site in sites]
            splits_by_club[team] = sites_split_by_club
        return splits_by_club

    def run(self):
        PipeBase.players_left_by_site = self.players_left_by_site(PipeBase.site_dfs)
        PipeBase.players_by_club = self.split_site_dfs(PipeBase.players_left_by_site)

class PlayerNameMatches(PipeBase):
    async def get_player_match_df(self, session):
        matches_by_club = (PlayerMatchesBySite(players, club, session) for club, players in PipeBase.players_by_club.items())
        match_result_tasks = [club.main() for club in matches_by_club]
        match_results = await asyncio.gather(*match_result_tasks)
        PipeBase.player_name_matches_df = pd.concat(match_results)

    def match_cols(self):
        return list(filter(lambda x: x != 'team', PipeBase.player_name_matches_df.columns))

    def get_name_matches(self, row):
        cols = self.match_cols()
        for col in cols:
            PipeBase.player_match_map[row[col] + '---' + row['team']] = row['fotmob'] + '---' + row['team']

    async def run(self, session):
        await self.get_player_match_df(session)
        df_c = PipeBase.player_name_matches_df.copy()
        df_c.apply(self.get_name_matches, axis=1)

class PlayerSiteJoin(PipeBase):
    def get_namematch_index(self, row):
        try:
            return PipeBase.player_match_map[row['processedName'] + '---' + row['team']]
        except KeyError:
            return np.nan

    def set_namematch_col(self, site):
        site['processedName'] = site.apply(self.get_namematch_index, axis=1)
        return site.dropna(subset='processedName')

    def second_join(self, sites) -> pd.DataFrame:
        join_df = sites[0]
        for site in sites[1:]:
            join_df = join_df.merge(site, on='processedName', suffixes=(None, f'_{site.site.iat[1]}'))
        final_join_result = join_df.rename(columns={'processedName': 'namematch_index'})
        return final_join_result

    def run(self):
        sites_to_join = [self.set_namematch_col(site) for site in PipeBase.players_left_by_site]
        match_df_c = PipeBase.player_match_df.copy()
        PipeBase.player_match_df = (pd.concat([match_df_c, self.second_join(sites_to_join)], ignore_index=True)
                            .drop('namematch_index', axis=1)
                            .rename(columns={col: col + '_transfermrkt' for col in ['name', 'id', 'team', 'url', 'site']})) # with the way join is set up, transfermrkt info only one that doesn't have site suffix

async def transform_main(player_df, club_df, session):
    pipe = PipeBase()
    LoadPlayerDataFrame().load_dfs(player_df, club_df)
    FormatNames().run()
    SplitBySite().run()
    InitialSiteJoin().run()
    ClubNameMatches().run()
    ClubSiteJoin().run()
    SplitSitesByClub().run()
    await PlayerNameMatches().run(session)
    PlayerSiteJoin().run()
    return pipe



    




