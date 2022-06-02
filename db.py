import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import pandas as pd
import os

class PgInitTables:
    """Class to initialize database, establish a connection and creating tables"""
    def __init__(self) -> None:
        """Loads environment variables and generates site table names for database"""
        load_dotenv()
        self.conn = psycopg2.connect(
                        host = os.getenv("HOST"),
                        port = os.getenv("PORT"),
                        database = os.getenv("DATABASE"),
                        user = os.getenv("USER"),
                        password = os.getenv("PASSWORD"),
        )
        self.sites = [
                'transfermrkt', 
                'sofascore', 
                'fbref', 
                'understat', 
                'whoscored', 
                'soccerment',
                'capology',
                'fotmob',
        ]
        self.all_table_names = [site + '_teams' for site in self.sites] + [site + '_players' for site in self.sites] + ['players', 'teams']

    def create_id_tables(self) -> None:
        """Creates base table to store primary IDs (ID is row index of loaded dataframe)"""
        team_table = """
            CREATE TABLE IF NOT EXISTS teams (
                team_id INT,
                name VARCHAR(255),
                PRIMARY KEY (team_id)
            );
        """
        player_table = """
            CREATE TABLE IF NOT EXISTS players (
                player_id INT,
                name VARCHAR(255),
                team_id INT,
                PRIMARY KEY (player_id),
                FOREIGN KEY (team_id)
                    REFERENCES teams (team_id)
            );
        """
        with self.conn.cursor() as cur:
            cur.execute(team_table)
            cur.execute(player_table)

    def create_site_team_tables(self) -> None:
        """Creates team tables to store data specific to each website"""
        table_names = []
        site_table = """
            CREATE TABLE IF NOT EXISTS {} (
                team_id INT,
                site_name VARCHAR(255),
                site_team_id VARCHAR(255),
                site_url VARCHAR(255),
                FOREIGN KEY (team_id)
                    REFERENCES teams (team_id)
            );
        """
        with self.conn.cursor() as cur:
            for site in self.sites:
                site_table_name = site + '_teams'
                table_names.append(site_table_name)
                cur.execute(
                    sql.SQL(site_table).format(sql.Identifier(site_table_name))
                )

    def create_site_player_tables(self) -> None:
        """Creates player tables to store data specific to each website"""
        table_names = []
        site_table = """
            CREATE TABLE IF NOT EXISTS {} (
                player_id INT,
                site_player_name VARCHAR(255),
                site_player_id VARCHAR(255),
                site_url VARCHAR(255),
                FOREIGN KEY (player_id)
                    REFERENCES players (player_id)
            );
        """
        with self.conn.cursor() as cur:
            for site in self.sites:
                site_table_name = site + '_players'
                table_names.append(site_table_name)
                cur.execute(
                    sql.SQL(site_table).format(sql.Identifier(site_table_name))
                )

    def drop_all(self) -> None:
        """Drops all tables in db to ensure duplicate data isn't being inserted"""
        drop_query = """
            DROP TABLE IF EXISTS {} CASCADE;
        """
        with self.conn.cursor() as cur:
            for name in self.all_table_names:
                cur.execute(
                    sql.SQL(drop_query).format(sql.Identifier(name))
                )

class PgInsertMatches(PgInitTables):
    """Class to insert data from loaded dataframes into the appropriate tables"""
    def __init__(self, player_match_df: pd.DataFrame, club_match_df: pd.DataFrame) -> None:
        """Calls init of parent to initialize db and loads in player and team dataframes"""
        super().__init__()
        self.player_match_df = player_match_df
        self.club_match_df = club_match_df
        self.teams_insert = """
            INSERT INTO teams
            VALUES (%s, %s);
        """
        self.players_insert = """
            INSERT INTO players
            VALUES (%s, %s, %s);
        """
        self.site_teams_insert = """
            INSERT INTO {}
            VALUES (%s, %s, %s, %s);
        """
        self.site_players_insert = """
            INSERT INTO {}
            VALUES (%s, %s, %s, %s);
        """

    def insert_team_id(self, row, cur) -> None:
        """Inserts team row into base team table"""
        id_values = [row.name, row['name_sofascore']]
        cur.execute(self.teams_insert, id_values)

    def insert_player_id(self, row, cur) -> None:
        """Inserts player row into base team table"""
        team_id = int(self.club_match_df[self.club_match_df.name_sofascore == row['team_sofascore']].index[0])
        id_values = [row.name, row['name_fotmob'], team_id]
        cur.execute(self.players_insert, id_values)

    def get_site_values(self, row, site, site_type) -> tuple[str, list]:
        """For a given row, retrieves data specific to the given site"""
        site_indices = list(filter(lambda x: x.endswith(site) and x.split('_')[0] not in ['league', 'team', 'site'], row.index))
        table_name = site + site_type
        site_values = [row.name] + row[site_indices].values.tolist()
        return table_name, site_values

    def insert_site_team(self, row, site, cur) -> None:
        """Inserts team data into the team site table of the given site"""
        table_name, site_values = self.get_site_values(row, site, '_teams')
        cur.execute(
            sql.SQL(self.site_teams_insert).format(sql.Identifier(table_name)),
            site_values
        )

    def insert_site_player(self, row, site, cur) -> None:
        """Inserts player data into the player site table of the given site"""
        table_name, site_values = self.get_site_values(row, site, '_players')
        cur.execute(
            sql.SQL(self.site_players_insert).format(sql.Identifier(table_name)),
            site_values
        )

    def insert_team_main(self, row) -> None:
        """Inserts team row into the base team table and every team site table"""
        with self.conn.cursor() as cur:
            self.insert_team_id(row, cur)
            for site in self.sites:
                self.insert_site_team(row, site, cur)

    def insert_player_main(self, row) -> None:
        """Inserts player row into the base player table and every player site table"""
        with self.conn.cursor() as cur:
            self.insert_player_id(row, cur)
            for site in self.sites:
                self.insert_site_player(row, site, cur)

    def run(self) -> None:
        """Runs create functions of parent class and inserts dataframe rows into the appropriate tables"""
        self.drop_all()
        self.create_id_tables()
        self.create_site_team_tables()
        self.create_site_player_tables()
        self.club_match_df.apply(self.insert_team_main, axis=1)
        self.player_match_df.apply(self.insert_player_main, axis=1)
        self.conn.commit()
        self.conn.close()

def load_main(player_match_df, club_match_df):
    """Loads player and team dataframes into the specified PostgreSQL database"""
    PgInsertMatches(player_match_df, club_match_df).run()

        
        


