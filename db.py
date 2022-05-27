import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

class PgInitTables:
    def __init__(self):
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
                'fotmob',
        ]
        self.all_table_names = [site + '_teams' for site in self.sites] + [site + '_players' for site in self.sites] + ['players', 'teams']

    def create_id_tables(self):
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

    def create_site_team_tables(self):
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

    def create_site_player_tables(self):
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

    def drop_all(self):
        drop_query = """
            DROP TABLE IF EXISTS {} CASCADE;
        """
        with self.conn.cursor() as cur:
            for name in self.all_table_names:
                cur.execute(
                    sql.SQL(drop_query).format(sql.Identifier(name))
                )

class PgInsertMatches(PgInitTables):
    def __init__(self, player_match_df, club_match_df):
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

    def insert_team_id(self, row, cur):
        id_values = [row.name, row['name_sofascore']]
        cur.execute(self.teams_insert, id_values)

    def insert_player_id(self, row, cur):
        team_id = int(self.club_match_df[self.club_match_df.name_sofascore == row['team_sofascore']].index[0])
        id_values = [row.name, row['name_fotmob'], team_id]
        cur.execute(self.players_insert, id_values)

    def get_site_values(self, row, site, site_type):
        site_indices = list(filter(lambda x: x.endswith(site) and x.split('_')[0] not in ['league', 'team', 'site'], row.index))
        table_name = site + site_type
        site_values = [row.name] + row[site_indices].values.tolist()
        return table_name, site_values

    def insert_site_team(self, row, site, cur):
        table_name, site_values = self.get_site_values(row, site, '_teams')
        cur.execute(
            sql.SQL(self.site_teams_insert).format(sql.Identifier(table_name)),
            site_values
        )

    def insert_site_player(self, row, site, cur):
        table_name, site_values = self.get_site_values(row, site, '_players')
        cur.execute(
            sql.SQL(self.site_players_insert).format(sql.Identifier(table_name)),
            site_values
        )

    def insert_team_main(self, row):
        with self.conn.cursor() as cur:
            self.insert_team_id(row, cur)
            for site in self.sites:
                self.insert_site_team(row, site, cur)

    def insert_player_main(self, row):
        with self.conn.cursor() as cur:
            self.insert_player_id(row, cur)
            for site in self.sites:
                self.insert_site_player(row, site, cur)

    def run(self):
        self.drop_all()
        self.create_id_tables()
        self.create_site_team_tables()
        self.create_site_player_tables()
        self.club_match_df.apply(self.insert_team_main, axis=1)
        self.player_match_df.apply(self.insert_player_main, axis=1)
        self.conn.commit()
        self.conn.close()

def load_main(player_match_df, club_match_df):
    PgInsertMatches(player_match_df, club_match_df).run()

        
        


