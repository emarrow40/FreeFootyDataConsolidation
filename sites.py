from bs4 import BeautifulSoup, SoupStrainer
import re
from idObjects import ClubData, PlayerData
from itertools import chain
import urllib.parse

"""
Sofascore and TransferMarket data can be accessed directly through http requests
"""

class Sofa:
    name = 'sofascore'
    leagues = {
        'Premier League': (17, 37036),
        'LaLiga': (8, 37223),
        'Ligue 1': (34, 37167),
        'Serie A': (23, 37475),
        'Bundesliga': (35, 37166),
    }
    origin = 'https://www.sofascore.com'
    referer = 'https://www.sofascore.com/'
    
    def get_league_url(self, league):
        return f'https://api.sofascore.com/api/v1/unique-tournament/{league[0]}/season/{league[1]}/standings/total'

    def club_url(self, club):
        id = club['id']
        slug = club['slug']
        return f'https://www.sofascore.com/team/football/{slug}/{id}'

    def process_club_json(self, club_json):
        table = club_json['standings'][0]['rows']
        league = club_json['standings'][0]['name']
        clubs = map(lambda x: x['team'], table)
        return [ClubData(c['name'], c['id'], league, self.club_url(c), self.name) for c in clubs]

    def club_api_url(self, club: ClubData):
        return f'https://api.sofascore.com/api/v1/team/{club.id}/players'

    def player_url(self, player):
        id = player['id']
        slug = player['slug']
        return f'https://www.sofascore.com/player/{slug}/{id}'

    def process_player_json(self, player_json, team_name):
        table = player_json['players']
        players = map(lambda x: x['player'], table)
        return [PlayerData(p['name'], p['id'], team_name, self.player_url(p), self.name) for p in players]

class Tm:
    name = 'transfermarkt'
    leagues = {
        'Premier League': 'GB1',
        'LaLiga': 'ES1',
        'Ligue 1': 'FR1',
        'Serie A': 'IT1',
        'Bundesliga': 'L1',      
    }
    origin = None
    referer = None

    def get_league_url(self, league):
        return f'https://www.transfermarkt.us/quickselect/teams/{league}'

    def club_url(self, club):
        link = club['link']
        return f'https://www.transfermarkt.us{link}'

    def process_club_json(self, club_json):
        return [ClubData(c['name'], c['id'], 'League N/A', self.club_url(c), self.name) for c in club_json]

    def club_api_url(self, club: ClubData):
        return f'https://www.transfermarkt.us/quickselect/players/{club.id}'

    def player_url(self, player):
        link = player['link']
        return f'https://www.transfermarkt.us{link}'

    def process_player_json(self, player_json, team_name):
        return [PlayerData(p['name'], p['id'], team_name, self.player_url(p), self.name) for p in player_json]

"""
Soccerment can be scraped fully without browser automation with http requests but from html responses instead of json
"""

class Soccerment:
    name = 'soccerment'
    leagues = {
        'Premier League': 'premier_league',
        'LaLiga': 'la_liga',
        'Ligue 1': 'ligue_1',
        'Serie A': 'serie_a',
        'Bundesliga': 'bundesliga',
    }
    club_strainer = SoupStrainer('tbody', attrs={'id': 'table_container'})
    player_strainer = SoupStrainer('div', attrs={'id': 'teams_tabs_content'})
    player_teamname = SoupStrainer('h1', {'class': 'team_name'})
    player_id_pat = re.compile(r'player/(\d+)/')
    origin = None
    referer = None

    def get_league_url(self, league):
        return f'https://analytics.soccerment.com/en/league/{league}'

    def club_url(self, club):
        link = club.get('href')
        return f'https://analytics.soccerment.com{link}'

    def process_club_html(self, club_html):
        soup = BeautifulSoup(club_html, 'lxml', parse_only=self.club_strainer)
        trs = soup.find_all('tr')
        clubs = map(lambda x: x.find_all('td')[1].a, trs)
        return [ClubData(c.text.replace('"', '').strip(), 'Club ID N/A', 'League N/A', self.club_url(c), self.name) for c in clubs]

    def player_url(self, player):
        link = player.get('href')
        return f'https://analytics.soccerment.com{link}'

    def player_id(self, player):
        link = player.get('href')
        return self.player_id_pat.search(link).group(1)

    def process_player_html(self, player_html):
        team_name = BeautifulSoup(player_html, 'lxml', parse_only=self.player_teamname).text
        player_cards = BeautifulSoup(player_html, 'lxml', parse_only=self.player_strainer).find_all('div', attrs={'class': 'card_info'})
        end_index = len(player_cards) // 2 # html response returns duplicate set of cards -- only need first set
        players = map(lambda x: x.a, player_cards[:end_index])
        return [PlayerData(p.text, self.player_id(p), team_name, self.player_url(p), self.name) for p in players]

"""
Fbref and understat require full browser automation, content loaded dynamically and requests don't appear in dev tools
"""

class Fbref:
    name = 'fbref'
    club_table = 'table[id$="_overall"] > tbody'
    player_table = 'table[id^="stats_standard"] > tbody'
    club_id_pat = re.compile(r'squads/(\w+)/')
    player_id_pat = re.compile(r'players/(\w+)/')
    leagues = {
        'Premier League': (9, 'Premier-League'),
        'LaLiga': (12, 'La-Liga'),
        'Ligue 1': (13, 'Ligue-1'),
        'Serie A': (11, 'Serie-A'),
        'Bundesliga': (20, 'Bundesliga'),
    }

    def get_league_url(self, league):
        return f'https://www.fbref.com/en/comps/{league[0]}/{league[1]}-Stats'

    def club_id(self, club):
        return self.club_id_pat.search(club.get('href')).group(1)

    def club_url(self, club):
        link = club.get('href')
        return f'https://www.fbref.com{link}'

    def process_club_table(self, club_html):
        soup = BeautifulSoup(club_html, 'lxml')
        trs = soup.find_all('tr')
        clubs = map(lambda x: x.find_all('td')[0].a, trs)
        return [ClubData(c.text, self.club_id(c), 'League N/A', self.club_url(c), self.name) for c in clubs]

    def player_id(self, player):
        return self.player_id_pat.search(player.get('href')).group(1)

    def player_url(self, player):
        link = player.get('href')
        return f'https://www.fbref.com{link}'

    def process_player_table(self, player_html, team_name):
        soup = BeautifulSoup(player_html, 'lxml')
        trs = soup.find_all('tr')
        players = [row.th.a for row in trs if row.th.a]
        return [PlayerData(p.text, self.player_id(p), team_name, self.player_url(p), self.name) for p in players]

class Under:
    name = 'understat'
    club_table = 'div#league-chemp > table > tbody'
    player_table = '#team-players > table > tbody:not(.table-total)'
    club_id_pat = re.compile(r'team/(.+)/\d{4}')
    player_id_pat = re.compile(r'/(\d+)$')
    leagues = {
        'Premier League': 'EPL',
        'LaLiga': 'La_liga',
        'Ligue 1': 'Ligue_1',
        'Serie A': 'Serie_A',
        'Bundesliga': 'Bundesliga', 
    }

    def get_league_url(self, league):
        return f'https://www.understat.com/league/{league}'

    def club_id(self, club):
        return self.club_id_pat.search(club.get('href')).group(1)

    def club_url(self, club):
        link = club.get('href')
        return f'https://www.understat.com/{link}'

    def process_club_table(self, club_html):
        soup = BeautifulSoup(club_html, 'lxml')
        trs = soup.find_all('tr')
        clubs = map(lambda x: x.find_all('td')[1].a, trs)
        return [ClubData(c.text, self.club_id(c), 'League N/A', self.club_url(c), self.name) for c in clubs]

    def player_id(self, player):
        return self.player_id_pat.search(player.get('href')).group(1)

    def player_url(self, player):
        link = player.get('href')
        return f'https://www.understat.com/{link}'

    def process_player_table(self, player_html, team_name):
        soup = BeautifulSoup(player_html, 'lxml')
        trs = soup.find_all('tr')
        players = map(lambda x: x.find_all('td')[1].a, trs)
        return [PlayerData(p.text, self.player_id(p), team_name, self.player_url(p), self.name) for p in players]

"""
Fotmob, whoscored require extracting cookie headers through browser automation before making http requests
"""

class Who:
    name = 'whoscored'
    club_table = 'tbody.standings'
    club_id_pat = re.compile(r'/Teams/(\d+)/')
    cookie_filter = ['ct', '_qca', '_ga', '_xpid', '_xpkey', '_gid', '_fbp']
    cookie_keyword = 'incap'
    leagues = {
        'Premier League': (252, 2, 'England-Premier-League'),
        'LaLiga': (206, 4, 'Spain-LaLiga'),
        'Ligue 1': (74, 22, 'France-Ligue-1'),
        'Seria A': (108, 5, 'Italy-Serie-A'),  
        'Bundesliga': (81, 3, 'Germany-Bundesliga'),
    }

    def get_league_url(self, league):
        return f'https://www.whoscored.com/Regions/{league[0]}/Tournaments/{league[1]}/{league[2]}'

    def club_id(self, club):
        return self.club_id_pat.search(club.get('href')).group(1)

    def club_url(self, club):
        link = club.get('href')
        return f'https://www.whoscored.com{link}'

    def process_club_table(self, club_html, league):
        league_map = {value: key for key, value in self.leagues.items()}
        soup = BeautifulSoup(club_html, 'lxml')
        trs = soup.find_all('tr')
        clubs = map(lambda x: x.find_all('td')[0].a, trs)
        return [ClubData(c.text, self.club_id(c), league_map[league], self.club_url(c), self.name) for c in clubs]

    def club_api_url(self, club: ClubData):
        return ('https://www.whoscored.com/StatisticsFeed/1/GetPlayerStatistics'
                '?category=summary&subcategory=all&statsAccumulationType=0&isCurrent=true'
                f'&playerId=&teamIds={club.id}&matchId=&stageId=&tournamentOptions={self.leagues[club.league][1]}'
                '&sortBy=Rating&sortAscending=&age=&ageComparisonType=&appearances='
                '&appearancesComparisonType=&field=Overall&nationality=&positionOptions='
                '&timeOfTheGameEnd=&timeOfTheGameStart=&isMinApp=false&page=&includeZeroValues=true&numberOfPlayersToPick=')
    
    def player_url(self, player):
        slug = player['name'].replace(' ', '-')
        player_id = player['playerId']
        return f'https://www.whoscored.com/Players/{player_id}/Show/{slug}'

    def process_player_json(self, player_json, club: ClubData):
        players = player_json['playerTableStats']
        return [PlayerData(p['name'], p['playerId'], p['teamName'], self.player_url(p), self.name) for p in players]

class FotMob:
    name = 'fotmob'
    club_table = '#__next > main > section > section > div > section > section > div > div > section > div > article > div.css-c0041o-TableContainer.ecspc023 > table > tbody'
    club_id_pat = re.compile(r'/teams/(\d+)/')
    club_name_pat = re.compile(r'overview/(.+)$')
    cookie_filter = ['_ga', 'u:location']
    cookie_keyword = '_hj'
    leagues = {
        'Premier League': (47, 'premier-league'),
        'Bundesliga': (54, '1.-bundesliga'),
        'LaLiga': (87, 'laliga'),
        'Ligue 1': (53, 'ligue-1'),
        'Serie A': (55, 'serie-a'),
    }

    def get_league_url(self, league):
        return f'https://www.fotmob.com/leagues/{league[0]}/overview/{league[1]}'

    def club_id(self, club):
        return self.club_id_pat.search(club.get('href')).group(1)

    def club_name(self, club): # name that appears in table does not match name in player json response -- lower and remove special characters to create uniformity
        name = self.club_name_pat.search(club.get('href')).group(1)
        return urllib.parse.unquote(name.replace('-', ''))

    def club_url(self, club):
        link = club.get('href')
        return f'https://www.fotmob.com{link}'

    def process_club_table(self, club_html, league):
        soup = BeautifulSoup(club_html, 'lxml')
        trs = soup.find_all('tr')
        clubs = map(lambda x: x.find_all('td')[1].a, trs)
        return [ClubData(self.club_name(c), self.club_id(c), 'League N/A', self.club_url(c), self.name) for c in clubs]

    def club_api_url(self, club: ClubData): # 4 digit id next to data updates every matchweek
        slug = re.search(r'overview/(.+)$', club.url).group(1)
        return (f'https://www.fotmob.com/_next/data/3298/teams/{club.id}'
                f'/squad/{slug}.json?id=9825&tab=overview&slug={slug}')

    def player_url(self, player):
        slug = player['name'].replace(' ', '-')
        id = player['id']
        return f'https://www.fotmob.com/players/{id}/{slug}'

    def process_player_json(self, player_json, club: ClubData):
        team_data = player_json['pageProps']['initialState']['team'][club.id]['data']
        team_name = re.sub(r'[\'\-\s]', '', team_data['details']['name'].lower())
        squad = team_data['squad']
        squad_by_position = [position[1] for position in squad[1:]]
        players = chain(*squad_by_position)
        return [PlayerData(p['name'], p['id'], team_name, self.player_url(p), self.name) for p in players]
