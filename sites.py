from bs4 import BeautifulSoup, SoupStrainer
import re
from idObjects import ClubData, PlayerData
from siteHeaders import set_headers
from itertools import chain

"""
Sofascore and Fotmob data can be accessed directly through http requests
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
    headers = set_headers(
        origin = 'https://www.sofascore.com',
        referer = 'https://www.sofascore.com/',
    )
    
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

    def process_player_json(self, player_json, club: ClubData):
        table = player_json['players']
        players = map(lambda x: x['player'], table)
        return [PlayerData(p['name'], p['id'], club.name, self.player_url(p), self.name) for p in players]

class FotMob:
    name = 'fotmob'
    leagues = { # using teams that have stayed in the top division to provide season modularity, current codes reflect 21/22
        'Premier League': (9825, 47, 16390),
        'Bundesliga': (9823, 54, 16494),
        'LaLiga': (8633, 87, 16520),
        'Ligue 1': (9847, 53, 16499),
        'Serie A': (8564, 55, 16621),
    }
    headers = set_headers()

    def get_league_url(self, league):
        return f'https://www.fotmob.com/api/historicaltable?teamId={league[0]}&tableLink=historic/{league[1]}/season/{league[2]}/table.fot'

    def club_url(self, link):
        return f'https://www.fotmob.com{link}'

    def process_club_json(self, club_json):
        clubs = club_json['table']['all']
        return [ClubData(c['name'], c['id'], 'League N/A', self.club_url(c['pageUrl']), self.name) for c in clubs]

    def club_api_url(self, club: ClubData): # 4 digit id next to data updates every matchweek !!!
        slug = re.search(r'overview/(.+)$', club.url).group(1)
        return (f'https://www.fotmob.com/_next/data/3353/teams/{club.id}'
                f'/squad/{slug}.json?id={club.id}&tab=overview&slug={slug}')

    def player_url(self, player):
        slug = player['name'].replace(' ', '-')
        id = player['id']
        return f'https://www.fotmob.com/players/{id}/{slug}'

    def process_player_json(self, player_json, club: ClubData):
        team_data = player_json['pageProps']['initialState']['team'][str(club.id)]['data']
        team_name = team_data['details']['name']
        squad = team_data['squad']
        squad_by_position = [position[1] for position in squad[1:]]
        players = chain(*squad_by_position)
        return [PlayerData(p['name'], p['id'], team_name, self.player_url(p), self.name) for p in players]

"""
Tranfermarkt and Soccerment can be scraped fully without browser automation with http requests but from html responses instead of json
"""

class Tm:
    name = 'transfermarkt'
    leagues = {
        'Premier League': ('premier-league', 'GB1'),
        'LaLiga': ('la-liga', 'ES1'),
        'Ligue 1': ('ligue-1', 'FR1'),
        'Serie A': ('serie-a', 'IT1'),
        'Bundesliga': ('bundesliga', 'L1'),      
    }
    club_strainer = SoupStrainer('table', attrs={'class': 'items'})
    club_id_pat = re.compile(r'/verein/(\d+)/saison')
    player_id_pat = re.compile(r'/(\d+)$')
    headers = set_headers()

    def get_league_url(self, league):
        return f'https://www.transfermarkt.us/{league[0]}/tabelle/wettbewerb/{league[1]}/saison_id/2021'

    def club_url(self, club):
        link = club.get('href')
        return f'https://www.transfermarkt.us{link}'

    def club_id(self, club):
        return self.club_id_pat.search(club.get('href')).group(1)

    def process_club_html(self, club_html):
        soup = BeautifulSoup(club_html, 'lxml', parse_only=self.club_strainer).tbody
        trs = soup.find_all('tr')
        clubs = map(lambda x: x.find_all('td')[2].a, trs)
        return [ClubData(c.get('title'), self.club_id(c), 'League N/A', self.club_url(c), self.name) for c in clubs]

    def club_api_url(self, club: ClubData):
        return club.url.replace('spielplan', 'kader')

    def player_url(self, player):
        link = player.get('href')
        return f'https://www.transfermarkt.us{link}'

    def player_id(self, player):
        link = player.get('href')
        return self.player_id_pat.search(link).group(1)

    def transfer_check(self, player_tr):
        player_td = player_tr.find_all('td')[1]
        span_check = player_td.span
        if span_check:
            player = player_td.table.a
            print(player)
            return player
        else:
            return player_td.a

    def process_player_html(self, player_html, club: ClubData):
        trs = BeautifulSoup(player_html, 'lxml', parse_only=self.club_strainer).tbody.find_all('tr', attrs={'class': ['odd', 'even']})
        players = map(lambda x: self.transfer_check(x), trs)
        return [PlayerData(p.text.strip(), self.player_id(p), club.name, self.player_url(p), self.name) for p in players]

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
    player_teamname = SoupStrainer('h1', attrs={'class': 'team_name'})
    player_id_pat = re.compile(r'player/(\d+)/')
    headers = set_headers()

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

    def club_api_url(self, club: ClubData):
        return club.url

    def player_url(self, player):
        link = player.get('href')
        return f'https://analytics.soccerment.com{link}'

    def player_id(self, player):
        link = player.get('href')
        return self.player_id_pat.search(link).group(1)

    def process_player_html(self, player_html, club: ClubData):
        #team_name = BeautifulSoup(player_html, 'lxml', parse_only=self.player_teamname).find('h1').text
        player_cards = BeautifulSoup(player_html, 'lxml', parse_only=self.player_strainer).find_all('div', attrs={'class': 'card_info'})
        end_index = len(player_cards) // 2 # html response returns duplicate set of cards -- only need first set
        players = map(lambda x: x.a, player_cards[:end_index])
        return [PlayerData(p.text, self.player_id(p), club.name, self.player_url(p), self.name) for p in players]

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
Capology requires browser automation but has different data structure to that of other sites
as you are able to extract all the player and team data for a given league from one page
"""

class Cap:
    name = 'capology'
    club_table = '#panel > div.content-block > div > div.col.s12.team-row-container > div.col.s12.team-row'
    player_table = '#table > tbody'
    club_id_pat = re.compile(r'/club/(.+)/salaries/')
    player_id_pat = re.compile(r'/player/(.+)/profile/')
    leagues = {
        'Premier League': ('uk', 'premier-league'),
        'LaLiga': ('es', 'la-liga'),
        'Ligue 1': ('fr', 'ligue-1'),
        'Serie A': ('it', 'serie-a'),
        'Bundesliga': ('de', '1-bundesliga'),
    }

    def get_league_url(self, league):
        return f'https://www.capology.com/{league[0]}/{league[1]}/salaries/'

    def club_id(self, club):
        return self.club_id_pat.search(club.get('href')).group(1)

    def club_url(self, club):
        link = club.get('href')
        return f'https://www.capology.com{link}'

    def process_club_table(self, club_html):
        soup = BeautifulSoup(club_html, 'lxml')
        clubs = soup.find_all('a')
        return [ClubData(c.text.replace('"', '').strip(), self.club_id(c), 'League N/A', self.club_url(c), self.name) for c in clubs]

    def player_id(self, player):
        return self.player_id_pat.search(player.get('href')).group(1)

    def player_url(self, player):
        link = player.get('href')
        return f'https://www.capology.com{link}'

    def player_tds(self, tr):
        tds = tr.find_all('td')
        return tds[0].a, tds[-1] # first tuple item is player a tag, second is the player team name

    def process_player_table(self, player_html):
        soup = BeautifulSoup(player_html, 'lxml')
        trs = soup.find_all('tr')
        players = map(self.player_tds, trs)
        return [PlayerData(p[0].text, self.player_id(p[0]), p[1].text, self.player_url(p[0]), self.name) for p in players]

"""
whoscored requires extracting cookie headers through browser automation before making http requests
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
        return [PlayerData(p['name'], p['playerId'], club.name, self.player_url(p), self.name) for p in players]